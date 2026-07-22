// Package messagedb implements the eventstore gRPC service on top of Message-DB
// (https://github.com/message-db/message-db), a Postgres-based event store.
//
// It serves the identical eventstorepb.EventStore contract as the native
// eventstore server, translating that contract's conventions to Message-DB's:
//
//   - Positions: the contract is 1-based / count-based; Message-DB is 0-based
//     (stream_version = max position). So contract expected_version E maps to
//     Message-DB E-1 (E=0 -> -1, "no stream"), and a returned version is the
//     last written 0-based position + 1.
//   - Batches: Message-DB writes one message per write_message call, so a batch
//     is written as N calls inside a single transaction (one commit / WAL fsync),
//     pipelined in one round trip.
//   - Payloads: the contract carries raw bytes; Message-DB data is jsonb, so
//     payload/meta are marshalled to a base64 JSON string and decoded on read.
package messagedb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pb "github.com/leow93/eventstore/eventstorepb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const writeSQL = `SELECT message_store.write_message(gen_random_uuid()::varchar, $1, $2, $3::jsonb, $4::jsonb, $5)`

// readColumns is the projection shared by all read queries. Timestamp is
// converted to unix nanoseconds to match the contract's Event.timestamp.
const readColumns = `stream_name, type, position, global_position, data::text, metadata::text, (extract(epoch from time)*1e9)::bigint`

// Server implements eventstorepb.EventStoreServer backed by Message-DB.
type Server struct {
	pb.UnimplementedEventStoreServer
	pool *pgxpool.Pool
}

// New connects to Message-DB at the given DSN and returns a Server.
func New(ctx context.Context, dsn string) (*Server, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.MaxConns < 16 {
		cfg.MaxConns = 16
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Server{pool: pool}, nil
}

func (s *Server) Close() {
	s.pool.Close()
}

func (s *Server) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	events := req.GetEvents()
	if len(events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "append: no events provided")
	}

	// Contract expected_version -> Message-DB expected_version.
	anyVersion := req.GetExpectedVersion() < 0
	base := req.GetExpectedVersion() - 1 // current 0-based version the first write expects

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	batch := &pgx.Batch{}
	for i, e := range events {
		var expected any // NULL disables Message-DB's OCC check
		if !anyVersion {
			expected = base + int64(i)
		}
		batch.Queue(writeSQL, req.GetStreamName(), e.GetEventType(), toJSON(e.GetPayload()), toJSON(e.GetMeta()), expected)
	}

	results := tx.SendBatch(ctx, batch)
	var lastPosition int64
	for range events {
		if err := results.QueryRow().Scan(&lastPosition); err != nil {
			results.Close()
			return nil, mapWriteError(err)
		}
	}
	if err := results.Close(); err != nil {
		return nil, mapWriteError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Message-DB position is 0-based; contract version is the event count.
	return &pb.AppendResponse{Version: uint64(lastPosition + 1)}, nil
}

func (s *Server) ReadStream(req *pb.ReadStreamRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
	ctx := stream.Context()

	var rows pgx.Rows
	var err error
	if req.GetDirection() == pb.Direction_DIRECTION_BACKWARDS {
		// Message-DB has no backwards reader, so query the messages table directly.
		sql := `SELECT ` + readColumns + ` FROM message_store.messages
		        WHERE stream_name = $1 AND ($2 < 0 OR position <= $2)
		        ORDER BY position DESC`
		if limit := req.GetLimit(); limit > 0 {
			sql += fmt.Sprintf(" LIMIT %d", limit)
		}
		positionCap := int64(-1) // from == 0 means "from the tip" (no cap)
		if req.GetFrom() > 0 {
			positionCap = int64(req.GetFrom()) - 1
		}
		rows, err = s.pool.Query(ctx, sql, req.GetStreamName(), positionCap)
	} else {
		position := int64(0) // 0-based; from 0 or 1 both mean "from the start"
		if req.GetFrom() > 1 {
			position = int64(req.GetFrom()) - 1
		}
		rows, err = s.pool.Query(ctx,
			`SELECT `+readColumns+` FROM message_store.get_stream_messages($1, $2, $3)`,
			req.GetStreamName(), position, batchSize(req.GetLimit()))
	}
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return streamRows(stream, rows)
}

func (s *Server) ReadCategory(req *pb.ReadCategoryRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
	ctx := stream.Context()

	// NOTE: Message-DB positions category reads by GLOBAL position, whereas the
	// contract's `from` is category-local. from<=0 ("from the start") maps to
	// global position 1, which is what the load test uses.
	position := int64(1)
	if req.GetFrom() > 0 {
		position = int64(req.GetFrom())
	}

	rows, err := s.pool.Query(ctx,
		`SELECT `+readColumns+` FROM message_store.get_category_messages($1, $2, $3)`,
		req.GetCategory(), position, batchSize(req.GetLimit()))
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return streamRows(stream, rows)
}

func streamRows(out grpc.ServerStreamingServer[pb.Event], rows pgx.Rows) error {
	defer rows.Close()

	for rows.Next() {
		var (
			streamName, eventType string
			position, globalPos   int64
			timestamp             int64
			data, meta            *string
		)
		if err := rows.Scan(&streamName, &eventType, &position, &globalPos, &data, &meta, &timestamp); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := out.Send(&pb.Event{
			StreamName:     streamName,
			EventType:      eventType,
			Position:       uint64(position) + 1, // 0-based -> 1-based to match the contract
			GlobalPosition: uint64(globalPos),
			Timestamp:      uint64(timestamp),
			Payload:        fromJSON(data),
			Meta:           fromJSON(meta),
		}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// batchSize maps the contract's limit (<=0 = unbounded) to Message-DB's
// batch_size (-1 = unbounded).
func batchSize(limit int64) int64 {
	if limit <= 0 {
		return -1
	}
	return limit
}

// toJSON marshals raw bytes into a value suitable for a jsonb column: a base64
// JSON string, or NULL for empty input.
func toJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	encoded, err := json.Marshal(b) // []byte -> base64 JSON string
	if err != nil {
		return nil
	}
	return string(encoded)
}

// fromJSON reverses toJSON, recovering the original bytes.
func fromJSON(s *string) []byte {
	if s == nil {
		return nil
	}
	var b []byte
	if err := json.Unmarshal([]byte(*s), &b); err != nil {
		return []byte(*s) // not a base64 JSON string; return the raw text
	}
	return b
}

func mapWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Message, "Wrong expected version") {
		// Match the native server: a concurrency conflict is gRPC Aborted.
		return status.Error(codes.Aborted, pgErr.Message)
	}
	return status.Error(codes.Internal, err.Error())
}
