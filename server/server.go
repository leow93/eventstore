// Package server exposes an eventstore.Store over gRPC.
package server

import (
	"context"
	"errors"
	"iter"

	"github.com/leow93/eventstore"
	pb "github.com/leow93/eventstore/eventstorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adapts a *eventstore.Store to the generated EventStore gRPC service.
type Server struct {
	pb.UnimplementedEventStoreServer
	store *eventstore.Store
}

func New(store *eventstore.Store) *Server {
	return &Server{store: store}
}

func (s *Server) Append(_ context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	events := make([]*eventstore.Event, len(req.GetEvents()))
	for i, e := range req.GetEvents() {
		events[i] = &eventstore.Event{
			EventType: e.GetEventType(),
			Payload:   e.GetPayload(),
			Meta:      e.GetMeta(),
		}
	}

	version, err := s.store.AppendToStream(
		req.GetStreamName(),
		eventstore.ExpectedVersion(req.GetExpectedVersion()),
		events...,
	)
	if err != nil {
		return nil, toStatusErr(err)
	}

	return &pb.AppendResponse{Version: version}, nil
}

func (s *Server) ReadStream(req *pb.ReadStreamRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
	limit := int(req.GetLimit())

	var seq iter.Seq2[*eventstore.Event, error]
	if req.GetDirection() == pb.Direction_DIRECTION_BACKWARDS {
		seq = s.store.ReadStreamBackwards(req.GetStreamName(), req.GetFrom(), limit)
	} else {
		seq = s.store.ReadStreamForwards(req.GetStreamName(), req.GetFrom(), limit)
	}

	return sendAll(stream, seq)
}

func (s *Server) ReadCategory(req *pb.ReadCategoryRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
	seq := s.store.ReadCategory(req.GetCategory(), req.GetFrom(), int(req.GetLimit()))
	return sendAll(stream, seq)
}

// sendAll streams every event from a read iterator to the client, stopping early
// if the client disconnects or the context is cancelled.
func sendAll(stream grpc.ServerStreamingServer[pb.Event], seq iter.Seq2[*eventstore.Event, error]) error {
	for evt, err := range seq {
		if err != nil {
			return status.Errorf(codes.Internal, "read failed: %v", err)
		}
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if err := stream.Send(toProto(evt)); err != nil {
			return err
		}
	}
	return nil
}

func toProto(e *eventstore.Event) *pb.Event {
	return &pb.Event{
		StreamName:     e.StreamName,
		EventType:      e.EventType,
		Position:       e.Position,
		GlobalPosition: e.GlobalPosition,
		Timestamp:      e.Timestamp,
		Payload:        e.Payload,
		Meta:           e.Meta,
	}
}

// toStatusErr maps store errors to appropriate gRPC status codes.
func toStatusErr(err error) error {
	var occErr eventstore.ErrWrongExpectedVersion
	if errors.As(err, &occErr) {
		// Aborted is gRPC's conventional code for a concurrency conflict.
		return status.Error(codes.Aborted, err.Error())
	}

	var noCatErr eventstore.ErrStreamHasNoCategory
	if errors.As(err, &noCatErr) || errors.Is(err, eventstore.ErrNoEvents) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}
