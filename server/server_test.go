package server

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/leow93/eventstore"
	pb "github.com/leow93/eventstore/eventstorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestClient spins up an in-memory gRPC server backed by a fresh store and
// returns a connected client.
func newTestClient(t *testing.T) pb.EventStoreClient {
	t.Helper()

	store, err := eventstore.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterEventStoreServer(srv, New(store))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return pb.NewEventStoreClient(conn)
}

// drainStream collects every event from a server stream.
func drainStream(t *testing.T, stream grpc.ServerStreamingClient[pb.Event]) []*pb.Event {
	t.Helper()

	var out []*pb.Event
	for {
		evt, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		out = append(out, evt)
	}
	return out
}

func eventTypes(events []*pb.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.GetEventType()
	}
	return out
}

func TestServer_AppendAndReadStreamForwards(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	resp, err := client.Append(ctx, &pb.AppendRequest{
		StreamName:      "user-1",
		ExpectedVersion: 0,
		Events: []*pb.NewEvent{
			{EventType: "Created", Payload: []byte("a")},
			{EventType: "Renamed", Payload: []byte("b")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), resp.GetVersion())

	stream, err := client.ReadStream(ctx, &pb.ReadStreamRequest{
		StreamName: "user-1",
		Direction:  pb.Direction_DIRECTION_FORWARDS,
	})
	require.NoError(t, err)

	actual := drainStream(t, stream)

	assert.Equal(t, []string{"Created", "Renamed"}, eventTypes(actual))
	assert.Equal(t, uint64(1), actual[0].GetPosition())
	assert.Equal(t, "user-1", actual[0].GetStreamName())
}

func TestServer_ReadStreamBackwards(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.Append(ctx, &pb.AppendRequest{
		StreamName:      "user-1",
		ExpectedVersion: 0,
		Events: []*pb.NewEvent{
			{EventType: "E1"},
			{EventType: "E2"},
			{EventType: "E3"},
		},
	})
	require.NoError(t, err)

	stream, err := client.ReadStream(ctx, &pb.ReadStreamRequest{
		StreamName: "user-1",
		Direction:  pb.Direction_DIRECTION_BACKWARDS,
	})
	require.NoError(t, err)

	actual := drainStream(t, stream)

	assert.Equal(t, []string{"E3", "E2", "E1"}, eventTypes(actual))
}

func TestServer_ReadCategory(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.Append(ctx, &pb.AppendRequest{StreamName: "account-1", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Opened"}}})
	require.NoError(t, err)
	_, err = client.Append(ctx, &pb.AppendRequest{StreamName: "account-2", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Opened"}}})
	require.NoError(t, err)
	_, err = client.Append(ctx, &pb.AppendRequest{StreamName: "user-1", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Registered"}}})
	require.NoError(t, err)

	stream, err := client.ReadCategory(ctx, &pb.ReadCategoryRequest{Category: "account"})
	require.NoError(t, err)

	actual := drainStream(t, stream)

	require.Len(t, actual, 2)
	assert.Equal(t, []string{"account-1", "account-2"}, []string{actual[0].GetStreamName(), actual[1].GetStreamName()})
}

func TestServer_Append_wrongExpectedVersionReturnsAborted(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.Append(ctx, &pb.AppendRequest{StreamName: "user-1", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Created"}}})
	require.NoError(t, err)

	// The stream is now at version 1; expecting 0 again is a concurrency conflict.
	_, err = client.Append(ctx, &pb.AppendRequest{StreamName: "user-1", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Created"}}})

	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
}

func TestServer_Append_streamWithoutCategoryReturnsInvalidArgument(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	_, err := client.Append(ctx, &pb.AppendRequest{StreamName: "-bad", ExpectedVersion: 0, Events: []*pb.NewEvent{{EventType: "Created"}}})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
