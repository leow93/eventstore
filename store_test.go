package eventstore

import (
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collect drains a read iterator into a slice, failing the test on any read error.
func collect(t *testing.T, seq iter.Seq2[*Event, error]) []*Event {
	t.Helper()

	var out []*Event
	for evt, err := range seq {
		require.NoError(t, err)
		out = append(out, evt)
	}
	return out
}

// eventTypes projects a slice of events to their event types, for concise assertions.
func eventTypes(events []*Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventType
	}
	return out
}

func TestStore_AppendToStream_assignsSequentialPositions(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	version, err := store.AppendToStream("user-1", 0,
		&Event{EventType: "Created"},
		&Event{EventType: "Renamed"},
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), version)

	actual := collect(t, store.ReadStreamForwards("user-1", 0, 0))

	require.Len(t, actual, 2)
	assert.Equal(t, uint64(1), actual[0].Position)
	assert.Equal(t, uint64(2), actual[1].Position)
	assert.Equal(t, "user-1", actual[0].StreamName)
	// The log assigns global positions across the whole store.
	assert.Equal(t, uint64(1), actual[0].GlobalPosition)
	assert.Equal(t, uint64(2), actual[1].GlobalPosition)
}

func TestStore_AppendToStream_enforcesExpectedVersion(t *testing.T) {
	cases := []struct {
		name        string
		expected    ExpectedVersion
		expectError bool
	}{
		{
			name:        "correct version succeeds",
			expected:    2,
			expectError: false,
		},
		{
			name:        "stale version is rejected",
			expected:    1,
			expectError: true,
		},
		{
			name:        "future version is rejected",
			expected:    5,
			expectError: true,
		},
		{
			name:        "expecting empty on an existing stream is rejected",
			expected:    0,
			expectError: true,
		},
		{
			name:        "any version bypasses the check",
			expected:    AnyVersion,
			expectError: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := Open(t.TempDir())
			require.NoError(t, err)
			defer store.Close()

			// Seed the stream with two events (version becomes 2).
			_, err = store.AppendToStream("account-1", 0,
				&Event{EventType: "Opened"},
				&Event{EventType: "Deposited"},
			)
			require.NoError(t, err)

			_, err = store.AppendToStream("account-1", tc.expected, &Event{EventType: "Withdrawn"})

			if tc.expectError {
				require.Error(t, err)
				var occErr ErrWrongExpectedVersion
				require.ErrorAs(t, err, &occErr)
				assert.Equal(t, uint64(2), occErr.Actual)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestStore_AppendToStream_rejectsStreamWithoutCategory(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("-nocategory", 0, &Event{EventType: "Created"})

	require.Error(t, err)
	assert.Equal(t, ErrStreamHasNoCategory{stream: "-nocategory"}, err)
}

func TestStore_AppendToStream_rejectsEmptyBatch(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("user-1", 0)

	require.ErrorIs(t, err, ErrNoEvents)
}

func TestStore_ReadStreamForwards_fromPositionWithLimit(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("user-1", 0,
		&Event{EventType: "E1"},
		&Event{EventType: "E2"},
		&Event{EventType: "E3"},
		&Event{EventType: "E4"},
	)
	require.NoError(t, err)

	actual := collect(t, store.ReadStreamForwards("user-1", 2, 2))

	assert.Equal(t, []string{"E2", "E3"}, eventTypes(actual))
}

func TestStore_ReadStreamForwards_unknownStreamYieldsNothing(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	actual := collect(t, store.ReadStreamForwards("never-written", 0, 0))

	assert.Empty(t, actual)
}

func TestStore_ReadStreamBackwards_fromTip(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("user-1", 0,
		&Event{EventType: "E1"},
		&Event{EventType: "E2"},
		&Event{EventType: "E3"},
	)
	require.NoError(t, err)

	actual := collect(t, store.ReadStreamBackwards("user-1", 0, 0))

	assert.Equal(t, []string{"E3", "E2", "E1"}, eventTypes(actual))
}

func TestStore_ReadStreamBackwards_fromPositionWithLimit(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("user-1", 0,
		&Event{EventType: "E1"},
		&Event{EventType: "E2"},
		&Event{EventType: "E3"},
		&Event{EventType: "E4"},
	)
	require.NoError(t, err)

	actual := collect(t, store.ReadStreamBackwards("user-1", 3, 2))

	assert.Equal(t, []string{"E3", "E2"}, eventTypes(actual))
}

func TestStore_ReadCategory_spansStreamsInAppendOrder(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("account-1", 0, &Event{EventType: "Opened"})
	require.NoError(t, err)
	_, err = store.AppendToStream("account-2", 0, &Event{EventType: "Opened"})
	require.NoError(t, err)
	_, err = store.AppendToStream("account-1", 1, &Event{EventType: "Closed"})
	require.NoError(t, err)
	// A different category must not appear in the "account" category read.
	_, err = store.AppendToStream("user-1", 0, &Event{EventType: "Registered"})
	require.NoError(t, err)

	actual := collect(t, store.ReadCategory("account", 0, 0))

	require.Len(t, actual, 3)
	assert.Equal(t, []string{"account-1", "account-2", "account-1"}, streamNames(actual))
	assert.Equal(t, []string{"Opened", "Opened", "Closed"}, eventTypes(actual))
}

func TestStore_ReadCategory_fromPosition(t *testing.T) {
	store, err := Open(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	_, err = store.AppendToStream("order-1", 0, &Event{EventType: "Placed"})
	require.NoError(t, err)
	_, err = store.AppendToStream("order-2", 0, &Event{EventType: "Placed"})
	require.NoError(t, err)
	_, err = store.AppendToStream("order-3", 0, &Event{EventType: "Placed"})
	require.NoError(t, err)

	actual := collect(t, store.ReadCategory("order", 2, 0))

	require.Len(t, actual, 2)
	assert.Equal(t, []string{"order-2", "order-3"}, streamNames(actual))
}

func TestStore_reopenRecoversStateAndContinues(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir)
	require.NoError(t, err)

	_, err = store.AppendToStream("user-1", 0,
		&Event{EventType: "Created", Payload: []byte("a")},
		&Event{EventType: "Renamed", Payload: []byte("b")},
	)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Reopen: the index is rebuilt from the log.
	reopened, err := Open(dir)
	require.NoError(t, err)
	defer reopened.Close()

	// Existing events are readable.
	actual := collect(t, reopened.ReadStreamForwards("user-1", 0, 0))
	assert.Equal(t, []string{"Created", "Renamed"}, eventTypes(actual))

	// OCC still knows the stream is at version 2: a stale expectation is rejected.
	_, err = reopened.AppendToStream("user-1", 1, &Event{EventType: "Deleted"})
	require.Error(t, err)

	// Appending with the correct version continues the stream and the global sequence.
	version, err := reopened.AppendToStream("user-1", 2, &Event{EventType: "Deleted"})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), version)

	last := collect(t, reopened.ReadStreamBackwards("user-1", 0, 1))
	require.Len(t, last, 1)
	assert.Equal(t, uint64(3), last[0].GlobalPosition)
}

func streamNames(events []*Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.StreamName
	}
	return out
}
