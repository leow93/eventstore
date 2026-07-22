package eventstore

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFastLog returns a SegmentedLog rooted at dir with fsync-on-write disabled,
// for tests and benchmarks that exercise logic unrelated to durability and don't
// want to pay the fsync cost.
func newFastLog(dir string) (*SegmentedLog, error) {
	l, err := NewSegmentedLog(dir, DefaultSegmentSize)
	if err != nil {
		return nil, err
	}
	l.syncOnWrite = false
	return l, nil
}

func TestSegmentedLog_Append_returnsSequentialOffsets(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping disk I/O test in short mode")
	}
	dir := t.TempDir()

	log, err := NewSegmentedLog(dir, DefaultSegmentSize)
	require.NoError(t, err)
	defer log.Close()

	evt1 := &Event{StreamName: "stream-1", EventType: "ItemAdded", Payload: []byte(`{"item": "apple"}`), Meta: []byte(`{"user": "alice"}`)}
	evt2 := &Event{StreamName: "stream-1", EventType: "ItemAdded", Payload: []byte(`{"item": "banana"}`), Meta: []byte(`{"user": "bob"}`)}

	offset1, err := log.Append(evt1)
	require.NoError(t, err)
	assert.Equal(t, MakeLogPos(0, 0), offset1)
	assert.Equal(t, uint64(1), evt1.GlobalPosition)

	offset2, err := log.Append(evt2)
	require.NoError(t, err)
	assert.Equal(t, MakeLogPos(0, uint32(len(evt1.Encode()))), offset2)
	assert.Equal(t, uint64(2), evt2.GlobalPosition)

	// Both records landed in segment 0, at exactly their encoded sizes.
	stat, err := os.Stat(filepath.Join(dir, segmentName(0)))
	require.NoError(t, err)
	assert.Equal(t, int64(len(evt1.Encode())+len(evt2.Encode())), stat.Size())
}

// TestSegmentedLog_ConcurrentAppend bombards the log with concurrent writes to
// ensure the write mutex strictly sequences events without corruption or skipped
// global positions.
func TestSegmentedLog_ConcurrentAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent disk I/O test in short mode")
	}
	log, err := NewSegmentedLog(t.TempDir(), DefaultSegmentSize)
	require.NoError(t, err)
	defer log.Close()

	const numGoroutines = 100
	const eventsPerGoroutine = 20
	total := numGoroutines * eventsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range eventsPerGoroutine {
				_, err := log.Append(&Event{StreamName: "busy-stream", EventType: "StressTest", Payload: []byte("concurrent data payload"), Meta: []byte("meta data")})
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(total), log.globalPosition.Load())
	assert.Greater(t, log.Size(), int64(0))
}

func TestSegmentedLog_GlobalPosition_seedsAndAdvances(t *testing.T) {
	log, err := newFastLog(t.TempDir())
	require.NoError(t, err)
	defer log.Close()

	log.SetGlobalPosition(100)

	evt1 := &Event{StreamName: "a", EventType: "T1"}
	evt2 := &Event{StreamName: "b", EventType: "T2"}
	_, err = log.Append(evt1)
	require.NoError(t, err)
	_, err = log.Append(evt2)
	require.NoError(t, err)

	assert.Equal(t, uint64(101), evt1.GlobalPosition)
	assert.Equal(t, uint64(102), evt2.GlobalPosition)
	assert.Equal(t, uint64(102), log.globalPosition.Load())
}

func TestSegmentedLog_AppendBatch_offsetsAndReadback(t *testing.T) {
	log, err := newFastLog(t.TempDir())
	require.NoError(t, err)
	defer log.Close()

	events := []*Event{
		{StreamName: "user-1", EventType: "Created", Payload: []byte("a")},
		{StreamName: "user-1", EventType: "Renamed", Payload: []byte("b")},
		{StreamName: "user-1", EventType: "Deleted", Payload: []byte("c")},
	}

	offsets, err := log.AppendBatch(events)
	require.NoError(t, err)
	require.Len(t, offsets, 3)

	// The first event starts at 0 and each subsequent offset follows the previous
	// event's encoded length.
	assert.Equal(t, MakeLogPos(0, 0), offsets[0])
	assert.Equal(t, MakeLogPos(0, uint32(len(events[0].Encode()))), offsets[1])
	assert.Equal(t, MakeLogPos(0, uint32(len(events[0].Encode())+len(events[1].Encode()))), offsets[2])

	assert.Equal(t, uint64(1), events[0].GlobalPosition)
	assert.Equal(t, uint64(2), events[1].GlobalPosition)
	assert.Equal(t, uint64(3), events[2].GlobalPosition)

	for i, off := range offsets {
		actual, err := log.ReadAt(off)
		require.NoError(t, err)
		assert.Equal(t, events[i].EventType, actual.EventType)
		assert.Equal(t, events[i].Payload, actual.Payload)
	}
}

func TestSegmentedLog_AppendBatch_emptyIsNoOp(t *testing.T) {
	log, err := newFastLog(t.TempDir())
	require.NoError(t, err)
	defer log.Close()

	offsets, err := log.AppendBatch(nil)

	require.NoError(t, err)
	assert.Empty(t, offsets)
	assert.Equal(t, int64(0), log.Size())
}

func TestSegmentedLog_ReadAt_roundTrip(t *testing.T) {
	log, err := newFastLog(t.TempDir())
	require.NoError(t, err)
	defer log.Close()

	stdEvt := &Event{StreamName: "user-123", EventType: "UserCreated", Payload: []byte(`{"name": "Alice"}`), Meta: []byte(`{"ip": "127.0.0.1"}`)}
	offset1, err := log.Append(stdEvt)
	require.NoError(t, err)

	// A larger event forces reads to span more than the first record's bytes.
	largeEvt := &Event{StreamName: "user-123", EventType: "LargeDataAdded", Payload: bytes.Repeat([]byte("A"), 1500), Meta: []byte{}}
	offset2, err := log.Append(largeEvt)
	require.NoError(t, err)

	read1, err := log.ReadAt(offset1)
	require.NoError(t, err)
	assert.Equal(t, stdEvt.StreamName, read1.StreamName)
	assert.Equal(t, stdEvt.EventType, read1.EventType)
	assert.Equal(t, stdEvt.Payload, read1.Payload)
	assert.Equal(t, stdEvt.Meta, read1.Meta)
	// offset2 - offset1 should equal the total encoded size of the first event.
	assert.Equal(t, offset2.Offset()-offset1.Offset(), read1.TotalSize())

	read2, err := log.ReadAt(offset2)
	require.NoError(t, err)
	assert.Equal(t, largeEvt.StreamName, read2.StreamName)
	assert.Len(t, read2.Payload, 1500)
}
