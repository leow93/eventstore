package eventstore

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_Apply_recordsStreamAndCategoryOffsets(t *testing.T) {
	idx := NewIndex()

	require.NoError(t, idx.Apply(&Event{StreamName: "user-1", EventType: "Created", GlobalPosition: 1}, 0))
	require.NoError(t, idx.Apply(&Event{StreamName: "user-2", EventType: "Created", GlobalPosition: 2}, 40))
	require.NoError(t, idx.Apply(&Event{StreamName: "user-1", EventType: "Renamed", GlobalPosition: 3}, 90))

	// Stream offsets are recorded in stream order.
	assert.Equal(t, []int64{0, 90}, idx.StreamOffsets("user-1"))
	assert.Equal(t, []int64{40}, idx.StreamOffsets("user-2"))

	// Both streams share the "user" category, ordered by append order.
	assert.Equal(t, []int64{0, 40, 90}, idx.CategoryOffsets("user"))

	// The tip of each stream reflects the number of events applied.
	version, exists := idx.StreamVersion("user-1")
	require.True(t, exists)
	assert.Equal(t, uint64(2), version)

	// The high-water global position tracks the maximum seen.
	assert.Equal(t, uint64(3), idx.MaxGlobalPosition())
}

func TestIndex_StreamVersion_reportsUnknownStreamAsAbsent(t *testing.T) {
	idx := NewIndex()

	version, exists := idx.StreamVersion("never-written")

	assert.False(t, exists)
	assert.Equal(t, uint64(0), version)
}

func TestIndex_Apply_returnsErrorForStreamWithoutCategory(t *testing.T) {
	idx := NewIndex()

	err := idx.Apply(&Event{StreamName: "-nocategory", EventType: "Created", GlobalPosition: 1}, 0)

	require.Error(t, err)
	assert.Equal(t, ErrStreamHasNoCategory{stream: "-nocategory"}, err)
}

func TestIndex_Rebuild_reconstructsFromLog(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "rebuild.log")

	dataLog, err := newFastDataLog(logPath)
	require.NoError(t, err)
	defer dataLog.Close()

	evt1 := &Event{StreamName: "user-1", EventType: "Created", Payload: []byte("a")}
	evt2 := &Event{StreamName: "order-9", EventType: "Placed", Payload: []byte("b")}
	evt3 := &Event{StreamName: "user-1", EventType: "Renamed", Payload: []byte("c")}

	offset1, err := dataLog.Append(evt1)
	require.NoError(t, err)
	offset2, err := dataLog.Append(evt2)
	require.NoError(t, err)
	offset3, err := dataLog.Append(evt3)
	require.NoError(t, err)

	idx := NewIndex()
	require.NoError(t, idx.Rebuild(dataLog))

	assert.Equal(t, []int64{offset1, offset3}, idx.StreamOffsets("user-1"))
	assert.Equal(t, []int64{offset2}, idx.StreamOffsets("order-9"))
	assert.Equal(t, []int64{offset1, offset3}, idx.CategoryOffsets("user"))
	assert.Equal(t, []int64{offset2}, idx.CategoryOffsets("order"))

	version, exists := idx.StreamVersion("user-1")
	require.True(t, exists)
	assert.Equal(t, uint64(2), version)

	// evt3 was assigned the last global position by the log.
	assert.Equal(t, evt3.GlobalPosition, idx.MaxGlobalPosition())
}

func TestIndex_Rebuild_stopsAtTornTailWithoutError(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "torn.log")

	dataLog, err := newFastDataLog(logPath)
	require.NoError(t, err)

	evt1 := &Event{StreamName: "user-1", EventType: "Created", Payload: []byte("a")}
	evt2 := &Event{StreamName: "user-1", EventType: "Renamed", Payload: []byte("b")}
	offset1, err := dataLog.Append(evt1)
	require.NoError(t, err)
	offset2, err := dataLog.Append(evt2)
	require.NoError(t, err)
	require.NoError(t, dataLog.Close())

	// Simulate a torn write: a record header claiming a length whose body never
	// made it to disk (e.g. a crash between append and fsync).
	tornHeader := make([]byte, 4)
	binary.LittleEndian.PutUint32(tornHeader, 100_000)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write(tornHeader)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen the log so its size includes the torn tail, then rebuild.
	reopened, err := newFastDataLog(logPath)
	require.NoError(t, err)
	defer reopened.Close()

	idx := NewIndex()
	require.NoError(t, idx.Rebuild(reopened))

	// Only the two complete events are indexed; the torn tail is ignored.
	assert.Equal(t, []int64{offset1, offset2}, idx.StreamOffsets("user-1"))
	version, exists := idx.StreamVersion("user-1")
	require.True(t, exists)
	assert.Equal(t, uint64(2), version)
}
