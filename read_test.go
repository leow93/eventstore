package eventstore

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataLog_ReadAt_recordLargerThanReadAhead(t *testing.T) {
	log, err := newFastDataLog(filepath.Join(t.TempDir(), "large.log"))
	require.NoError(t, err)
	defer log.Close()

	// A payload well beyond readAheadSize forces the second, exactly-sized pread.
	expected := &Event{
		StreamName: "user-1",
		EventType:  "BlobAttached",
		Payload:    bytes.Repeat([]byte("x"), 8*readAheadSize),
	}
	pos, err := log.Append(expected)
	require.NoError(t, err)

	actual, err := log.ReadAt(pos)

	require.NoError(t, err)
	assert.Equal(t, expected.EventType, actual.EventType)
	assert.Equal(t, expected.Payload, actual.Payload)
}

func TestDataLog_ReadAt_seesRecordsAppendedLater(t *testing.T) {
	log, err := newFastDataLog(filepath.Join(t.TempDir(), "grow.log"))
	require.NoError(t, err)
	defer log.Close()

	firstPos, err := log.Append(&Event{StreamName: "s-1", EventType: "First", Payload: []byte("one")})
	require.NoError(t, err)

	first, err := log.ReadAt(firstPos)
	require.NoError(t, err)
	assert.Equal(t, "First", first.EventType)

	// A record appended after the first read is immediately readable: pread reads
	// against the current durable size, so there is no mapping to refresh.
	secondPos, err := log.Append(&Event{StreamName: "s-1", EventType: "Second", Payload: []byte("two")})
	require.NoError(t, err)

	second, err := log.ReadAt(secondPos)

	require.NoError(t, err)
	assert.Equal(t, "Second", second.EventType)
}

func TestDataLog_ReadAt_rejectsOffsetBeyondSize(t *testing.T) {
	log, err := newFastDataLog(filepath.Join(t.TempDir(), "bounds.log"))
	require.NoError(t, err)
	defer log.Close()

	_, err = log.Append(&Event{StreamName: "s-1", EventType: "Only", Payload: []byte("x")})
	require.NoError(t, err)

	_, err = log.ReadAt(MakeLogPos(0, 1<<20))

	require.Error(t, err)
}

func TestDataLog_ReadAt_rejectsUnknownSegment(t *testing.T) {
	log, err := newFastDataLog(filepath.Join(t.TempDir(), "segment.log"))
	require.NoError(t, err)
	defer log.Close()

	pos, err := log.Append(&Event{StreamName: "s-1", EventType: "Only", Payload: []byte("x")})
	require.NoError(t, err)

	// There is only segment 0 today; anything else must be rejected.
	_, err = log.ReadAt(MakeLogPos(1, pos.Offset()))

	require.Error(t, err)
}
