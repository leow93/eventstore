package eventstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFastLogMax returns a SegmentedLog with a small roll-over size and fsync
// disabled, so a handful of events is enough to exercise segmentation.
func newFastLogMax(t *testing.T, dir string, maxSize int64) *SegmentedLog {
	t.Helper()
	l, err := NewSegmentedLog(dir, maxSize)
	require.NoError(t, err)
	l.syncOnWrite = false
	return l
}

func TestSegmentedLog_RollsOverAtMaxSize(t *testing.T) {
	dir := t.TempDir()
	log := newFastLogMax(t, dir, 512)
	defer log.Close()

	const n = 50
	positions := make([]LogPos, n)
	for i := range positions {
		pos, err := log.Append(&Event{StreamName: "acct-1", EventType: "Moved", Payload: []byte("0123456789ABCDEF")})
		require.NoError(t, err)
		positions[i] = pos
	}

	// Writing well past the roll-over size produced more than one segment.
	assert.Greater(t, positions[n-1].Segment(), uint32(0))
	nums, err := discoverSegments(dir)
	require.NoError(t, err)
	assert.Greater(t, len(nums), 1)

	// Segment numbers only ever increase across the append order.
	for i := 1; i < n; i++ {
		assert.GreaterOrEqual(t, positions[i].Segment(), positions[i-1].Segment())
	}

	// Every event is readable across segments, with global positions intact.
	for i, pos := range positions {
		evt, err := log.ReadAt(pos)
		require.NoError(t, err)
		assert.Equal(t, uint64(i+1), evt.GlobalPosition)
	}
}

func TestSegmentedLog_RollOverSealsPriorSegments(t *testing.T) {
	dir := t.TempDir()
	log := newFastLogMax(t, dir, 256)
	defer log.Close()

	for range 20 {
		_, err := log.Append(&Event{StreamName: "s-1", EventType: "E", Payload: []byte("payload-bytes")})
		require.NoError(t, err)
	}

	log.mu.RLock()
	defer log.mu.RUnlock()
	require.Greater(t, len(log.segments), 1)
	for num, seg := range log.segments {
		if num == log.active.num {
			assert.False(t, seg.sealed, "active segment must not be sealed")
		} else {
			assert.True(t, seg.sealed, "inactive segment %d must be sealed", num)
		}
	}
}

func TestSegmentedLog_BatchNeverSpansSegments(t *testing.T) {
	dir := t.TempDir()
	log := newFastLogMax(t, dir, 250)
	defer log.Close()

	// Fill segment 0 most of the way.
	_, err := log.Append(&Event{StreamName: "s-1", EventType: "Seed", Payload: bytes.Repeat([]byte("x"), 150)})
	require.NoError(t, err)

	// A batch that will not fit in the remainder must roll over and land wholly in
	// the next segment, not straddle the boundary.
	batch := []*Event{
		{StreamName: "s-1", EventType: "A", Payload: []byte("payload-aaaaaaa")},
		{StreamName: "s-1", EventType: "B", Payload: []byte("payload-bbbbbbb")},
		{StreamName: "s-1", EventType: "C", Payload: []byte("payload-ccccccc")},
	}
	positions, err := log.AppendBatch(batch)
	require.NoError(t, err)
	require.Len(t, positions, 3)

	assert.Greater(t, positions[0].Segment(), uint32(0), "batch should have rolled to a new segment")
	assert.Equal(t, positions[0].Segment(), positions[1].Segment())
	assert.Equal(t, positions[0].Segment(), positions[2].Segment())
}

func TestSegmentedLog_ReplayReconstructsAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	log := newFastLogMax(t, dir, 300)

	const n = 40
	for range n {
		_, err := log.Append(&Event{StreamName: "user-1", EventType: "E", Payload: []byte("payloadxxxxxxx")})
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	// Reopen and rebuild the index by replaying every segment in order.
	reopened := newFastLogMax(t, dir, 300)
	defer reopened.Close()
	idx := NewIndex()
	require.NoError(t, idx.Rebuild(reopened))

	assert.Len(t, idx.StreamOffsets("user-1"), n)
	version, ok := idx.StreamVersion("user-1")
	require.True(t, ok)
	assert.Equal(t, uint64(n), version)
}
