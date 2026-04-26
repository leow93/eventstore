package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootSequence_TailScanRecovery(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	streamIdxPath := filepath.Join(tempDir, "stream_shards")
	catIdxPath := filepath.Join(tempDir, "categories")

	// --- STEP 1: Normal Operation ---
	log, _ := newFastDataLog(logPath)
	sIdx, _ := NewShardedStreamIndex(streamIdxPath)
	cIdx, _ := NewCategoryIndex(catIdxPath)
	tracker := NewStreamTracker()
	writer := NewWriter(tracker, log, sIdx, cIdx)

	validEvt := &Event{
		StreamName: "order-99",
		EventType:  "OrderPlaced",
		Timestamp:  uint64(time.Now().UnixNano()),
		Payload:    []byte(`{"total": 100}`),
	}

	_, err := writer.AppendToStream(validEvt, 0) // Stream version 1
	require.NoError(t, err)

	// --- STEP 2: Simulate Crash (Dangling Log Write) ---
	// We write directly to the log, completely bypassing the writer/indexes
	danglingEvt := &Event{
		StreamName: "order-99",
		EventType:  "OrderShipped",
		Position:   2,
		Timestamp:  uint64(time.Now().UnixNano()),
		Payload:    []byte(`{"tracking": "123"}`),
	}
	_, err = log.Append(danglingEvt)
	require.NoError(t, err)

	// Close everything down (simulating process death)
	writer.Close()

	ctx := context.Background()

	// --- STEP 3: Boot Sequence Recovery ---
	// We open the database using our new orchestrator, which triggers the tail-scan
	recoveredWriter, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)
	defer recoveredWriter.Close()

	// --- STEP 4: Verify Memory & Indexes were Repaired ---
	recoveredTracker := recoveredWriter.tracker

	// 1. Did the StreamTracker get updated?
	h := recoveredTracker.GetHash("order-99")
	ver, exists := recoveredTracker.GetCurrentVersion(h)
	assert.True(t, exists)
	assert.Equal(t, uint64(2), ver, "Tracker should know about position 2 after recovery")

	// 2. Can we write position 3 now? (Validates OCC works post-recovery)
	nextEvt := &Event{StreamName: "order-99", EventType: "OrderDelivered"}
	_, err = recoveredWriter.AppendToStream(nextEvt, 2)
	assert.NoError(t, err, "Should be able to append pos 3 after recovering pos 2")
}
