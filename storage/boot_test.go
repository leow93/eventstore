package storage

import (
	"context"
	"os"
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
	writer := NewEngine(tracker, log, sIdx, cIdx)

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

func TestBoot_LoadsGlobalPosition(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	streamIdxPath := filepath.Join(tempDir, "stream_idx")
	catIdxPath := filepath.Join(tempDir, "cat_idx")

	// 1. Boot normally and write some events
	ctx := context.Background()
	writer, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	_, err = writer.AppendToStream(&Event{StreamName: "stream-a", EventType: "T1"}, 0)
	require.NoError(t, err)
	_, err = writer.AppendToStream(&Event{StreamName: "stream-b", EventType: "T2"}, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), writer.log.GetGlobalPosition())

	err = writer.Close()
	require.Nil(t, err)

	// 2. Re-open the database from disk
	writer2, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	// 3. Verify Global Position was restored correctly
	assert.Equal(t, uint64(2), writer2.log.GetGlobalPosition())

	// 4. Verify new writes increment correctly from the restored position
	evt3 := &Event{StreamName: "stream-c", EventType: "T3"}
	_, err = writer2.AppendToStream(evt3, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), writer2.log.GetGlobalPosition())
	assert.Equal(t, uint64(3), evt3.GlobalPosition)
}

func TestBoot_Recovery_CategoryIndexBehind(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	streamIdxPath := filepath.Join(tempDir, "stream_idx")
	catIdxPath := filepath.Join(tempDir, "cat_idx")

	ctx := context.Background()
	writer, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	_, err = writer.AppendToStream(&Event{StreamName: "test-stream", EventType: "Evt1"}, 0)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// SIMULATE CRASH: Nuke the category index.
	// StreamMax > 0, but CatMax = 0. The tail-scan MUST start at 0.
	err = os.RemoveAll(catIdxPath)
	require.NoError(t, err)

	// Boot database
	writer2, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	// Verify the tail-scan repaired the missing data and synced the global state
	assert.Equal(t, uint64(1), writer2.log.GetGlobalPosition())

	// Ensure the stream is still fully readable (proves StreamIndex duplicate pointers were handled)
	events, err := writer2.ReadStream("test-stream", 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "Evt1", events[0].EventType)
}

func TestBoot_Recovery_StreamIndexBehind(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	streamIdxPath := filepath.Join(tempDir, "stream_idx")
	catIdxPath := filepath.Join(tempDir, "cat_idx")

	ctx := context.Background()
	writer, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	_, err = writer.AppendToStream(&Event{StreamName: "test-stream-2", EventType: "EvtA"}, 0)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// SIMULATE CRASH: Nuke the stream index.
	// CatMax > 0, but StreamMax = 0. The tail-scan MUST start at 0.
	err = os.RemoveAll(streamIdxPath)
	require.NoError(t, err)

	// Boot database
	writer2, err := Boot(ctx, Config{logPath, streamIdxPath, catIdxPath})
	require.NoError(t, err)

	// Verify global state
	assert.Equal(t, uint64(1), writer2.log.GetGlobalPosition())

	// Since the StreamIndex was wiped and rebuilt, verify we can read the stream normally
	events, err := writer2.ReadStream("test-stream-2", 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "EvtA", events[0].EventType)
}
