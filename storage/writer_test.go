package storage

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFastDataLog(p string) (*DataLog, error) {
	l, err := NewDataLog(p)
	if err != nil {
		return nil, err
	}

	l.syncOnWrite = false
	return l, nil
}

func TestWriter_AppendToStream_OCC(t *testing.T) {
	tempDir := t.TempDir()
	logPath := fmt.Sprintf("%s/writer_occ.log", tempDir)

	// Setup components
	log, err := newFastDataLog(logPath)
	require.NoError(t, err, "Failed to create data log")

	sIdxPath := fmt.Sprintf("%s/streamidx", tempDir)
	sIdx, err := NewShardedStreamIndex(sIdxPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := NewWriter(tracker, log, sIdx)

	streamName := "account-123"

	// 1. First write (Expected version 0)
	evt1 := &Event{StreamName: streamName, EventType: "Created"}
	_, err = writer.AppendToStream(evt1, 0)
	assert.NoError(t, err, "First append should succeed with version 0")

	// 2. Try to write again with the WRONG expected version
	evt2 := &Event{StreamName: streamName, EventType: "Updated"}
	_, err = writer.AppendToStream(evt2, 0)
	assert.Error(t, err, "Should fail because stream already exists")
	assert.Equal(t, ErrStreamAlreadyExists{streamName}, err)

	// 3. Write with the CORRECT expected version (version is currently 1)
	_, err = writer.AppendToStream(evt2, 1)
	assert.NoError(t, err, "Second append should succeed with version 1")

	// 4. Verify version in tracker
	ver, ok := tracker.GetCurrentVersion(tracker.GetHash(streamName))
	assert.True(t, ok, "Stream should exist in tracker")
	assert.Equal(t, uint64(2), ver, "Expected stream version to be 2")

	// 5. Finally, write with wrong version
	_, err = writer.AppendToStream(evt2, 1)
	assert.Equal(t, ErrWrongExpectedVersion{streamName, 1, 2}, err)
}

func TestWriter_ConcurrentStreamContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping contention test in short mode")
	}

	tempDir := t.TempDir()
	logPath := fmt.Sprintf("%s/writer_contention.log", tempDir)
	log, err := newFastDataLog(logPath)
	require.NoError(t, err)

	sIdxPath := fmt.Sprintf("%s/streamidx", tempDir)
	sIdx, err := NewShardedStreamIndex(sIdxPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := NewWriter(tracker, log, sIdx)

	streamName := "hot-stream"
	const increments = 100
	var wg sync.WaitGroup
	wg.Add(increments)

	// Channel to capture errors from goroutines
	errChan := make(chan error, increments)

	h := tracker.GetHash(streamName)
	for range increments {
		go func() {
			defer wg.Done()
			lock := tracker.GetLock(h)
			// Simulate a client with a retry policy
			for range 50 {
				// SAFELY get the current version to attempt the write
				lock.RLock()
				current, _ := tracker.GetCurrentVersion(h)
				lock.RUnlock()
				_, err := writer.AppendToStream(&Event{
					StreamName: streamName,
					EventType:  "Incremented",
				}, current)

				if err == nil {
					return // Success!
				}
				// If we hit a version conflict, we loop and try again
			}
			errChan <- fmt.Errorf("failed to append after 50 retries")
		}()
	}

	wg.Wait()
	close(errChan)

	// Check if any goroutines failed to eventually write their event
	for err := range errChan {
		assert.NoError(t, err)
	}

	// The final version must be exactly equal to the number of successful increments
	ver, ok := tracker.GetCurrentVersion(tracker.GetHash(streamName))
	assert.True(t, ok)
	assert.Equal(t, uint64(increments), ver)
}

func TestWriter_RecoveryFromIndex(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	indexPath := filepath.Join(tempDir, "streamidx")

	// 1. Initial Start: Write some data
	log, err := newFastDataLog(logPath)
	require.NoError(t, err)

	idx, err := NewShardedStreamIndex(indexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := &Writer{
		tracker:   tracker,
		log:       log,
		streamIdx: idx,
	}

	streamID := "user-99"
	h := tracker.GetHash(streamID)

	// Append 3 events
	for i := range 3 {
		_, err := writer.AppendToStream(&Event{
			StreamName: streamID,
			EventType:  "TestEvent",
		}, uint64(i))
		require.NoError(t, err)
	}

	// 2. Simulate "Crash" / Shutdown
	err = idx.Close()
	require.Nil(t, err)
	err = log.Close()
	require.Nil(t, err)

	// 3. Reboot: New tracker, new writer, same files
	newTracker := NewStreamTracker()
	newIdx, _ := NewShardedStreamIndex(indexPath)

	// Rebuild in-memory state from the index file
	err = newIdx.Load(func(hash, pos, offset uint64) {
		newTracker.UpdateVersion(hash, pos)
	})
	require.NoError(t, err)

	// 4. Verify: The tracker should know the stream is at version 3
	ver, ok := newTracker.GetCurrentVersion(h)
	assert.True(t, ok, "Stream should be known after recovery")
	assert.Equal(t, uint64(3), ver, "Tracker should have recovered version 3")

	// 5. Verify OCC still works after recovery
	newLog, _ := newFastDataLog(logPath)
	newWriter := &Writer{
		tracker:   newTracker,
		log:       newLog,
		streamIdx: newIdx,
	}

	// This should fail because expected version 1 is now ancient history
	_, err = newWriter.AppendToStream(&Event{StreamName: streamID}, 1)
	assert.Error(t, err, "Should reject stale version after recovery")
}

func TestWriter_FullLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	indexPath := filepath.Join(tempDir, "shards")

	// 1. Setup
	log, err := newFastDataLog(logPath)
	require.NoError(t, err)

	idx, err := NewShardedStreamIndex(indexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := NewWriter(tracker, log, idx)
	defer writer.Close()

	streamName := "inventory-sh1"

	// 2. Test Successful Append
	evt := &Event{StreamName: streamName, EventType: "StockAdded"}
	offset, err := writer.AppendToStream(evt, 0) // Expecting new stream
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	// 3. Test OCC Failure (Wrong version)
	_, err = writer.AppendToStream(evt, 0)
	assert.Error(t, err, "Should fail because version is now 1")
	assert.Contains(t, err.Error(), "already exists")

	// 4. Test OCC Success (Correct version)
	_, err = writer.AppendToStream(evt, 1)
	assert.NoError(t, err)

	// 5. Verify Tracker State
	h := tracker.GetHash(streamName)
	ver, ok := tracker.GetCurrentVersion(h)
	assert.True(t, ok)
	assert.Equal(t, uint64(2), ver)
}

func TestWriter_ParallelShardThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high-concurrency test")
	}

	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "bench.log")
	indexPath := filepath.Join(tempDir, "bench_shards")

	log, _ := newFastDataLog(logPath)
	idx, _ := NewShardedStreamIndex(indexPath)
	tracker := NewStreamTracker()
	writer := NewWriter(tracker, log, idx)
	defer writer.Close()

	var wg sync.WaitGroup
	workerCount := 100
	eventsPerWorker := 10

	wg.Add(workerCount)

	for i := range workerCount {
		// Each worker writes to its OWN unique stream
		// This tests that our sharding allows parallel progress
		go func(id int) {
			defer wg.Done()
			sName := fmt.Sprintf("stream-%d", id)
			for j := range eventsPerWorker {
				_, err := writer.AppendToStream(&Event{
					StreamName: sName,
					EventType:  "ParallelEvent",
				}, uint64(j))
				if err != nil {
					t.Errorf("Worker %d failed at event %d: %v", id, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify total counts
	for i := range workerCount {
		sName := fmt.Sprintf("stream-%d", i)
		h := tracker.GetHash(sName)
		ver, ok := tracker.GetCurrentVersion(h)
		assert.True(t, ok)
		assert.Equal(t, uint64(eventsPerWorker), ver)
	}
}
