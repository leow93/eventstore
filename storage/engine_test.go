package storage

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	en := setupTestEngine(t)
	defer en.Close()

	streamName := "account-123"

	// 1. First write (Expected version 0)
	evt1 := &Event{StreamName: streamName, EventType: "Created"}
	_, err := en.AppendToStream(evt1, 0)
	assert.NoError(t, err, "First append should succeed with version 0")

	// 2. Try to write again with the WRONG expected version
	evt2 := &Event{StreamName: streamName, EventType: "Updated"}
	_, err = en.AppendToStream(evt2, 0)
	assert.Error(t, err, "Should fail because stream already exists")
	assert.Equal(t, ErrStreamAlreadyExists{streamName}, err)

	// 3. Write with the CORRECT expected version (version is currently 1)
	_, err = en.AppendToStream(evt2, 1)
	assert.NoError(t, err, "Second append should succeed with version 1")

	// 4. Verify version in tracker
	ver, ok := en.tracker.GetCurrentVersion(en.tracker.GetHash(streamName))
	assert.True(t, ok, "Stream should exist in tracker")
	assert.Equal(t, uint64(2), ver, "Expected stream version to be 2")

	// 5. Finally, write with wrong version
	_, err = en.AppendToStream(evt2, 1)
	assert.Equal(t, ErrWrongExpectedVersion{streamName, 1, 2}, err)
}

func TestWriter_ConcurrentStreamContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping contention test in short mode")
	}
	engine := setupTestEngine(t)
	defer engine.Close()

	streamName := "hot-stream"
	const increments = 100
	var wg sync.WaitGroup
	wg.Add(increments)

	errChan := make(chan error, increments)
	h := engine.tracker.GetHash(streamName)

	for range increments {
		go func() {
			defer wg.Done()
			lock := engine.tracker.GetLock(h)

			// 1. Give them enough retries to survive a 100-way collision
			for range 200 {
				lock.RLock()
				current, _ := engine.tracker.GetCurrentVersion(h)
				lock.RUnlock()

				_, err := engine.AppendToStream(&Event{
					StreamName: streamName,
					EventType:  "Incremented",
				}, current)

				if err == nil {
					return // Success!
				}

				// 2. JITTER: Sleep for a tiny random duration (0 to 5ms) to break the lockstep
				jitter := time.Duration(rand.Intn(5)) * time.Millisecond
				time.Sleep(jitter)
			}
			errChan <- fmt.Errorf("failed to append after 200 retries")
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		assert.NoError(t, err)
	}

	ver, ok := engine.tracker.GetCurrentVersion(engine.tracker.GetHash(streamName))
	assert.True(t, ok)
	assert.Equal(t, uint64(increments), ver)
}

func TestWriter_RecoveryFromIndex(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")
	indexPath := filepath.Join(tempDir, "streamidx")
	cIndexPath := filepath.Join(tempDir, "catidx")

	// 1. Initial Start: Write some data
	log, err := newFastDataLog(logPath)
	require.NoError(t, err)

	idx, err := NewShardedStreamIndex(indexPath)
	require.NoError(t, err)

	cIdx, err := NewCategoryIndex(cIndexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	engine := NewEngine(tracker, log, idx, cIdx)
	defer engine.Close()

	streamID := "user-99"
	h := tracker.GetHash(streamID)

	// Append 3 events
	for i := range 3 {
		_, err := engine.AppendToStream(&Event{
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
	newWriter := &Engine{
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

	sIdx, err := NewShardedStreamIndex(indexPath)
	require.NoError(t, err)

	cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
	cIndex, err := NewCategoryIndex(cIndexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	engine := NewEngine(tracker, log, sIdx, cIndex)
	defer engine.Close()

	streamName := "inventory-sh1"

	// 2. Test Successful Append
	evt := &Event{StreamName: streamName, EventType: "StockAdded"}
	offset, err := engine.AppendToStream(evt, 0) // Expecting new stream
	assert.NoError(t, err)
	assert.Equal(t, int64(0), offset)

	// 3. Test OCC Failure (Wrong version)
	_, err = engine.AppendToStream(evt, 0)
	assert.Error(t, err, "Should fail because version is now 1")
	assert.Contains(t, err.Error(), "already exists")

	// 4. Test OCC Success (Correct version)
	_, err = engine.AppendToStream(evt, 1)
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
	sIdx, _ := NewShardedStreamIndex(indexPath)

	cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
	cIndex, err := NewCategoryIndex(cIndexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := NewEngine(tracker, log, sIdx, cIndex)
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

// setupTestEngine is a quick helper to boot a fresh database for each test
func setupTestEngine(t *testing.T) *Engine {
	tempDir := t.TempDir()

	// Assuming OpenDatabase was updated to return *Engine
	engine, err := Boot(
		context.Background(),
		Config{
			filepath.Join(tempDir, "data.log"),
			filepath.Join(tempDir, "stream_idx"),
			filepath.Join(tempDir, "cat_idx"),
		},
	)
	require.NoError(t, err)
	return engine
}

func TestEngine_HighConcurrencyBatching(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping contention test in short mode")
	}
	engine := setupTestEngine(t)
	defer engine.Close()

	var wg sync.WaitGroup
	numWorkers := 100
	eventsPerWorker := 10

	var successCount atomic.Int32

	// Spawn 100 workers, each writing 10 events to their own unique stream.
	// This forces the queue to fill up and the batchProcessor to sweep them in chunks.
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			streamName := fmt.Sprintf("testcat-worker-%d", workerID)

			for i := range eventsPerWorker {
				evt := &Event{
					StreamName: streamName,
					EventType:  "TestFired",
					Payload:    []byte(`{"data":"test"}`),
				}

				// Expected version increments cleanly
				_, err := engine.AppendToStream(evt, uint64(i))
				if err == nil {
					successCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()

	// Every single write should have succeeded
	assert.Equal(t, int32(numWorkers*eventsPerWorker), successCount.Load())

	// The DataLog should have advanced the global position correctly
	assert.Equal(t, uint64(numWorkers*eventsPerWorker), engine.log.GetGlobalPosition())
}

func TestEngine_OCC_StreamAlreadyExists(t *testing.T) {
	engine := setupTestEngine(t)
	defer engine.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	// Fire two creation requests at the EXACT same time for the EXACT same stream.
	// This proves our locks inside the batchProcessor prevent race conditions.
	for range 2 {
		wg.Go(func() {
			evt := &Event{StreamName: "racecat-stream", EventType: "Created"}
			_, err := engine.AppendToStream(evt, 0) // Both expect 0 (new stream)
			errs <- err
		})
	}

	wg.Wait()
	close(errs)

	var successCount, failCount int
	for err := range errs {
		if err == nil {
			successCount++
		} else {
			failCount++
			// Verify we got the exact custom error type you implemented
			assert.IsType(t, ErrStreamAlreadyExists{}, err)
		}
	}

	// Exactly 1 must win, exactly 1 must fail.
	// If it hangs here, a lock.Unlock() is missing.
	assert.Equal(t, 1, successCount)
	assert.Equal(t, 1, failCount)
}

func TestEngine_OCC_WrongExpectedVersion(t *testing.T) {
	engine := setupTestEngine(t)
	defer engine.Close()

	streamName := "testcat-versioning"

	// 1. Create the stream successfully
	_, err := engine.AppendToStream(&Event{StreamName: streamName, EventType: "Created"}, 0)
	require.NoError(t, err)

	// 2. Try to append with the wrong expected version
	_, err = engine.AppendToStream(&Event{StreamName: streamName, EventType: "Updated"}, 99)

	require.Error(t, err)
	assert.IsType(t, ErrWrongExpectedVersion{}, err)
}

func TestEngine_GracefulClose(t *testing.T) {
	engine := setupTestEngine(t)

	// Write one event to ensure it's alive
	_, err := engine.AppendToStream(&Event{StreamName: "testcat-1", EventType: "A"}, 0)
	require.NoError(t, err)

	// Trigger the close
	err = engine.Close()
	require.NoError(t, err)

	// Attempting to write after close should immediately fail and not hang
	_, err = engine.AppendToStream(&Event{StreamName: "testcat-2", EventType: "B"}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine is shutting down")

	// Calling Close twice should be safe and return nil
	err = engine.Close()
	require.NoError(t, err)
}

func TestEngine_HaltState(t *testing.T) {
	engine := setupTestEngine(t)
	defer engine.Close()

	// 1. Manually trigger a catastrophic failure state
	simulatedCrash := fmt.Errorf("simulated disk failure")
	engine.haltError.Store(simulatedCrash)

	// 2. The next request should instantly bounce back with the error
	_, err := engine.AppendToStream(&Event{StreamName: "testcat-1", EventType: "A"}, 0)

	require.Error(t, err)
	assert.Equal(t, simulatedCrash, err)
}

func TestEngine_InvalidCategory(t *testing.T) {
	engine := setupTestEngine(t)
	defer engine.Close()

	// Assuming GetCategory returns an error if the format is bad
	evt := &Event{StreamName: "-badstreamname", EventType: "A"}
	_, err := engine.AppendToStream(evt, 0)

	require.Error(t, err)
}

func TestReadStream_Deduplication(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "bench.log")
	indexPath := filepath.Join(tempDir, "bench_shards")

	log, _ := newFastDataLog(logPath)
	sIdx, _ := NewShardedStreamIndex(indexPath)

	cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
	cIndex, err := NewCategoryIndex(cIndexPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer := NewEngine(tracker, log, sIdx, cIndex)
	defer writer.Close()
	// Write an event normally
	evt := &Event{StreamName: "cart-1", EventType: "ItemAdded", Payload: []byte("A")}
	_, err = writer.AppendToStream(evt, 0)
	require.NoError(t, err)

	// Simulate a duplicate index write (e.g. from a messy tail-scan recovery)
	// We reach directly into the index and write Position 1 again, pointing to the same offset
	h := writer.tracker.GetHash("cart-1")
	err = writer.streamIdx.Append(h, 1, 0) // Writing Pos 1 again
	require.NoError(t, err)

	// Write a second event normally
	evt2 := &Event{StreamName: "cart-1", EventType: "ItemRemoved", Payload: []byte("B")}
	_, err = writer.AppendToStream(evt2, 1)
	require.NoError(t, err)

	// READ THE STREAM
	events, err := writer.ReadStream("cart-1", 0, 100)
	require.NoError(t, err)

	// We should only get 2 events back, not 3. The duplicate Pos 1 should be ignored.
	require.Len(t, events, 2)
	assert.Equal(t, uint64(1), events[0].Position)
	assert.Equal(t, []byte("A"), events[0].Payload)
	assert.Equal(t, uint64(2), events[1].Position)
	assert.Equal(t, []byte("B"), events[1].Payload)
}
