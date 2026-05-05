package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_AppendToStream_OCC(t *testing.T) {
	tempDir := t.TempDir()
	logPath := fmt.Sprintf("%s/writer_occ.log", tempDir)

	// Setup components
	log, err := newFastLog(logPath)
	require.NoError(t, err, "Failed to create data log")

	tracker := NewStreamTracker()
	writer, err := NewEngine(tracker, log)
	require.NoError(t, err)

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
	log, err := newFastLog(logPath)
	require.NoError(t, err)

	tracker := NewStreamTracker()
	writer, err := NewEngine(tracker, log)
	require.NoError(t, err)

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
