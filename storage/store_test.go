package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataLog_Append verifies that events are correctly written to the physical file
// and that the returned byte offsets are perfectly accurate.
func TestDataLog_Append(t *testing.T) {
	// Skip this test if the -short flag is provided
	if testing.Short() {
		t.Skip("Skipping disk I/O test in short mode")
	}
	// t.TempDir() creates a unique temporary directory that Go automatically deletes after the test.
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "test.log")

	log, err := NewDataLog(logPath)
	if err != nil {
		t.Fatalf("failed to create data log: %v", err)
	}
	defer log.Close()

	// Create events using our fully updated struct (including Meta)
	evt1 := &Event{
		StreamName: "stream-1",
		EventType:  "ItemAdded",
		Payload:    []byte(`{"item": "apple"}`),
		Meta:       []byte(`{"user": "alice"}`),
	}
	evt2 := &Event{
		StreamName: "stream-1",
		EventType:  "ItemAdded",
		Payload:    []byte(`{"item": "banana"}`),
		Meta:       []byte(`{"user": "bob"}`),
	}

	// 1. Append first event
	offset1, err := log.Append(evt1)
	if err != nil {
		t.Fatalf("failed to append evt1: %v", err)
	}

	// First event should start at the very beginning of the file (offset 0)
	if offset1 != 0 {
		t.Fatalf("expected first offset to be 0, got %d", offset1)
	}
	if evt1.GlobalPosition != 1 {
		t.Fatalf("expected global position 1, got %d", evt1.GlobalPosition)
	}

	// 2. Append second event
	offset2, err := log.Append(evt2)
	if err != nil {
		t.Fatalf("failed to append evt2: %v", err)
	}

	// The second event should start exactly where the first event's byte array ended
	expectedOffset2 := int64(len(evt1.Encode()))
	if offset2 != expectedOffset2 {
		t.Fatalf("expected second offset to be %d, got %d", expectedOffset2, offset2)
	}
	if evt2.GlobalPosition != 2 {
		t.Fatalf("expected global position 2, got %d", evt2.GlobalPosition)
	}

	// 3. Verify actual physical file size on the hard drive
	stat, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	expectedFileSize := int64(len(evt1.Encode()) + len(evt2.Encode()))
	if stat.Size() != expectedFileSize {
		t.Fatalf("expected physical file size %d, got %d", expectedFileSize, stat.Size())
	}
}

// TestDataLog_ConcurrentAppend bombards the file with concurrent writes to ensure
// the global mutex strictly sequences events without data corruption or skipped numbers.
func TestDataLog_ConcurrentAppend(t *testing.T) {
	// Skip this test if the -short flag is provided
	if testing.Short() {
		t.Skip("Skipping concurrent disk I/O test in short mode")
	}
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "concurrent.log")

	log, err := NewDataLog(logPath)
	if err != nil {
		t.Fatalf("failed to create data log: %v", err)
	}
	defer log.Close()

	const numGoroutines = 100
	const eventsPerGoroutine = 20
	totalExpectedEvents := numGoroutines * eventsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Fire off 100 goroutines simultaneously trying to append 50 events each
	for i := range numGoroutines {
		go func(routineID int) {
			defer wg.Done()
			for range eventsPerGoroutine {
				evt := &Event{
					StreamName: "busy-stream",
					EventType:  "StressTest",
					Payload:    []byte("concurrent data payload"),
					Meta:       []byte("meta data"),
				}
				_, err := log.Append(evt)
				if err != nil {
					t.Errorf("routine %d failed to append: %v", routineID, err)
					return // Use return to exit this specific goroutine on error
				}
			}
		}(i)
	}

	// Wait for all 100 goroutines to finish
	wg.Wait()

	// Verify the final Global Position exactly matches the total number of events written
	if log.globalPosition.Load() != uint64(totalExpectedEvents) {
		t.Fatalf("expected final global position to be %d, got %d", totalExpectedEvents, log.globalPosition.Load())
	}

	// Verify the file actually has data and didn't just increment the counter in memory
	stat, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("expected file to have data, but size is 0")
	}
}

func TestDataLog_GlobalPosition(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")

	dataLog, err := newFastDataLog(logPath)
	require.NoError(t, err)

	// Seed position to 100
	dataLog.SetGlobalPosition(100)

	evt1 := &Event{StreamName: "a", EventType: "T1"}
	evt2 := &Event{StreamName: "b", EventType: "T2"}

	// Note: AppendBatch expects GlobalPosition to be assigned internally by the log
	_, err = dataLog.Append(evt1)
	require.NoError(t, err)
	_, err = dataLog.Append(evt2)
	require.NoError(t, err)

	// Verify the log correctly assigned sequential positions
	assert.Equal(t, uint64(101), evt1.GlobalPosition)
	assert.Equal(t, uint64(102), evt2.GlobalPosition)

	// Verify the atomic counter has the correct max
	assert.Equal(t, uint64(102), dataLog.globalPosition.Load())
}

func TestDataLog_ReadAt(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "data.log")

	log, err := newFastDataLog(logPath)
	require.NoError(t, err)
	defer log.Close()

	// 1. Standard Event (Fits in 1KB optimistic read)
	stdEvt := &Event{
		StreamName: "user-123",
		EventType:  "UserCreated",
		Payload:    []byte(`{"name": "Alice"}`),
		Meta:       []byte(`{"ip": "127.0.0.1"}`),
	}

	offset1, err := log.Append(stdEvt)
	require.NoError(t, err)

	// 2. Large Event (Forces the > 1KB slow-path read)
	largePayload := bytes.Repeat([]byte("A"), 1500) // 1.5KB payload
	largeEvt := &Event{
		StreamName: "user-123",
		EventType:  "LargeDataAdded",
		Payload:    largePayload,
		Meta:       []byte{}, // Empty meta
	}

	offset2, err := log.Append(largeEvt)
	require.NoError(t, err)

	// --- Verify Standard Event ---
	readEvt1, err := log.ReadAt(offset1)
	require.NoError(t, err)

	assert.Equal(t, stdEvt.StreamName, readEvt1.StreamName)
	assert.Equal(t, stdEvt.EventType, readEvt1.EventType)
	assert.Equal(t, stdEvt.Position, readEvt1.Position)
	assert.Equal(t, stdEvt.Timestamp, readEvt1.Timestamp)
	assert.Equal(t, stdEvt.Payload, readEvt1.Payload)
	assert.Equal(t, stdEvt.Meta, readEvt1.Meta)

	// Verify TotalSize calculation is perfectly symmetrical
	// (offset2 - offset1 should exactly equal the total size of evt1)
	assert.Equal(t, uint32(offset2-offset1), readEvt1.TotalSize())

	// --- Verify Large Event ---
	readEvt2, err := log.ReadAt(offset2)
	require.NoError(t, err)

	assert.Equal(t, largeEvt.StreamName, readEvt2.StreamName)
	assert.Equal(t, largeEvt.EventType, readEvt2.EventType)
	assert.Len(t, readEvt2.Payload, 1500)
}
