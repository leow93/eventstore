package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestDataLog_Append verifies that events are correctly written to the physical file
// and that the returned byte offsets are perfectly accurate.
func TestDataLog_Append(t *testing.T) {
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
	if log.globalPosition != uint64(totalExpectedEvents) {
		t.Fatalf("expected final global position to be %d, got %d", totalExpectedEvents, log.globalPosition)
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
