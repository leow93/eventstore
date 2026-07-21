package eventstore

import (
	"path/filepath"
	"testing"
)

func TestMmapReader_ReadAt(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "mmap_test.log")

	log, err := NewDataLog(logPath)
	if err != nil {
		t.Fatalf("failed to create data log: %v", err)
	}
	defer log.Close()

	// Write an event
	originalEvt := &Event{
		StreamName: "user-1",
		EventType:  "UserCreated",
		Payload:    []byte(`{"name": "Alice"}`),
		Meta:       []byte(`{"ip": "127.0.0.1"}`),
	}

	offset, err := log.Append(originalEvt)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Append with fsync guarantees the OS sees the file size before we mmap it.
	// Create a fresh MmapReader over the same file descriptor.
	reader, err := NewMmapReader(log.file)
	if err != nil {
		t.Fatalf("failed to create mmap reader: %v", err)
	}
	defer reader.Close()

	// Read the event back using the offset provided by Append
	decodedEvt, err := reader.ReadAt(offset)
	if err != nil {
		t.Fatalf("failed to read event at offset %d: %v", offset, err)
	}

	// Verify the data survived the round trip
	if decodedEvt.StreamName != originalEvt.StreamName {
		t.Errorf("expected stream %s, got %s", originalEvt.StreamName, decodedEvt.StreamName)
	}
	if string(decodedEvt.Payload) != string(originalEvt.Payload) {
		t.Errorf("expected payload %s, got %s", originalEvt.Payload, decodedEvt.Payload)
	}
	if decodedEvt.GlobalPosition != originalEvt.GlobalPosition {
		t.Errorf("expected global position %d, got %d", originalEvt.GlobalPosition, decodedEvt.GlobalPosition)
	}
}

// TestMmapReader_Remap verifies that a reader refreshes its view to cover bytes
// appended after the mapping was first established.
func TestMmapReader_Remap(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "remap_test.log")

	log, err := NewDataLog(logPath)
	if err != nil {
		t.Fatalf("failed to create data log: %v", err)
	}
	defer log.Close()

	first := &Event{StreamName: "s-1", EventType: "First", Payload: []byte("one")}
	firstOffset, err := log.Append(first)
	if err != nil {
		t.Fatalf("failed to append first: %v", err)
	}

	reader, err := NewMmapReader(log.file)
	if err != nil {
		t.Fatalf("failed to create mmap reader: %v", err)
	}
	defer reader.Close()

	// Append a second event after the reader was created; it is not yet visible.
	second := &Event{StreamName: "s-1", EventType: "Second", Payload: []byte("two")}
	secondOffset, err := log.Append(second)
	if err != nil {
		t.Fatalf("failed to append second: %v", err)
	}

	if _, err := reader.ReadAt(secondOffset); err == nil {
		t.Fatal("expected read of unmapped tail to fail before remap")
	}

	if err := reader.Remap(); err != nil {
		t.Fatalf("failed to remap: %v", err)
	}

	// Both events are now readable.
	firstEvt, err := reader.ReadAt(firstOffset)
	if err != nil {
		t.Fatalf("failed to read first event after remap: %v", err)
	}
	if firstEvt.EventType != "First" {
		t.Errorf("expected event type First, got %s", firstEvt.EventType)
	}

	secondEvt, err := reader.ReadAt(secondOffset)
	if err != nil {
		t.Fatalf("failed to read second event after remap: %v", err)
	}
	if secondEvt.EventType != "Second" {
		t.Errorf("expected event type Second, got %s", secondEvt.EventType)
	}
}
