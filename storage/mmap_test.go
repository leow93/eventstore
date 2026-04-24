package storage

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

	// We MUST close or flush the writer to guarantee the OS sees the file size
	// before we mmap it, though Sync() handles the flush for us here.

	// Create the MmapReader using the same file descriptor
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
