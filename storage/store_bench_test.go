package storage

import (
	"path/filepath"
	"testing"
)

func BenchmarkDataLog_Append(b *testing.B) {
	evt := &Event{
		StreamName: "benchmark-stream",
		EventType:  "TestFired",
		Payload:    []byte(`{"status": "running", "metric": 99.9}`),
		Meta:       []byte(`{"actor": "system"}`),
	}

	b.Run("WithSync", func(b *testing.B) {
		tempDir := b.TempDir()
		logPath := filepath.Join(tempDir, "sync.log")

		// Enable syncOnWrite
		log, err := NewDataLog(logPath)
		if err != nil {
			b.Fatalf("failed to create log: %v", err)
		}
		defer log.Close()

		b.ResetTimer() // Reset the timer so setup time isn't counted
		for i := 0; i < b.N; i++ {
			if _, err := log.Append(evt); err != nil {
				b.Fatalf("append failed: %v", err)
			}
		}
	})

	b.Run("WithoutSync", func(b *testing.B) {
		tempDir := b.TempDir()
		logPath := filepath.Join(tempDir, "nosync.log")

		// Disable syncOnWrite (Relies on OS Page Cache)
		log, err := NewDataLog(logPath)
		if err != nil {
			b.Fatalf("failed to create log: %v", err)
		}
		log.syncOnWrite = false
		defer log.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := log.Append(evt); err != nil {
				b.Fatalf("append failed: %v", err)
			}
		}
	})
}
