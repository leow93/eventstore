package eventstore

import (
	"path/filepath"
	"testing"
)

// BenchmarkDataLog_ReadAt measures random single-record reads via pread over a
// warm log.
func BenchmarkDataLog_ReadAt(b *testing.B) {
	const n = 10_000

	log, err := newFastDataLog(filepath.Join(b.TempDir(), "read.log"))
	if err != nil {
		b.Fatalf("failed to create log: %v", err)
	}
	defer log.Close()

	positions := make([]LogPos, n)
	for i := range positions {
		pos, err := log.Append(&Event{
			StreamName: "benchmark-stream",
			EventType:  "TestFired",
			Payload:    []byte(`{"status": "running", "metric": 99.9}`),
			Meta:       []byte(`{"actor": "system"}`),
		})
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
		positions[i] = pos
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.ReadAt(positions[i%n]); err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
}

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

		// Enable syncOnWrite (the default)
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

		// Disable syncOnWrite (relies on OS page cache)
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

// BenchmarkDataLog_Batch10 compares writing a 10-event batch as ten individual
// appends (ten fsyncs) versus one AppendBatch call (a single fsync).
func BenchmarkDataLog_Batch10(b *testing.B) {
	const batchSize = 10

	makeBatch := func() []*Event {
		batch := make([]*Event, batchSize)
		for i := range batch {
			batch[i] = &Event{
				StreamName: "benchmark-stream",
				EventType:  "TestFired",
				Payload:    []byte(`{"status": "running", "metric": 99.9}`),
				Meta:       []byte(`{"actor": "system"}`),
			}
		}
		return batch
	}

	b.Run("TenIndividualAppends", func(b *testing.B) {
		log, err := NewDataLog(filepath.Join(b.TempDir(), "loop.log"))
		if err != nil {
			b.Fatalf("failed to create log: %v", err)
		}
		defer log.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, evt := range makeBatch() {
				if _, err := log.Append(evt); err != nil {
					b.Fatalf("append failed: %v", err)
				}
			}
		}
	})

	b.Run("OneBatchAppend", func(b *testing.B) {
		log, err := NewDataLog(filepath.Join(b.TempDir(), "batch.log"))
		if err != nil {
			b.Fatalf("failed to create log: %v", err)
		}
		defer log.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := log.AppendBatch(makeBatch()); err != nil {
				b.Fatalf("batch append failed: %v", err)
			}
		}
	})
}
