package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkEngine_AppendToStream(b *testing.B) {
	evt := &Event{
		StreamName: "bench-stream",
		EventType:  "TestFired",
		Payload:    []byte(`{"data": "benchmark"}`),
	}

	b.Run("Sync-Enabled", func(b *testing.B) {
		tempDir := b.TempDir()
		logPath := filepath.Join(tempDir, "sync.log")

		// DataLog with syncOnWrite = true
		log, _ := NewLog(logPath)
		tracker := NewStreamTracker()
		writer, err := NewEngine(tracker, log)
		require.NoError(b, err)
		defer writer.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// We increment the expected version to simulate a real stream growth
			_, err := writer.AppendToStream(evt, uint64(i))
			if err != nil {
				b.Fatalf("append failed: %v", err)
			}
		}
	})

	b.Run("Sync-Disabled", func(b *testing.B) {
		tempDir := b.TempDir()
		logPath := filepath.Join(tempDir, "nosync.log")

		// DataLog with syncOnWrite = false (OS Page Cache only)
		log, _ := newFastLog(logPath)

		tracker := NewStreamTracker()
		writer, err := NewEngine(tracker, log)
		require.NoError(b, err)
		defer writer.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := writer.AppendToStream(evt, uint64(i))
			if err != nil {
				b.Fatalf("append failed: %v", err)
			}
		}
	})
}

func BenchmarkEngine_AppendParallel_MultiStream_SyncDisabled(b *testing.B) {
	tempDir := b.TempDir()
	logPath := filepath.Join(tempDir, "parallel.log")

	// We'll test with sync disabled to see the maximum
	// throughput of the locking and indexing logic.
	log, err := newFastLog(logPath)
	require.NoError(b, err)

	tracker := NewStreamTracker()
	writer, err := NewEngine(tracker, log)
	require.NoError(b, err)
	defer writer.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own unique-ish stream ID to minimize collisions
		// We use a local counter to vary the stream name
		i := 0
		for pb.Next() {
			streamName := fmt.Sprintf("parallel-stream-%d", i%shardCount)
			evt := &Event{
				StreamName: streamName,
				EventType:  "ParallelWrite",
				Payload:    []byte(`{"val": 1}`),
			}

			// We use 0 as expected version here just for bench simplicity,
			// though in a real scenario you'd track the version.
			// To keep it simple, we ignore the error since the first
			// write per stream per goroutine will succeed.
			writer.AppendToStream(evt, 0)
			i++
		}
	})
}

func BenchmarkEngine_AppendParallel_MultiStream_SyncEnabled(b *testing.B) {
	tempDir := b.TempDir()
	logPath := filepath.Join(tempDir, "parallel.log")

	log, err := NewLog(logPath)
	require.NoError(b, err)
	tracker := NewStreamTracker()
	writer, err := NewEngine(tracker, log)
	require.NoError(b, err)
	defer writer.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own unique-ish stream ID to minimize collisions
		// We use a local counter to vary the stream name
		i := 0
		for pb.Next() {
			streamName := fmt.Sprintf("parallel-stream-%d", i%shardCount)
			evt := &Event{
				StreamName: streamName,
				EventType:  "ParallelWrite",
				Payload:    []byte(`{"val": 1}`),
			}

			// We use 0 as expected version here just for bench simplicity,
			// though in a real scenario you'd track the version.
			// To keep it simple, we ignore the error since the first
			// write per stream per goroutine will succeed.
			writer.AppendToStream(evt, 0)
			i++
		}
	})
}
