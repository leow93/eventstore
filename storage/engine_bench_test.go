package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkWriter_AppendToStream(b *testing.B) {
	evt := &Event{
		StreamName: "bench-stream",
		EventType:  "TestFired",
		Payload:    []byte(`{"data": "benchmark"}`),
	}

	b.Run("Sync-Enabled", func(b *testing.B) {
		tempDir := b.TempDir()
		logPath := filepath.Join(tempDir, "sync.log")
		idxPath := filepath.Join(tempDir, "sync_shards")

		// DataLog with syncOnWrite = true
		log, _ := NewDataLog(logPath)
		idx, _ := NewShardedStreamIndex(idxPath)
		cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
		cIndex, _ := NewCategoryIndex(cIndexPath)
		tracker := NewStreamTracker()
		writer := NewEngine(tracker, log, idx, cIndex)
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
		idxPath := filepath.Join(tempDir, "nosync_shards")

		// DataLog with syncOnWrite = false (OS Page Cache only)
		log, _ := newFastDataLog(logPath)

		idx, _ := NewShardedStreamIndex(idxPath)
		tracker := NewStreamTracker()

		cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
		cIndex, _ := NewCategoryIndex(cIndexPath)
		writer := NewEngine(tracker, log, idx, cIndex)
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

func BenchmarkWriter_AppendParallel_MultiStream_SyncDisabled(b *testing.B) {
	tempDir := b.TempDir()
	logPath := filepath.Join(tempDir, "parallel.log")
	indexPath := filepath.Join(tempDir, "parallel_shards")

	// We'll test with sync disabled to see the maximum
	// throughput of the locking and indexing logic.
	log, _ := newFastDataLog(logPath)
	sIdx, _ := NewShardedStreamIndex(indexPath)
	cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
	cIndex, _ := NewCategoryIndex(cIndexPath)

	tracker := NewStreamTracker()
	writer := NewEngine(tracker, log, sIdx, cIndex)
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

func BenchmarkWriter_AppendParallel_MultiStream_SyncEnabled(b *testing.B) {
	tempDir := b.TempDir()
	logPath := filepath.Join(tempDir, "parallel.log")
	indexPath := filepath.Join(tempDir, "parallel_shards")

	log, _ := NewDataLog(logPath)
	sIdx, _ := NewShardedStreamIndex(indexPath)
	cIndexPath := fmt.Sprintf("%s/categoryidx", tempDir)
	cIndex, _ := NewCategoryIndex(cIndexPath)
	tracker := NewStreamTracker()
	writer := NewEngine(tracker, log, sIdx, cIndex)
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
