package storage

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIndex_AppendAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	idxPath := filepath.Join(tempDir, "shards")

	// 1. Create a new sharded index
	idx, err := NewShardedStreamIndex(idxPath)
	require.NoError(t, err)

	// 2. Append entries that will land in different shards
	// We'll use specific hashes to ensure we touch multiple files
	testEntries := []struct {
		hash   uint64
		pos    uint64
		offset uint64
	}{
		{hash: 1, pos: 1, offset: 100},     // Shard 1
		{hash: 1025, pos: 2, offset: 200},  // Shard 1 (Collision by design)
		{hash: 50, pos: 1, offset: 300},    // Shard 50
		{hash: 9999, pos: 5, offset: 1000}, // Shard 9999 % 1024
	}

	for _, entry := range testEntries {
		err := idx.Append(entry.hash, entry.pos, entry.offset)
		assert.NoError(t, err)
	}

	// 3. Close the index to flush everything to disk
	err = idx.Close()
	require.NoError(t, err)

	// 4. Re-open a new instance to simulate a reboot
	rebootIdx, err := NewShardedStreamIndex(idxPath)
	require.NoError(t, err)
	defer rebootIdx.Close()

	// 5. Use the Load method with a collector
	collected := make(map[uint64]uint64)
	var mu sync.Mutex

	err = rebootIdx.Load(func(hash, pos, offset uint64) {
		mu.Lock()
		defer mu.Unlock()
		collected[hash] = pos
	})
	require.NoError(t, err)

	// 6. Verify all entries were recovered correctly
	assert.Equal(t, len(testEntries), len(collected), "Should have recovered all unique hashes")
	for _, entry := range testEntries {
		assert.Equal(t, entry.pos, collected[entry.hash])
	}
}

func TestStreamIndex_ConcurrentAppends(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent index test")
	}

	tempDir := t.TempDir()
	idxPath := filepath.Join(tempDir, "concurrent_shards")

	idx, err := NewShardedStreamIndex(idxPath)
	require.NoError(t, err)
	defer idx.Close()

	const workers = 50
	const entriesPerWorker = 100
	var wg sync.WaitGroup
	wg.Add(workers)

	// Fire off multiple goroutines appending to the SAME and DIFFERENT shards
	for i := range workers {
		go func(workerID int) {
			defer wg.Done()
			for j := range entriesPerWorker {
				// Mix of unique hashes and shared hashes to test locking
				hash := uint64(workerID*10 + (j % 10))
				err := idx.Append(hash, uint64(j), uint64(j*10))
				if err != nil {
					t.Errorf("Append failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// If the test reaches here without a panic or race detector hit,
	// the internal shard mutexes are doing their job.
}
