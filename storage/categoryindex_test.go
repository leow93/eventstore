package storage

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCategory(t *testing.T) {
	tests := []struct {
		streamName string
		expected   string
		err        error
	}{
		{"user-123", "user", nil},
		{"inventory-abc-456", "inventory", nil},
		{"global", "global", nil},
		{"default-", "default", nil},
		{"-badprefix", "", ErrStreamHasNoCategory{"-badprefix"}},
	}

	for _, tc := range tests {
		t.Run(tc.streamName, func(t *testing.T) {
			c, err := GetCategory(tc.streamName)
			require.Equal(t, tc.err, err)
			assert.Equal(t, tc.expected, c)
		})
	}
}

func TestCategoryIndex_AppendAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	idxPath := filepath.Join(tempDir, "categories")

	// 1. Create a new category index
	ci, err := NewCategoryIndex(idxPath)
	require.NoError(t, err)

	// 2. Append entries to different categories
	testData := []struct {
		category string
		offset   uint64
	}{
		{"user", 100},
		{"user", 250},
		{"order", 500},
		{"inventory", 1000},
	}

	for _, entry := range testData {
		err := ci.Append(entry.category, entry.offset)
		assert.NoError(t, err)
	}

	// 3. Close to flush files
	err = ci.Close()
	require.NoError(t, err)

	// 4. Reboot and Load
	rebootCi, err := NewCategoryIndex(idxPath)
	require.NoError(t, err)
	defer rebootCi.Close()

	// Use a map to collect loaded offsets (map of category -> slice of offsets)
	loaded := make(map[string][]uint64)
	var mu sync.Mutex

	err = rebootCi.Load(func(category string, offset uint64) {
		mu.Lock()
		defer mu.Unlock()
		loaded[category] = append(loaded[category], offset)
	})
	require.NoError(t, err)

	// 5. Verify the data
	assert.Len(t, loaded, 3, "Should have loaded exactly 3 unique categories")
	assert.ElementsMatch(t, []uint64{100, 250}, loaded["user"])
	assert.ElementsMatch(t, []uint64{500}, loaded["order"])
	assert.ElementsMatch(t, []uint64{1000}, loaded["inventory"])
}

func TestCategoryIndex_ConcurrentAppends(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent category index test")
	}

	tempDir := t.TempDir()
	idxPath := filepath.Join(tempDir, "concurrent_categories")

	ci, err := NewCategoryIndex(idxPath)
	require.NoError(t, err)
	defer ci.Close()

	const workers = 20
	const entriesPerWorker = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	// Fire off multiple goroutines appending to the SAME and DIFFERENT categories
	for i := range workers {
		go func(workerID int) {
			defer wg.Done()

			// Workers 0-9 write to "user", Workers 10-19 write to "order"
			category := "user"
			if workerID >= 10 {
				category = "order"
			}

			for j := range entriesPerWorker {
				offset := uint64(workerID*1000 + j)
				err := ci.Append(category, offset)
				assert.NoError(t, err) // Inside goroutine, but safe enough for tracking
			}
		}(i)
	}

	wg.Wait()

	// Flush to disk
	require.NoError(t, ci.Close())

	// Reload and verify counts
	rebootCi, err := NewCategoryIndex(idxPath)
	require.NoError(t, err)
	defer rebootCi.Close()

	counts := make(map[string]int)
	var mu sync.Mutex

	err = rebootCi.Load(func(category string, offset uint64) {
		mu.Lock()
		defer mu.Unlock()
		counts[category]++
	})
	require.NoError(t, err)

	// 10 workers * 50 entries = 500 entries per category
	assert.Equal(t, 500, counts["user"], "Expected 500 entries in the 'user' category")
	assert.Equal(t, 500, counts["order"], "Expected 500 entries in the 'order' category")
}
