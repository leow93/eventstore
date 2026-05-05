package storage

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIndex_AppendBatch(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "stream_idx.dat")

	// 1. Initialize the index
	idx, err := NewStreamIndex(indexPath)
	require.NoError(t, err)
	defer idx.Close()

	// 2. Prepare a test batch
	events := []*Event{
		{StreamName: "user-123", Position: 1},
		{StreamName: "user-123", Position: 2},
		{StreamName: "order-999", Position: 1},
	}
	offsets := []int64{1048, 2096, 3144}

	// 3. Write to the index
	err = idx.AppendBatch(events, offsets)
	require.NoError(t, err)

	// 4. Verify file size (3 events * 24 bytes = 72 bytes)
	info, err := os.Stat(indexPath)
	require.NoError(t, err)
	assert.Equal(t, int64(72), info.Size())

	// 5. Open the raw physical file to verify exact binary layout
	file, err := os.Open(indexPath)
	require.NoError(t, err)
	defer file.Close()

	buf := make([]byte, StreamIndexRecordSize)

	// --- Verify Record 1 ---
	_, err = io.ReadFull(file, buf)
	require.NoError(t, err)

	expectedHash1 := xxhash.Sum64String("user-123")
	assert.Equal(t, expectedHash1, binary.LittleEndian.Uint64(buf[0:8]), "Stream Hash mismatch")
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(buf[8:16]), "Position mismatch")
	assert.Equal(t, uint64(1048), binary.LittleEndian.Uint64(buf[16:24]), "Offset mismatch")

	// --- Verify Record 2 ---
	_, err = io.ReadFull(file, buf)
	require.NoError(t, err)

	assert.Equal(t, expectedHash1, binary.LittleEndian.Uint64(buf[0:8])) // Same stream, same hash
	assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(buf[8:16]))
	assert.Equal(t, uint64(2096), binary.LittleEndian.Uint64(buf[16:24]))

	// --- Verify Record 3 ---
	_, err = io.ReadFull(file, buf)
	require.NoError(t, err)

	expectedHash2 := xxhash.Sum64String("order-999")
	assert.Equal(t, expectedHash2, binary.LittleEndian.Uint64(buf[0:8]))
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(buf[8:16]))
	assert.Equal(t, uint64(3144), binary.LittleEndian.Uint64(buf[16:24]))

	// --- Verify EOF ---
	_, err = io.ReadFull(file, buf)
	assert.ErrorIs(t, err, io.EOF, "Expected end of file but found more data")
}

func TestStreamIndex_AppendEmptyBatch(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "stream_idx_empty.dat")

	idx, err := NewStreamIndex(indexPath)
	require.NoError(t, err)
	defer idx.Close()

	// Writing an empty batch should not panic or corrupt the file
	err = idx.AppendBatch([]*Event{}, []int64{})
	require.NoError(t, err)

	info, err := os.Stat(indexPath)
	require.NoError(t, err)

	// File should remain exactly 0 bytes
	assert.Equal(t, int64(0), info.Size())
}

func TestStreamIndex_MultipleBatches(t *testing.T) {
	tempDir := t.TempDir()
	indexPath := filepath.Join(tempDir, "stream_idx_multi.dat")

	idx, err := NewStreamIndex(indexPath)
	require.NoError(t, err)
	defer idx.Close()

	// Batch 1
	err = idx.AppendBatch(
		[]*Event{{StreamName: "s1", Position: 1}},
		[]int64{100},
	)
	require.NoError(t, err)

	// Batch 2
	err = idx.AppendBatch(
		[]*Event{{StreamName: "s2", Position: 1}, {StreamName: "s2", Position: 2}},
		[]int64{200, 300},
	)
	require.NoError(t, err)

	// Total 3 records = 72 bytes
	info, err := os.Stat(indexPath)
	require.NoError(t, err)
	assert.Equal(t, int64(72), info.Size())
}
