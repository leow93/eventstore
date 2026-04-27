package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// The Stream Index Design
// We store records as 24-byte chunks:
// 8 bytes: Stream Hash (uint64)
// 8 bytes: Stream Position (uint64)
// 8 bytes: Data Log Offset (uint64) — This allows us to find the event on disk later.

const streamIndexEntrySize = 24

type StreamIndex struct {
	basePath string
	files    [shardCount]*os.File
	locks    [shardCount]sync.Mutex // One lock per index shard
}

func NewShardedStreamIndex(dirPath string) (*StreamIndex, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, err
	}

	si := &StreamIndex{basePath: dirPath}

	for i := range shardCount {
		path := filepath.Join(dirPath, fmt.Sprintf("%d.idx", i))
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("failed to open shard index %d: %w", i, err)
		}
		si.files[i] = f
	}

	return si, nil
}

func (si *StreamIndex) Append(h uint64, pos, offset uint64) error {
	shardIdx := h % shardCount

	// Lock only this specific index shard
	si.locks[shardIdx].Lock()
	defer si.locks[shardIdx].Unlock()

	buf := make([]byte, streamIndexEntrySize)
	binary.LittleEndian.PutUint64(buf[0:8], h)
	binary.LittleEndian.PutUint64(buf[8:16], pos)
	binary.LittleEndian.PutUint64(buf[16:24], offset)

	if _, err := si.files[shardIdx].Write(buf); err != nil {
		return err
	}

	// Until we have ability to replay the log to build the index, we just sync to disk.
	return si.files[shardIdx].Sync()
}

// RecoveryHandler is a function type that the index calls for every record found
type RecoveryHandler func(hash uint64, position uint64, offset uint64)

func (si *StreamIndex) Load(handler RecoveryHandler) error {
	var wg sync.WaitGroup
	errChan := make(chan error, shardCount)

	for i := range shardCount {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()

			f := si.files[shardIdx]
			// Seek to start in case the file descriptor was moved
			if _, err := f.Seek(0, 0); err != nil {
				errChan <- err
				return
			}

			buf := make([]byte, streamIndexEntrySize)
			for {
				_, err := f.Read(buf)
				if err != nil {
					break // EOF
				}

				h := binary.LittleEndian.Uint64(buf[0:8])
				pos := binary.LittleEndian.Uint64(buf[8:16])
				offset := binary.LittleEndian.Uint64(buf[16:24])

				// Pass the data back to the tracker/handler
				handler(h, pos, offset)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err
	}
	return nil
}

// GetOffsetsForStream scans the index shard and returns up to 'limit' offsets.
// If limit is 0, it returns all offsets from the given position.
func (si *StreamIndex) GetOffsetsForStream(hash uint64, fromPos uint64, limit int) ([]uint64, error) {
	shardIdx := hash % uint64(shardCount)

	si.locks[shardIdx].Lock()
	defer si.locks[shardIdx].Unlock()

	f := si.files[shardIdx]

	// Reset file pointer to the beginning for the scan
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}

	seen := make(map[uint64]struct{})
	var offsets []uint64
	buf := make([]byte, 24) // 8(Hash) + 8(Position) + 8(Offset)

	for {
		if limit > 0 && len(offsets) >= limit {
			break
		}

		n, err := f.Read(buf)
		if n < 24 {
			break // EOF or partial read at the tail
		}
		if err != nil {
			break
		}

		entryHash := binary.LittleEndian.Uint64(buf[0:8])
		entryPos := binary.LittleEndian.Uint64(buf[8:16])
		entryOff := binary.LittleEndian.Uint64(buf[16:24])

		if entryHash == hash && entryPos >= fromPos {
			if _, ok := seen[entryOff]; ok {
				continue
			}
			seen[entryOff] = struct{}{}
			offsets = append(offsets, entryOff)
		}
	}

	return offsets, nil
}

// Close gracefully shuts down all shard files.
func (si *StreamIndex) Close() error {
	var firstErr error

	for i := range shardCount {
		if si.files[i] != nil {
			// It is often good practice to Sync before Close
			// if your Append doesn't do it aggressively.
			if err := si.files[i].Close(); err != nil {
				// Capture the first error we see to return at the end
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to close shard %d: %w", i, err)
				}
			}
			// Set to nil to prevent double-close issues
			si.files[i] = nil
		}
	}

	return firstErr
}
