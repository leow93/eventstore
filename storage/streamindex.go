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

const IndexEntrySize = 24

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

	buf := make([]byte, IndexEntrySize)
	binary.LittleEndian.PutUint64(buf[0:8], h)
	binary.LittleEndian.PutUint64(buf[8:16], pos)
	binary.LittleEndian.PutUint64(buf[16:24], offset)

	if _, err := si.files[shardIdx].Write(buf); err != nil {
		return err
	}

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

			buf := make([]byte, IndexEntrySize)
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
