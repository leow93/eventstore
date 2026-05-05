package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const StreamIndexRecordSize = 24

type StreamIndex struct {
	mu   sync.Mutex
	file *os.File
}

// NewStreamIndex opens or creates the append-only index file.
func NewStreamIndex(path string) (*StreamIndex, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream index: %w", err)
	}

	return &StreamIndex{
		file: file,
	}, nil
}

// AppendBatch hashes the stream names and writes the 24-byte records to disk.
func (s *StreamIndex) AppendBatch(events []*Event, offsets []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Pre-allocate the exact buffer size needed for the entire batch
	buf := make([]byte, len(events)*StreamIndexRecordSize)
	bufOffset := 0

	for i, evt := range events {
		hash := hashStreamName(evt.StreamName)

		binary.LittleEndian.PutUint64(buf[bufOffset:], hash)
		binary.LittleEndian.PutUint64(buf[(bufOffset+8):], evt.Position)
		binary.LittleEndian.PutUint64(buf[(bufOffset+16):], uint64(offsets[i]))

		bufOffset += StreamIndexRecordSize
	}

	// Flush the entire batch to the index file in one syscall
	if _, err := s.file.Write(buf); err != nil {
		return fmt.Errorf("failed to write to stream index: %w", err)
	}

	return nil
}

// Close safely shuts down the file handle.
func (s *StreamIndex) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

func BootStreamIndexer(
	logStore *Log,
	streamIdx *StreamIndex,
	checkpoint Checkpointer,
	wakeSignal <-chan struct{},
) *LogSubscription {
	handler := func(events []*Event, offsets []int64) error {
		return streamIdx.AppendBatch(events, offsets)
	}

	return NewLogSubscription(logStore, checkpoint, handler, wakeSignal)
}
