package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DataLog represents the append-only physical file on disk.
type DataLog struct {
	mu             sync.Mutex // Protects concurrent writes to the file pointer
	file           *os.File
	size           int64 // Tracks current file size to return offsets
	globalPosition atomic.Uint64
	syncOnWrite    bool // Whether to sync to disk on each write. Always on, but an option for benchmarking.
}

func NewDataLog(filepath string) (*DataLog, error) {
	// os.O_APPEND: Force all writes to the end of the file
	// os.O_CREATE: Create the file if it doesn't exist
	// os.O_RDWR: We are reading and writing through this descriptor (mmap for reads)
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR

	file, err := os.OpenFile(filepath, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data log: %w", err)
	}

	// Get current file size so we know where we are appending
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat data log: %w", err)
	}

	return &DataLog{
		mu:          sync.Mutex{},
		file:        file,
		size:        stat.Size(),
		syncOnWrite: true,
	}, nil
}

func (l *DataLog) SetGlobalPosition(gp uint64) {
	l.globalPosition.Store(gp)
}

func (l *DataLog) GetGlobalPosition() uint64 {
	return l.globalPosition.Load()
}

func (l *DataLog) Append(event *Event) (int64, error) {
	if event.Timestamp == 0 {
		event.Timestamp = uint64(time.Now().UnixNano())
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	event.GlobalPosition = l.globalPosition.Add(1)
	data := event.Encode()

	writeOffset := l.size

	n, err := l.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write event to data log: %w", err)
	}

	if l.syncOnWrite {
		if err := l.file.Sync(); err != nil {
			return 0, fmt.Errorf("failed to sync to disk: %w", err)
		}
	}

	l.size += int64(n)

	return writeOffset, nil
}

// AppendBatch writes multiple events in a single locked operation and syncs ONCE.
func (l *DataLog) AppendBatch(events []*Event) ([]int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offsets := make([]int64, len(events))
	var buf bytes.Buffer // Pre-allocate a buffer for the entire batch

	for i, evt := range events {
		// Assign global position atomically as we process the batch
		evt.GlobalPosition = l.globalPosition.Add(1)
		if evt.Timestamp == 0 {
			evt.Timestamp = uint64(time.Now().UnixNano())
		}

		data := evt.Encode()

		// The event's offset is the current file size + whatever is already in the buffer
		offsets[i] = l.size + int64(buf.Len())
		buf.Write(data)
	}

	// 1. One massive write to the OS
	n, err := l.file.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to write batch to data log: %w", err)
	}

	// 2. ONE single sync for the entire batch
	if l.syncOnWrite {
		if err := l.file.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync batch to disk: %w", err)
		}
	}

	l.size += int64(n)

	return offsets, nil
}

const optimisticReadSize = 1024

// ReadAt implements an Optimistic read strategy.
// We pre-allocate 1KB, assuming that this is  enough to read an entire event. If it isn't enough we allocate the exact amount needed from the headers.
func (l *DataLog) ReadAt(offset int64) (*Event, error) {
	buf := make([]byte, optimisticReadSize)
	n, err := l.file.ReadAt(buf, offset)
	// io.EOF is expected if the file ends before our 4KB buffer is full
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed optimistic read at %d: %w", offset, err)
	}

	totalSize := binary.LittleEndian.Uint32(buf[0:4])

	var fullData []byte

	if int(totalSize) <= n {
		// FAST PATH: We have everything. 1 Syscall.
		fullData = buf[:totalSize]
	} else {
		// SLOW PATH: The event is larger than our optimistic buffer.
		// Allocate the exact size and do a second read for the missing bytes.
		fullData = make([]byte, totalSize)
		copy(fullData, buf[:n]) // Keep what we already read

		// Read directly into the remaining slice of fullData
		if _, err := l.file.ReadAt(fullData[n:], offset+int64(n)); err != nil {
			return nil, fmt.Errorf("failed to read large event remainder: %w", err)
		}
	}

	return Decode(fullData)
}

func (l *DataLog) Size() int64 {
	return l.size
}

// Close safely shuts down the file.
func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
