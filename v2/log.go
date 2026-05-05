package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Log represents the append-only physical file on disk.
type Log struct {
	mu             sync.Mutex // Protects concurrent writes to the file pointer
	file           *os.File
	size           int64 // Tracks current file size to return offsets
	globalPosition atomic.Uint64
	syncOnWrite    bool // Whether to sync to disk on each write. Always on, but an option for benchmarking.
}

func newFastLog(p string) (*Log, error) {
	l, err := NewLog(p)
	if err != nil {
		return nil, err
	}

	l.syncOnWrite = false
	return l, nil
}

func NewLog(filepath string) (*Log, error) {
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

	return &Log{
		mu:          sync.Mutex{},
		file:        file,
		size:        stat.Size(),
		syncOnWrite: true,
	}, nil
}

func (l *Log) SetGlobalPosition(gp uint64) {
	l.globalPosition.Store(gp)
}

func (l *Log) GetGlobalPosition() uint64 {
	return l.globalPosition.Load()
}

func (l *Log) Append(event *Event) (int64, error) {
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

const optimisticReadSize = 1024

// ReadAt implements an Optimistic read strategy.
// We pre-allocate 1KB, assuming that this is  enough to read an entire event. If it isn't enough we allocate the exact amount needed from the headers.
func (l *Log) ReadAt(offset int64) (*Event, error) {
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

// ReadBatchAt reads up to maxEvents sequentially, starting from the given file offset.
// It returns the decoded events, their exact file offsets, the new file offset for the next read, and any error.
func (l *Log) ReadBatchAt(startOffset int64, maxEvents int) ([]*Event, []int64, int64, error) {
	// We use ReadAt to avoid changing the file's global read/write pointer.
	// In a high-performance system, you might mmap the file here instead.

	events := make([]*Event, 0, maxEvents)
	offsets := make([]int64, 0, maxEvents)
	currentOffset := startOffset

	sizeBuf := make([]byte, 4)

	for i := 0; i < maxEvents; i++ {
		// 1. Read the 4-byte size header
		_, err := l.file.ReadAt(sizeBuf, currentOffset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // We've reached the end of the currently written log
			}
			return events, offsets, currentOffset, err
		}

		size := binary.LittleEndian.Uint32(sizeBuf)

		// 2. Read the full event based on the size
		dataBuf := make([]byte, size)
		_, err = l.file.ReadAt(dataBuf, currentOffset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// This shouldn't happen unless the file was truncated mid-write,
				// but we break safely.
				break
			}
			return events, offsets, currentOffset, err
		}

		// 3. Decode
		evt, err := Decode(dataBuf)
		if err != nil {
			return events, offsets, currentOffset, err
		}

		events = append(events, evt)
		offsets = append(offsets, currentOffset)

		// Advance the offset for the next loop iteration
		currentOffset += int64(size)
	}

	return events, offsets, currentOffset, nil
}

func (l *Log) Size() int64 {
	return l.size
}

// Close safely shuts down the file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
