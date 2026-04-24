package storage

import (
	"fmt"
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
	globalPosition uint64
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

func (l *DataLog) Append(event *Event) (int64, error) {
	if event.Timestamp == 0 {
		event.Timestamp = uint64(time.Now().UnixNano())
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	event.GlobalPosition = atomic.AddUint64(&l.globalPosition, 1)
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

// Close safely shuts down the file.
func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
