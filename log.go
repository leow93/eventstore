package eventstore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// optimisticEventSize is a rough per-event size used only to pre-size the batch
// write buffer; the buffer grows as needed, so the exact value is not important.
const optimisticEventSize = 256

// DataLog represents the append-only physical file on disk.
//
// Writes go through the file descriptor (append + fsync). Reads are served from
// a memory-mapped view of the same file, refreshed lazily when a read needs bytes
// that were appended after the current mapping was established.
type DataLog struct {
	mu             sync.RWMutex // Guards appends and mmap remapping; readers hold RLock while decoding.
	file           *os.File
	size           int64 // Tracks current file size to return offsets.
	globalPosition atomic.Uint64
	syncOnWrite    bool // Whether to fsync on each write. On by default; an option for benchmarking.
	reader         *MmapReader
}

func NewDataLog(filepath string) (*DataLog, error) {
	// os.O_APPEND: Force all writes to the end of the file
	// os.O_CREATE: Create the file if it doesn't exist
	// os.O_RDWR: We need read access for the mmap-backed reader
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR

	file, err := os.OpenFile(filepath, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data log: %w", err)
	}

	// Get current file size so we know where we are appending.
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat data log: %w", err)
	}

	reader, err := NewMmapReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create mmap reader: %w", err)
	}

	return &DataLog{
		file:        file,
		size:        stat.Size(),
		syncOnWrite: true,
		reader:      reader,
	}, nil
}

func (l *DataLog) SetGlobalPosition(gp uint64) {
	l.globalPosition.Store(gp)
}

func (l *DataLog) GetGlobalPosition() uint64 {
	return l.globalPosition.Load()
}

func (l *DataLog) Append(event *Event) (int64, error) {
	offsets, err := l.AppendBatch([]*Event{event})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

// AppendBatch appends a batch of events with a single fsync for the whole batch,
// returning the byte offset of each event. fsync is a barrier: once it returns,
// every write that preceded it is durable, so one sync at the end gives the whole
// batch the same durability guarantee as syncing each event individually — but
// pays the (dominant) fsync cost once instead of len(events) times.
func (l *DataLog) AppendBatch(events []*Event) ([]int64, error) {
	if len(events) == 0 {
		return nil, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	offsets := make([]int64, len(events))
	cursor := l.size
	buf := make([]byte, 0, len(events)*optimisticEventSize)

	for i, event := range events {
		if event.Timestamp == 0 {
			event.Timestamp = uint64(time.Now().UnixNano())
		}
		event.GlobalPosition = l.globalPosition.Add(1)

		offsets[i] = cursor
		data := event.Encode()
		buf = append(buf, data...)
		cursor += int64(len(data))
	}

	n, err := l.file.Write(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to write batch to data log: %w", err)
	}

	if l.syncOnWrite {
		if err := l.file.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync to disk: %w", err)
		}
	}

	l.size += int64(n)

	return offsets, nil
}

// ReadAt reads a single event at the given byte offset via the memory-mapped view.
// If the offset falls beyond the current mapping (because the log has grown since
// the last read), the mapping is refreshed first.
func (l *DataLog) ReadAt(offset int64) (*Event, error) {
	l.mu.RLock()
	stale := offset >= l.reader.size
	l.mu.RUnlock()

	if stale {
		l.mu.Lock()
		err := l.reader.Remap()
		l.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to refresh mmap: %w", err)
		}
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.reader.ReadAt(offset)
}

func (l *DataLog) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

// Close safely shuts down the file and releases the memory mapping.
func (l *DataLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reader != nil {
		_ = l.reader.Close()
	}
	return l.file.Close()
}
