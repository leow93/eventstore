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
// Writes go through the file descriptor (append + fsync), serialised by writeMu.
// Reads are served with pread (see read.go): they only read the atomic size and
// never take writeMu, so a read never blocks on an in-flight append or its fsync.
type DataLog struct {
	writeMu        sync.Mutex // Serialises appends. Reads do not take it.
	file           *os.File
	size           atomic.Int64 // Current durable file size; the upper bound for reads.
	globalPosition atomic.Uint64
	syncOnWrite    bool // Whether to fsync on each write. On by default; an option for benchmarking.
}

func NewDataLog(filepath string) (*DataLog, error) {
	// os.O_APPEND: Force all writes to the end of the file
	// os.O_CREATE: Create the file if it doesn't exist
	// os.O_RDWR: reads (pread) and writes (append) share one descriptor
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

	l := &DataLog{
		file:        file,
		syncOnWrite: true,
	}
	l.size.Store(stat.Size())
	return l, nil
}

func (l *DataLog) SetGlobalPosition(gp uint64) {
	l.globalPosition.Store(gp)
}

func (l *DataLog) GetGlobalPosition() uint64 {
	return l.globalPosition.Load()
}

func (l *DataLog) Append(event *Event) (LogPos, error) {
	positions, err := l.AppendBatch([]*Event{event})
	if err != nil {
		return 0, err
	}
	return positions[0], nil
}

// AppendBatch appends a batch of events with a single fsync for the whole batch,
// returning the byte offset of each event. fsync is a barrier: once it returns,
// every write that preceded it is durable, so one sync at the end gives the whole
// batch the same durability guarantee as syncing each event individually — but
// pays the (dominant) fsync cost once instead of len(events) times.
func (l *DataLog) AppendBatch(events []*Event) ([]LogPos, error) {
	if len(events) == 0 {
		return nil, nil
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	positions := make([]LogPos, len(events))
	cursor := l.size.Load()
	buf := make([]byte, 0, len(events)*optimisticEventSize)

	for i, event := range events {
		if event.Timestamp == 0 {
			event.Timestamp = uint64(time.Now().UnixNano())
		}
		event.GlobalPosition = l.globalPosition.Add(1)

		// Single-segment log: every record lives in segment 0. When the log is
		// split into segments (doc/adr/0001), this is where an over-full segment
		// rolls over to the next one.
		if cursor >= MaxSegmentSize {
			return nil, fmt.Errorf("data log: offset %d exceeds max segment size %d", cursor, MaxSegmentSize)
		}
		positions[i] = MakeLogPos(0, uint32(cursor))
		data := event.Encode()
		buf = append(buf, data...)
		cursor += int64(len(data))
	}

	// A nil error from Write guarantees the whole buffer was written, so cursor
	// (start size + every encoded record) is the new file size.
	if _, err := l.file.Write(buf); err != nil {
		return nil, fmt.Errorf("failed to write batch to data log: %w", err)
	}

	if l.syncOnWrite {
		if err := l.file.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync to disk: %w", err)
		}
	}

	// Publish the new size only after the bytes are durable (and readable): a
	// reader that observes the size can safely pread everything below it.
	l.size.Store(cursor)

	return positions, nil
}

// ReadAt reads a single event at the given position with pread. It never takes
// writeMu: it reads the atomic size as an upper bound and reads the record via
// the file descriptor, so reads do not contend with in-flight appends.
func (l *DataLog) ReadAt(pos LogPos) (*Event, error) {
	if pos.Segment() != 0 {
		return nil, fmt.Errorf("data log: segment %d does not exist (single-segment log)", pos.Segment())
	}
	return readRecordAt(l.file, int64(pos.Offset()), l.size.Load())
}

func (l *DataLog) Size() int64 {
	return l.size.Load()
}

// Close shuts down the underlying file.
func (l *DataLog) Close() error {
	return l.file.Close()
}
