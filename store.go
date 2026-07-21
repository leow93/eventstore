package eventstore

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sync"
)

// ExpectedVersion expresses an optimistic-concurrency expectation for a write.
//
// A value >= 0 requires the stream's current version (its number of events) to
// equal exactly that value; 0 therefore means "the stream must not yet exist".
// AnyVersion disables the check entirely.
type ExpectedVersion int64

// AnyVersion disables the optimistic-concurrency check for a write.
const AnyVersion ExpectedVersion = -1

var ErrNoEvents = errors.New("append: no events provided")

// ErrWrongExpectedVersion is returned when a write's ExpectedVersion does not
// match the stream's current version.
type ErrWrongExpectedVersion struct {
	Stream   string
	Expected ExpectedVersion
	Actual   uint64
}

func (e ErrWrongExpectedVersion) Error() string {
	return fmt.Sprintf("wrong expected version: stream %s is at %d, expected %d", e.Stream, e.Actual, e.Expected)
}

// Store ties the append-only log together with the in-memory index to provide
// optimistic-concurrency writes and ordered stream/category reads.
type Store struct {
	log   *DataLog
	index *Index

	// writeMu serializes writers so the check-then-append critical section is
	// atomic. Reads do not take it (they use the index and mmap directly).
	writeMu sync.Mutex
}

// Open opens (or creates) a store rooted at dir. It rebuilds the in-memory index
// by replaying the log, and reseeds the log's global-position counter.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %w", err)
	}

	dataLog, err := NewDataLog(filepath.Join(dir, "events.log"))
	if err != nil {
		return nil, err
	}

	index := NewIndex()
	if err := index.Rebuild(dataLog); err != nil {
		_ = dataLog.Close()
		return nil, fmt.Errorf("failed to rebuild index: %w", err)
	}
	dataLog.SetGlobalPosition(index.MaxGlobalPosition())

	return &Store{log: dataLog, index: index}, nil
}

func (s *Store) Close() error {
	return s.log.Close()
}

// AppendToStream appends events to a stream under optimistic-concurrency control.
// It sets each event's StreamName and (1-based) Position; the log assigns the
// GlobalPosition. It returns the stream's new version.
func (s *Store) AppendToStream(streamName string, expected ExpectedVersion, events ...*Event) (uint64, error) {
	if len(events) == 0 {
		return 0, ErrNoEvents
	}
	if expected < AnyVersion {
		return 0, fmt.Errorf("invalid expected version %d", expected)
	}
	if _, err := GetCategory(streamName); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current, _ := s.index.StreamVersion(streamName) // 0 if the stream does not exist

	if expected != AnyVersion && uint64(expected) != current {
		return 0, ErrWrongExpectedVersion{Stream: streamName, Expected: expected, Actual: current}
	}

	for i, evt := range events {
		evt.StreamName = streamName
		evt.Position = current + uint64(i) + 1
	}

	// Write the whole batch with a single fsync, then reflect it in the index.
	offsets, err := s.log.AppendBatch(events)
	if err != nil {
		return 0, err
	}
	for i, evt := range events {
		if err := s.index.Apply(evt, offsets[i]); err != nil {
			return 0, err
		}
	}

	return current + uint64(len(events)), nil
}

// ReadStreamForwards reads a stream in ascending position order, starting at the
// inclusive 1-based position from (0 or 1 both mean "from the start"). A limit of
// 0 or less reads to the end of the stream.
func (s *Store) ReadStreamForwards(streamName string, from uint64, limit int) iter.Seq2[*Event, error] {
	offsets := forwardSlice(s.index.StreamOffsets(streamName), from, limit)
	return s.readOffsets(offsets)
}

// ReadStreamBackwards reads a stream in descending position order, starting at the
// inclusive 1-based position from (0 means "from the tip"). A limit of 0 or less
// reads back to the start of the stream.
func (s *Store) ReadStreamBackwards(streamName string, from uint64, limit int) iter.Seq2[*Event, error] {
	offsets := backwardSlice(s.index.StreamOffsets(streamName), from, limit)
	return s.readOffsets(offsets)
}

// ReadCategory reads all events in a category in ascending (append) order,
// starting at the inclusive 1-based category-local position from. A limit of 0 or
// less reads to the end of the category.
func (s *Store) ReadCategory(category string, from uint64, limit int) iter.Seq2[*Event, error] {
	offsets := forwardSlice(s.index.CategoryOffsets(category), from, limit)
	return s.readOffsets(offsets)
}

// readOffsets yields the events at the given log offsets in order. If a read
// fails it yields the error once and stops.
func (s *Store) readOffsets(offsets []int64) iter.Seq2[*Event, error] {
	return func(yield func(*Event, error) bool) {
		for _, off := range offsets {
			evt, err := s.log.ReadAt(off)
			if !yield(evt, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// forwardSlice returns the sub-slice of offsets starting at the inclusive 1-based
// position from, bounded by limit (<=0 means unbounded).
func forwardSlice(offsets []int64, from uint64, limit int) []int64 {
	start := 0
	if from > 1 {
		start = int(from - 1)
	}
	if start >= len(offsets) {
		return nil
	}
	selected := offsets[start:]
	if limit > 0 && limit < len(selected) {
		selected = selected[:limit]
	}
	return selected
}

// backwardSlice returns offsets in descending order starting at the inclusive
// 1-based position from (0 means the tip), bounded by limit (<=0 means unbounded).
func backwardSlice(offsets []int64, from uint64, limit int) []int64 {
	n := len(offsets)
	start := n - 1
	if from > 0 && int(from) <= n {
		start = int(from) - 1
	}

	count := start + 1
	if limit > 0 && limit < count {
		count = limit
	}
	if count <= 0 {
		return nil
	}

	out := make([]int64, count)
	for i := 0; i < count; i++ {
		out[i] = offsets[start-i]
	}
	return out
}
