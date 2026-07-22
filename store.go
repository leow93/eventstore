package eventstore

import (
	"errors"
	"fmt"
	"iter"
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
	log   *SegmentedLog
	index *Index

	// writeMu serializes writers so the check-then-append critical section is
	// atomic. Reads do not take it (they use the index and pread directly).
	writeMu sync.Mutex
}

// Open opens (or creates) a store rooted at dir. It rebuilds the in-memory index
// by replaying the log, and reseeds the log's global-position counter.
func Open(dir string) (*Store, error) {
	segLog, err := NewSegmentedLog(dir, DefaultSegmentSize)
	if err != nil {
		return nil, err
	}

	index := NewIndex()
	if err := index.Rebuild(segLog); err != nil {
		_ = segLog.Close()
		return nil, fmt.Errorf("failed to rebuild index: %w", err)
	}
	segLog.SetGlobalPosition(index.MaxGlobalPosition())

	return &Store{log: segLog, index: index}, nil
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
	positions, err := s.log.AppendBatch(events)
	if err != nil {
		return 0, err
	}
	for i, evt := range events {
		if err := s.index.Apply(evt, positions[i]); err != nil {
			return 0, err
		}
	}

	return current + uint64(len(events)), nil
}

// Streams returns the names of every stream in the store, sorted. It is intended
// for administrative browsing (e.g. the web console), not the hot read path.
func (s *Store) Streams() []string { return s.index.Streams() }

// Categories returns the names of every category in the store, sorted.
func (s *Store) Categories() []string { return s.index.Categories() }

// StreamLen returns the number of events in a stream.
func (s *Store) StreamLen(stream string) int { return s.index.StreamLen(stream) }

// CategoryLen returns the number of events in a category.
func (s *Store) CategoryLen(category string) int { return s.index.CategoryLen(category) }

// ReadStreamForwards reads a stream in ascending position order, starting at the
// inclusive 1-based position from (0 or 1 both mean "from the start"). A limit of
// 0 or less reads to the end of the stream.
func (s *Store) ReadStreamForwards(streamName string, from uint64, limit int) iter.Seq2[*Event, error] {
	positions := forwardSlice(s.index.StreamOffsets(streamName), from, limit)
	return s.readOffsets(positions)
}

// ReadStreamBackwards reads a stream in descending position order, starting at the
// inclusive 1-based position from (0 means "from the tip"). A limit of 0 or less
// reads back to the start of the stream.
func (s *Store) ReadStreamBackwards(streamName string, from uint64, limit int) iter.Seq2[*Event, error] {
	positions := backwardSlice(s.index.StreamOffsets(streamName), from, limit)
	return s.readOffsets(positions)
}

// ReadCategory reads all events in a category in ascending (append) order,
// starting at the inclusive 1-based category-local position from. A limit of 0 or
// less reads to the end of the category.
func (s *Store) ReadCategory(category string, from uint64, limit int) iter.Seq2[*Event, error] {
	positions := forwardSlice(s.index.CategoryOffsets(category), from, limit)
	return s.readOffsets(positions)
}

// readOffsets yields the events at the given log positions in order. If a read
// fails it yields the error once and stops.
func (s *Store) readOffsets(positions []LogPos) iter.Seq2[*Event, error] {
	return func(yield func(*Event, error) bool) {
		for _, pos := range positions {
			evt, err := s.log.ReadAt(pos)
			if !yield(evt, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// forwardSlice returns the sub-slice of positions starting at the inclusive
// 1-based position from, bounded by limit (<=0 means unbounded).
func forwardSlice(positions []LogPos, from uint64, limit int) []LogPos {
	start := 0
	if from > 1 {
		start = int(from - 1)
	}
	if start >= len(positions) {
		return nil
	}
	selected := positions[start:]
	if limit > 0 && limit < len(selected) {
		selected = selected[:limit]
	}
	return selected
}

// backwardSlice returns positions in descending order starting at the inclusive
// 1-based position from (0 means the tip), bounded by limit (<=0 means unbounded).
func backwardSlice(positions []LogPos, from uint64, limit int) []LogPos {
	n := len(positions)
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

	out := make([]LogPos, count)
	for i := 0; i < count; i++ {
		out[i] = positions[start-i]
	}
	return out
}
