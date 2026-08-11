package eventstore

import (
	"sort"
	"sync"
)

// Index is the in-memory, derived view over the data log. It maps streams and
// categories to the byte offsets of their events in the log, tracks each stream's
// tip (for optimistic concurrency control), and remembers the highest global
// position seen (to reseed the log's counter on boot).
//
// The index owns no durable state: it is rebuilt from scratch by replaying the
// log on boot (see Rebuild). The log is the single source of truth.
type Index struct {
	mu sync.RWMutex

	// streams maps a stream name to its event positions in stream order.
	// The event at stream position p (1-based) lives at positions[p-1].
	streams map[string][]LogPos

	// categories maps a category to its event positions in global (append) order.
	categories map[string][]LogPos

	maxGlobalPosition uint64
}

func NewIndex() *Index {
	return &Index{
		streams:    make(map[string][]LogPos),
		categories: make(map[string][]LogPos),
	}
}

// Apply records a single event located at the given position in the log.
// It appends the position to the event's stream and category, and advances the
// index max global position. It returns an error only if the stream name has
// no extractable category.
func (idx *Index) Apply(evt *Event, pos LogPos) error {
	category, err := GetCategory(evt.StreamName)
	if err != nil {
		return err
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.streams[evt.StreamName] = append(idx.streams[evt.StreamName], pos)
	idx.categories[category] = append(idx.categories[category], pos)
	if evt.GlobalPosition > idx.maxGlobalPosition {
		idx.maxGlobalPosition = evt.GlobalPosition
	}
	return nil
}

// StreamVersion returns the current tip (number of events) of a stream, and
// whether the stream exists at all.
func (idx *Index) StreamVersion(stream string) (uint64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	n := len(idx.streams[stream])
	if n == 0 {
		return 0, false
	}
	return uint64(n), true
}

// StreamOffsets returns a copy of the ordered log positions for a stream.
func (idx *Index) StreamOffsets(stream string) []LogPos {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return cloneOffsets(idx.streams[stream])
}

// CategoryOffsets returns a copy of the ordered log positions for a category.
func (idx *Index) CategoryOffsets(category string) []LogPos {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return cloneOffsets(idx.categories[category])
}

// Streams returns the names of every stream known to the index, sorted. It is
// intended for administrative browsing rather than the hot path.
func (idx *Index) Streams() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	out := make([]string, 0, len(idx.streams))
	for name := range idx.streams {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Categories returns the names of every category known to the index, sorted.
func (idx *Index) Categories() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	out := make([]string, 0, len(idx.categories))
	for name := range idx.categories {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// StreamLen returns the number of events in a stream.
func (idx *Index) StreamLen(stream string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.streams[stream])
}

// CategoryLen returns the number of events in a category.
func (idx *Index) CategoryLen(category string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.categories[category])
}

// MaxGlobalPosition returns the highest global position applied to the index.
func (idx *Index) MaxGlobalPosition() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.maxGlobalPosition
}

// Rebuild reconstructs the index by replaying the whole log, applying every
// complete event. The log's replay enforces recovery policy: a torn trailing
// record in the active segment ends the scan cleanly (a crash between append and
// fsync), whereas corruption or a torn record in a sealed segment fails loudly.
func (idx *Index) Rebuild(l *SegmentedLog) error {
	return l.replay(func(pos LogPos, evt *Event) error {
		return idx.Apply(evt, pos)
	})
}

func cloneOffsets(src []LogPos) []LogPos {
	out := make([]LogPos, len(src))
	copy(out, src)
	return out
}
