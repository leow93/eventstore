package eventstore

import (
	"errors"
	"fmt"
	"log"
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

	// streams maps a stream name to its event offsets in stream order.
	// The event at stream position p (1-based) lives at offsets[p-1].
	streams map[string][]int64

	// categories maps a category to its event offsets in global (append) order.
	categories map[string][]int64

	maxGlobalPosition uint64
}

func NewIndex() *Index {
	return &Index{
		streams:    make(map[string][]int64),
		categories: make(map[string][]int64),
	}
}

// Apply records a single event located at the given byte offset in the log.
// It appends the offset to the event's stream and category, and advances the
// high-water global position. It returns an error only if the stream name has
// no extractable category.
func (idx *Index) Apply(evt *Event, offset int64) error {
	category, err := GetCategory(evt.StreamName)
	if err != nil {
		return err
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.streams[evt.StreamName] = append(idx.streams[evt.StreamName], offset)
	idx.categories[category] = append(idx.categories[category], offset)
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

// StreamOffsets returns a copy of the ordered log offsets for a stream.
func (idx *Index) StreamOffsets(stream string) []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return cloneOffsets(idx.streams[stream])
}

// CategoryOffsets returns a copy of the ordered log offsets for a category.
func (idx *Index) CategoryOffsets(category string) []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return cloneOffsets(idx.categories[category])
}

// MaxGlobalPosition returns the highest global position applied to the index.
func (idx *Index) MaxGlobalPosition() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.maxGlobalPosition
}

// Rebuild replays the log from the beginning, applying every complete event to
// the index. A trailing partial record (a torn write left by a crash between
// append and fsync) stops the scan cleanly rather than failing the boot.
//
// A per-record CRC lets Rebuild tell the two apart: a checksum mismatch means the
// bytes of an otherwise-complete record were corrupted, which is unrecoverable and
// fails the boot loudly; any other decode failure is treated as a torn tail and
// stops the scan cleanly at that offset.
func (idx *Index) Rebuild(dataLog *DataLog) error {
	size := dataLog.Size()
	offset := int64(0)

	for offset < size {
		evt, err := dataLog.ReadAt(offset)
		if err != nil {
			if errors.Is(err, ErrChecksumMismatch) {
				return fmt.Errorf("index rebuild: corruption at offset %d of %d: %w", offset, size, err)
			}
			log.Printf("index rebuild: stopping at offset %d of %d (torn tail: %v)", offset, size, err)
			break
		}
		if err := idx.Apply(evt, offset); err != nil {
			return err
		}
		offset += int64(evt.TotalSize())
	}

	return nil
}

func cloneOffsets(src []int64) []int64 {
	out := make([]int64, len(src))
	copy(out, src)
	return out
}
