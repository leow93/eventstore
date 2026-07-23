package eventstore

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// optimisticEventSize is a rough per-event size used only to pre-size the batch
// write buffer; the buffer grows as needed, so the exact value is not important.
const optimisticEventSize = 256

// DefaultSegmentSize is the target size at which the active segment rolls over to
// a new one. It is a soft target: a single batch larger than this still lands in
// one segment (bounded only by MaxSegmentSize).
const DefaultSegmentSize int64 = 256 << 20 // 256 MiB

// legacyLogName is the pre-segmentation single-file log; it is adopted as
// segment 0 on first open (see doc/adr/0012).
const legacyLogName = "events.log"

// SegmentedLog is the append-only event log, split across numbered fixed-size
// segment files. Exactly one segment is active (append + fsync); the rest are
// sealed and immutable. It is the single source of truth (doc/adr/0001, 0012).
//
// Writes are serialised by writeMu and assign each event's GlobalPosition. Reads
// take only a brief RLock to resolve a segment pointer, then pread lock-free, so a
// read never blocks on an in-flight append or its fsync — only on the rare
// roll-over that adds a segment.
type SegmentedLog struct {
	dir         string
	maxSize     int64
	syncOnWrite bool // fsync on each append; on by default, off for benchmarking

	writeMu        sync.Mutex // serialises appends and roll-over
	globalPosition atomic.Uint64

	mu       sync.RWMutex // guards segments and active against roll-over
	segments map[uint32]*Segment
	active   *Segment
}

// NewSegmentedLog opens (or creates) the log rooted at dir, rolling over at
// maxSize (clamped to a sane range; 0 selects the default). A legacy single-file
// log is adopted as segment 0.
func NewSegmentedLog(dir string, maxSize int64) (*SegmentedLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	if maxSize <= 0 || maxSize > MaxSegmentSize {
		maxSize = DefaultSegmentSize
	}

	if err := migrateLegacyLog(dir); err != nil {
		return nil, err
	}

	nums, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}

	l := &SegmentedLog{
		dir:         dir,
		maxSize:     maxSize,
		syncOnWrite: true,
		segments:    make(map[uint32]*Segment),
	}

	if len(nums) == 0 {
		// Fresh log: create segment 0 as the active segment.
		seg, err := openSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments[0] = seg
		l.active = seg
		return l, nil
	}

	for _, n := range nums {
		seg, err := openSegment(dir, n)
		if err != nil {
			_ = l.Close()
			return nil, err
		}
		l.segments[n] = seg
	}
	// The highest-numbered segment is active; every earlier one is sealed.
	for _, n := range nums[:len(nums)-1] {
		l.segments[n].sealed = true
	}
	l.active = l.segments[nums[len(nums)-1]]
	return l, nil
}

// migrateLegacyLog adopts a pre-segmentation events.log as segment 0, but only if
// no segment files exist yet.
func migrateLegacyLog(dir string) error {
	legacy := filepath.Join(dir, legacyLogName)
	if _, err := os.Stat(legacy); err != nil {
		return nil // no legacy file
	}
	nums, err := discoverSegments(dir)
	if err != nil {
		return err
	}
	if len(nums) > 0 {
		return nil // already segmented; leave the legacy file in place
	}
	if err := os.Rename(legacy, filepath.Join(dir, segmentName(0))); err != nil {
		return fmt.Errorf("migrate legacy log: %w", err)
	}
	return nil
}

// discoverSegments returns the numbers of the NNNNNN.seg files in dir, ascending.
func discoverSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read log dir: %w", err)
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentFileSuffix) {
			continue
		}
		var n uint32
		if _, err := fmt.Sscanf(strings.TrimSuffix(e.Name(), segmentFileSuffix), "%d", &n); err != nil {
			continue // ignore anything not named NNNNNN.seg
		}
		nums = append(nums, n)
	}
	slices.Sort(nums)
	return nums, nil
}

func (l *SegmentedLog) SetGlobalPosition(gp uint64) { l.globalPosition.Store(gp) }
func (l *SegmentedLog) GetGlobalPosition() uint64   { return l.globalPosition.Load() }

func (l *SegmentedLog) Append(event *Event) (LogPos, error) {
	positions, err := l.AppendBatch([]*Event{event})
	if err != nil {
		return 0, err
	}
	return positions[0], nil
}

// AppendBatch appends a batch of events with a single fsync, returning each
// event's position. A batch is never split across segments: if it would push the
// active segment past maxSize, the log rolls over to a fresh segment first. fsync
// is a barrier, so one sync per batch gives the whole batch the same durability as
// syncing each event, at the (dominant) fsync cost paid once.
func (l *SegmentedLog) AppendBatch(events []*Event) ([]LogPos, error) {
	if len(events) == 0 {
		return nil, nil
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	// Encode the whole batch, assigning each event its global position and noting
	// where each record starts within the buffer.
	buf := make([]byte, 0, len(events)*optimisticEventSize)
	intra := make([]int64, len(events))
	for i, event := range events {
		if event.Timestamp == 0 {
			event.Timestamp = uint64(time.Now().UnixNano())
		}
		event.GlobalPosition = l.globalPosition.Add(1)
		intra[i] = int64(len(buf))
		buf = append(buf, event.Encode()...)
	}

	if int64(len(buf)) > MaxSegmentSize {
		return nil, fmt.Errorf("append: batch of %d bytes exceeds max segment size %d", len(buf), MaxSegmentSize)
	}

	// Only l.active is mutated by roll-over, and only from here under writeMu, so
	// reading it unlocked is safe.
	active := l.active
	if active.size.Load() > 0 && active.size.Load()+int64(len(buf)) > l.maxSize {
		var err error
		if active, err = l.rollOver(); err != nil {
			return nil, err
		}
	}

	start, err := active.append(buf, l.syncOnWrite)
	if err != nil {
		return nil, err
	}

	positions := make([]LogPos, len(events))
	for i := range events {
		positions[i] = MakeLogPos(active.num, uint32(start+intra[i]))
	}
	return positions, nil
}

// rollOver seals the active segment and opens the next one. Called under writeMu.
func (l *SegmentedLog) rollOver() (*Segment, error) {
	if err := l.active.seal(); err != nil {
		return nil, err
	}
	next, err := openSegment(l.dir, l.active.num+1)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.segments[next.num] = next
	l.active = next
	l.mu.Unlock()
	return next, nil
}

// ReadAt reads a single event at the given position. It resolves the segment
// under a brief RLock, then preads lock-free.
func (l *SegmentedLog) ReadAt(pos LogPos) (*Event, error) {
	l.mu.RLock()
	seg := l.segments[pos.Segment()]
	l.mu.RUnlock()
	if seg == nil {
		return nil, fmt.Errorf("read: unknown segment %d", pos.Segment())
	}
	return seg.readAt(int64(pos.Offset()))
}

// Size returns the total durable bytes across all segments.
func (l *SegmentedLog) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var total int64
	for _, s := range l.segments {
		total += s.size.Load()
	}
	return total
}

// replay calls fn for every record in the log, in global (segment then offset)
// order, decoding each segment sequentially in large buffered chunks. A torn
// trailing record in the active segment ends replay cleanly (a crash between
// append and fsync). A torn or corrupt record anywhere else is an error: sealed
// segments were fsynced whole, so any decode failure in one is real corruption.
func (l *SegmentedLog) replay(fn func(pos LogPos, evt *Event) error) error {
	l.mu.RLock()
	nums := make([]uint32, 0, len(l.segments))
	for n := range l.segments {
		nums = append(nums, n)
	}
	active := l.active
	l.mu.RUnlock()
	slices.Sort(nums)

	for _, n := range nums {
		l.mu.RLock()
		seg := l.segments[n]
		l.mu.RUnlock()

		r := seg.newReader()
		for {
			pos, evt, err := r.next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break // clean end of this segment
				}
				if errors.Is(err, ErrChecksumMismatch) {
					return fmt.Errorf("replay: corruption in segment %d at offset %d: %w", n, pos.Offset(), err)
				}
				// A torn/short record is legitimate only as the active segment's tail.
				if seg == active {
					log.Printf("replay: torn tail in active segment %d at offset %d: %v", n, pos.Offset(), err)
					return nil
				}
				return fmt.Errorf("replay: torn record in sealed segment %d at offset %d (corruption): %w", n, pos.Offset(), err)
			}
			if err := fn(pos, evt); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close closes every segment file.
func (l *SegmentedLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, s := range l.segments {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
