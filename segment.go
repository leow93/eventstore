package eventstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// segmentFileSuffix is the extension of a segment file. A segment is named by its
// zero-padded number, e.g. 000000.seg.
const segmentFileSuffix = ".seg"

func segmentName(num uint32) string {
	return fmt.Sprintf("%06d%s", num, segmentFileSuffix)
}

// Segment is a single append-only file — one slice of the log. Writes append
// bytes (the SegmentedLog serialises them and assigns positions); reads are
// lock-free preads. Exactly one segment in a log is active (writable); the rest
// are sealed and immutable.
type Segment struct {
	num    uint32
	path   string
	file   *os.File
	size   atomic.Int64 // durable bytes; the upper bound for reads
	sealed bool         // set under the log's writeMu; a sealed segment never grows
}

// openSegment opens (creating if absent) segment number num in dir.
func openSegment(dir string, num uint32) (*Segment, error) {
	path := filepath.Join(dir, segmentName(num))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", num, err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat segment %d: %w", num, err)
	}
	s := &Segment{num: num, path: path, file: file}
	s.size.Store(stat.Size())
	return s, nil
}

// append writes buf at the end of the segment, optionally fsyncing, and returns
// the byte offset the write started at. The new size is published only after the
// bytes are durable, so a reader that observes the size can safely pread them.
func (s *Segment) append(buf []byte, sync bool) (int64, error) {
	start := s.size.Load()
	if _, err := s.file.Write(buf); err != nil {
		return 0, fmt.Errorf("write segment %d: %w", s.num, err)
	}
	if sync {
		if err := s.file.Sync(); err != nil {
			return 0, fmt.Errorf("sync segment %d: %w", s.num, err)
		}
	}
	s.size.Store(start + int64(len(buf)))
	return start, nil
}

// readAt reads the record at byte offset off within this segment.
func (s *Segment) readAt(off int64) (*Event, error) {
	return readRecordAt(s.file, off, s.size.Load())
}

// seal fsyncs the segment and marks it immutable. Called under the log's writeMu
// when rolling over to a new active segment.
func (s *Segment) seal() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("seal segment %d: %w", s.num, err)
	}
	s.sealed = true
	return nil
}

func (s *Segment) Close() error {
	return s.file.Close()
}
