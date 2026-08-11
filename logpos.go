package eventstore

// LogPos addresses a single record in the log. The high 32 bits are the segment
// number; the low 32 bits are the byte offset of the record within that segment.
// Packing both into one uint64 keeps the in-memory index at 8 bytes per entry
// (see doc/adr/0001).
//
// A 32-bit in-segment offset caps a single segment at 4 GiB; the segmented log
// keeps segments well below that.
type LogPos uint64

// MaxSegmentSize is one past the largest addressable byte offset within a
// segment. An append whose offset would reach this must roll to a new segment.
const MaxSegmentSize int64 = 1 << 32 // 4 GiB

// MakeLogPos packs a segment number and an in-segment byte offset into a LogPos.
func MakeLogPos(segment, offset uint32) LogPos {
	return LogPos(segment)<<32 | LogPos(offset)
}

// Segment returns the segment number this position lives in.
func (p LogPos) Segment() uint32 { return uint32(p >> 32) }

// Offset returns the byte offset of the record within its segment.
func (p LogPos) Offset() uint32 { return uint32(p) }
