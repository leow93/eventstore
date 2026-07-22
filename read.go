package eventstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// readAheadSize is how many bytes a single random read speculatively pulls in
	// one pread. Records at or below this size are read in a single syscall; a
	// larger record costs a second, exactly-sized read. One page is a good default
	// for typical events without over-reading on tiny ones.
	readAheadSize = 4096

	// replayBufferSize is the buffer a sequential replay reads the log in. Large,
	// so a full-log rebuild costs roughly (log size / this) syscalls rather than
	// one per record.
	replayBufferSize = 1 << 20 // 1 MiB
)

// readRecordAt reads and decodes the single record at offset, using src (a
// pread-backed file) and size as the readable upper bound. It first reads a small
// speculative window covering the header and, usually, the whole record; only an
// unusually large record needs a second, exactly-sized read.
func readRecordAt(src io.ReaderAt, offset, size int64) (*Event, error) {
	if offset < 0 || offset >= size {
		return nil, fmt.Errorf("offset %d out of bounds (size %d)", offset, size)
	}
	if offset+4 > size {
		return nil, fmt.Errorf("incomplete record at offset %d", offset)
	}

	window := size - offset
	if window > readAheadSize {
		window = readAheadSize
	}
	buf := make([]byte, window)
	if _, err := src.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("read record at offset %d: %w", offset, err)
	}

	totalLen := int64(binary.LittleEndian.Uint32(buf[:4]))
	if offset+totalLen > size {
		return nil, fmt.Errorf("event at offset %d extends beyond log size", offset)
	}
	if totalLen <= int64(len(buf)) {
		return Decode(buf[:totalLen])
	}

	// Record larger than the speculative window: read exactly its bytes.
	rec := make([]byte, totalLen)
	if _, err := src.ReadAt(rec, offset); err != nil {
		return nil, fmt.Errorf("read record at offset %d: %w", offset, err)
	}
	return Decode(rec)
}

// logReader replays a single segment sequentially, buffering large chunks so a
// full scan (e.g. index rebuild) issues far fewer syscalls than one pread per
// record.
type logReader struct {
	br     *bufio.Reader
	segNum uint32
	offset int64 // byte offset of the next record within the segment
}

// newReader returns a sequential reader over the durable prefix of the segment.
func (s *Segment) newReader() *logReader {
	size := s.size.Load()
	return &logReader{
		br:     bufio.NewReaderSize(io.NewSectionReader(s.file, 0, size), replayBufferSize),
		segNum: s.num,
		offset: 0,
	}
}

// next reads and decodes the next record, returning its position. It returns
// io.EOF at the clean end of the segment, io.ErrUnexpectedEOF for a torn trailing
// record (a crash between append and fsync), and ErrChecksumMismatch for a
// corrupt record.
func (r *logReader) next() (LogPos, *Event, error) {
	pos := MakeLogPos(r.segNum, uint32(r.offset))

	var header [4]byte
	if _, err := io.ReadFull(r.br, header[:]); err != nil {
		// io.EOF => clean end; io.ErrUnexpectedEOF => partial length prefix (torn).
		return 0, nil, err
	}

	totalLen := binary.LittleEndian.Uint32(header[:])
	if totalLen < minRecordSize {
		return 0, nil, io.ErrUnexpectedEOF // implausible length: treat as a torn tail
	}

	rec := make([]byte, totalLen)
	copy(rec, header[:])
	if _, err := io.ReadFull(r.br, rec[4:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // body was cut short by the end of the log
		}
		return 0, nil, err
	}

	evt, err := Decode(rec)
	if err != nil {
		return 0, nil, err
	}
	r.offset += int64(totalLen)
	return pos, evt, nil
}
