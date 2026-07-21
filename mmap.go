package eventstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// MmapReader handles zero-copy reads from the event log.
//
// The event log is append-only, so the mapping only ever needs to grow. Callers
// refresh the mapping to cover newly appended bytes with Remap.
type MmapReader struct {
	file *os.File
	data []byte // The memory-mapped file
	size int64
}

func NewMmapReader(file *os.File) (*MmapReader, error) {
	r := &MmapReader{file: file}
	if err := r.Remap(); err != nil {
		return nil, err
	}
	return r, nil
}

// Remap refreshes the mapping so it covers the current on-disk size of the file.
// It is a no-op if the file has not grown since the last mapping.
func (r *MmapReader) Remap() error {
	stat, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	size := stat.Size()
	if size == r.size {
		return nil
	}

	// Drop the old mapping before establishing a new one.
	if r.data != nil {
		if err := syscall.Munmap(r.data); err != nil {
			return fmt.Errorf("failed to munmap file: %w", err)
		}
		r.data = nil
	}

	// An empty file cannot be mapped; leave the reader with a nil mapping.
	if size == 0 {
		r.size = 0
		return nil
	}

	// Map the file into memory:
	// int(file.Fd()) = The OS file descriptor
	// 0 = Offset to start mapping (start of file)
	// int(size) = How many bytes to map
	// PROT_READ = We only want to read, not modify memory
	// MAP_SHARED = See updates made by other processes
	data, err := syscall.Mmap(int(r.file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %w", err)
	}

	r.data = data
	r.size = size
	return nil
}

// ReadAt reads a single event using the provided byte offset.
func (r *MmapReader) ReadAt(offset int64) (*Event, error) {
	if offset < 0 || offset >= r.size {
		return nil, fmt.Errorf("offset %d out of bounds", offset)
	}

	// Read the first 4 bytes to find out how long this event is.
	if offset+4 > r.size {
		return nil, fmt.Errorf("incomplete record at offset %d", offset)
	}
	totalLen := binary.LittleEndian.Uint32(r.data[offset : offset+4])

	endOffset := offset + int64(totalLen)
	if endOffset > r.size {
		return nil, fmt.Errorf("event extends beyond mapped file size")
	}

	// Decode copies the bytes out, so the returned Event does not alias the mapping.
	return Decode(r.data[offset:endOffset])
}

// Close unmaps the memory to prevent memory leaks.
func (r *MmapReader) Close() error {
	if r.data != nil {
		err := syscall.Munmap(r.data)
		r.data = nil
		return err
	}
	return nil
}
