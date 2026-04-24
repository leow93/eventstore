package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// MmapReader handles zero-copy reads from the event log.
type MmapReader struct {
	data []byte // The memory-mapped file
	size int64
}

func NewMmapReader(file *os.File) (*MmapReader, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	size := stat.Size()
	if size == 0 {
		return &MmapReader{data: nil, size: 0}, nil
	}

	// Map the file into memory:
	// int(file.Fd()) = The OS file descriptor
	// 0 = Offset to start mapping (start of file)
	// int(size) = How many bytes to map
	// PROT_READ = We only want to read, not modify memory
	// MAP_SHARED = See updates made by other processes
	mmapData, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &MmapReader{
		data: mmapData,
		size: size,
	}, nil
}

// ReadAt reads a single event instantly using the provided byte offset.
func (r *MmapReader) ReadAt(offset int64) (*Event, error) {
	if offset < 0 || offset >= r.size {
		return nil, fmt.Errorf("offset %d out of bounds", offset)
	}

	// Read the first 4 bytes to find out how long this event is
	if offset+4 > r.size {
		return nil, fmt.Errorf("incomplete record at offset %d", offset)
	}
	totalLen := binary.LittleEndian.Uint32(r.data[offset : offset+4])

	// Slice out the exact bytes for this event (zero-copy slice!)
	endOffset := offset + 4 + int64(totalLen)
	if endOffset > r.size {
		return nil, fmt.Errorf("event extends beyond mapped file size")
	}

	eventData := r.data[offset:endOffset]

	// Decode the byte slice back into an Event struct
	return Decode(eventData)
}

// Close unmaps the memory to prevent memory leaks.
func (r *MmapReader) Close() error {
	if r.data != nil {
		return syscall.Munmap(r.data)
	}
	return nil
}
