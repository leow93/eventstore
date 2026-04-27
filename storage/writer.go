package storage

import (
	"fmt"
)

const shardCount = 1024

type Writer struct {
	log           *DataLog
	tracker       *StreamTracker
	streamIdx     *StreamIndex
	categoryIndex *CategoryIndex
}

func NewWriter(tracker *StreamTracker, log *DataLog, streamIndex *StreamIndex, categoryIndex *CategoryIndex) *Writer {
	w := &Writer{
		log:           log,
		tracker:       tracker,
		streamIdx:     streamIndex,
		categoryIndex: categoryIndex,
	}
	return w
}

type ErrStreamAlreadyExists struct {
	stream string
}

func (e ErrStreamAlreadyExists) Error() string {
	return fmt.Sprintf("stream %s already exists", e.stream)
}

type ErrWrongExpectedVersion struct {
	stream   string
	expected uint64
	current  uint64
}

func (e ErrWrongExpectedVersion) Error() string {
	return fmt.Sprintf("wrong expected version: stream %s is at %d, expected %d", e.stream, e.current, e.expected)
}

// FatalError indicates the database is in a split-brain state and must be restarted.
type FatalError struct {
	Err error
}

func (e *FatalError) Error() string {
	return fmt.Sprintf("FATAL INDEX FAILURE: %v", e.Err)
}

func (w *Writer) AppendToStream(evt *Event, expectedVersion uint64) (int64, error) {
	category, err := GetCategory(evt.StreamName)
	if err != nil {
		return 0, err
	}
	h := w.tracker.GetHash(evt.StreamName)
	lock := w.tracker.GetLock(h)
	lock.Lock()
	defer lock.Unlock()

	currentVersion, exists := w.tracker.GetCurrentVersion(h)

	// OCC Logic:
	// If the user expects 0, but the stream exists, it's a conflict.
	if expectedVersion == 0 && exists {
		return 0, ErrStreamAlreadyExists{evt.StreamName}
	}
	// If the user expects a specific version, but it doesn't match the tip.
	if expectedVersion > 0 && currentVersion != expectedVersion {
		return 0, ErrWrongExpectedVersion{
			stream:   evt.StreamName,
			expected: expectedVersion,
			current:  currentVersion,
		}
	}

	// Set the stream-specific position
	evt.Position = currentVersion + 1

	// Append to the physical log (The log has its own internal lock for global ordering)
	offset, err := w.log.Append(evt)
	if err != nil {
		return 0, err
	}

	// Update the in-memory tracker
	w.tracker.UpdateVersion(h, evt.Position)

	// Write to Stream Index
	// For now if this fails, we return a fatal error so that the client can crash the process and restart it.
	// TOCONSIDER: signal that index needs rebuilding and rebuild in the background to allow for better availability.
	err = w.streamIdx.Append(h, evt.Position, uint64(offset))
	if err != nil {
		return 0, &FatalError{err}
	}

	// Write to the category index
	// For now if this fails, we return a fatal error so that the client can crash the process and restart it.
	// TOCONSIDER: signal that index needs rebuilding and rebuild in the background to allow for better availability.
	err = w.categoryIndex.Append(category, uint64(offset))
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// ReadStream fetches a slice of events for a given stream
func (w *Writer) ReadStream(streamName string, fromPos uint64, limit int) ([]*Event, error) {
	h := w.tracker.GetHash(streamName)

	// 1. Get physical locations from the index
	offsets, err := w.streamIdx.GetOffsetsForStream(h, fromPos, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets from index: %w", err)
	}

	// 2. Fetch the physical events from the DataLog
	events := make([]*Event, 0, len(offsets))
	for _, off := range offsets {
		evt, err := w.log.ReadAt(int64(off))
		if err != nil {
			return nil, fmt.Errorf("failed to read event at offset %d: %w", off, err)
		}
		events = append(events, evt)
	}

	return events, nil
}

func (w *Writer) Close() error {
	// Close the index shards first
	idxErr := w.streamIdx.Close()

	// Close the main data log
	logErr := w.log.Close()

	if idxErr != nil {
		return idxErr
	}
	return logErr
}
