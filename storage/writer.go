package storage

import (
	"fmt"
)

const shardCount = 1024

type Writer struct {
	log       *DataLog
	tracker   *StreamTracker
	streamIdx *StreamIndex
}

func NewWriter(tracker *StreamTracker, log *DataLog, streamIndex *StreamIndex) *Writer {
	return &Writer{
		log:       log,
		tracker:   tracker,
		streamIdx: streamIndex,
	}
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

func (w *Writer) AppendToStream(evt *Event, expectedVersion uint64) (int64, error) {
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

	// Write to Stream Index
	// If this fails, we are in a tricky spot, but for now we return error.
	// TODO: implement index repair when this happens.
	err = w.streamIdx.Append(h, evt.Position, uint64(offset))
	if err != nil {
		return 0, err
	}

	// Update the in-memory tracker
	w.tracker.UpdateVersion(h, evt.Position)

	return offset, nil
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
