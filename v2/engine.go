package storage

import (
	"errors"
	"fmt"
)

const shardCount = 1024

type Engine struct {
	log     *Log
	tracker *StreamTracker
}

func NewEngine(tracker *StreamTracker, log *Log) (*Engine, error) {
	e := &Engine{
		log:     log,
		tracker: tracker,
	}

	return e, nil
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

func (e *Engine) AppendToStream(evt *Event, expectedVersion uint64) (int64, error) {
	// Category format check
	_, err := GetCategory(evt.StreamName)
	if err != nil {
		return 0, err
	}
	h := e.tracker.GetHash(evt.StreamName)
	lock := e.tracker.GetLock(h)
	lock.Lock()
	defer lock.Unlock()

	currentVersion, exists := e.tracker.GetCurrentVersion(h)

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
	offset, err := e.log.Append(evt)
	if err != nil {
		return 0, err
	}

	// Update the in-memory tracker
	e.tracker.UpdateVersion(h, evt.Position)

	return offset, nil
}

// ReadStream fetches a slice of events for a given stream
func (e *Engine) ReadStream(streamName string, fromPos uint64, limit int) ([]*Event, error) {
	return nil, errors.New("not implemented")
	// h := e.tracker.GetHash(streamName)
	//
	// // 1. Get physical locations from the index
	// offsets, err := e.streamIdx.GetOffsetsForStream(h, fromPos, limit)
	//
	//	if err != nil {
	//		return nil, fmt.Errorf("failed to fetch offsets from index: %w", err)
	//	}
	//
	// // 2. Fetch the physical events from the DataLog
	// events := make([]*Event, 0, len(offsets))
	//
	//	for _, off := range offsets {
	//		evt, err := e.log.ReadAt(int64(off))
	//		if err != nil {
	//			return nil, fmt.Errorf("failed to read event at offset %d: %w", off, err)
	//		}
	//		events = append(events, evt)
	//	}
	//
	// return events, nil
}

func (e *Engine) Close() error {
	// Close the main data log
	logErr := e.log.Close()

	return logErr
}
