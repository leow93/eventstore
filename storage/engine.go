package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const shardCount = 1024

type CommitRequest struct {
	Event           *Event
	ExpectedVersion uint64
	Offset          int64
	Err             error
	Result          chan error
}

var reqPool = sync.Pool{
	New: func() any {
		return &CommitRequest{
			Result: make(chan error, 1), // Pre-allocated once forever
		}
	},
}

type Engine struct {
	log           *DataLog
	tracker       *StreamTracker
	streamIdx     *StreamIndex
	categoryIndex *CategoryIndex

	commitQueue chan *CommitRequest // The funnel for all concurrent writes
	haltError   atomic.Value        // Stores the fatal error if the engine crashes

	// Lifecycle management
	shutdownWg sync.WaitGroup
	stateMu    sync.RWMutex
	closed     bool
}

func NewEngine(tracker *StreamTracker, log *DataLog, streamIndex *StreamIndex, categoryIndex *CategoryIndex) *Engine {
	e := &Engine{
		log:           log,
		tracker:       tracker,
		streamIdx:     streamIndex,
		categoryIndex: categoryIndex,
		commitQueue:   make(chan *CommitRequest, 10_000),
		haltError:     atomic.Value{},

		shutdownWg: sync.WaitGroup{},
		stateMu:    sync.RWMutex{},
		closed:     false,
	}
	e.shutdownWg.Add(1)
	go e.batchProcessor()
	return e
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
	if err := e.haltError.Load(); err != nil {
		return 0, err.(error)
	}
	// Grab a pooled request
	req := reqPool.Get().(*CommitRequest)
	req.Event = evt
	req.ExpectedVersion = expectedVersion
	req.Offset = 0
	req.Err = nil // <-- MUST clear this so old errors don't bleed over	// Protect against queue closing

	e.stateMu.RLock()
	if e.closed {
		e.stateMu.RUnlock()
		reqPool.Put(req) // Return to pool
		return 0, fmt.Errorf("engine is shutting down")
	}

	select {
	case e.commitQueue <- req:
	default:
		e.stateMu.RUnlock()
		reqPool.Put(req)
		return 0, fmt.Errorf("engine overloaded")
	}
	e.stateMu.RUnlock()

	// Wait for processing
	err := <-req.Result

	offset := req.Offset

	// Release back to pool safely
	req.Event = nil
	reqPool.Put(req)

	return offset, err
}

// ReadStream fetches a slice of events for a given stream
func (e *Engine) ReadStream(streamName string, fromPos uint64, limit int) ([]*Event, error) {
	h := e.tracker.GetHash(streamName)

	// 1. Get physical locations from the index
	offsets, err := e.streamIdx.GetOffsetsForStream(h, fromPos, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets from index: %w", err)
	}

	// 2. Fetch the physical events from the DataLog
	events := make([]*Event, 0, len(offsets))
	for _, off := range offsets {
		evt, err := e.log.ReadAt(int64(off))
		if err != nil {
			return nil, fmt.Errorf("failed to read event at offset %d: %w", off, err)
		}
		events = append(events, evt)
	}

	return events, nil
}

func (e *Engine) batchProcessor() {
	defer e.shutdownWg.Done() // Signal that we have fully exited

	const maxBatchSize = 1000
	batch := make([]*CommitRequest, 0, maxBatchSize)

	for req := range e.commitQueue {
		batch = append(batch, req)

		// 1. Drain the queue up to maxBatchSize
	drain:
		for len(batch) < maxBatchSize {
			select {
			case r := <-e.commitQueue:
				batch = append(batch, r)
			default:
				break drain // Queue is empty, commit immediately
			}
		}

		// 2. Prepare the valid events
		var validEvents []*Event
		var validReqs []*CommitRequest

		for _, r := range batch {
			_, err := GetCategory(r.Event.StreamName)
			if err != nil {
				r.Result <- err
				continue
			}
			h := e.tracker.GetHash(r.Event.StreamName)
			lock := e.tracker.GetLock(h)
			lock.Lock()

			currentVersion, exists := e.tracker.GetCurrentVersion(h)

			// OCC Checks
			if r.ExpectedVersion == 0 && exists {
				r.Err = ErrStreamAlreadyExists{r.Event.StreamName} // Set state, do not send!
				lock.Unlock()
				continue
			}

			if r.ExpectedVersion > 0 && currentVersion != r.ExpectedVersion {
				r.Err = ErrWrongExpectedVersion{
					stream:   r.Event.StreamName,
					expected: r.ExpectedVersion,
					current:  currentVersion,
				}
				lock.Unlock()
				continue
			}

			r.Event.Position = currentVersion + 1

			// Pre-update memory so subsequent events in the SAME batch see the new version
			e.tracker.UpdateVersion(h, r.Event.Position)
			lock.Unlock()

			validEvents = append(validEvents, r.Event)
			validReqs = append(validReqs, r)
		}

		// 3. Bulk Write to DataLog
		if len(validEvents) > 0 {
			offsets, err := e.log.AppendBatch(validEvents)
			if err != nil {
				// A failure here is a split-brain hardware failure. Panic the server.
				panic(fmt.Sprintf("CRITICAL: Failed to write batch. Crashing: %v", err))
			}

			// 4. Update Indexes sequentially
			for i, r := range validReqs {
				h := e.tracker.GetHash(r.Event.StreamName)
				// Category err already checked above
				category, _ := GetCategory(r.Event.StreamName)
				offset := uint64(offsets[i])

				if err := e.streamIdx.Append(h, r.Event.Position, offset); err != nil {
					e.halt(fmt.Errorf("stream index write failed: %w", err), batch)
					return // Kill the background goroutine
				}
				if err := e.categoryIndex.Append(category, offset); err != nil {
					e.halt(fmt.Errorf("stream index write failed: %w", err), batch)
					return // Kill the background goroutine
				}

				r.Offset = int64(offset)
			}
		}

		// 5. Wake up the waiting callers
		for _, r := range batch {
			r.Result <- r.Err
		}

		// Clear the slice for the next iteration without dropping capacity
		batch = batch[:0]
	}
}

// halt safely shuts down the processor and notifies waiting requests.
func (e *Engine) halt(err error, currentBatch []*CommitRequest) {
	fatalErr := fmt.Errorf("FATAL ENGINE FAILURE: %w", err)
	e.haltError.Store(fatalErr)

	// 1. Wake up and fail the requests currently being processed
	for _, r := range currentBatch {
		r.Result <- fatalErr
	}

	// 2. Lock the state to prevent AppendToStream from pushing new requests
	e.stateMu.Lock()
	if !e.closed {
		e.closed = true
		// Closing the channel ensures the range loop below will eventually terminate
		close(e.commitQueue)
	}
	e.stateMu.Unlock()

	// 3. Drain and reject everything else that was already stuck in the queue
	for r := range e.commitQueue {
		r.Result <- fatalErr
	}
}

func (e *Engine) Close() error {
	// Prevent any new writes and close the channel
	e.stateMu.Lock()
	if e.closed {
		e.stateMu.Unlock()
		return nil // Already closed
	}
	e.closed = true

	// Set the halt error so waiting requests back out instantly
	e.haltError.Store(fmt.Errorf("engine is shutting down"))

	close(e.commitQueue)
	e.stateMu.Unlock()

	// Wait for the batch processor to drain the queue and exit
	e.shutdownWg.Wait()

	// Close the index shards first
	idxErr := e.streamIdx.Close()
	cIdxErr := e.categoryIndex.Close()
	// Close the main data log
	logErr := e.log.Close()

	err := errors.Join(idxErr, cIdxErr, logErr)
	return err
}
