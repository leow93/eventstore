package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// 1. IN-MEMORY CHECKPOINTER
// -----------------------------------------------------------------------------

type InMemoryCheckpointer struct {
	mu       sync.RWMutex
	cp       uint64
	notFound bool
}

func NewInMemoryCheckpointer() *InMemoryCheckpointer {
	return &InMemoryCheckpointer{
		notFound: true,
	}
}

func (c *InMemoryCheckpointer) StoreCheckpoint(cp uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cp = cp
	c.notFound = false
	return nil
}

func (c *InMemoryCheckpointer) LoadCheckpoint() (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.notFound {
		return 0, os.ErrNotExist
	}
	return c.cp, nil
}

func (c *InMemoryCheckpointer) Get() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cp
}

// -----------------------------------------------------------------------------
// 2. TEST HELPERS
// -----------------------------------------------------------------------------

// setupLog creates a fully initialized Log instance for testing.
// (Adjust this if your Log constructor is named differently).
func setupLog(t *testing.T) *Log {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "test_datalog.log")

	// Assuming NewLog is your constructor.
	// If it requires different parameters, plug them in here.
	l, err := NewLog(logPath)
	require.NoError(t, err)

	return l
}

// -----------------------------------------------------------------------------
// 3. TESTS
// -----------------------------------------------------------------------------

func appendEvents(t *testing.T, log *Log, ev []*Event) []int64 {
	offsets := make([]int64, 0, len(ev))
	for i, e := range ev {
		o, err := log.Append(e)
		require.NoErrorf(t, err, "expected no err for event %d, got: %w", i, err)
		offsets = append(offsets, o)
	}
	return offsets
}

func TestLogSubscription_FreshBootAndProcess(t *testing.T) {
	log := setupLog(t)
	defer log.Close()

	// 1. Append real data using the actual engine
	appendEvents(t, log, []*Event{
		{StreamName: "stream-1", EventType: "A"},
		{StreamName: "stream-1", EventType: "B"},
		{StreamName: "stream-1", EventType: "C"},
	})

	checkpointer := NewInMemoryCheckpointer()
	signal := make(chan struct{}, 1)

	var handlerWg sync.WaitGroup
	handlerWg.Add(3)

	var seenEvents []*Event
	var mu sync.Mutex

	handler := func(batch []*Event, _ []int64) error {
		mu.Lock()
		defer mu.Unlock()
		for _, evt := range batch {
			seenEvents = append(seenEvents, evt)
			handlerWg.Done()
		}
		return nil
	}

	sub := NewLogSubscription(log, checkpointer, handler, signal)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	err := sub.Start(ctx, &wg, "test-sub")
	require.NoError(t, err)

	// Wait for the handler to process the 3 events
	handlerWg.Wait()

	mu.Lock()
	require.Len(t, seenEvents, 3)
	assert.Equal(t, "A", seenEvents[0].EventType)
	assert.Equal(t, "C", seenEvents[2].EventType)
	mu.Unlock()

	// Shutdown safely
	cancel()
	wg.Wait()

	// The checkpointer should have advanced beyond 0
	assert.Greater(t, checkpointer.Get(), uint64(0))
}

func TestLogSubscription_ResumeFromCheckpoint(t *testing.T) {
	log := setupLog(t)
	defer log.Close()

	// 1. Append the first batch (We want to skip this)
	appendEvents(t, log, []*Event{
		{StreamName: "stream-1", EventType: "Skipped"},
	})

	// 2. Append the second batch (We want to read this)
	offsets2 := appendEvents(t, log, []*Event{
		{StreamName: "stream-1", EventType: "ReadMe"},
	})

	// 3. Set the checkpoint exactly to the start of the second batch
	checkpointer := NewInMemoryCheckpointer()
	err := checkpointer.StoreCheckpoint(uint64(offsets2[0]))
	require.NoError(t, err)

	signal := make(chan struct{}, 1)

	var handlerWg sync.WaitGroup
	handlerWg.Add(1) // We strictly expect only 1 event to arrive

	var seenEvents []*Event

	handler := func(batch []*Event, _ []int64) error {
		for _, evt := range batch {
			seenEvents = append(seenEvents, evt)
			handlerWg.Done()
		}
		return nil
	}

	sub := NewLogSubscription(log, checkpointer, handler, signal)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	err = sub.Start(ctx, &wg, "test-sub")
	require.NoError(t, err)

	handlerWg.Wait()
	cancel()
	wg.Wait()

	require.Len(t, seenEvents, 1)
	assert.Equal(t, "ReadMe", seenEvents[0].EventType)
}

func TestLogSubscription_WakeSignal(t *testing.T) {
	log := setupLog(t)
	defer log.Close()

	checkpointer := NewInMemoryCheckpointer()
	signal := make(chan struct{}, 1)

	var handlerWg sync.WaitGroup
	handlerWg.Add(1)

	handler := func(batch []*Event, _ []int64) error {
		handlerWg.Done()
		return nil
	}

	sub := NewLogSubscription(log, checkpointer, handler, signal)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	err := sub.Start(ctx, &wg, "test-sub")
	require.NoError(t, err)

	// Let the subscription boot up, read EOF, and idle
	time.Sleep(50 * time.Millisecond)

	// Write new data to the log
	appendEvents(t, log, []*Event{
		{StreamName: "stream-1", EventType: "New"},
	})

	// Fire the non-blocking wake signal
	select {
	case signal <- struct{}{}:
	default:
	}

	// This will hang forever if the subscription doesn't wake up
	handlerWg.Wait()

	cancel()
	wg.Wait()
}

func TestLogSubscription_HandlerRetryOnError(t *testing.T) {
	log := setupLog(t)
	defer log.Close()

	appendEvents(t, log, []*Event{
		{StreamName: "stream-1", EventType: "A"},
	})

	checkpointer := NewInMemoryCheckpointer()
	signal := make(chan struct{}, 1)

	attempts := 0
	var handlerWg sync.WaitGroup
	handlerWg.Add(2) // Wait for 2 attempts

	handler := func(batch []*Event, _ []int64) error {
		attempts++
		handlerWg.Done()
		if attempts == 1 {
			// Fail the first time to trigger the retry loop
			return errors.New("simulated handler failure")
		}
		// Succeed the second time
		return nil
	}

	sub := NewLogSubscription(log, checkpointer, handler, signal)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	err := sub.Start(ctx, &wg, "test-sub")
	require.NoError(t, err)

	handlerWg.Wait()
	cancel()
	wg.Wait()

	assert.Equal(t, 2, attempts)
}
