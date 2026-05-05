package storage

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Checkpointer interface {
	StoreCheckpoint(cp uint64) error
	LoadCheckpoint() (uint64, error)
}

type subscriptionHandler func(batch []*Event, offsets []int64) error

type LogSubscription struct {
	log          *Log
	handler      subscriptionHandler
	checkpointer Checkpointer
	signal       <-chan struct{}
}

func NewLogSubscription(log *Log, checkpointer Checkpointer, handler subscriptionHandler, signal <-chan struct{}) *LogSubscription {
	return &LogSubscription{
		log:          log,
		handler:      handler,
		checkpointer: checkpointer,
		signal:       signal,
	}
}

// Start boots up the background subscription loop.
// It returns an error synchronously if the checkpoint is hopelessly corrupted,
// otherwise it spawns the background goroutine.
func (s *LogSubscription) Start(ctx context.Context, wg *sync.WaitGroup, subName string) error {
	// 1. Synchronously load the starting position
	startOffset, err := s.checkpointer.LoadCheckpoint()
	if err != nil {
		// If the file simply doesn't exist, this is a fresh start. Default to 0.
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
			log.Printf("[%s] No checkpoint found. Starting from offset 0.", subName)
			startOffset = 0
		} else {
			return err // Fatal error (e.g., corrupted file)
		}
	} else {
		log.Printf("[%s] Checkpoint loaded. Resuming at offset %d.", subName, startOffset)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		currentOffset := int64(startOffset)

		// Ensure we always attempt to save our final position on shutdown
		defer func() {
			if err := s.checkpointer.StoreCheckpoint(uint64(currentOffset)); err != nil {
				log.Printf("[%s] ERROR: Failed to flush final checkpoint: %v", subName, err)
			}
			log.Printf("[%s] Subscription gracefully shut down.", subName)
		}()

		for {
			// 1. Read a batch from the log
			events, offsets, nextOffset, err := s.log.ReadBatchAt(currentOffset, 1000)
			if err != nil && !errors.Is(err, io.EOF) {
				log.Printf("[%s] ERROR: Failed to read log: %v", subName, err)
				time.Sleep(1 * time.Second) // Backoff on physical disk read error
				continue
			}

			// 2. Process the batch if we have events
			if len(events) > 0 {
				// Pass the batch to the user-defined handler
				if err := s.handler(events, offsets); err != nil {
					log.Printf("[%s] ERROR: Handler failed: %v", subName, err)
					// We do NOT advance the offset.
					// Sleep briefly and retry the exact same batch.
					time.Sleep(1 * time.Second)
					continue
				}

				// Success! Advance memory state
				currentOffset = nextOffset

				// Store the checkpoint to disk
				// (For ultra-high throughput, you could throttle this to run every X seconds/events)
				if err := s.checkpointer.StoreCheckpoint(uint64(currentOffset)); err != nil {
					log.Printf("[%s] ERROR: Failed to save checkpoint: %v", subName, err)
				}

				// Check if we need to shut down without blocking
				select {
				case <-ctx.Done():
					return
				default:
				}

				// We had events, so there might be more right away. Continue immediately.
				continue
			}

			// 3. IDLE STATE: We reached EOF and have no events. Wait for a signal.
			select {
			case <-ctx.Done():
				// Database is shutting down
				return
			case <-s.signal:
				// Woken up by a new write! Loop immediately.
			case <-time.After(5 * time.Second):
				// Occasional fallback poll, just in case a wake signal was dropped
			}
		}
	}()

	return nil
}
