package storage

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
)

type Config struct {
	LogPath           string
	StreamIndexPath   string
	CategoryIndexPath string
}

func Boot(_ context.Context, cfg Config) (*Writer, error) {
	// 1. Initialize empty components
	dataLog, err := NewDataLog(cfg.LogPath)
	if err != nil {
		return nil, err
	}

	streamIdx, err := NewShardedStreamIndex(cfg.StreamIndexPath)
	if err != nil {
		return nil, err
	}

	catIdx, err := NewCategoryIndex(cfg.CategoryIndexPath)
	if err != nil {
		return nil, err
	}

	tracker := NewStreamTracker()

	// 2. Load existing indexes and find the High-Water Mark
	var maxIndexedOffset uint64 = 0

	// Helper to track the max offset concurrently safely
	updateMaxOffset := func(offset uint64) {
		for {
			current := atomic.LoadUint64(&maxIndexedOffset)
			if offset <= current || atomic.CompareAndSwapUint64(&maxIndexedOffset, current, offset) {
				break
			}
		}
	}

	log.Println("Loading Stream Index...")
	err = streamIdx.Load(func(hash, pos, offset uint64) {
		tracker.UpdateVersion(hash, pos)
		updateMaxOffset(offset)
	})
	if err != nil {
		return nil, fmt.Errorf("stream index load failed: %w", err)
	}

	log.Println("Loading Category Index...")
	err = catIdx.Load(func(category string, offset uint64) {
		updateMaxOffset(offset)
	})
	if err != nil {
		return nil, fmt.Errorf("category index load failed: %w", err)
	}

	// 3. Assemble the Writer
	writer := NewWriter(tracker, dataLog, streamIdx, catIdx)

	// 4. Run the Tail-Scan Recovery
	if err := runTailScan(writer, int64(maxIndexedOffset)); err != nil {
		return nil, fmt.Errorf("recovery tail-scan failed: %w", err)
	}

	log.Println("Database boot sequence complete.")
	return writer, nil
}

func runTailScan(w *Writer, maxIndexedOffset int64) error {
	logSize := w.log.Size()
	if logSize == 0 {
		return nil // Brand new database
	}

	// If maxIndexedOffset is 0, we might have no index at all.
	// Otherwise, we need to read the event at maxIndexedOffset to find out
	// where the *unindexed* tail begins.
	scanStart := int64(0)
	if maxIndexedOffset > 0 {
		lastEvt, err := w.log.ReadAt(maxIndexedOffset)
		if err != nil {
			return fmt.Errorf("failed to read last indexed event at %d: %w", maxIndexedOffset, err)
		}
		// The unindexed tail starts immediately after the last known indexed event
		scanStart = maxIndexedOffset + int64(lastEvt.TotalSize())
	}

	if scanStart >= logSize {
		log.Println("Tail-scan: No dangling events found. Indexes are fully synced.")
		return nil
	}

	log.Printf("Tail-scan: Found gap between index and log. Repairing from offset %d to %d...", scanStart, logSize)

	currentOffset := scanStart
	repairedCount := 0

	for currentOffset < logSize {
		evt, err := w.log.ReadAt(currentOffset)
		if err != nil {
			log.Printf("Tail-scan stopped early due to read error (possible partial tail write): %v", err)
			break
		}

		h := w.tracker.GetHash(evt.StreamName)
		category, err := GetCategory(evt.StreamName)
		if err != nil {
			return err
		}

		// 1. Repair Stream Index
		if err := w.streamIdx.Append(h, evt.Position, uint64(currentOffset)); err != nil {
			return fmt.Errorf("failed to repair stream index: %w", err)
		}

		// 2. Repair Category Index
		if err := w.categoryIndex.Append(category, uint64(currentOffset)); err != nil {
			return fmt.Errorf("failed to repair category index: %w", err)
		}

		// 3. Update Memory
		w.tracker.UpdateVersion(h, evt.Position)

		currentOffset += int64(evt.TotalSize())
		repairedCount++
	}

	log.Printf("Tail-scan complete. Repaired %d events.", repairedCount)
	return nil
}
