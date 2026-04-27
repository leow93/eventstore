package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type Config struct {
	LogPath           string
	StreamIndexPath   string
	CategoryIndexPath string
}

func Boot(_ context.Context, cfg Config) (*Writer, error) {
	directories := []string{
		filepath.Dir(cfg.LogPath),
		cfg.StreamIndexPath,
		cfg.CategoryIndexPath,
	}
	for _, dir := range directories {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create data directory %s: %w", dir, err)
		}
	}

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
	var streamMax uint64 = 0
	var catMax uint64 = 0

	log.Println("Loading Stream Index...")
	err = streamIdx.Load(func(hash, pos, offset uint64) {
		tracker.UpdateVersion(hash, pos)
		if offset > streamMax {
			streamMax = offset
		}
	})
	if err != nil {
		return nil, fmt.Errorf("stream index load failed: %w", err)
	}

	log.Println("Loading Category Index...")
	err = catIdx.Load(func(category string, offset uint64) {
		if offset > catMax {
			catMax = offset
		}
	})
	if err != nil {
		return nil, fmt.Errorf("category index load failed: %w", err)
	}

	// Find the EARLIEST point of failure
	lowestMax := streamMax
	if catMax < lowestMax {
		lowestMax = catMax
	}
	// Find the LATEST known event to baseline our GlobalPosition
	highestMax := streamMax
	if catMax > highestMax {
		highestMax = catMax
	}

	var startingGlobalPos uint64 = 0

	// If the log isn't empty, read the event at the highest known index offset
	if dataLog.size > 0 {
		evt, err := dataLog.ReadAt(int64(highestMax))
		if err == nil && evt != nil {
			startingGlobalPos = evt.GlobalPosition
		}
	}

	// 3. Assemble the Writer
	writer := NewWriter(tracker, dataLog, streamIdx, catIdx)

	// 4. Run the Tail-Scan Recovery
	finalGlobalPos, err := runTailScan(writer, int64(lowestMax), startingGlobalPos)
	if err != nil {
		return nil, fmt.Errorf("recovery tail-scan failed: %w", err)
	}

	dataLog.SetGlobalPosition(finalGlobalPos)

	log.Println("Database boot sequence complete.")
	return writer, nil
}

func runTailScan(w *Writer, maxIndexedOffset int64, currentGlobalPos uint64) (uint64, error) {
	logSize := w.log.Size()
	if logSize == 0 {
		return 0, nil // Brand new database
	}

	// If maxIndexedOffset is 0, we might have no index at all.
	// Otherwise, we need to read the event at maxIndexedOffset to find out
	// where the *unindexed* tail begins.
	scanStart := int64(0)
	if maxIndexedOffset > 0 {
		lastEvt, err := w.log.ReadAt(maxIndexedOffset)
		if err != nil {
			return 0, fmt.Errorf("failed to read last indexed event at %d: %w", maxIndexedOffset, err)
		}
		// The unindexed tail starts immediately after the last known indexed event
		scanStart = maxIndexedOffset + int64(lastEvt.TotalSize())
	}

	if scanStart >= logSize {
		log.Println("Tail-scan: No dangling events found. Indexes are fully synced.")
		return currentGlobalPos, nil
	}

	log.Printf("Tail-scan: Found gap between index and log. Repairing from offset %d to %d...", scanStart, logSize)

	currentOffset := scanStart
	latestGlobalPos := currentGlobalPos
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
			return 0, err
		}

		if err := w.streamIdx.Append(h, evt.Position, uint64(currentOffset)); err != nil {
			return 0, fmt.Errorf("failed to repair stream index: %w", err)
		}

		if err := w.categoryIndex.Append(category, uint64(currentOffset)); err != nil {
			return 0, fmt.Errorf("failed to repair category index: %w", err)
		}

		w.tracker.UpdateVersion(h, evt.Position)

		// Update our running global position
		latestGlobalPos = evt.GlobalPosition

		currentOffset += int64(evt.TotalSize())
		repairedCount++
	}

	log.Printf("Tail-scan complete. Repaired %d events.", repairedCount)
	return latestGlobalPos, nil
}
