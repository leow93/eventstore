package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type ErrStreamHasNoCategory struct {
	stream string
}

func (e ErrStreamHasNoCategory) Error() string {
	return fmt.Sprintf("stream %s has no category", e.stream)
}

func GetCategory(streamName string) (string, error) {
	parts := strings.SplitN(streamName, "-", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0], nil
	}
	return "", ErrStreamHasNoCategory{streamName}
}

// The Category Index Design
// Each record is 8 bytes which represents the offset in the data log for that entry in the category.
const categoryIndexEntrySize = 8

type categoryFile struct {
	mu   sync.Mutex
	file *os.File
}

type CategoryIndex struct {
	basePath string

	// RWMutex protects the map of open files
	mu    sync.RWMutex
	files map[string]*categoryFile
}

func NewCategoryIndex(dirPath string) (*CategoryIndex, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, err
	}

	return &CategoryIndex{
		basePath: dirPath,
		files:    make(map[string]*categoryFile),
		mu:       sync.RWMutex{},
	}, nil
}

// getOrCreateFile safely retrieves an existing file handle or opens a new one
func (ci *CategoryIndex) getOrCreateFile(category string) (*categoryFile, error) {
	// Fast path: Read lock
	ci.mu.RLock()
	cf, exists := ci.files[category]
	ci.mu.RUnlock()

	if exists {
		return cf, nil
	}

	// Slow path: Write lock
	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Double-check in case another goroutine created it while we upgraded locks
	cf, exists = ci.files[category]
	if exists {
		return cf, nil
	}

	path := filepath.Join(ci.basePath, fmt.Sprintf("%s.cidx", category))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open category index %s: %w", category, err)
	}

	cf = &categoryFile{file: f}
	ci.files[category] = cf

	return cf, nil
}

// Append writes the 8-byte offset to the category's index file
func (ci *CategoryIndex) Append(category string, offset uint64) error {
	cf, err := ci.getOrCreateFile(category)
	if err != nil {
		return err
	}

	cf.mu.Lock()
	defer cf.mu.Unlock()

	buf := make([]byte, categoryIndexEntrySize)
	binary.LittleEndian.PutUint64(buf, offset)

	if _, err := cf.file.Write(buf); err != nil {
		return err
	}

	// Until we have ability to replay the log to build the index, we just sync to disk.
	return cf.file.Sync()
}

type CategoryRecoveryHandler func(category string, offset uint64)

// Load scans the directory for .cidx files and loads them in parallel.
func (ci *CategoryIndex) Load(handler CategoryRecoveryHandler) error {
	entries, err := os.ReadDir(ci.basePath)
	if os.IsNotExist(err) {
		return nil // Directory doesn't exist yet, nothing to load
	}
	if err != nil {
		return fmt.Errorf("failed to read category directory: %w", err)
	}

	var wg sync.WaitGroup
	// Create a buffered channel large enough to hold an error from each file
	errChan := make(chan error, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".cidx" {
			continue // Skip non-category files
		}

		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()

			category := strings.TrimSuffix(fileName, ".cidx")
			path := filepath.Join(ci.basePath, fileName)

			f, err := os.Open(path)
			if err != nil {
				errChan <- fmt.Errorf("failed to open category file %s: %w", fileName, err)
				return
			}
			defer f.Close()

			buf := make([]byte, 8)
			for {
				n, err := f.Read(buf)
				if n < 8 {
					// We've reached EOF or a partial read (which indicates corruption)
					// TODO: This indicates a need to rebuild the index, which should be signalled somehow.
					break
				}
				if err != nil {
					break
				}

				offset := binary.LittleEndian.Uint64(buf)
				handler(category, offset)
			}
		}(entry.Name())
	}

	wg.Wait()
	close(errChan)

	// Return the first error encountered, if any
	for err := range errChan {
		return err
	}

	return nil
}

func (ci *CategoryIndex) Close() error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	var firstErr error
	for name, cf := range ci.files {
		cf.mu.Lock()
		if err := cf.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close category %s: %w", name, err)
		}
		cf.mu.Unlock()
	}
	return firstErr
}
