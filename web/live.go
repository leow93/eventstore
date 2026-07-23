package main

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/leow93/eventstore"
)

// liveStore keeps a *eventstore.Store in sync with a data directory that another
// process (e.g. eventstored) may be appending to.
//
// A store's in-memory index is a boot-time snapshot: it is only advanced by this
// process's own appends, so events written by another process are invisible until
// the store is reopened. To serve live data, liveStore reopens the store — which
// rebuilds the index by replaying the log, the single source of truth — whenever
// the on-disk log has grown. Reopens are debounced (minInterval) and skipped when
// the log is unchanged, so an idle console does no work and a burst of requests
// triggers at most one reopen.
//
// The swap is concurrency-safe: readers hold an RLock for the whole of a read, and
// the old store is closed only after the pointer swap, which waits for in-flight
// reads to drain — so a segment file is never closed out from under a pread.
type liveStore struct {
	dir         string
	minInterval time.Duration

	mu    sync.RWMutex // guards store; RLocked for the duration of a read
	store *eventstore.Store

	// refreshMu serialises refresh decisions and reopens so at most one reopen
	// runs at a time. It is independent of mu so a reopen's log replay does not
	// block readers — they keep serving from the current store until the swap.
	refreshMu sync.Mutex
	lastSize  int64
	lastCheck time.Time
}

func newLiveStore(dir string, initial *eventstore.Store, minInterval time.Duration) *liveStore {
	ls := &liveStore{dir: dir, store: initial, minInterval: minInterval}
	ls.lastSize, _ = dirLogSize(dir)
	ls.lastCheck = time.Now()
	return ls
}

// acquire brings the store up to date if the log changed, then returns the
// current store together with a release function. Hold the release until the read
// is fully drained (segment preads must finish before the store can be swapped
// out and closed).
func (ls *liveStore) acquire() (*eventstore.Store, func()) {
	ls.refreshIfChanged()
	ls.mu.RLock()
	return ls.store, ls.mu.RUnlock
}

// refreshIfChanged reopens the store when the on-disk log has grown since the last
// check, subject to the debounce interval. On any error it logs and keeps serving
// the existing (stale) store rather than dropping requests.
func (ls *liveStore) refreshIfChanged() {
	ls.refreshMu.Lock()
	defer ls.refreshMu.Unlock()

	if time.Since(ls.lastCheck) < ls.minInterval {
		return
	}
	ls.lastCheck = time.Now()

	size, err := dirLogSize(ls.dir)
	if err != nil || size == ls.lastSize {
		return // cannot stat, or nothing new on disk
	}

	// Reopen outside mu so the (potentially long) index rebuild does not block
	// readers; they continue against the current store until the swap below.
	newStore, err := eventstore.Open(ls.dir)
	if err != nil {
		log.Printf("live refresh: reopen failed, serving stale data: %v", err)
		return
	}
	ls.lastSize = size

	ls.mu.Lock()
	old := ls.store
	ls.store = newStore
	ls.mu.Unlock()

	// The write lock above drained all readers of the old store, so no pread is
	// in flight against its segment files; closing it now is safe.
	if err := old.Close(); err != nil {
		log.Printf("live refresh: closing previous store: %v", err)
	}
}

func (ls *liveStore) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.store.Close()
}

// dirLogSize sums the sizes of the log's on-disk files (segment files, or the
// legacy single-file log). It is a cheap change-detector: the log is append-only,
// so its total size grows if and only if events were written.
func dirLogSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (!strings.HasSuffix(name, ".seg") && name != "events.log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total, nil
}
