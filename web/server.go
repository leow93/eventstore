package main

import (
	"embed"
	"encoding/json"
	"io/fs"
	"iter"
	"net/http"
	"strconv"
	"strings"

	"github.com/leow93/eventstore"
)

// eventstore.Store is used only through the methods below; liveStore.acquire hands
// out a current one per request and reopens it when the log grows underneath us.

//go:embed static
var staticFiles embed.FS

// defaultPageSize bounds how many events a single stream/category request
// returns when the caller does not ask for a specific limit. It keeps category
// infinite-scroll batches modest and stream reads responsive.
const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

func newRouter(live *liveStore) http.Handler {
	mux := http.NewServeMux()

	api := &api{live: live}
	mux.HandleFunc("GET /api/stats", api.stats)
	mux.HandleFunc("GET /api/streams", api.listStreams)
	mux.HandleFunc("GET /api/categories", api.listCategories)
	mux.HandleFunc("GET /api/stream", api.readStream)
	mux.HandleFunc("GET /api/category", api.readCategory)

	// Serve the embedded single-page UI at the root.
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err) // static/ is embedded at build time; this cannot fail at runtime.
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	return mux
}

type api struct {
	live *liveStore
}

func (a *api) stats(w http.ResponseWriter, _ *http.Request) {
	store, release := a.live.acquire()
	defer release()

	categories := store.Categories()
	var events int
	for _, c := range categories {
		events += store.CategoryLen(c)
	}
	writeJSON(w, http.StatusOK, map[string]int{
		"streams":    len(store.Streams()),
		"categories": len(categories),
		"events":     events,
	})
}

type streamSummary struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	Count    int    `json:"count"`
}

// listStreams returns stream summaries, optionally filtered by name prefix or
// category, paged by offset/limit.
func (a *api) listStreams(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	category := q.Get("category")
	offset := clampAtLeast(parseInt(q.Get("offset"), 0), 0)
	limit := clampRange(parseInt(q.Get("limit"), defaultPageSize), 1, maxPageSize)

	store, release := a.live.acquire()
	defer release()

	all := store.Streams()
	filtered := make([]streamSummary, 0, len(all))
	for _, name := range all {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		cat, _ := eventstore.GetCategory(name)
		if category != "" && cat != category {
			continue
		}
		filtered = append(filtered, streamSummary{Name: name, Category: cat, Count: store.StreamLen(name)})
	}

	page := paginate(filtered, offset, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"total":   len(filtered),
		"offset":  offset,
		"limit":   limit,
		"streams": page,
	})
}

type categorySummary struct {
	Name        string `json:"name"`
	EventCount  int    `json:"eventCount"`
	StreamCount int    `json:"streamCount"`
}

func (a *api) listCategories(w http.ResponseWriter, _ *http.Request) {
	store, release := a.live.acquire()
	defer release()

	// Count streams per category by grouping the stream list.
	streamsPerCat := make(map[string]int)
	for _, name := range store.Streams() {
		cat, err := eventstore.GetCategory(name)
		if err != nil {
			continue
		}
		streamsPerCat[cat]++
	}

	cats := store.Categories()
	out := make([]categorySummary, 0, len(cats))
	for _, c := range cats {
		out = append(out, categorySummary{
			Name:        c,
			EventCount:  store.CategoryLen(c),
			StreamCount: streamsPerCat[c],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"categories": out})
}

// readStream reads a whole stream (or a bounded window of it) in the requested
// direction. from is an inclusive 1-based stream position.
func (a *api) readStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing stream name")
		return
	}
	from := uint64(clampAtLeast(parseInt(q.Get("from"), 0), 0))
	limit := clampRange(parseInt(q.Get("limit"), defaultPageSize), 1, maxPageSize)
	backwards := q.Get("direction") == "backwards"

	store, release := a.live.acquire()
	defer release()

	version := store.StreamLen(name)
	cat, _ := eventstore.GetCategory(name)

	var seq iter.Seq2[*eventstore.Event, error]
	if backwards {
		seq = store.ReadStreamBackwards(name, from, limit)
	} else {
		seq = store.ReadStreamForwards(name, from, limit)
	}

	events, err := collect(seq, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":      name,
		"category":  cat,
		"version":   version,
		"from":      from,
		"limit":     limit,
		"direction": directionLabel(backwards),
		"events":    events,
	})
}

// readCategory returns one page of a category in append order, along with a
// cursor (next) the UI uses to fetch the following page for infinite scroll.
// from is an inclusive 1-based category-local position.
func (a *api) readCategory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "missing category name")
		return
	}
	from := uint64(clampAtLeast(parseInt(q.Get("from"), 1), 1))
	limit := clampRange(parseInt(q.Get("limit"), defaultPageSize), 1, maxPageSize)

	store, release := a.live.acquire()
	defer release()

	total := store.CategoryLen(name)

	events, err := collect(store.ReadCategory(name, from, limit), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// The next cursor is the category-local position immediately after this page.
	// It is nil once the page reaches the end of the category.
	var next *uint64
	if n := from + uint64(len(events)); int(n)-1 < total && len(events) == limit {
		next = &n
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":     name,
		"total":    total,
		"from":     from,
		"limit":    limit,
		"returned": len(events),
		"next":     next,
		"events":   events,
	})
}

// collect drains a read iterator into a slice of event DTOs, stopping at limit.
func collect(seq iter.Seq2[*eventstore.Event, error], limit int) ([]eventDTO, error) {
	out := make([]eventDTO, 0, limit)
	for evt, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, toDTO(evt))
	}
	return out, nil
}

func directionLabel(backwards bool) string {
	if backwards {
		return "backwards"
	}
	return "forwards"
}

// --- small helpers -------------------------------------------------------

func paginate[T any](items []T, offset, limit int) []T {
	if offset >= len(items) {
		return []T{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func clampAtLeast(v, min int) int {
	if v < min {
		return min
	}
	return v
}

func clampRange(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
