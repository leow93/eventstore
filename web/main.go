// Command eventstore-web serves a read-only admin console over an eventstore data
// directory: browse streams and categories, read a whole stream, and scroll a
// category page by page.
//
// It opens the store directly (eventstore.Open) rather than talking to the gRPC
// server. To show live data, it reopens the store when the on-disk log grows —
// so events written by another process (e.g. eventstored) appear on the next
// request, and thus on a page refresh (see liveStore).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leow93/eventstore"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data", "./data", "directory for the event store data")
	seed := flag.Bool("seed", false, "write demo data if the store is empty, then serve")
	refresh := flag.Duration("refresh", time.Second, "min interval between checks for new on-disk data (live refresh debounce)")
	flag.Parse()

	store, err := eventstore.Open(*dataDir)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", *dataDir, err)
	}

	if *seed {
		// Seeding appends through this store, whose index is updated in-process, so
		// the seeded data is visible immediately — no reopen required.
		if n := seedIfEmpty(store); n > 0 {
			log.Printf("seeded %d demo events", n)
		}
	}

	live := newLiveStore(*dataDir, store, *refresh)
	defer live.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           newRouter(live),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		_ = srv.Close()
	}()

	log.Printf("eventstore web console on http://localhost%s (data: %s)", *addr, *dataDir)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	log.Println("server stopped")
}
