package main

import (
	"fmt"
	"log"

	"github.com/leow93/eventstore"
)

// seedIfEmpty writes a spread of demo events across a few categories and streams
// when the store has none, so the console has something to show on first run. It
// returns the number of events written (0 if the store already had data).
func seedIfEmpty(store *eventstore.Store) int {
	if len(store.Streams()) > 0 {
		return 0
	}

	written := 0
	append := func(stream string, events ...*eventstore.Event) {
		if _, err := store.AppendToStream(stream, eventstore.AnyVersion, events...); err != nil {
			log.Printf("seed: append to %s failed: %v", stream, err)
			return
		}
		written += len(events)
	}

	ev := func(eventType, payload string) *eventstore.Event {
		return &eventstore.Event{EventType: eventType, Payload: []byte(payload)}
	}

	// A handful of user aggregates.
	for i := 1; i <= 6; i++ {
		stream := fmt.Sprintf("user-%d", i)
		append(stream,
			ev("UserRegistered", fmt.Sprintf(`{"id":%d,"email":"user%d@example.com"}`, i, i)),
			ev("EmailVerified", fmt.Sprintf(`{"id":%d}`, i)),
			ev("ProfileUpdated", fmt.Sprintf(`{"id":%d,"name":"User %d"}`, i, i)),
		)
	}

	// An order category with a longer history per stream, good for scrolling.
	for i := 1; i <= 8; i++ {
		stream := fmt.Sprintf("order-%d", 1000+i)
		append(stream, ev("OrderPlaced", fmt.Sprintf(`{"order":%d,"total":%d}`, 1000+i, i*25)))
		for line := 1; line <= i; line++ {
			append(stream, ev("LineItemAdded", fmt.Sprintf(`{"order":%d,"sku":"SKU-%d","qty":%d}`, 1000+i, line, line)))
		}
		append(stream, ev("OrderConfirmed", fmt.Sprintf(`{"order":%d}`, 1000+i)))
	}

	// A high-volume category to exercise infinite scroll.
	for i := 1; i <= 250; i++ {
		append(fmt.Sprintf("pageview-session%d", i%20),
			ev("PageViewed", fmt.Sprintf(`{"path":"/p/%d","session":%d}`, i, i%20)))
	}

	// A stream whose payload is plain text rather than JSON.
	append("log-system", ev("Booted", "system started"), ev("Notice", "all systems nominal"))

	return written
}
