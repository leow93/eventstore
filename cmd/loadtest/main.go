// Command loadtest drives the eventstore gRPC server to measure end-to-end
// write and read throughput.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	pb "github.com/leow93/eventstore/eventstorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "server address")
	writers := flag.Int("writers", 8, "number of concurrent writers")
	events := flag.Int("events", 2000, "events appended per writer")
	batch := flag.Int("batch", 1, "events per Append call")
	payloadSize := flag.Int("payload", 64, "payload size in bytes")
	prefix := flag.String("prefix", "loadtest", "stream/category prefix (use a fresh value per run to avoid OCC conflicts)")
	categories := flag.Int("categories", 1, "number of categories to spread writers across")
	flag.Parse()

	if *categories < 1 {
		log.Fatalf("categories must be >= 1, got %d", *categories)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewEventStoreClient(conn)

	payload := make([]byte, *payloadSize)
	totalEvents := *writers * *events

	fmt.Printf("writers=%d events/writer=%d batch=%d payload=%dB categories=%d total=%d\n",
		*writers, *events, *batch, *payloadSize, *categories, totalEvents)

	// --- Write phase ---
	// Writers are spread round-robin across the categories. Each writer owns a
	// distinct stream within its category, so writers never contend on version.
	latencies := make([][]time.Duration, *writers)
	var wg sync.WaitGroup
	wg.Add(*writers)

	start := time.Now()
	for w := 0; w < *writers; w++ {
		go func(id int) {
			defer wg.Done()
			category := categoryName(*prefix, id%*categories, *categories)
			stream := fmt.Sprintf("%s-%d", category, id)
			latencies[id] = runWriter(client, stream, id, *events, *batch, payload)
		}(w)
	}
	wg.Wait()
	writeElapsed := time.Since(start)

	all := merge(latencies)
	fmt.Println("\n=== WRITE ===")
	fmt.Printf("  %d events in %s\n", totalEvents, writeElapsed.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f events/sec  (%d Append calls)\n",
		float64(totalEvents)/writeElapsed.Seconds(), len(all))
	printLatencies("  Append latency", all)

	// --- Read phase: stream each category back ---
	fmt.Println("\n=== READ (category stream) ===")
	readStart := time.Now()
	count := 0
	for c := 0; c < *categories; c++ {
		count += readCategory(client, categoryName(*prefix, c, *categories))
	}
	readElapsed := time.Since(readStart)
	fmt.Printf("  %d events across %d categories in %s\n", count, *categories, readElapsed.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f events/sec\n", float64(count)/readElapsed.Seconds())
}

// categoryName returns the category for the given index. With a single category
// the bare prefix is used (preserving the original single-category behaviour);
// otherwise the index is appended so each category is distinct and dash-free.
func categoryName(prefix string, idx, total int) string {
	if total == 1 {
		return prefix
	}
	return fmt.Sprintf("%s%d", prefix, idx)
}

// runWriter appends events to a single stream it owns, tracking its expected
// version, and returns the per-Append latencies.
func runWriter(client pb.EventStoreClient, stream string, id, events, batch int, payload []byte) []time.Duration {
	var latencies []time.Duration
	var version int64 // stream starts empty (version 0)

	for written := 0; written < events; {
		n := batch
		if remaining := events - written; remaining < n {
			n = remaining
		}

		newEvents := make([]*pb.NewEvent, n)
		for i := range newEvents {
			newEvents[i] = &pb.NewEvent{EventType: "LoadTest", Payload: payload}
		}

		callStart := time.Now()
		resp, err := client.Append(context.Background(), &pb.AppendRequest{
			StreamName:      stream,
			ExpectedVersion: version,
			Events:          newEvents,
		})
		latencies = append(latencies, time.Since(callStart))
		if err != nil {
			log.Fatalf("writer %d append failed: %v", id, err)
		}

		version = int64(resp.GetVersion())
		written += n
	}

	return latencies
}

func readCategory(client pb.EventStoreClient, category string) int {
	stream, err := client.ReadCategory(context.Background(), &pb.ReadCategoryRequest{Category: category})
	if err != nil {
		log.Fatalf("read category failed: %v", err)
	}

	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("read recv failed: %v", err)
		}
		count++
	}
	return count
}

func merge(perWriter [][]time.Duration) []time.Duration {
	var all []time.Duration
	for _, ls := range perWriter {
		all = append(all, ls...)
	}
	return all
}

func printLatencies(label string, latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("%s: p50=%s p99=%s max=%s\n",
		label,
		percentile(latencies, 0.50).Round(time.Microsecond),
		percentile(latencies, 0.99).Round(time.Microsecond),
		latencies[len(latencies)-1].Round(time.Microsecond),
	)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
