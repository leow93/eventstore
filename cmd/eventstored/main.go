// Command eventstored runs the event store as a gRPC server.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/leow93/eventstore"
	pb "github.com/leow93/eventstore/eventstorepb"
	"github.com/leow93/eventstore/server"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "address to listen on")
	dataDir := flag.String("data", "./data", "directory for the event store data")
	flag.Parse()

	store, err := eventstore.Open(*dataDir)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", *dataDir, err)
	}
	defer store.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventStoreServer(grpcServer, server.New(store))

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		grpcServer.GracefulStop()
	}()

	log.Printf("eventstore listening on %s (data: %s)", *addr, *dataDir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("server stopped")
}
