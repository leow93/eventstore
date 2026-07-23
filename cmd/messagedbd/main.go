// Command messagedbd serves the eventstore gRPC API backed by Message-DB (Postgres).
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/leow93/eventstore/eventstorepb"
	"github.com/leow93/eventstore/messagedb"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "address to listen on")
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5433/message_store", "Message-DB Postgres DSN")
	flag.Parse()

	backend, err := messagedb.New(context.Background(), *dsn)
	if err != nil {
		log.Fatalf("failed to connect to message-db: %v", err)
	}
	defer backend.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventStoreServer(grpcServer, backend)

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		grpcServer.GracefulStop()
	}()

	log.Printf("messagedb eventstore listening on %s", *addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("server stopped")
}
