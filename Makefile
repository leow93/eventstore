.PHONY: all fmt vet test test-full test-race test-full-race bench proto build run loadtest

# The default target runs the FAST tests
all: fmt vet test-race

## fmt: Format all Go code
fmt:
	@echo "==> Formatting code..."
	go fmt ./...

## vet: Run go vet to catch common mistakes
vet:
	@echo "==> Running go vet..."
	go vet ./...

## mod-tidy: Run go mod tidy 
mod-tidy:
	@echo "==> Running go mod tidy..."
	go mod tidy

## test: Run FAST unit tests (skips disk I/O)
test:
	@echo "==> Running fast tests..."
	go test -short -v ./...

## test-full: Run ALL tests (including slow disk I/O)
test-full:
	@echo "==> Running all tests..."
	go test -v ./...

## test-race: Run FAST tests with the race detector
test-race:
	@echo "==> Running fast tests with race detector..."
	go test -short -v -race ./...

## test-full-race: Run ALL tests with the race detector
test-full-race:
	@echo "==> Running all tests with race detector..."
	go test -v -race ./...

## bench: Run all benchmarks with memory allocation statistics
bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./...

## proto: Regenerate Go code from the .proto definitions (requires protoc + plugins)
proto:
	@echo "==> Generating protobuf/gRPC code..."
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--go_out=. --go_opt=module=github.com/leow93/eventstore \
		--go-grpc_out=. --go-grpc_opt=module=github.com/leow93/eventstore \
		proto/eventstore.proto

## build: Build the server and loadtest binaries into ./bin
build:
	@echo "==> Building binaries..."
	go build -o bin/eventstored ./cmd/eventstored
	go build -o bin/loadtest ./cmd/loadtest

## run: Run the gRPC server (ADDR and DATA are overridable)
run:
	go run ./cmd/eventstored -addr $(or $(ADDR),:50051) -data $(or $(DATA),./data)

## loadtest: Run the load-test client against a running server (see README for flags)
loadtest:
	go run ./cmd/loadtest -addr $(or $(ADDR),localhost:50051) \
		-writers $(or $(WRITERS),8) -events $(or $(EVENTS),2000) \
		-batch $(or $(BATCH),1) -payload $(or $(PAYLOAD),64) \
		-prefix $(or $(PREFIX),loadtest)

## clean-local-data: removes local data
clean-local-data:
	@echo "==> Removing local data"
	rm -rf data
	mkdir data
