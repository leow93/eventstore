.PHONY: all fmt vet test test-full test-race test-full-race bench

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

## clean-local-data: removes local data
clean-local-data:
	@echo "==> Removing local data"
	rm -rf data
	mkdir data
