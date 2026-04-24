.PHONY: all fmt vet test test-race

# The default target runs when you just type 'make'
all: fmt vet test-race

## fmt: Format all Go code
fmt:
	@echo "==> Formatting code..."
	go fmt ./...

## vet: Run go vet to catch common mistakes
vet:
	@echo "==> Running go vet..."
	go vet ./...

## test: Run standard unit tests
test:
	@echo "==> Running tests..."
	go test -v ./...

## test-race: Run unit tests with the race detector enabled
test-race:
	@echo "==> Running tests with race detector..."
	go test -v -race ./...

## test-bench: Run benchmarking tests
test-bench:
	@echo "==> Running bench tests..."
	go test -bench=. -benchmem ./...
