# eventstore

A single-node, append-only event-sourcing store with optimistic concurrency,
ordered stream/category reads, and a gRPC API.

- **Durable log** — every write is appended to the active segment file and
  `fsync`ed; the log is split into fixed-size segment files (one active, the rest
  sealed and immutable).
- **pread reads** — reads are served with `pread` (a small speculative window for
  random reads, a buffered sequential scan for replay); no lock, no SIGBUS.
- **In-memory indexes** — stream and category indexes are derived from the log
  and rebuilt by replaying it on boot (the log is the single source of truth).
- **gRPC API** — writes plus server-streaming reads.
- **Web console** — a read-only admin UI (`web/`) for browsing streams and
  categories, reading a stream, and scrolling a category feed.

## Requirements

- Go 1.25+
- To **regenerate** the gRPC code (only needed if you change `proto/eventstore.proto`):
  - `protoc` (e.g. `brew install protobuf`)
  - `protoc-gen-go` and `protoc-gen-go-grpc`:
    ```sh
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    ```
  The generated code (`eventstorepb/`) is committed, so you don't need `protoc`
  just to build and run.

## Running the server

The server binary is `cmd/eventstored`.

```sh
# via make (defaults: -addr :50051, -data ./data)
make run

# override the listen address / data directory
make run ADDR=:6000 DATA=/tmp/es-data

# or directly
go run ./cmd/eventstored -addr :50051 -data ./data
```

| Flag    | Default     | Description                             |
| ------- | ----------- | --------------------------------------- |
| `-addr` | `:50051`    | TCP address to listen on                |
| `-data` | `./data`    | Directory for the event log (segment files) |

On boot the server replays the log's segment files to rebuild its in-memory
indexes, so restarting against the same `-data` directory preserves all data. Stop
it with `Ctrl-C` (it shuts down gracefully).

## Running the web console

A read-only admin UI for browsing the store — see [`web/README.md`](web/README.md)
for the full docs and HTTP API. It opens the data directory directly and shows
live data: it follows another process's writes by reopening the store when the log
grows, so new events appear on a page refresh.

```sh
# via make (defaults: -addr :8080, -data ./data)
make web

# seed demo data if the store is empty, then serve
make web SEED=1

# override the address / data directory
make web ADDR=:9000 DATA=/tmp/es-data SEED=1

# or directly
go run ./web -addr :8080 -data ./data -seed
```

Then open <http://localhost:8080>. It can browse streams and categories, read a
whole stream, and infinite-scroll a category feed.

| Flag       | Default  | Description                                       |
| ---------- | -------- | ------------------------------------------------- |
| `-addr`    | `:8080`  | TCP address to listen on                          |
| `-data`    | `./data` | Event store data directory                        |
| `-seed`    | off      | Write demo events if the store is empty, then serve |
| `-refresh` | `1s`     | Min interval between live-data checks (debounce)  |

Typical setup: run `eventstored` (the gRPC writer) and the web console against the
same `-data` directory; the console follows the writer's appends on refresh.

## Running the load test

The load-test client (`cmd/loadtest`) drives a **running** server: it appends
events concurrently, then streams them back, reporting throughput and latency.

Start a server in one terminal, then in another:

```sh
# via make (see the table below for overridable variables)
make loadtest BATCH=100 EVENTS=5000

# or directly
go run ./cmd/loadtest -addr localhost:50051 -writers 8 -events 5000 -batch 100 -prefix run1
```

| Flag       | Default          | Description                                                        |
| ---------- | ---------------- | ------------------------------------------------------------------ |
| `-addr`    | `localhost:50051`| Server address                                                     |
| `-writers` | `8`              | Number of concurrent writers (each owns one stream)                |
| `-events`  | `2000`           | Events appended per writer                                         |
| `-batch`   | `1`              | Events per `Append` call (the key durability/throughput lever)     |
| `-payload` | `64`             | Payload size in bytes                                              |
| `-prefix`  | `loadtest`       | Stream/category prefix — **use a fresh value per run** (see below) |

### Important: use a fresh `-prefix` per run

Each writer owns the stream `<prefix>-<id>` and appends with optimistic
concurrency control. If you rerun the load test against the same server with the
same `-prefix`, those streams already exist and the writes are rejected with a
concurrency conflict (`Aborted`). Use a new `-prefix` each run, or restart the
server against a fresh/empty `-data` directory.

### Example output

```
writers=8 events/writer=5000 batch=100 payload=64B total=40000

=== WRITE ===
  40000 events in 1.884s
  throughput: 21232 events/sec  (400 Append calls)
  Append latency: p50=34.982ms p99=135.099ms max=148.788ms

=== READ (category stream) ===
  40000 events in 40ms
  throughput: 999387 events/sec
```

### Interpreting the numbers

Writes are bounded by `fsync`: the store serializes writers and flushes to disk,
so **batch size is the dominant throughput lever**. One `fsync` covers a whole
batch, so larger batches amortise it. Rough numbers on an Apple M2 Pro:

| `-batch` | write throughput |
| -------- | ---------------- |
| 1        | ~240 events/s    |
| 100      | ~21,000 events/s |
| 1000     | ~180,000 events/s|

Reads come straight from the log via `pread` and stream back at ~1M events/s.

## Development

```sh
make            # fmt + vet + fast tests with the race detector
make test-full  # all tests, including disk I/O
make bench      # benchmarks with allocation stats
make proto      # regenerate gRPC code from proto/eventstore.proto
```
