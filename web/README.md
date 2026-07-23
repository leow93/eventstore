# eventstore web console

A read-only admin console for browsing an eventstore data directory: browse
streams and categories, read a whole stream, and scroll a category page by page
(infinite scroll, kafka-ui style).

It is a single Go binary — an HTTP server plus a small vanilla-JS single-page UI
embedded via `go:embed` (no build step, no external assets). It opens the store
directly with `eventstore.Open` (no gRPC hop).

It shows **live data**: a store's in-memory index is a boot-time snapshot, so the
console reopens the store when the on-disk log grows — events written by another
process (e.g. `eventstored`) appear on the next request, and therefore on a page
refresh. The check is debounced (`-refresh`, default 1s) and skipped entirely when
the log is unchanged, so an idle console does no work. See `live.go`.

## Running

```sh
# via make (defaults: -addr :8080, -data ./data)
make web

# seed demo data if the store is empty, then serve
make web SEED=1

# override address / data directory
make web ADDR=:9000 DATA=/tmp/es-data SEED=1

# or directly
go run ./web -addr :8080 -data ./data -seed
```

Then open <http://localhost:8080>.

| Flag    | Default  | Description                                              |
| ------- | -------- | ------------------------------------------------------- |
| `-addr`    | `:8080`  | TCP address to listen on                                |
| `-data`    | `./data` | Event store data directory (the same one `eventstored` uses) |
| `-seed`    | off      | Write a spread of demo events **if the store is empty**, then serve |
| `-refresh` | `1s`     | Min interval between checks for new on-disk data (live-refresh debounce) |

> The console reads the on-disk log directly and is safe to point at the same
> `-data` directory a running `eventstored` uses: it only ever reads, and it
> follows new writes by reopening the store when the log grows.

## UI

- **Streams** — every stream with its category and event count; filter by name
  prefix (e.g. `user-`). Click a stream to read it.
- **Categories** — every category with its stream and event counts. Click to
  open the category feed.
- **Stream view** — the stream's events oldest- or newest-first. Each row expands
  to show the decoded payload and meta.
- **Category view** — the category's events in append order, loaded a page at a
  time as you scroll to the bottom.

## HTTP API

All endpoints return JSON. Event `payload`/`meta` are emitted as JSON values:
a JSON document is passed through as-is, plain text becomes a JSON string, and
opaque bytes become a base64 JSON string.

| Method & path        | Query params                          | Returns |
| -------------------- | ------------------------------------- | ------- |
| `GET /api/stats`     | —                                     | `{streams, categories, events}` |
| `GET /api/streams`   | `prefix`, `category`, `offset`, `limit` | `{total, offset, limit, streams:[{name,category,count}]}` |
| `GET /api/categories`| —                                     | `{categories:[{name,eventCount,streamCount}]}` |
| `GET /api/stream`    | `name` (required), `from`, `limit`, `direction=forwards\|backwards` | `{name,category,version,direction,events:[…]}` |
| `GET /api/category`  | `name` (required), `from`, `limit`    | `{name,total,from,returned,next,events:[…]}` |

`from` is an inclusive 1-based position (stream position, or category-local
position for categories). For `/api/category`, `next` is the `from` cursor for
the following page, or `null` at the end — the UI uses it to drive infinite
scroll. `limit` is capped at 1000.
