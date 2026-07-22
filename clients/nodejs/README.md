# eventstore — Node.js client

A small gRPC client for the eventstore server. It loads
[`proto/eventstore.proto`](../../proto/eventstore.proto) dynamically at runtime,
so there is **no code-generation step**.

## Install

```sh
cd clients/nodejs
npm install
```

Requires Node 18+ (for native `fetch`-era `for await` streams and ESM).

## Run the example

Start the server from the repo root, then run the demo:

```sh
# terminal 1
go run ./cmd/eventstored          # listens on :50051

# terminal 2
cd clients/nodejs
npm run example
```

Set `EVENTSTORE_ADDR=host:port` to target a different server.

## Usage

```js
import { EventStoreClient, Direction, NoStream, jsonEvent } from './client.js';

const client = new EventStoreClient('localhost:50051');

// Append (optimistic concurrency: NoStream means "must not exist yet")
const version = await client.append(
  'user-123',
  [jsonEvent('UserRegistered', { name: 'Ada' })],
  { expectedVersion: NoStream },
);

// Read a single stream (async iterable)
for await (const e of client.readStream('user-123', { direction: Direction.Forwards })) {
  console.log(e.position, e.eventType, e.payloadJson());
}

// Read a whole category (the part of a stream name before the first '-')
for await (const e of client.readCategory('user', { limit: 100 })) {
  console.log(e.globalPosition, e.streamName, e.eventType);
}

client.close();
```

## API

### `new EventStoreClient(address?, credentials?)`

`address` defaults to `localhost:50051`. `credentials` defaults to insecure —
the server speaks plaintext gRPC.

### `append(streamName, events, { expectedVersion })` → `Promise<number>`

Appends events and resolves to the stream's new version.

- `events`: array of `{ eventType, payload?, meta? }`, where `payload`/`meta`
  are `Buffer`s. Use `jsonEvent(type, payload, meta)` to build them from plain
  objects.
- `expectedVersion` follows the store's OCC rules:
  - `AnyVersion` (`-1`) — disable the check.
  - `NoStream` (`0`) — the stream must not exist yet.
  - `n >= 0` — the current version must equal `n` exactly.
  - On mismatch the promise rejects with a gRPC error whose `code` is
    `grpc.status.ABORTED`.

### `readStream(streamName, { from, limit, direction })` → async iterable

- `from`: inclusive 1-based position; `0` means the natural start.
- `limit`: `<= 0` means unbounded.
- `direction`: `Direction.Forwards` (default) or `Direction.Backwards`.

### `readCategory(category, { from, limit })` → async iterable

Streams a category's events in global append order. A category is the portion
of a stream name before the first `-` (e.g. `user-123` → `user`).

### Decoded events

Each yielded event has:

| field            | type            | notes                                        |
| ---------------- | --------------- | -------------------------------------------- |
| `streamName`     | `string`        |                                              |
| `eventType`      | `string`        |                                              |
| `position`       | `number`        | 1-based position within its stream           |
| `globalPosition` | `number`        | position within the whole store              |
| `timestampNanos` | `bigint`        | unix nanoseconds (server-assigned)           |
| `timestamp`      | `Date`          | millisecond-truncated convenience view       |
| `payload`        | `Buffer`        | raw bytes                                    |
| `meta`           | `Buffer`        | raw bytes                                    |
| `payloadJson()`  | `() => any`     | parse `payload` as JSON (`null` if empty)    |
| `metaJson()`     | `() => any`     | parse `meta` as JSON (`null` if empty)       |
