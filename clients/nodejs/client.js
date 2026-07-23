// A small Promise/async-iterator based client for the eventstore gRPC service.
//
// It loads proto/eventstore.proto dynamically at runtime (via @grpc/proto-loader)
// so there is no code-generation step to run.

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// The canonical .proto lives at the repo root. Override with EVENTSTORE_PROTO
// if you copy this client somewhere else.
const PROTO_PATH =
  process.env.EVENTSTORE_PROTO ??
  path.resolve(__dirname, '../../proto/eventstore.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false, // expose fields as camelCase (streamName, not stream_name)
  longs: String, // uint64/int64 as strings: timestamps (unix nanos) overflow Number
  enums: String, // enums as their string names
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition).eventstore.v1;

/** expected_version sentinel: disable the optimistic-concurrency check. */
export const AnyVersion = -1;

/** expected_version value requiring the stream not to exist yet. */
export const NoStream = 0;

/** Read direction for readStream. */
export const Direction = Object.freeze({
  Forwards: 'DIRECTION_FORWARDS',
  Backwards: 'DIRECTION_BACKWARDS',
});

/**
 * Build a NewEvent whose payload/meta are JSON-encoded.
 * @param {string} eventType
 * @param {unknown} [payload]
 * @param {unknown} [meta]
 */
export function jsonEvent(eventType, payload = null, meta = null) {
  return {
    eventType,
    payload: Buffer.from(JSON.stringify(payload)),
    meta: meta == null ? Buffer.alloc(0) : Buffer.from(JSON.stringify(meta)),
  };
}

/**
 * Decode a wire Event into a friendlier shape. Positions become Numbers,
 * the nanosecond timestamp becomes both a BigInt and a JS Date, and the
 * payload/meta bytes are exposed as Buffers plus lazy JSON parsers.
 */
function decodeEvent(e) {
  const payload = Buffer.from(e.payload ?? []);
  const meta = Buffer.from(e.meta ?? []);
  const timestampNanos = BigInt(e.timestamp ?? '0');
  return {
    streamName: e.streamName,
    eventType: e.eventType,
    position: Number(e.position),
    globalPosition: Number(e.globalPosition),
    timestampNanos,
    timestamp: new Date(Number(timestampNanos / 1_000_000n)),
    payload,
    meta,
    payloadJson: () => (payload.length ? JSON.parse(payload.toString()) : null),
    metaJson: () => (meta.length ? JSON.parse(meta.toString()) : null),
  };
}

// Turn a grpc server-streaming call (a Readable) into an async generator of
// decoded events, propagating gRPC errors as thrown exceptions.
async function* streamEvents(call) {
  try {
    for await (const raw of call) {
      yield decodeEvent(raw);
    }
  } finally {
    // Ensure we release the stream if the consumer breaks out early.
    call.cancel?.();
  }
}

export class EventStoreClient {
  /**
   * @param {string} address host:port of the eventstore server (default localhost:50051)
   * @param {grpc.ChannelCredentials} [credentials] defaults to insecure (server is plaintext)
   */
  constructor(address = 'localhost:50051', credentials = grpc.credentials.createInsecure()) {
    this._client = new proto.EventStore(address, credentials);
  }

  /**
   * Append events to a stream under optimistic-concurrency control.
   * @param {string} streamName
   * @param {Array<{eventType: string, payload?: Buffer, meta?: Buffer}>} events
   * @param {{expectedVersion?: number}} [opts]
   * @returns {Promise<number>} the stream's new version
   */
  append(streamName, events, { expectedVersion = AnyVersion } = {}) {
    const request = {
      streamName,
      expectedVersion: String(expectedVersion),
      events: events.map((e) => ({
        eventType: e.eventType,
        payload: e.payload ?? Buffer.alloc(0),
        meta: e.meta ?? Buffer.alloc(0),
      })),
    };
    return new Promise((resolve, reject) => {
      this._client.append(request, (err, res) => {
        if (err) reject(err);
        else resolve(Number(res.version));
      });
    });
  }

  /**
   * Stream a single stream's events. Async-iterable.
   * @param {string} streamName
   * @param {{from?: number, limit?: number, direction?: string}} [opts]
   *   from: inclusive 1-based position, 0 = natural start.
   *   limit: <= 0 means unbounded.
   */
  readStream(streamName, { from = 0, limit = 0, direction = Direction.Forwards } = {}) {
    const call = this._client.readStream({
      streamName,
      from: String(from),
      limit: String(limit),
      direction,
    });
    return streamEvents(call);
  }

  /**
   * Stream a category's events in append order. Async-iterable.
   * The category is the part of a stream name before the first '-'
   * (e.g. stream "user-123" is in category "user").
   * @param {string} category
   * @param {{from?: number, limit?: number}} [opts]
   */
  readCategory(category, { from = 0, limit = 0 } = {}) {
    const call = this._client.readCategory({
      category,
      from: String(from),
      limit: String(limit),
    });
    return streamEvents(call);
  }

  /** Close the underlying channel. */
  close() {
    this._client.close();
  }
}

export default EventStoreClient;
