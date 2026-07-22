// Runnable demo of the eventstore Node client.
//
//   1. start the server:  go run ./cmd/eventstored   (listens on :50051)
//   2. install deps:       cd clients/nodejs && npm install
//   3. run this:           npm run example
//
// Point at a different server with EVENTSTORE_ADDR=host:port.

import grpc from '@grpc/grpc-js';
import { EventStoreClient, Direction, NoStream, AnyVersion, jsonEvent } from './client.js';

const address = process.env.EVENTSTORE_ADDR ?? 'localhost:50051';
const client = new EventStoreClient(address);

// Unique-ish stream id per run so re-runs don't collide on the version check.
const id = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
const stream = `user-${id}`; // category "user"

async function main() {
  // --- Append (expecting a brand-new stream) ------------------------------
  let version = await client.append(
    stream,
    [
      jsonEvent('UserRegistered', { name: 'Ada', email: 'ada@example.com' }),
      jsonEvent('EmailVerified', { email: 'ada@example.com' }),
    ],
    { expectedVersion: NoStream },
  );
  console.log(`appended 2 events to ${stream}, version -> ${version}`);

  // --- Append again, this time asserting the version we just got ----------
  version = await client.append(
    stream,
    [jsonEvent('NameChanged', { name: 'Ada Lovelace' }, { source: 'example.js' })],
    { expectedVersion: version },
  );
  console.log(`appended 1 more event, version -> ${version}`);

  // --- Demonstrate the optimistic-concurrency conflict --------------------
  try {
    await client.append(stream, [jsonEvent('NameChanged', { name: 'nope' })], {
      expectedVersion: NoStream, // wrong: the stream already exists
    });
  } catch (err) {
    if (err.code === grpc.status.ABORTED) {
      console.log(`OCC conflict as expected: ${err.details}`);
    } else {
      throw err;
    }
  }

  // --- Read the stream forwards -------------------------------------------
  console.log(`\nreading ${stream} forwards:`);
  for await (const e of client.readStream(stream, { from: 0, direction: Direction.Forwards })) {
    console.log(
      `  #${e.position} ${e.eventType} @ ${e.timestamp.toISOString()} -> ${JSON.stringify(e.payloadJson())}`,
    );
  }

  // --- Read the stream backwards, limited ---------------------------------
  console.log(`\nreading ${stream} backwards (limit 2):`);
  for await (const e of client.readStream(stream, { limit: 2, direction: Direction.Backwards })) {
    console.log(`  #${e.position} ${e.eventType}`);
  }

  // --- Read the whole "user" category -------------------------------------
  console.log(`\nreading category "user" (limit 1000):`);
  for await (const e of client.readCategory('user', { limit: 1000 })) {
    console.log(`  [global ${e.globalPosition}] ${e.streamName} ${e.eventType}`);
  }
}

main()
  .catch((err) => {
    console.error('error:', err.details ?? err.message ?? err);
    process.exitCode = 1;
  })
  .finally(() => client.close());
