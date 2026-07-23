# 7. Single-writer with optimistic concurrency control

Date: 2026-07-21

Status: Accepted

## Context

Concurrent appends to the same stream must not violate ordering or the caller's
expected-version guarantee.

## Decision

Serialise all writes behind a single write mutex. Inside that critical section,
enforce optimistic concurrency via `ExpectedVersion`:

- `AnyVersion` (-1) skips the check.
- `N >= 0` requires the stream's current version to equal `N` (`0` = the stream
  must not exist yet).
- A mismatch returns `ErrWrongExpectedVersion`.

## Consequences

- A simple, correct check-then-append; global ordering is trivial to assign.
- OCC gives lost-update protection without holding locks across a round trip.
- A single writer caps write concurrency — acceptable, since writes are
  fsync/append-bound, not CPU-bound.
- Per-stream sharded locking is a future option if the single writer becomes the
  bottleneck.
