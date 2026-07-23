# 3. Durability: fsync before acknowledging a write

Date: 2026-04-24

Status: Accepted

## Context

A write is only durable once the OS has flushed it to physical disk. Buffered
writes are lost on a crash or power failure, so we must decide when a write counts
as committed.

## Decision

Call `file.Sync()` (fsync) after writing and **before** acknowledging the write to
the caller. A write is not considered committed until fsync returns.

## Consequences

- Acknowledged writes survive a crash.
- fsync is the dominant cost of a write; a per-event fsync caps throughput at
  ~240 ev/s. Addressed by batching — see [0009](0009-batch-appends-single-fsync.md).
- A configurable sync strategy (fsync-per-write vs. time/size-batched) remains a
  possible future option.
