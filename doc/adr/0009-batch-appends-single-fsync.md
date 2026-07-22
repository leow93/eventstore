# 9. Batch appends behind a single fsync

Date: 2026-07-21

Status: Accepted

## Context

fsync ([0003](0003-fsync-before-acknowledging-writes.md)) dominates write latency,
and it is a barrier: once it returns, every write that preceded it is durable. A
per-event fsync throws that leverage away.

## Decision

`AppendToStream` accepts a variadic batch and writes the whole batch with **one**
fsync (`AppendBatch`); a single append is just a batch of one. Durability is
unchanged — one fsync makes every write in the batch durable.

## Consequences

- ~9× faster for a 10-event batch; batch=1000 reaches ~180k ev/s versus ~240 ev/s
  at batch=1.
- Identical durability guarantee to a per-event fsync.
- A batch is all-or-nothing with respect to the fsync barrier.
- Related: [0003](0003-fsync-before-acknowledging-writes.md),
  [0007](0007-single-writer-occ.md).
