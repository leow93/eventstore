# 6. Derived in-memory indexes, rebuilt from the log on boot

Date: 2026-07-21

Status: Accepted (supersedes an earlier on-disk index design)

## Context

Reads by stream and category need offset lookups. An earlier iteration wrote
on-disk `stream.idx` / `category.idx` files alongside the log. That duplicated the
source of truth and added a second structure to keep crash-consistent with the log.

## Decision

Keep indexes **purely in memory** — maps of stream/category → ordered positions,
plus each stream's tip and the max global position. Persist nothing. Rebuild the
whole index by replaying the log on boot (`Index.Rebuild`). The log
([0001](0001-append-only-log-as-source-of-truth.md)) stays the only durable state.

## Consequences

- The index can never diverge from, or outlive, the log; no second file to keep
  crash-consistent.
- Simpler write path — no index file to sync.
- Boot cost is O(log size) to replay. Mitigated later by optional snapshots and/or
  per-segment parallel rebuild — see [0012](0012-segment-the-log.md).
- Index memory grows with distinct streams and events — see
  [0014](0014-logpos-packed-offset.md) for the per-entry footprint choice.
