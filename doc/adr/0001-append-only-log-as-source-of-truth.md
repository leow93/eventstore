# 1. Append-only log as the single source of truth

Date: 2026-04-24

Status: Accepted

## Context

An event store needs a durable, totally-ordered record of every event. We must
decide what holds the authoritative state that everything else is derived from.

## Decision

All events are appended to a single append-only log file (opened
`O_APPEND|O_CREATE|O_RDWR`). The log is the **only** durable source of truth.
Every other structure — indexes, stream tips, positions — is derived from it and
can be reconstructed by replaying it.

## Consequences

- Writes are sequential appends: simple and fast.
- Derived structures can always be rebuilt, so they can never be authoritative or
  diverge from the data.
- A natural fit for later replication / Raft (ship the log).
- Reads need an index to avoid scanning the whole log — see [0006](0006-derived-in-memory-indexes.md).
- Reclaiming space needs more than truncation — see [0012](0012-segment-the-log.md).
