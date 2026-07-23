# 2. Length-prefixed binary event record format

Date: 2026-04-24

Status: Accepted

## Context

Events must be serialised into the log in a way that supports forward scans and
lets a reader skip a record without decoding all of its fields.

## Decision

Each record is a compact little-endian binary layout: a leading `TotalLength`
(uint32), then length-prefixed variable fields (stream name, event type, payload,
meta) interleaved with fixed fields (position, global position, timestamp). The
leading `TotalLength` lets a scanner jump to the next record without decoding the
current one.

## Consequences

- Fast forward scan: advance by `TotalLength`.
- Compact, with no schema/reflection overhead.
- The format is bespoke; changing it is a migration — see [0011](0011-per-record-crc.md),
  which did exactly that.
- No per-record version/type discriminator byte yet (a possible future decision).
