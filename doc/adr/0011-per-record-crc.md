# 11. Per-record CRC-32C for corruption detection

Date: 2026-07-22

Status: Accepted

## Context

On replay, a record that won't decode is ambiguous: it could be a **torn tail** (a
crash mid-write — truncate and continue) or **mid-log corruption** (must fail
loudly). A length-prefix-only format ([0002](0002-length-prefixed-binary-record-format.md))
cannot tell them apart, and a flipped length byte can even mis-frame corruption as
a plausible record. Cheapest to fix before real data exists, since it changes the
on-disk format.

## Decision

Append a trailing **CRC-32C** (Castagnoli) covering every preceding byte of the
record (length-prefix inclusive). `Decode` verifies it *before* trusting any field
length and returns `ErrChecksumMismatch` on failure. `Index.Rebuild` fails loudly
on a mismatch; any other decode failure is treated as a torn tail and stops the
scan cleanly.

## Consequences

- Recovery distinguishes a recoverable torn tail from unrecoverable corruption,
  which prevents silently discarding durable, acknowledged events.
- Enables the segmented log's "a sealed segment must be intact" invariant —
  see [0012](0012-segment-the-log.md).
- CRC-32C is hardware-accelerated (SSE4.2 / ARM64) — negligible next to fsync.
- Changed the on-disk format ([0002](0002-length-prefixed-binary-record-format.md));
  done before any production data existed, so no migration was provided.
