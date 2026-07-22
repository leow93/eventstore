# 12. Segment the log into fixed-size files

Date: 2026-07-22

Status: Accepted (segmentation implemented; retention still to come)

## Context

The log is a single unbounded file. Space can never be reclaimed, and there is no
natural unit for backup, incremental replication, or Raft snapshots.

OS/filesystem file-size limits are **not** the driver: APFS (~8 EB) and ext4
(16 TiB) ceilings sit far beyond any operational limit we would hit first. The
driver is retention and clustering.

## Decision

Split the log into numbered, fixed-size **segment** files (target 256 MB–1 GB).
Exactly one is **active** (append + fsync); the rest are **sealed** (immutable,
read-only). A batch never spans a segment. The target size is soft — a single
over-large batch gets its own segment. Migration: adopt the existing single file as
segment 0.

## Consequences

- Retention becomes "delete whole sealed segments" (plus pruning their index
  entries).
- Sealed segments are the natural unit for backup, replication, and Raft snapshots
  (Epic 4).
- Recovery can parallelise across sealed segments; torn-tail tolerance is confined
  to the active segment (a sealed segment that won't decode is corruption — see
  [0011](0011-per-record-crc.md)).
- More moving parts: segment lifecycle, roll-over, multi-segment rebuild, index
  pruning.
- Related: [0001](0001-append-only-log-as-source-of-truth.md),
  [0013](0013-chunked-pread-reads.md), [0014](0014-logpos-packed-offset.md).

## Implementation notes

- `SegmentedLog` (in `log.go`) manages the segments; `Segment` (in `segment.go`)
  is a single append-only file. Files are named `%06d.seg` (`000000.seg`, …).
- Roll-over: `AppendBatch` seals the active segment (fsync + mark immutable) and
  opens the next when the active segment already holds data and the batch would
  push it past the target size. `DefaultSegmentSize` is 256 MiB; the hard ceiling
  is `MaxSegmentSize` (4 GiB, from the 32-bit offset in [0014](0014-logpos-packed-offset.md)).
- Reads take a brief `RLock` to resolve the target segment, then pread lock-free;
  normal appends never touch that lock, so only the rare roll-over blocks readers.
- Rebuild is `SegmentedLog.replay`: it scans each segment in order and enforces the
  recovery policy — a torn tail is tolerated only in the active (last) segment;
  a torn or corrupt record in a sealed segment fails loudly.
- Migration: a pre-segmentation `events.log` is renamed to `000000.seg` on first
  open (only when no `.seg` files exist yet).
- **Not yet implemented:** retention (`DropSegmentsBefore` + index pruning) — the
  next step. Segmenting is what makes it possible; nothing deletes segments today.
