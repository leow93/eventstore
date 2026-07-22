# Architecture Decision Records

Each file records **one** decision: its context, the decision itself, and its
consequences. Records are numbered in the order the decisions were made and are
kept even once superseded — the history is the point.

**Status legend:** _Accepted_ — in force. _Superseded_ — replaced by a later ADR
(linked). "Not yet implemented" marks an accepted decision whose code has not
landed yet.

## Foundation (storage engine)

| #  | Decision | Status |
|----|----------|--------|
| [0001](0001-append-only-log-as-source-of-truth.md) | Append-only log as the single source of truth | Accepted |
| [0002](0002-length-prefixed-binary-record-format.md) | Length-prefixed binary event record format | Accepted |
| [0003](0003-fsync-before-acknowledging-writes.md) | Durability: fsync before acknowledging a write | Accepted |
| [0004](0004-memory-mapped-reads.md) | Memory-mapped reads | Superseded by 0013 |
| [0005](0005-category-from-stream-name-prefix.md) | Extract the category from the stream name prefix | Accepted |
| [0006](0006-derived-in-memory-indexes.md) | Derived in-memory indexes, rebuilt from the log on boot | Accepted |

## Read & write semantics / API

| #  | Decision | Status |
|----|----------|--------|
| [0007](0007-single-writer-occ.md) | Single-writer with optimistic concurrency control | Accepted |
| [0008](0008-iterator-read-api.md) | Iterator-based read API | Accepted |
| [0009](0009-batch-appends-single-fsync.md) | Batch appends behind a single fsync | Accepted |
| [0010](0010-grpc-streaming-api.md) | gRPC API with server-streaming reads | Accepted |

## Durability & scaling (current work)

| #  | Decision | Status |
|----|----------|--------|
| [0011](0011-per-record-crc.md) | Per-record CRC-32C for corruption detection | Accepted |
| [0012](0012-segment-the-log.md) | Segment the log into fixed-size files | Accepted (not yet implemented) |
| [0013](0013-chunked-pread-reads.md) | Serve reads with chunked pread instead of mmap | Accepted (implemented) |
| [0014](0014-logpos-packed-offset.md) | Pack (segment, offset) into a uint64 LogPos | Accepted |
