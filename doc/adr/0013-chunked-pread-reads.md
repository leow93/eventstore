# 13. Serve reads with chunked pread instead of mmap

Date: 2026-07-22

Status: Accepted (not yet implemented — build-order step 3)

## Context

Memory-mapped reads ([0004](0004-memory-mapped-reads.md)) are fast but bring
SIGBUS-on-fault (which kills the process), whole-file remap churn, and
reader/writer lock contention on tail reads — all of which worsen as the log grows.

## Decision

Replace mmap with `pread`. Because reads are sequential and iterator-shaped
([0008](0008-iterator-read-api.md)), read a large block (64 KB–1 MB) once and decode
many records from it in userspace (a `bufio`-style reader), refilling only when the
block is exhausted. This amortises the syscall over many events.

## Consequences

- No SIGBUS — I/O errors and truncation return catchable errors.
- No virtual-address-space growth, no whole-file remap, no reader/writer lock
  contention.
- Competitive on cold/large scans (explicit reads get kernel readahead).
- Some peak warm-scan throughput is lost versus zero-copy mmap — estimated within
  ~1.5–2×, to be confirmed by benchmark.

Supersedes [0004](0004-memory-mapped-reads.md).
