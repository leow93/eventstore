# 13. Serve reads with chunked pread instead of mmap

Date: 2026-07-22

Status: Accepted (implemented)

## Context

Memory-mapped reads ([0004](0004-memory-mapped-reads.md)) are fast but bring
SIGBUS-on-fault (which kills the process), whole-file remap churn, and
reader/writer lock contention on tail reads — all of which worsen as the log grows.

## Decision

Replace mmap with `pread`, in two shapes matched to the two access patterns:

- **Random single-record reads** (the store's stream/category iterators, which
  resolve scattered offsets): read a small speculative window (one page, 4 KiB)
  in a single `pread`. Records at or below that size decode from the window in one
  syscall; a larger record costs a second, exactly-sized read.
- **Sequential replay** (index rebuild on boot): a `bufio` reader over the log with
  a 1 MiB buffer, so a full scan costs ~(log size / 1 MiB) syscalls rather than one
  per record.

Reads take no lock: `DataLog` publishes its durable size as an atomic that a reader
snapshots as the upper bound, then preads via the shared file descriptor.

## Consequences

- No SIGBUS — I/O errors and truncation return catchable errors.
- No virtual-address-space growth, no whole-file remap, and reads never take the
  write lock, so they no longer contend with an in-flight append or its fsync.
- Competitive on cold/large scans (explicit reads get kernel readahead).
- **Warm random reads are ~10× slower than mmap.** A like-for-like benchmark on an
  M2 Pro (same harness, same data): mmap ~0.10 µs/read (~9.6M reads/s, zero-copy),
  pread ~1.1 µs/read (~900k reads/s). The gap is the syscall + copy that mmap
  avoided entirely — this is far larger than the ~1.5–2× originally feared, though
  ~900k reads/s is still ample in absolute terms. This is the price paid for the
  robustness wins above.
- The 4 KiB speculative buffer (4.3 KB/op) dominates the *avoidable* cost: a 256 B
  window measures ~0.65 µs/read, and pooling the buffer would cut allocation
  further. But the pread syscall itself (~0.5 µs on macOS) is a floor pread cannot
  get under — tuning narrows the gap to mmap to ~6×, not away.

Supersedes [0004](0004-memory-mapped-reads.md).

## Revisit when

The ~10× cost is accepted deliberately: pread is simpler and ~900k reads/s is ample
for now, so it is not worth extra machinery to claw back speed we do not need yet.
Revisit if read throughput becomes a real constraint — the likely path is
**bounded per-segment mmap** (map hot segments, munmap cold ones) once the log is
segmented ([0012](0012-segment-the-log.md)), which recovers zero-copy reads *and*
bounds the VA-space/remap problem that motivated leaving mmap. Cheaper interim
levers (a smaller speculative window, a pooled read buffer) narrow the gap to ~6×
but do not remove the syscall floor.
