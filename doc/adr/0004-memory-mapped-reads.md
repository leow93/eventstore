# 4. Memory-mapped reads

Date: 2026-04-24

Status: Superseded by [0013](0013-chunked-pread-reads.md) (mmap has been removed)

## Context

Reads must resolve a byte offset to an event cheaply. A `read()` syscall per event
is costly.

## Decision

`mmap` the log file into the process address space and decode events directly from
the mapped `[]byte` (zero-copy up to the decode). Refresh the mapping lazily as the
file grows.

## Consequences

- ~1M ev/s on warm sequential reads, with no per-event syscall.
- The whole file is remapped on growth, and cold regions are never unmapped.
- A read that needs freshly-appended bytes takes the exclusive write lock to remap,
  serialising readers against the writer.
- A SIGBUS on I/O error or truncation kills the process rather than returning an
  error.

These last three drawbacks motivated the move to chunked `pread` —
see [0013](0013-chunked-pread-reads.md).
