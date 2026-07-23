# 14. Pack (segment, offset) into a uint64 LogPos

Date: 2026-07-22

Status: Accepted

## Context

With segments ([0012](0012-segment-the-log.md)), a position must identify both a
segment and a byte offset within it. The in-memory index
([0006](0006-derived-in-memory-indexes.md)) stores one position per event, so the
per-entry size directly drives index memory.

## Decision

Represent a position as `LogPos`, a `uint64` with the **segment number in the high
32 bits** and the **in-segment byte offset in the low 32 bits**. A 32-bit offset
caps a segment at 4 GiB (comfortably above the target segment size); a 32-bit
segment number allows ~4 billion segments. Today everything is segment 0.

## Consequences

- Keeps the index at 8 bytes per entry — the same footprint as a bare `int64`
  offset, and GC-friendly (no pointers).
- One value type threads through index, store, and log; implementing segments
  ([0012](0012-segment-the-log.md)) only starts populating real segment numbers.
- A hard 4 GiB cap per segment, enforced on append (this is what triggers
  roll-over in [0012](0012-segment-the-log.md)).
- Related: [0006](0006-derived-in-memory-indexes.md).
