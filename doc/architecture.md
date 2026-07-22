# Architecture

An append-only event store. The **data log** on disk is the single source of
truth; the **index** is a derived, in-memory view rebuilt by replaying the log on
boot. Writes are single-writer with optimistic concurrency control; reads are
served as fallible iterators over log positions.

See the [Architecture Decision Records](adr/README.md) for the decisions behind
the design — notably
[0001 — append-only log as source of truth](adr/0001-append-only-log-as-source-of-truth.md),
[0011 — per-record CRC](adr/0011-per-record-crc.md),
[0013 — chunked pread reads](adr/0013-chunked-pread-reads.md), and
[0012 — segment the log](adr/0012-segment-the-log.md).

## Component overview

```mermaid
flowchart TB
    client["gRPC client"]

    subgraph proc["eventstored process"]
        subgraph net["Transport"]
            grpc["gRPC server<br/><i>server.Server</i><br/>Append · ReadStream · ReadCategory"]
        end

        subgraph core["Store — eventstore.Store"]
            store["writeMu (single-writer)<br/>optimistic concurrency (ExpectedVersion)<br/>assigns Position; orchestrates read slices"]
        end

        subgraph derived["In-memory index — eventstore.Index (derived, not durable)"]
            idx_streams["streams: name → []LogPos"]
            idx_cats["categories: name → []LogPos"]
            idx_gp["maxGlobalPosition"]
        end

        subgraph logsub["Data log — eventstore.DataLog (source of truth)"]
            writepath["write: append + single fsync per batch<br/>assigns GlobalPosition"]
            readpath["read: pread (speculative window;<br/>buffered sequential replay)"]
        end
    end

    disk[("events.log<br/>append-only, CRC-per-record")]

    client -->|"protobuf RPC"| grpc
    grpc -->|"AppendToStream / ReadStream* / ReadCategory"| store

    store -->|"check version / read offsets"| idx_streams
    store --> idx_cats
    store -->|"AppendBatch"| writepath
    store -->|"ReadAt(LogPos)"| readpath

    writepath -->|"write() + fsync()"| disk
    readpath -->|"pread (lock-free)"| disk

    writepath -.->|"Apply(evt, pos) after durable"| idx_streams
    disk -.->|"Rebuild replays log on boot"| derived

    classDef truth fill:#1e3a5f,stroke:#4a90d9,color:#fff
    classDef mem fill:#3d2f1e,stroke:#c9902a,color:#fff
    class logsub,disk truth
    class derived mem
```

## Write path — `AppendToStream`

A batch is checked against the in-memory stream tip, appended to the log with a
single fsync, and only then reflected in the index. The `writeMu` mutex makes the
check-then-append critical section atomic (single writer).

```mermaid
sequenceDiagram
    participant C as gRPC client
    participant S as server.Server
    participant St as Store
    participant Ix as Index (in-mem)
    participant L as DataLog
    participant D as events.log

    C->>S: Append(stream, expectedVersion, events)
    S->>St: AppendToStream(...)
    activate St
    Note over St: lock writeMu (single writer)
    St->>Ix: StreamVersion(stream)
    Ix-->>St: current version
    alt expected ≠ AnyVersion and ≠ current
        St-->>S: ErrWrongExpectedVersion
        S-->>C: gRPC Aborted
    else version matches
        Note over St: assign each event's Position (1-based)
        St->>L: AppendBatch(events)
        activate L
        Note over L: assign GlobalPosition (monotonic)<br/>encode records (+ CRC-32C)
        L->>D: write(batch)
        L->>D: fsync()  ← durability barrier, once per batch
        L-->>St: []LogPos
        deactivate L
        St->>Ix: Apply(evt, pos)  (streams, categories, maxGP)
        St-->>S: new stream version
        S-->>C: AppendResponse{version}
    end
    Note over St: unlock writeMu
    deactivate St
```

## Read path — stream / category iterators

Reads never take the write lock: the log publishes its durable size as an atomic,
and each read snapshots it and preads below it. The store resolves an ordered slice
of `LogPos` from the index, then streams each record from the log as an
`iter.Seq2[*Event, error]`. gRPC forwards each event to the client, stopping early
on client disconnect.

```mermaid
sequenceDiagram
    participant C as gRPC client
    participant S as server.Server
    participant St as Store
    participant Ix as Index (in-mem)
    participant L as DataLog
    participant D as events.log

    C->>S: ReadStream(stream, from, limit, direction)
    S->>St: ReadStreamForwards / Backwards(...)
    St->>Ix: StreamOffsets(stream) → []LogPos
    Note over St: forwardSlice / backwardSlice<br/>(apply from + limit)
    St-->>S: iter.Seq2[*Event, error]
    loop for each LogPos in slice
        S->>St: next()
        St->>L: ReadAt(pos)
        L->>D: pread record (speculative 4 KiB window)
        L->>L: Decode + verify CRC
        L-->>St: *Event
        St-->>S: (event, nil)
        S->>C: stream.Send(event)
    end
    Note over S,C: stops early on client cancel / read error
```

## Boot / recovery — `Store.Open`

The index holds no durable state. On open, the log is replayed from offset 0 with
a buffered sequential reader (`logReader`, 1 MiB buffer); each complete record is
applied to the index. A **torn tail** (partial trailing record from a crash between
write and fsync) stops the scan cleanly, whereas a **CRC mismatch** is real
corruption and fails the boot loudly.

```mermaid
flowchart TB
    open["Store.Open(dir)"] --> newlog["NewDataLog(events.log)"]
    newlog --> rebuild["Index.Rebuild — buffered replay from offset 0"]
    rebuild --> read["logReader.next() → Decode + verify CRC"]
    read --> ok{"decode ok?"}
    ok -->|"yes"| apply["Index.Apply(evt, pos)"]
    apply --> more{"more records?"}
    more -->|"yes"| read
    more -->|"io.EOF (clean end)"| seed
    ok -->|"ErrChecksumMismatch"| corrupt["mid-log corruption<br/>FAIL boot loudly"]
    ok -->|"io.ErrUnexpectedEOF / short record"| torn["torn tail<br/>stop scan cleanly, truncate"]
    torn --> seed
    seed["SetGlobalPosition(index.MaxGlobalPosition)"] --> ready["Store ready"]

    classDef bad fill:#4a1e1e,stroke:#d94a4a,color:#fff
    class corrupt bad
```

## On-disk record format

Each record is length-prefixed and ends with a CRC-32C over every preceding byte
(length prefix included). Trailing placement means a short/torn write fails the
length check (recoverable) while a bit-flip in a complete record fails the CRC
check (unrecoverable) — see [ADR 0011](adr/0011-per-record-crc.md).

```mermaid
flowchart LR
    subgraph rec["Event record (little-endian)"]
        direction LR
        f1["TotalLength<br/>u32"]
        f2["StreamNameLen u16<br/>+ StreamName"]
        f3["EventTypeLen u16<br/>+ EventType"]
        f4["Position u64"]
        f5["GlobalPosition u64"]
        f6["Timestamp u64"]
        f7["PayloadLen u32<br/>+ Payload"]
        f8["MetaLen u32<br/>+ Meta"]
        f9["CRC-32C<br/>u32"]
        f1 --- f2 --- f3 --- f4 --- f5 --- f6 --- f7 --- f8 --- f9
    end
    note["CRC covers TotalLength … Meta (record[:totalLen-4])"]
    f9 -.-> note
```

## Log positions & segmentation (planned)

A `LogPos` packs a 32-bit segment number and a 32-bit in-segment byte offset into
one `uint64`, keeping the index at 8 bytes per entry ([ADR 0014](adr/0014-logpos-packed-offset.md)).
Today there is a single segment (number 0);
[ADR 0012](adr/0012-segment-the-log.md) evolves this into numbered fixed-size
segment files (one active, the rest sealed and immutable) to enable retention,
backup, and clustering.

```mermaid
flowchart LR
    subgraph now["Today — single segment"]
        s0["000000 (active)<br/>events.log"]
    end
    subgraph planned["Planned — segmented log (ADR 0012)"]
        p0["000000.seg<br/>sealed, immutable"]
        p1["000001.seg<br/>sealed, immutable"]
        p2["000002.seg<br/>active (append + fsync)"]
        p0 --- p1 --- p2
    end
    now --> planned

    subgraph pos["LogPos = uint64"]
        hi["high 32 bits<br/>segment number"]
        lo["low 32 bits<br/>offset within segment"]
    end
```
