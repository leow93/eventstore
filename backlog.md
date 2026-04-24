# Backlog 

## Epic 1: Foundation – The Raw Storage Engine
Before you can index or cluster, you need a rock-solid, crash-safe way to write bytes to disk.

[x] Task 1.1: Implement the Append-Only Data Log

[ ] Open files using Go's os package with os.O_APPEND|os.O_CREATE|os.O_WRONLY.

[ ] Define the binary encoding format for an Event Record (Stream Name length, Stream Name, Event Type, Position, Timestamp, Payload length, Payload).

[ ] Task 1.2: Implement File Syncing & Durability

[ ] Implement an fsync mechanism (via file.Sync()) to ensure OS buffers are flushed to physical disk.

[ ] Add configuration for sync strategies (e.g., sync after every write vs. batch syncing every few milliseconds).

[ ] Task 1.3: Implement Memory-Mapped Reads (mmap)

[ ] Use syscall.Mmap to map the Data Log directly into the Go process address space as a []byte for zero-copy, highly performant reads.

## Epic 2: Custom Indexing & Memory Management
Building your from-scratch index files and the GC-optimized memory structures required for OCC.

[ ] Task 2.1: GC-Friendly In-Memory Stream Tracker

[ ] Implement a fast string hashing function (e.g., xxHash) for stream names.

[ ] Create a map[uint64]uint64 (StreamHash -> MaxStreamPosition) to track the current tip of every stream without triggering Go's Garbage Collector.

[ ] Task 2.2: The On-Disk Stream Index

[ ] Create an append-only stream.idx file.

[ ] Implement writing fixed-width binary records (StreamHash, StreamPosition, DataLogOffset) to this file whenever an event is written to the Data Log.

[ ] Task 2.3: The On-Disk Category Index

[ ] Implement category extraction (parsing the prefix before the dash in category-id).

[ ] Create an append-only category.idx file.

[ ] Implement writing fixed-width binary records (CategoryHash, GlobalPosition, DataLogOffset).

[ ] Task 2.4: Crash Recovery & Snapshots

[ ] Implement a boot sequence that scans stream.idx to rebuild the in-memory map[uint64]uint64.

[ ] (Optional) Implement periodic index snapshots (snapshot.bin) so the system doesn't have to scan the entire index file on boot.

## Epic 3: Core Read & Write Semantics (Message-DB Style)
Tying the indices and data log together to fulfill your core querying requirements.

[ ] Task 3.1: Write to a Stream with Optimistic Concurrency Control (OCC)

[ ] Implement the write handler: Check the incoming ExpectedVersion against the in-memory stream map.

[ ] Reject with a ConcurrencyException if they do not match.

[ ] If valid, lock the specific stream (using sharded mutexes or channels), append to the Data Log, append to the indices, and update the in-memory map.

[ ] Task 3.2: Read a Stream Forwards / From Position

[ ] Query stream.idx for a specific stream_position.

[ ] Resolve the physical byte offset, jump to that offset in the mmap Data Log, and decode events forwards.

[ ] Task 3.3: Read a Stream Backwards

[ ] Query stream.idx for the tip of the stream (or a specific position).

[ ] Follow the offsets to the Data Log, read the events, and yield them to the client in reverse order.

[ ] Task 3.4: Read a Category in Order / From Position

[ ] Query category.idx for a specific global_position (or start from 0).

[ ] Resolve offsets, jump into the Data Log, and decode events strictly belonging to that category.

## Epic 4: Distributed Clustering (Raft & Quorum)
Transforming the single-node engine into a distributed, replicated, fault-tolerant cluster.

[ ] Task 4.1: Integrate hashicorp/raft

[ ] Implement the Raft FSM (Finite State Machine) interface in Go.

[ ] Wire the Apply() method so that committed Raft log entries actually trigger Epic 3's write logic.

[ ] Task 4.2: Cluster Bootstrapping & Leader Election

[ ] Implement configuration for a node to start as a seed node or join an existing cluster.

[ ] Ensure all write requests sent to followers are either rejected (with a "Go to Leader" redirect) or proxied internally to the Leader.

[ ] Task 4.3: Quorum Writes

[ ] Configure Raft to require a majority acknowledgment before returning a success response to the client.

[ ] Task 4.4: Quorum Reads

[ ] Implement linearizable reads: When a client requests a strict read, ensure the node verifies its leadership status with the cluster before serving the read from its local disk.

## Epic 5: Networking & API
Exposing the database to the outside world.

[ ] Task 5.1: Define Protocol Buffers

[ ] Write the .proto files defining the AppendRequest, ReadStreamRequest, ReadCategoryRequest, and the respective responses.

[ ] Task 5.2: Implement gRPC Server

[ ] Generate the Go gRPC code.

[ ] Implement the server interfaces, connecting incoming network requests to your underlying Raft and Storage engine logic.

[ ] Utilize gRPC server-side streaming for the read operations so clients can stream large batches of events seamlessly. operations so clients can stream large batches of events seamlessly.
