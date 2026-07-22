# 10. gRPC API with server-streaming reads

Date: 2026-07-21

Status: Accepted

## Context

The store must be reachable over the network with a typed, cross-language API.

## Decision

Expose the store over gRPC (`proto/eventstore.proto` → generated `eventstorepb`).
`Append` is unary; `ReadStream` and `ReadCategory` are **server-streaming**, mapping
the iterator API ([0008](0008-iterator-read-api.md)) onto the wire. Error mapping:
OCC failure → `codes.Aborted`; bad category or empty batch → `InvalidArgument`.

## Consequences

- Cross-language clients; streaming reads without buffering whole result sets.
- A clear, typed error-code contract.
- Adds a gRPC/protobuf dependency and a codegen step (`make proto`).
- Related: [0008](0008-iterator-read-api.md).
