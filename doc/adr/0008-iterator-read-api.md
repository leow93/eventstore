# 8. Iterator-based read API

Date: 2026-07-21

Status: Accepted

## Context

A read can return a large number of events. Returning a slice forces the whole
result to be materialised in memory before the caller sees the first event.

## Decision

Read APIs return Go 1.25 fallible iterators, `iter.Seq2[*Event, error]`:
`ReadStreamForwards`, `ReadStreamBackwards`, `ReadCategory`. `from` is an inclusive
1-based position; `limit <= 0` means unbounded. A read error is yielded once and
then iteration stops.

## Consequences

- Streaming, lazy reads: the caller controls how far to consume; memory stays
  bounded.
- Maps cleanly onto gRPC server-streaming — see [0010](0010-grpc-streaming-api.md).
- Errors surface mid-iteration rather than up front, so callers must check the
  error slot on each step.
