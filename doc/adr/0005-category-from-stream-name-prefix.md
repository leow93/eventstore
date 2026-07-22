# 5. Extract the category from the stream name prefix

Date: 2026-04-26

Status: Accepted

## Context

Message-DB-style consumers read by *category* (a class of streams), not only by
individual stream. We need a rule to derive a category from a stream name.

## Decision

The category is the substring before the **first dash**:

- `user-123` → `user`
- `inventory-abc-456` → `inventory`
- `global` (no dash) → its own category, `global`
- `-badprefix` (empty category) → invalid, `ErrStreamHasNoCategory`

## Consequences

- Zero-config categorisation — no separate metadata to maintain.
- Matches Message-DB conventions.
- Stream naming is load-bearing: a mistyped prefix silently changes the category.
- The category is derived per event and indexed — see [0006](0006-derived-in-memory-indexes.md).
