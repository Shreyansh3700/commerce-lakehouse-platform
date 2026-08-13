# ADR 0001: Set up logical replication in Phase 1, defer the replication slot to Phase 2

## Status
Accepted

## Context
Debezium (Phase 2) needs PostgreSQL configured for logical replication to
stream CDC events. `wal_level=logical` requires a Postgres **restart** to
take effect.

## Decision
- `wal_level=logical`, `max_wal_senders`, `max_replication_slots` are set now,
  in Phase 1's `docker-compose.yml`, so Phase 2 never needs to touch Postgres
  config or bounce the container.
- `REPLICA IDENTITY FULL` is set on all 7 CDC-tracked tables now (needed so
  Debezium's `before` image on UPDATE/DELETE is complete, not just the PK).
- A `PUBLICATION commerce_cdc` is created now (cheap, no WAL retention cost).
- The `debezium` role (LOGIN REPLICATION + SELECT) is created now, unused
  until Phase 2.
- The actual **replication slot** is deliberately NOT created in Phase 1.
  An unconsumed slot causes Postgres to retain WAL indefinitely. Since no
  Debezium connector exists yet to consume it, creating the slot now would
  grow WAL unbounded for the entire duration of Phase 1. Phase 2's Debezium
  connector creates/consumes the slot itself on first startup.

## Consequences
Phase 2 is purely additive (stand up Debezium + register a connector) --
it never needs to modify Postgres configuration, roles, or table settings.
