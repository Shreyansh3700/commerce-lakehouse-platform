# CDC (Phase 2 -- not yet implemented)

This document will cover the Debezium connector configuration, topic
routing, and Bronze ingestion once Phase 2 begins.

What Phase 1 already put in place for this:
- `wal_level=logical`, `REPLICA IDENTITY FULL` on all 7 core tables, and the
  `commerce_cdc` publication (see
  [ADR 0001](decisions/0001-logical-replication-in-phase1.md)).
- A dedicated `debezium` Postgres role (LOGIN REPLICATION + SELECT).

Not yet done (Phase 2 scope, per spec section 7-8):
- Debezium connector registration and configuration.
- Kafka topic creation/partitioning strategy.
- Dead-letter topic handling.
