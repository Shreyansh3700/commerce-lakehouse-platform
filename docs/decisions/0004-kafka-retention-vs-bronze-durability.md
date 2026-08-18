# ADR 0004: Kafka topics get short, time-based retention -- Bronze is the durable store

## Status
Accepted

## Context
Spec section 8 asks for "appropriate retention" on CDC topics. Spec section
26 requires that backfills/replays never need to re-read PostgreSQL, and
explicitly calls this out as the reason the raw CDC Bronze layer must be
retained.

## Decision
All `cdc.<table>` topics use `cleanup.policy=delete` with a 3-day
(`retention.ms=259200000`) retention window -- **not** `compact`, and not a
long/infinite retention.

Compaction is deliberately avoided even though "compact the CDC topic" is a
common pattern elsewhere: compaction keeps only the latest value per key,
which would destroy the full sequence of intermediate state transitions
(e.g. every order-status change) that Bronze is required to retain in full.

3 days is chosen as enough buffer for the Bronze writer to be down over a
long weekend and catch up from Kafka, while being explicit that Kafka is
**not** the platform's backfill/replay mechanism -- Bronze (Iceberg) is.

## Consequences
Kafka disk usage stays bounded regardless of platform age or data volume.
Any replay/backfill need beyond a few days must read from Bronze, not from
Kafka -- this is enforced by Kafka's own retention, not just a convention.
