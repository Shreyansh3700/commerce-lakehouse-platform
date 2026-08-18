# ADR 0006: Dead-letter handling lives in the Bronze writer, not Kafka Connect's built-in DLQ

## Status
Accepted

## Context
Spec sections 8 and 25 require malformed/unprocessable events to be routed
to a dead-letter topic rather than silently dropped or stopping the
pipeline. Kafka Connect has a built-in `errors.deadletterqueue.topic.name`
mechanism.

## Decision
Kafka Connect's built-in DLQ is a **sink**-connector feature -- it catches
exceptions converting/transforming records consumed *from* Kafka. Debezium
here is a **source** connector (Postgres -> Kafka); it has no equivalent
"malformed incoming event" case in that sense; a WAL record it can't decode
would fail the connector task entirely, not produce a routable bad message.

Realistic malformed events in this pipeline actually originate downstream,
in the Bronze writer's own envelope parsing (unexpected JSON shape, a
missing required field, a future schema-drift edge case). So DLQ handling
is implemented in the Bronze writer application:
- `cdc.<table>.dlq` Kafka topics (one per entity, matching the spec's own
  `cdc.orders`/`cdc.orders.dlq` example) receive the raw failed message.
- `bronze.dlq_events` (Iceberg) durably stores the same record plus
  `error_message`/`error_type`/`source_topic`/`kafka_partition`/
  `kafka_offset`/`failed_at`, for inspection and future replay once Silver
  (Phase 3) exists to reprocess corrected events into.

## Consequences
A parsing failure for one row quarantines just that row -- the rest of the
micro-batch still commits to Bronze, and the streaming query itself never
crashes on bad data.
