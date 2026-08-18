# CDC (Phase 2)

Postgres WAL -> Debezium -> Kafka -> PySpark Structured Streaming -> Iceberg
Bronze. No polling anywhere in this chain.

## Connector configuration

`ingestion/debezium/postgres-connector.json`, registered idempotently by
`ingestion/debezium/register-connector.sh` (`PUT /connectors/postgres-cdc/config`
-- creates or updates in place, safe to rerun; also available via
`make register-connector`).

Notable non-default settings and why:

| Setting | Value | Why |
|---|---|---|
| `publication.autocreate.mode` | `disabled` | Reuses the `commerce_cdc` publication Phase 1 already created (see [ADR 0001](decisions/0001-logical-replication-in-phase1.md)) rather than letting Debezium manage it. |
| `slot.drop.on.stop` | `false` | A Kafka Connect restart must not drop the replication slot -- that was the whole point of Phase 1 deferring slot creation to Debezium's ownership. |
| `snapshot.mode` | `initial` | One-time snapshot of existing rows, then seamless continuous WAL streaming -- satisfies spec section 7 without custom code. |
| `schemas.enable` (both converters) | `false` | Plain JSON envelopes, no Confluent Schema Registry/Avro -- Schema Registry isn't in the spec's tech stack. |
| `decimal.handling.mode` | `string` | With `schemas.enable=false` there's no logical-type metadata to interpret Postgres `NUMERIC` columns (price/amount/total_amount) correctly otherwise. |
| `tombstones.on.delete` | `false` | Without this, every DELETE produces a second null-value Kafka record; suppressing it here is simpler than special-casing it in the Bronze writer. |
| `transforms=route` (RegexRouter) | `cdc\.commerce\.(.*)` -> `cdc.$1` | Debezium's default topic name is `<prefix>.<schema>.<table>`; this produces the spec's exact `cdc.<table>` names. **No** unwrap/flatten SMT -- Bronze needs the full envelope (`before`/`after`/`op`/`source`), not just the new row. |
| `topic.creation.enable` | `true` | Lets Kafka Connect auto-create all 7 topics with the partition count/retention below, no separate topic-creation script. |

## Partitioning strategy

Message key = the row's primary key -- this is Debezium's **default**
behavior, combined with Kafka's default hash partitioner. No `transforms`
are needed to get this; it's automatic, and is what preserves per-entity
ordering within a partition (all events for `order_id=42` always land in the
same partition, in commit order).

3 partitions per topic, replication factor 1 (single-broker dev cluster --
[ADR 0003](decisions/0003-kraft-no-zookeeper.md)). Increasing partition
count later is possible (spec's "Scalable" NFR) but changes the key->partition
mapping going forward -- it does not preserve ordering guarantees across
that change for existing history. This is fine because Kafka is not the
platform's ordering-of-record source of truth for anything beyond in-flight
transport: `source_lsn`/`transaction_id`, captured in Bronze, are.

## Retention

3-day time-based retention (`cleanup.policy=delete`, not `compact`) on every
`cdc.<table>` topic. See [ADR 0004](decisions/0004-kafka-retention-vs-bronze-durability.md)
-- Kafka is transient transport, Bronze is the durable historical store.
Compaction is specifically avoided because it would keep only the latest
value per key, destroying the sequence of intermediate state transitions
Bronze must retain.

## Dead-letter queue

See [ADR 0006](decisions/0006-dlq-lives-in-bronze-writer-not-kafka-connect.md).
Kafka Connect's built-in DLQ mechanism is a sink-connector feature and
doesn't apply to Debezium's source-connector role here. Malformed events
actually originate in the Bronze writer's own envelope parsing, so that's
where quarantining happens: bad rows go to both a `cdc.<table>.dlq` Kafka
topic and the `bronze.dlq_events` Iceberg table (original event, error
message/type, source topic/partition/offset, failure timestamp) in the same
micro-batch -- one bad row never fails the rest of the batch.

## Monitoring progress (interim, pre-Phase-5)

Full Prometheus/Grafana consumer-lag dashboards are Phase 5 scope. In the
meantime, `make consumer-lag` does **not** use `kafka-consumer-groups.sh` --
confirmed by testing, Spark Structured Streaming's Kafka source never
commits offsets to the broker under `kafka.group.id`, even though the
option is set on each query, so the Bronze writer's queries never appear in
`kafka-consumer-groups.sh --list` at all (checked directly: the list comes
back empty). `kafka.group.id` only labels the underlying consumer for
metadata/polling purposes; it doesn't opt into the group-coordinator
protocol Spark deliberately bypasses in favor of its own checkpoint.

The real, working way to see per-table progress today is each streaming
query's own progress log (`"Streaming query made progress: {...}"`, one per
table per micro-batch, containing input/processed row counts and the Kafka
offset range consumed) -- `make consumer-lag` greps these out of
`docker compose logs spark-bronze-writer` instead.

## Delivery semantics per hop (spec section 28)

| Hop | Semantics | Notes |
|---|---|---|
| Postgres -> Debezium | At-least-once | A crash between the slot's LSN advancing and Connect's offset commit can replay WAL on restart; no silent loss. |
| Debezium -> Kafka | At-least-once (producer is idempotent for its own retries) | Idempotent retries prevent the *producer* from double-sending, but don't erase the at-least-once inherited from the hop above. |
| Kafka -> Spark | At-least-once | Spark Structured Streaming tracks progress entirely in its own **checkpoint**. `kafka.group.id` is set per query, but -- confirmed empirically -- Spark's Kafka source never commits offsets under that group id to the broker, so it does **not** make the query visible to `kafka-consumer-groups.sh` (see the note below). A crash before checkpoint-advance re-reads and reprocesses the same micro-batch. |
| Spark -> Iceberg | At-least-once, atomic per snapshot | Iceberg commits are all-or-nothing (readers never see a partial write). Because checkpoint-advance happens strictly *after* a successful commit, a crash between those two steps causes the same batch to be re-appended as a **new** snapshot on restart. |

**Overall: effectively-once, not exactly-once.** Every hop guarantees no
silent data loss; duplicates are possible at two specific boundaries
(Debezium/Postgres restart, Spark/Iceberg restart). Bronze's `event_id`
(`sha2(kafka_topic|kafka_partition|kafka_offset, 256)`) does not prevent
these duplicates from landing in Bronze -- it exists so Phase 3's Silver
MERGE has a stable key to deduplicate on. Correctness is restored
downstream, not by preventing duplicates from being written.

## Deviation from the spec's repo tree

No `infrastructure/kafka/` config files exist -- like Postgres's `wal_level`
in Phase 1, Kafka's KRaft/broker config is entirely environment variables on
the `kafka` service in the single root `docker-compose.yml`, not a mounted
config file. See `ingestion/kafka/README.md`.
