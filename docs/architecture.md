# Architecture

See `real_time_commerce_data_platform.md` (section 3) for the full target
architecture. This project is being built in 5 phases; each phase's
components are documented here as they're implemented.

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 1 | Postgres schema + data generator | Done |
| 2 | Debezium/Kafka CDC + Iceberg Bronze | Done |
| 3 | Silver/Gold + dbt + data quality | Not started |
| 4 | Streaming analytics (PySpark) + late events + schema evolution | Not started |
| 5 | Airflow + Prometheus/Grafana + OpenLineage/Marquez + FastAPI + benchmarks | Not started |

## Phase 1: Postgres + Data Generator

```
PostgreSQL (commerce schema)
    |
    +-- 01_roles.sh / 02_schema.sql / 03_triggers_and_cdc.sql / 04_reference_data.sql
    |
    +-- generator/seeding/bulk_load.py   (one-time, COPY-based initial load)
    |
    +-- generator/streaming/simulator.py (long-running, rate-driven ongoing changes)
```

Postgres is already configured for logical replication (`wal_level=logical`,
`REPLICA IDENTITY FULL`, a `commerce_cdc` publication) so Phase 2 can add
Debezium without touching Postgres again -- see
[ADR 0001](decisions/0001-logical-replication-in-phase1.md).

Details: [data_model.md](data_model.md).

## Phase 2: Debezium/Kafka CDC + Iceberg Bronze

```
PostgreSQL WAL
    |
    v
Debezium (Kafka Connect, postgres-cdc connector)
    |
    v
Kafka (cdc.customers, cdc.products, cdc.orders, cdc.order_items,
       cdc.payments, cdc.inventory, cdc.shipments -- 3 partitions,
       3-day retention, + a cdc.<table>.dlq per entity)
    |
    v
streaming/pyspark/bronze_writer.py (PySpark Structured Streaming,
    one generic app, 7 queries, 10s micro-batch, one per topic)
    |
    v
Iceberg Bronze (bronze.<table>_cdc + bronze.dlq_events)
    via Nessie catalog (RocksDB-backed) + MinIO (S3A) object storage
```

New services in `docker-compose.yml`: `kafka` (KRaft), `kafka-connect`
(Debezium), `connect-init` (one-shot connector registration), `minio` +
`minio-init` (one-shot bucket creation), `nessie`, `spark-bronze-writer`
(`restart: unless-stopped` -- a genuinely long-running service, not
restarted by any orchestrator).

Verified end-to-end: initial Postgres snapshot lands in Bronze with
`operation='r'`; live INSERT/UPDATE/DELETE from the Phase 1 generator land
correctly (confirmed `before`/`after` populated per operation type);
`spark-bronze-writer` and `kafka-connect` both resume cleanly from their own
checkpoint/slot after a restart with no re-snapshot and no gap; a malformed
Kafka message is quarantined to both a `.dlq` topic and `bronze.dlq_events`
without stopping the rest of the batch.

Details: [cdc.md](cdc.md), ADRs [0003](decisions/0003-kraft-no-zookeeper.md)-[0006](decisions/0006-dlq-lives-in-bronze-writer-not-kafka-connect.md).
