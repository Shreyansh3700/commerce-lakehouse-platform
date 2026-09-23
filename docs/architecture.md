# Architecture

See `real_time_commerce_data_platform.md` (section 3) for the full target
architecture. This project is being built in 5 phases; each phase's
components are documented here as they're implemented.

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 1 | Postgres schema + data generator | Done |
| 2 | Debezium/Kafka CDC + Iceberg Bronze | Done |
| 3 | Silver/Gold (plain PySpark batch, not dbt -- see ADR 0007) + data quality | Done |
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

## Phase 3: Silver (CDC merge) / Gold (plain PySpark batch) / Data quality

```
Iceberg Bronze (bronze.<table>_cdc)
    |
    v
streaming/pyspark/silver_writer.py (PySpark Structured Streaming,
    one generic app, 7 queries, 30s micro-batch: readStream.format("iceberg")
    per Bronze table -- not Kafka -- foreachBatch -> validate -> dedupe
    (event_id) -> order-by-source_lsn -> MERGE INTO, guarded by
    source.source_lsn > target._source_lsn on every match)
    |
    +--> Iceberg Silver (silver.customers/products/orders/order_items/
    |    payments/inventory/shipments + silver.dlq_events) -- current
    |    state, one row per PK; order_items/inventory hard-deleted,
    |    everything else defensively soft-deleted (is_deleted/deleted_at)
    |
    +--> gold.dim_customer_history (SCD2, built by the same customers
         batch handler via streaming/pyspark/scd2.py, not by the Gold
         builder below -- SCD2 needs the ordered change stream Silver's
         current-state MERGE already discards)
    |
    v
batch/pyspark/gold_builder.py (plain Spark SQL, one-shot container,
    Iceberg/Nessie/S3A config mirroring the Spark writers -- no dbt, see
    ADR 0007) staging views -> intermediate (int_orders_enriched) -> gold
    (daily_sales, product_performance, customer_lifetime_value,
    inventory_health, order_fulfillment_metrics, payment_metrics)
    |
    v
Iceberg Gold (gold.*)
```

`data_quality/` (Great Expectations against Silver, Spark DataFrame engine)
gates Gold publication: `make gold-build` runs `dq-check` first and only
proceeds to `gold-build-run`/`gold-test` if there are zero CRITICAL failures
-- enforced today by Make's prerequisite-ordering/fail-fast semantics,
mapping directly onto an Airflow task dependency once Phase 5 adds Airflow.
Every run writes results to `nessie.audit.dq_results` (pass/fail, severity,
violation count, sample PKs).

New services in `docker-compose.yml`: `spark-silver-writer`
(`restart: unless-stopped`, same image as `spark-bronze-writer` via
`command:`); `gold-builder` and `dq` (`profiles: ["tools"]` -- one-shot, run
via `docker compose run --rm --profile tools ...`, not started by a plain
`docker compose up`). No Trino service yet -- the Gold builder runs directly
against Spark; Trino is deferred to whichever later phase adds interactive
BI/API serving, since it has no consumer in this project until then -- see
[ADR 0007](decisions/0007-plain-pyspark-gold-not-dbt.md).

Details: [data_quality.md](data_quality.md).
