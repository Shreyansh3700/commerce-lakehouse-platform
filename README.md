# Real-Time Commerce Data Platform

A production-style, end-to-end real-time commerce data platform: PostgreSQL
(CDC via Debezium) -> Kafka -> PySpark -> Apache Iceberg lakehouse
(Bronze/Silver/Gold) -> FastAPI, with Airflow orchestration, Great
Expectations data quality, Prometheus/Grafana monitoring, and
OpenLineage/Marquez lineage. Gold is built with plain PySpark batch jobs,
not dbt -- see [ADR 0007](docs/decisions/0007-plain-pyspark-gold-not-dbt.md).

Full requirements: [`real_time_commerce_data_platform.md`](real_time_commerce_data_platform.md).
Architecture and phase status: [`docs/architecture.md`](docs/architecture.md).

This is being built in 5 phases rather than all at once:

1. **Postgres schema + data generator** -- done
2. **Debezium/Kafka CDC + Iceberg Bronze** -- done (this README covers both)
3. **Silver/Gold (Iceberg current-state + analytics) + data quality** -- done
4. **Streaming analytics (PySpark) + late-arriving events + schema evolution** -- done
5. Airflow + Prometheus/Grafana + OpenLineage/Marquez + FastAPI + benchmarks

## Phase 1: Postgres + Data Generator

A realistic OLTP schema (`commerce` schema: customers, products, orders,
order_items, payments, inventory, shipments, plus a small `warehouses`
reference table) that's already CDC-ready for Phase 2 (see
[ADR 0001](docs/decisions/0001-logical-replication-in-phase1.md)), and a
Python data generator that:

- **Bulk-loads** a realistic initial dataset via batched `COPY` (not
  row-by-row `INSERT`) -- default targets meet the spec's suggested
  minimums: 10k+ customers/products, 1M+ orders, 2M+ order_items, 1M+
  payments, 100k+ inventory rows, 500k+ shipments.
- **Continuously simulates** ongoing business activity afterward (new
  customers/orders, order/payment/shipment status transitions, inventory
  restocks, profile updates, price/stock changes, cancellations, and the
  two documented delete cases) at configurable rates, for later performance
  testing.

See [`docs/data_model.md`](docs/data_model.md) for the full schema, state
machines, and per-entity delete-strategy documentation.

## Phase 2: Debezium/Kafka CDC + Iceberg Bronze

Debezium captures every Postgres WAL change (initial snapshot + continuous
CDC), Kafka carries it (`cdc.<table>`, 3 partitions, 3-day retention, plus a
`cdc.<table>.dlq` per entity), and a single generic PySpark Structured
Streaming app (`streaming/pyspark/bronze_writer.py`) lands the raw,
unmodified envelopes into an append-only Iceberg Bronze layer
(`bronze.<table>_cdc`), backed by a Nessie catalog and MinIO object storage.
Malformed messages are quarantined (both to a `.dlq` topic and
`bronze.dlq_events`) instead of stopping the pipeline.

See [`docs/cdc.md`](docs/cdc.md) for the full connector config rationale,
partitioning/retention strategy, DLQ design, and per-hop delivery semantics
(the system is effectively-once, not exactly-once -- documented explicitly).

### Prerequisites

- Docker Desktop, with **at least ~10GB** allocated to its VM (Kafka +
  Kafka Connect + Nessie + Spark + Postgres running together is real memory
  pressure; on Windows/WSL2 this means a `.wslconfig` `[wsl2] memory=` bump
  if you're still on Docker Desktop's default ~50%-of-RAM allocation)
- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/)

### Quickstart

```bash
cp .env.example .env        # edit passwords if you want non-default ones
make up                     # starts the full stack (postgres, kafka, kafka-connect,
                             # minio, nessie, spark-bronze-writer, adminer)
make install                # uv sync
make seed                   # bulk-loads the initial dataset (takes a while at 1M+ orders)
make register-connector     # idempotent; also runs automatically via the connect-init service
make generate                # runs the continuous simulator (Ctrl+C to stop) -- Debezium
                             # picks up every change and streams it into Bronze
```

Other useful targets: `make psql` (open a `psql` shell), `make ps` / `make
logs`, `make seed-reset` (truncate + reseed), `make reset-db` (drop the
Postgres volume entirely and restart fresh), `make test` (run `pytest`),
`make connector-status` (Debezium connector/task state), `make topics`
(list Kafka topics), `make consumer-lag` (Bronze writer's own per-table
streaming progress -- see `docs/cdc.md` for why this isn't
`kafka-consumer-groups.sh`-based).

Adminer (a simple DB browser) is available at `http://localhost:8081` once
`make up` is running (server: `postgres`, matching your `.env` credentials).
MinIO's console is at `http://localhost:9001`, Nessie's API at
`http://localhost:19120`.

> `make` itself isn't installed in every Windows shell -- if `make: command
> not found`, run the command from the relevant Makefile target directly, or
> install GNU Make (e.g. via `choco install make`) or use WSL.

### Design decisions

- [ADR 0001: logical replication set up now, slot deferred to Phase 2](docs/decisions/0001-logical-replication-in-phase1.md)
- [ADR 0002: schema evolution deferred to Phase 4](docs/decisions/0002-schema-evolution-timing.md)
- [ADR 0003: Kafka in KRaft mode, no Zookeeper](docs/decisions/0003-kraft-no-zookeeper.md)
- [ADR 0004: short Kafka retention -- Bronze is the durable store](docs/decisions/0004-kafka-retention-vs-bronze-durability.md)
- [ADR 0005: Bronze before/after as raw JSON, not typed structs](docs/decisions/0005-bronze-before-after-as-raw-json.md)
- [ADR 0006: DLQ lives in the Bronze writer, not Kafka Connect](docs/decisions/0006-dlq-lives-in-bronze-writer-not-kafka-connect.md)

### Known limitations (Phase 2)

- Full Prometheus/Grafana consumer-lag dashboards are Phase 5 scope; today,
  progress is only visible via each streaming query's own log output
  (`make consumer-lag`).
- The simulator's inventory reservation logic is best-effort (skips
  silently if a matching row/stock isn't found) rather than transactional,
  since it's simulating client-side application behavior, not implementing
  correctness guarantees the platform itself must provide.

## Phase 3: Silver (CDC merge) / Gold (plain PySpark batch) / Data Quality

A second long-running PySpark Structured Streaming app
(`streaming/pyspark/silver_writer.py`) reads each Bronze CDC table
incrementally via Iceberg's own streaming read (not Kafka), and per
micro-batch: validates, deduplicates by `event_id`, orders by Postgres WAL
`source_lsn` (never ingestion time), and applies a `MERGE INTO` guarded by
`source.source_lsn > target._source_lsn` -- the mechanism that makes
cross-batch duplicates and out-of-order replays safe no-ops, not just a
claim. `order_items`/`inventory` are hard-deleted (genuinely deleted at
source); everything else gets a defensive soft-delete
(`is_deleted`/`deleted_at`), since the source never actually deletes them.
The same customers batch also maintains `gold.dim_customer_history` (SCD2)
via `streaming/pyspark/scd2.py`, reusing the already-deduped/ordered batch --
SCD2 needs the ordered change stream, which current-state Silver has already
discarded.

`batch/pyspark/gold_builder.py` (plain Python + Spark SQL, run as a one-shot
container, not a standing service -- no dbt, see
[ADR 0007](docs/decisions/0007-plain-pyspark-gold-not-dbt.md)) builds the six
required Gold tables from Silver: staging views -> `int_orders_enriched` ->
`daily_sales` (the one incremental table, a hand-written `MERGE INTO` with a
3-day lookback -- Silver is mutable current-state, so a naive append-only
incremental would miss retroactive changes like late cancellations),
`product_performance`, `customer_lifetime_value`, `inventory_health`,
`order_fulfillment_metrics`, `payment_metrics`. `batch/pyspark/gold_tests.py`
runs a handful of plain assertion checks afterward (grain not-null/unique
per table, plus two cross-Silver business rules not already covered by
`data_quality/`'s checks).

`data_quality/` (Great Expectations, Spark DataFrame engine) validates Silver
and gates Gold publication: `make gold-build` runs `dq-check` first and only
proceeds to `gold-build-run`/`gold-test` on zero CRITICAL failures. See
[docs/data_quality.md](docs/data_quality.md) for the full severity model and
suite/table mapping.

### Quickstart (continued)

```bash
make silver-progress   # Silver writer's own per-table streaming progress
make dq-check           # standalone DQ run (non-blocking, for visibility)
make gold-build          # dq-check -> gold-build-run -> gold-test (the real gate)
make query-gold           # ad hoc SELECT against gold.daily_sales
```

### Design decisions (continued)

- [ADR 0007: Gold built with a plain PySpark batch job, not dbt; Trino still deferred](docs/decisions/0007-plain-pyspark-gold-not-dbt.md)

### Known limitations (Phase 3)

- `customer_lifetime_value.lifetime_value` is currently just an alias for
  `total_spend` -- a placeholder column, kept distinct from `total_spend` so
  a future predictive/discounted-LTV model doesn't force a rename.
- `daily_sales`'s incremental build only re-aggregates orders whose
  `updated_at` changed in the last 3 days after the first run; a change
  older than that needs a manual full rebuild (truncate
  `nessie.gold.daily_sales` and re-run `make gold-build-run`) to be
  reflected.
- No Prometheus/Grafana or Airflow yet (Phase 5) -- `nessie.audit.dq_results`
  exists today specifically so those dashboards have historical data to read
  once Phase 5 adds them.

## Phase 4: Streaming Analytics / Late-Arriving Events / Schema Evolution

A third long-running PySpark Structured Streaming app
(`streaming/pyspark/analytics_writer.py`, `spark-analytics-writer`) reads
directly from the `cdc.*` Kafka topics -- not Bronze/Silver Iceberg -- to
compute real-time metrics (orders/revenue per minute, 5-minute revenue,
payment failure rate, top products, inventory alerts), each MERGEd into a
`nessie.analytics.*` Iceberg table every micro-batch. All windowed metrics
are config-driven (`streaming/pyspark/analytics_config.py`) through one
generic windowed-aggregation implementation.

Late-arriving events (spec section 17) are demonstrated on order creation:
the simulator's new `event_late_order` commits an order to Postgres right
now but backdates its `created_at` 1-15 minutes. A 5-minute watermark on
`orders_per_minute` absorbs backdates under that threshold into an
already-written window; backdates beyond it would be silently dropped by
Spark's own watermark, so a small hand-rolled check
(`run_late_event_query`) catches those instead and appends them to
`nessie.analytics.late_events`, a dedicated reconciliation dataset. See
[docs/streaming_analytics.md](docs/streaming_analytics.md) for the full
metric list and the watermark/threshold rationale.

Schema evolution (spec section 18, Scenario 7) is demonstrated live against
the already-running pipeline, per ADR 0002's plan:
`infrastructure/postgres/migrations/` holds two on-demand `ALTER TABLE`
statements (unlike `infrastructure/postgres/init/`, which only ever runs
once). `make schema-evolve-add-discount` adds `orders.discount_amount` --
Bronze needs zero changes (raw JSON, ADR 0005), and Silver picks it up via
an idempotent, restart-safe `ALTER TABLE ... ADD COLUMN`
(`streaming/pyspark/schema_evolution.py`), visible all the way through to
`gold.daily_sales`'s new `total_discount`/`net_revenue` columns.
`make schema-evolve-rename-carrier` renames `shipments.carrier` to
`carrier_name` -- the "requires special handling" case: a rename fails
*silently* (not loudly) for any reader still expecting the old key, so it's
handled via expand-contract (dual-read both keys, resolve via `COALESCE`)
rather than an in-place change. `make detect-schema-drift TABLE=orders` is a
non-blocking, on-demand diagnostic that flags Bronze JSON keys Silver
doesn't yet parse. See
[ADR 0008](docs/decisions/0008-schema-evolution-strategy.md) for the general
compatibility rules this establishes.

### Quickstart (continued)

```bash
make analytics-progress          # spark-analytics-writer's own per-query streaming progress
make query-analytics              # ad hoc SELECT against nessie.analytics.orders_per_minute
make top-products                  # ranked SELECT against nessie.analytics.product_activity
make schema-evolve-add-discount     # ALTER TABLE commerce.orders ADD COLUMN discount_amount ...
make schema-evolve-rename-carrier    # ALTER TABLE commerce.shipments RENAME COLUMN carrier ...
make detect-schema-drift TABLE=orders # non-blocking Bronze-vs-Silver schema-drift diagnostic
```

### Design decisions (continued)

- [ADR 0008: Schema-evolution compatibility rules](docs/decisions/0008-schema-evolution-strategy.md)

### Known limitations (Phase 4)

- `orders_per_minute`/`revenue_5min` only count order-creation events
  (`operation IN ('c','r')`) -- an order's later status changes land in the
  same window (unchanging `created_at`) and are deliberately excluded to
  avoid double-counting; see docs/streaming_analytics.md.
- Silver only reflects `discount_amount` for orders touched by a CDC event
  after the migration was applied -- an order never touched again since
  shows `NULL`/0 in Silver until a Bronze -> Silver replay reprocesses its
  history (same category of limitation as `daily_sales`'s incremental
  lookback window above).
- `shipments.carrier`'s old JSON key is still parsed indefinitely (the
  expand-contract dual-read) -- dropping it is a documented future cleanup
  step once no historical/replay concern remains, not implemented now.
