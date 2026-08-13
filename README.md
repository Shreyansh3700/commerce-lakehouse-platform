# Real-Time Commerce Data Platform

A production-style, end-to-end real-time commerce data platform: PostgreSQL
(CDC via Debezium) -> Kafka -> PySpark -> Apache Iceberg lakehouse
(Bronze/Silver/Gold) -> Trino/dbt -> FastAPI, with Airflow orchestration,
Great Expectations data quality, Prometheus/Grafana monitoring, and
OpenLineage/Marquez lineage.

Full requirements: [`real_time_commerce_data_platform.md`](real_time_commerce_data_platform.md).
Architecture and phase status: [`docs/architecture.md`](docs/architecture.md).

This is being built in 5 phases rather than all at once:

1. **Postgres schema + data generator** -- done (this README covers it)
2. Debezium/Kafka CDC + Iceberg Bronze
3. Silver/Gold (Iceberg current-state + analytics) + dbt + data quality
4. Streaming analytics (PySpark) + late-arriving events + schema evolution
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

### Prerequisites

- Docker Desktop
- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/)

### Quickstart

```bash
cp .env.example .env        # edit passwords if you want non-default ones
make up                     # starts postgres + adminer
make install                # uv sync
make seed                   # bulk-loads the initial dataset (takes a while at 1M+ orders)
make generate                # runs the continuous simulator (Ctrl+C to stop)
```

Other useful targets: `make psql` (open a `psql` shell), `make ps` / `make
logs`, `make seed-reset` (truncate + reseed), `make reset-db` (drop the
Postgres volume entirely and restart fresh), `make test` (run `pytest`).

Adminer (a simple DB browser) is available at `http://localhost:8081` once
`make up` is running (server: `postgres`, matching your `.env` credentials).

### Design decisions

- [ADR 0001: logical replication set up now, slot deferred to Phase 2](docs/decisions/0001-logical-replication-in-phase1.md)
- [ADR 0002: schema evolution deferred to Phase 4](docs/decisions/0002-schema-evolution-timing.md)

### Known limitations (Phase 1)

- No CDC/streaming/lakehouse components yet -- this is Postgres + a
  generator only.
- The simulator's inventory reservation logic is best-effort (skips
  silently if a matching row/stock isn't found) rather than transactional,
  since it's simulating client-side application behavior, not implementing
  correctness guarantees the platform itself must provide.
