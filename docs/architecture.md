# Architecture

See `real_time_commerce_data_platform.md` (section 3) for the full target
architecture. This project is being built in 5 phases; each phase's
components are documented here as they're implemented.

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 1 | Postgres schema + data generator | Done |
| 2 | Debezium/Kafka CDC + Iceberg Bronze | Not started |
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
