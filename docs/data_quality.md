# Data Quality

Implemented in `data_quality/` using Great Expectations' Spark DataFrame
execution engine, validating the Silver Iceberg tables directly (no Trino/JDBC
path exists in this project -- see
[ADR 0007](decisions/0007-plain-pyspark-gold-not-dbt.md)).

## What Phase 1 already guarantees at the source

- `shipments.delivered_at >= shipments.shipped_at` is enforced by a DB
  `CHECK` constraint -- see [data_model.md](data_model.md#invariants-enforced-at-the-source).
- Monetary/quantity columns are non-negative; status columns are constrained
  to known enums.

Because the source is clean by construction, "bad data" test scenarios are
injected at the Kafka/Bronze layer (a deliberately malformed event) rather
than by weakening these Postgres constraints -- exercised in the negative-path
verification step below.

## Severity model

Every check declares one of two tiers; a third is a derived reporting label:

- **CRITICAL** -- `mostly_threshold = 0.0`. Any violation fails the check and
  blocks `make gold-build` from proceeding to the Gold build. Used for all
  completeness (PK/FK not-null), referential-integrity, and business-rule
  checks -- these indicate structural corruption or a Silver-merge bug, never
  safe to build Gold on top of.
- **WARNING** -- `mostly_threshold = 0.01` (mostly=0.99). Used for
  validity/range checks. A handful of stray rows don't block, but if the
  failure rate *exceeds* the threshold the check escalates to CRITICAL for
  blocking purposes (a systemic problem, not a tolerable data issue).
- **RECOVERABLE** -- not a declared tier; the reporting label for a WARNING
  check that had some violations but stayed under its threshold. These rows
  are still written to the audit table with sample PKs so they're never
  silently dropped, matching the DLQ philosophy applied one layer up.

See `data_quality/severity.py`'s `CheckResult` for the exact
`passed`/`is_blocking`/`effective_severity` logic.

## Suite / table / expectation mapping

| Table | Completeness (CRITICAL) | Validity (WARNING) | Referential integrity (CRITICAL) | Business rules (CRITICAL) |
|---|---|---|---|---|
| `silver.orders` | `order_id`, `customer_id` not null | `total_amount >= 0` | `customer_id` exists in `silver.customers` | -- |
| `silver.order_items` | `order_item_id`, `order_id`, `product_id` not null | `quantity > 0`, `unit_price >= 0` | `order_id` exists in `silver.orders`; `product_id` exists in `silver.products` | -- |
| `silver.payments` | `payment_id`, `order_id` not null | `amount >= 0` | -- | SUCCESS payments have `amount > 0` |
| `silver.shipments` | `shipment_id`, `order_id` not null | `shipment_status` in enum | -- | `delivered_at >= shipped_at`; DELIVERED orders have shipment info |
| `silver.customers` | `customer_id` not null | -- | -- | -- |
| `silver.products` | `product_id` not null | `price >= 0`, `stock_quantity >= 0` | -- | -- |
| `silver.inventory` | `inventory_id` not null | `available_quantity >= 0`, `reserved_quantity >= 0` | `product_id` exists in `silver.products` | -- |

Referential-integrity and business-rule checks are anti-join-derived
violation DataFrames validated as row-count-zero expectations (GE has no
first-class native cross-dataset "relationships" expectation).

## Blocking mechanism

```
make dq-check    # standalone run, non-blocking, for visibility
make gold-build  # dq-check -> gold-build-run -> gold-test, via Make
                  # prerequisite ordering: a non-zero dq-check exit stops
                  # the chain before the Gold build ever runs
```

`data_quality/runner.py` exits `0` iff zero checks are `is_blocking`
(CRITICAL, including WARNING-escalated-to-CRITICAL); non-zero otherwise. This
exit-code contract is orchestrator-agnostic on purpose: it maps directly onto
an Airflow task dependency once Phase 5 adds Airflow, with no changes to
`runner.py` itself.

Every run also writes one row per check to `nessie.audit.dq_results`
(`run_id`, `run_at`, `table_name`, `check_name`, `category`, `severity`,
`passed`, `is_blocking`, `element_count`, `violation_count`, `failure_rate`,
`threshold`, `sample_pks`) -- historical/observability data that will feed
Phase 5's Grafana dashboards.

## Verifying the gate actually blocks

Per the "inject at Bronze, not Postgres" convention above: publish one
deliberately malformed CDC event that would corrupt a Silver foreign key
(e.g. an order referencing a nonexistent `customer_id`), let it flow through
to Silver, then run `make gold-build` and confirm it halts at `dq-check` with
a non-zero exit -- `gold_builder.py` never runs.
