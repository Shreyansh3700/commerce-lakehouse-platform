# ADR 0007: Gold built with a plain PySpark batch job, not dbt; Trino still deferred

## Status
Accepted (supersedes an earlier "dbt on Spark session" version of this
decision -- a dbt-based `transformations/dbt/` project was implemented
first, then replaced with `batch/pyspark/` before ever being run against a
live cluster, once dbt turned out to be an unfamiliar tool for this
project's maintainer to own and debug).

## Context
Spec section 4 lists dbt as the required SQL transformation tool, "unless
there is a strong technical reason to substitute one." The spec's own
architecture diagram (section 3) places dbt and Trino at different points
in the pipeline: `Iceberg Silver -> dbt/Spark -> Iceberg Gold ->
Trino/FastAPI -> BI/Clients` -- the Gold-building tool runs on Spark either
way; Trino appears strictly downstream of Gold, as a read-only serving/BI
engine with no consumer until FastAPI/BI exist (Phase 5).

This project's maintainer does not know dbt and would own debugging it
going forward -- a framework-specific YAML DSL, Jinja templating, and a
`profiles.yml`/`dbt_project.yml` configuration surface add real learning
cost for a solo project, on top of the already-nontrivial `dbt-spark` +
custom Nessie-catalog combination this ADR's first version flagged as an
unvalidated risk. Meanwhile the project already has a fully working,
understood pattern for "a Python/Spark job that reads Iceberg tables via the
Nessie catalog and writes other Iceberg tables": `streaming/pyspark/
bronze_writer.py` and `silver_writer.py`, plus the one-shot batch pattern
already used for `data_quality/runner.py`.

## Decision
Gold is built by `batch/pyspark/gold_builder.py`, a plain Python script
using Spark SQL (`spark.sql(...)`) against the same Nessie/S3A-configured
`SparkSession` (`spark_session.py`, reused unmodified from Bronze/Silver).
It runs via `spark-submit` in a purpose-built image
(`batch/pyspark/Dockerfile`) that mirrors the JAR-baking pattern already
used everywhere else in this repo (same Iceberg/Nessie/hadoop-aws/aws-sdk
jars, minus Kafka), as a one-shot `docker compose run` container
(`gold-builder` service, `profiles: ["tools"]`) -- the same shape as
`dq`/`connect-init`/`minio-init`. Five of the six required Gold tables are
plain full-refresh overwrites (`df.writeTo(table).overwritePartitions()`)
each run; `daily_sales` is the one incremental table, using a hand-written
Iceberg `MERGE INTO` with a 3-day `updated_at` lookback (replacing what
would have been dbt's `incremental_strategy: merge`). Grain
uniqueness/not-null checks and the two Gold-level business-rule checks that
weren't already covered by `data_quality/`'s Great Expectations suite move
to `batch/pyspark/gold_tests.py`, a plain assertion script with the same
exit-code contract as `data_quality/runner.py` (replacing dbt's schema
tests and singular SQL tests).

Trino is still **not** stood up in this phase -- that part of the original
decision doesn't depend on dbt vs. plain PySpark either way. Standing it up
now would add infra with no consumer until FastAPI/BI exist. Adding it
later is a pure infra addition (an `infrastructure/trino/` config and a
compose service pointed at the same Nessie catalog and MinIO bucket Gold
already lives in), with zero rework of `gold_builder.py`, since Gold's
physical layout (Iceberg tables under the `nessie.gold` namespace) is
engine-agnostic regardless of what built them.

## Consequences
- Gold tables are ordinary Iceberg tables in the `nessie.gold` namespace,
  queryable by any engine that can read that Nessie catalog -- including a
  future Trino -- with no dependency on how they were built.
- No dbt means no auto-generated docs/lineage site (`dbt docs generate`) and
  no declarative schema-test framework; `gold_tests.py` is a hand-rolled,
  smaller substitute covering only what wasn't already covered by
  `data_quality/`'s Silver-level checks (see that file's module docstring
  for exactly what's skipped as a duplicate and why).
- `int_orders_enriched` and the seven `stg_*` staging queries -- previously
  separate dbt models -- are now Python string constants
  (`STG_VIEWS`, `_INT_ORDERS_ENRICHED_SQL_TEMPLATE`) registered as Spark
  temp views inside `gold_builder.py`'s `_create_views()`; there's no
  separate "run `dbt compile`" step to catch a typo'd column name before
  the job actually runs against real data.
- One fewer Docker image type to maintain overall (no
  `transformations/dbt/Dockerfile` with its pip-installed `dbt-core`/
  `dbt-spark` layer) -- `batch/pyspark/Dockerfile` is a close sibling of
  `streaming/pyspark/Dockerfile`, so there's one fewer JAR-baking pattern to
  keep in sync across the repo, not two.
