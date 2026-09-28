"""Idempotent, restart-safe Iceberg column additions -- the Silver-side half
of Part C's schema-evolution demo (see docs/decisions/0008-schema-evolution-
strategy.md and the Phase 4 plan).

silver_writer.py's ensure_silver_tables() (silver_ddl.sql / gold_ddl.sql) is
safe to blindly re-run on every startup because every statement in those
files is `CREATE TABLE IF NOT EXISTS` -- re-issuing a CREATE for a table that
already exists is a no-op. `ALTER TABLE ... ADD COLUMN` has no such built-in
guard: Iceberg (like most engines) raises if the column already exists, so
blindly re-running it on every restart would crash the Silver writer forever
after the first successful run. This module exists to give ALTER TABLE the
same "safe to call every startup" property CREATE TABLE IF NOT EXISTS already
has, by checking the table's current schema first.

Only ADD COLUMN is handled here -- per ADR 0008, that's the one schema
change that's safe to apply in place. Renames and narrowing type changes are
NOT safe to bolt onto this mechanism; they need the expand-contract pattern
(see silver_schemas.py's EXTRA_PARSE_FIELDS/FIELD_COALESCE for how the
shipments carrier->carrier_name rename is handled instead, entirely on the
read side, with no Iceberg ALTER at all).
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("schema_evolution")

# (iceberg_table, column_name, iceberg_type) -- additive columns that Silver
# needs to pick up via ALTER TABLE ... ADD COLUMN. Append new entries here
# for the *next* schema evolution someone adds (an additive or type-widening
# change only -- see the module docstring for what does NOT belong here).
#
# discount_amount: added at the source by
# infrastructure/postgres/migrations/001_add_orders_discount_amount.sql.
# silver_schemas.py's ORDERS_SCHEMA/DECIMAL_CAST_FIELDS already know how to
# parse+cast the Bronze JSON value; this is what makes the target Iceberg
# table itself able to hold it.
SCHEMA_EVOLUTIONS: list[tuple[str, str, str]] = [
    ("nessie.silver.orders", "discount_amount", "DECIMAL(10,2)"),
]


def ensure_column(spark: SparkSession, table: str, column: str, iceberg_type: str) -> None:
    """Adds `column` to `table` via ALTER TABLE ... ADD COLUMN, unless it's
    already present -- makes an otherwise-unsafe-to-repeat ALTER safe to call
    on every Silver writer startup (see module docstring).

    Checks spark.table(table).schema.fieldNames() rather than, say,
    swallowing the "column already exists" exception Iceberg would raise --
    an explicit check is unambiguous about *why* nothing happens on the
    common (already-evolved) path, and never risks masking an unrelated
    ALTER failure behind a broad except clause."""
    existing = {f.lower() for f in spark.table(table).schema.fieldNames()}
    if column.lower() in existing:
        logger.info("schema_evolution: %s.%s already present, skipping", table, column)
        return
    spark.sql(f"ALTER TABLE {table} ADD COLUMN {column} {iceberg_type}")
    logger.info("schema_evolution: added %s.%s %s", table, column, iceberg_type)


def apply_schema_evolutions(spark: SparkSession) -> None:
    """Runs every registered evolution in SCHEMA_EVOLUTIONS. Called once from
    silver_writer.py's main(), right after ensure_silver_tables() and before
    any streaming query starts, so every Silver table has its full current
    column set before the first micro-batch is processed."""
    for table, column, iceberg_type in SCHEMA_EVOLUTIONS:
        ensure_column(spark, table, column, iceberg_type)
