"""Referential-integrity checks: FK columns must resolve to an existing
parent row. All CRITICAL -- an orphaned FK means a Silver-merge/ordering
bug (e.g. an order's customer row was never applied, or was incorrectly
deleted), not a tolerable data issue."""

from __future__ import annotations

from ge_helpers import row_count_check
from pyspark.sql import DataFrame, SparkSession

from severity import Category, CheckResult, Severity


def _orphans(
    spark: SparkSession, child_table: str, child_fk: str, parent_table: str, parent_pk: str
) -> tuple[DataFrame, DataFrame]:
    child = spark.table(f"nessie.silver.{child_table}")
    parent = spark.table(f"nessie.silver.{parent_table}").select(parent_pk).withColumnRenamed(
        parent_pk, "_parent_pk"
    )
    orphans = child.join(parent, child[child_fk] == parent["_parent_pk"], "left_anti")
    return child, orphans


def check_fk(
    spark: SparkSession,
    child_table: str,
    child_fk: str,
    parent_table: str,
    parent_pk: str,
    pk_cols: list[str],
) -> CheckResult:
    child, orphans = _orphans(spark, child_table, child_fk, parent_table, parent_pk)
    return row_count_check(
        violations_df=orphans,
        total_df=child,
        table=child_table,
        check_name=f"{child_table}.{child_fk}_fk_{parent_table}",
        category=Category.REFERENTIAL_INTEGRITY,
        severity=Severity.CRITICAL,
        pk_cols=pk_cols,
    )


def run(spark: SparkSession) -> list[CheckResult]:
    return [
        check_fk(spark, "orders", "customer_id", "customers", "customer_id", ["order_id"]),
        check_fk(spark, "order_items", "order_id", "orders", "order_id", ["order_item_id"]),
        check_fk(spark, "order_items", "product_id", "products", "product_id", ["order_item_id"]),
        check_fk(spark, "inventory", "product_id", "products", "product_id", ["inventory_id"]),
    ]
