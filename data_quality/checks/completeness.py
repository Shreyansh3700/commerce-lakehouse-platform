"""Completeness checks: primary-key / required-FK NOT NULL, all CRITICAL
(mostly=1.0 -- a null PK/FK on a Silver row means a Silver-merge bug, never
a tolerable stray record)."""

from __future__ import annotations

from ge_helpers import column_check
from pyspark.sql import SparkSession

from severity import Category, CheckResult, Severity


def check_not_null(spark: SparkSession, table: str, column: str, pk_cols: list[str]) -> CheckResult:
    df = spark.table(f"nessie.silver.{table}")
    return column_check(
        df=df,
        table=table,
        check_name=f"{table}.{column}_not_null",
        category=Category.COMPLETENESS,
        severity=Severity.CRITICAL,
        pk_cols=pk_cols,
        expectation="expect_column_values_to_not_be_null",
        mostly_threshold=0.0,
        column=column,
    )


def run(spark: SparkSession) -> list[CheckResult]:
    return [
        check_not_null(spark, "customers", "customer_id", ["customer_id"]),
        check_not_null(spark, "products", "product_id", ["product_id"]),
        check_not_null(spark, "orders", "order_id", ["order_id"]),
        check_not_null(spark, "orders", "customer_id", ["order_id"]),
        check_not_null(spark, "order_items", "order_item_id", ["order_item_id"]),
        check_not_null(spark, "order_items", "order_id", ["order_item_id"]),
        check_not_null(spark, "order_items", "product_id", ["order_item_id"]),
        check_not_null(spark, "payments", "payment_id", ["payment_id"]),
        check_not_null(spark, "payments", "order_id", ["payment_id"]),
        check_not_null(spark, "shipments", "shipment_id", ["shipment_id"]),
        check_not_null(spark, "shipments", "order_id", ["shipment_id"]),
        check_not_null(spark, "inventory", "inventory_id", ["inventory_id"]),
    ]
