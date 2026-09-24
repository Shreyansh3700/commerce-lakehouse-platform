"""Validity checks: range/domain constraints. All WARNING-tier
(mostly=0.99) -- a handful of stray rows are logged as RECOVERABLE, but a
systemic problem (>1% of rows) escalates to blocking. See severity.py's
CheckResult.effective_severity for the escalation rule."""

from __future__ import annotations

from ge_helpers import column_check
from pyspark.sql import SparkSession

from severity import Category, CheckResult, Severity

_MOSTLY_THRESHOLD = 0.01  # mostly=0.99


def check_min_value(
    spark: SparkSession,
    table: str,
    column: str,
    pk_cols: list[str],
    min_value: float,
    strict: bool = False,
) -> CheckResult:
    df = spark.table(f"nessie.silver.{table}")
    return column_check(
        df=df,
        table=table,
        check_name=f"{table}.{column}_{'gt' if strict else 'gte'}_{min_value}",
        category=Category.VALIDITY,
        severity=Severity.WARNING,
        pk_cols=pk_cols,
        expectation="expect_column_values_to_be_between",
        mostly_threshold=_MOSTLY_THRESHOLD,
        column=column,
        min_value=min_value,
        max_value=None,
        strict_min=strict,
    )


def run(spark: SparkSession) -> list[CheckResult]:
    return [
        check_min_value(spark, "products", "price", ["product_id"], 0),
        check_min_value(spark, "products", "stock_quantity", ["product_id"], 0),
        check_min_value(spark, "orders", "total_amount", ["order_id"], 0),
        check_min_value(spark, "order_items", "quantity", ["order_item_id"], 0, strict=True),
        check_min_value(spark, "order_items", "unit_price", ["order_item_id"], 0),
        check_min_value(spark, "payments", "amount", ["payment_id"], 0),
        check_min_value(spark, "inventory", "available_quantity", ["inventory_id"], 0),
        check_min_value(spark, "inventory", "reserved_quantity", ["inventory_id"], 0),
    ]
