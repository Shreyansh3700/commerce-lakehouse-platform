"""Cross-column / cross-table business-rule checks. All CRITICAL -- these
encode invariants the source enforces by construction
(infrastructure/postgres/init/02_schema.sql's CHECK constraints and state
machine), so a violation in Silver indicates a merge bug, not a
naturally-occurring bad record."""

from __future__ import annotations

from ge_helpers import row_count_check
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from severity import Category, CheckResult, Severity


def check_successful_payments_positive(spark: SparkSession) -> CheckResult:
    payments = spark.table("nessie.silver.payments")
    violations = payments.filter((col("payment_status") == "SUCCESS") & (col("amount") <= 0))
    return row_count_check(
        violations_df=violations,
        total_df=payments,
        table="payments",
        check_name="payments.successful_payments_have_positive_amount",
        category=Category.BUSINESS_RULE,
        severity=Severity.CRITICAL,
        pk_cols=["payment_id"],
    )


def check_delivered_after_shipped(spark: SparkSession) -> CheckResult:
    shipments = spark.table("nessie.silver.shipments")
    violations = shipments.filter(
        col("delivered_at").isNotNull()
        & col("shipped_at").isNotNull()
        & (col("delivered_at") < col("shipped_at"))
    )
    return row_count_check(
        violations_df=violations,
        total_df=shipments,
        table="shipments",
        check_name="shipments.delivered_at_not_before_shipped_at",
        category=Category.BUSINESS_RULE,
        severity=Severity.CRITICAL,
        pk_cols=["shipment_id"],
    )


def check_delivered_orders_have_shipment(spark: SparkSession) -> CheckResult:
    orders = spark.table("nessie.silver.orders").filter(col("order_status") == "DELIVERED")
    shipments = spark.table("nessie.silver.shipments").filter(col("delivered_at").isNotNull())
    violations = orders.join(shipments, orders["order_id"] == shipments["order_id"], "left_anti")
    return row_count_check(
        violations_df=violations,
        total_df=orders,
        table="orders",
        check_name="orders.delivered_orders_have_shipment",
        category=Category.BUSINESS_RULE,
        severity=Severity.CRITICAL,
        pk_cols=["order_id"],
    )


def run(spark: SparkSession) -> list[CheckResult]:
    return [
        check_successful_payments_positive(spark),
        check_delivered_after_shipped(spark),
        check_delivered_orders_have_shipment(spark),
    ]
