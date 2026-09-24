"""Plain-Python smoke tests for the Gold tables gold_builder.py just built.
Replaces the retired dbt project's schema tests (grain not_null/unique) and
its two singular business-rule tests that weren't already covered by
data_quality/'s Great Expectations suite against Silver (see the comment on
_SKIPPED_AS_DUPLICATE_OF_DQ below for what WAS already covered and so isn't
repeated here).

Run via `make gold-test` (docker compose run --rm --profile tools
gold-builder /opt/spark-app/gold_tests.py). Exits 0 iff every check passes,
non-zero otherwise -- same orchestrator-agnostic contract as
data_quality/runner.py, so `make gold-build` (dq-check -> gold-build-run ->
gold-test) can chain all three via Make's fail-fast prerequisite ordering.

_SKIPPED_AS_DUPLICATE_OF_DQ: order/payment amount >= 0 and
delivered_at >= shipped_at are already CRITICAL checks in
data_quality/checks/validity.py and business_rules.py, run against Silver
*before* gold_builder.py ever runs (make gold-build's dq-check step) -- no
need to re-check them here. "DELIVERED orders have shipment info" is also
already covered by data_quality/checks/business_rules.py's
check_delivered_orders_have_shipment."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from spark_session import build_spark_session

logger = logging.getLogger("gold_tests")


@dataclass
class TestResult:
    name: str
    passed: bool
    violation_count: int = 0
    sample: list = field(default_factory=list)


def _grain_check(spark: SparkSession, table: str, grain_cols: list[str]) -> TestResult:
    """Passes iff `grain_cols` is both not-null and unique in `table` --
    the Gold-table equivalent of the retired dbt schema.yml's
    not_null/unique tests on each model's grain column(s)."""
    name = f"{table}: grain {tuple(grain_cols)} not_null+unique"
    df = spark.table(table)
    null_violations = df
    for c in grain_cols:
        null_violations = null_violations.filter(df[c].isNull())
    null_count = null_violations.count()

    dup_violations = df.groupBy(*grain_cols).count().filter("count > 1")
    dup_count = dup_violations.count()

    violation_count = null_count + dup_count
    sample = [str(r) for r in dup_violations.limit(5).collect()] if dup_count else []
    return TestResult(name=name, passed=violation_count == 0, violation_count=violation_count, sample=sample)


def _cancelled_orders_no_successful_fulfillment(spark: SparkSession) -> TestResult:
    """A CANCELLED order should never have a DELIVERED shipment."""
    violations = spark.sql("""
        SELECT o.order_id, o.order_status, s.shipment_status
        FROM stg_orders o
        INNER JOIN stg_shipments s ON o.order_id = s.order_id
        WHERE o.order_status = 'CANCELLED' AND s.shipment_status = 'DELIVERED'
    """)
    count = violations.count()
    sample = [str(r) for r in violations.limit(5).collect()] if count else []
    return TestResult(
        name="cancelled_orders_no_successful_fulfillment", passed=count == 0, violation_count=count, sample=sample
    )


def _payment_total_not_exceeding_order_total(spark: SparkSession) -> TestResult:
    """1% slack for rounding; catches double-charge / Silver-merge
    duplication bugs, not routine rounding."""
    violations = spark.sql("""
        WITH successful_payments AS (
            SELECT order_id, SUM(amount) AS total_successful_amount
            FROM stg_payments WHERE payment_status = 'SUCCESS' GROUP BY order_id
        )
        SELECT o.order_id, o.total_amount, sp.total_successful_amount
        FROM stg_orders o
        INNER JOIN successful_payments sp ON o.order_id = sp.order_id
        WHERE sp.total_successful_amount > o.total_amount * 1.01
    """)
    count = violations.count()
    sample = [str(r) for r in violations.limit(5).collect()] if count else []
    return TestResult(
        name="payment_total_not_exceeding_order_total", passed=count == 0, violation_count=count, sample=sample
    )


_GRAIN_CHECKS = [
    ("nessie.gold.daily_sales", ["date"]),
    ("nessie.gold.product_performance", ["product_id"]),
    ("nessie.gold.customer_lifetime_value", ["customer_id"]),
    ("nessie.gold.inventory_health", ["product_id", "warehouse_id"]),
    ("nessie.gold.order_fulfillment_metrics", ["date"]),
    ("nessie.gold.payment_metrics", ["date"]),
]


def run_all(spark: SparkSession) -> list[TestResult]:
    # gold_builder.py's staging views only exist for the lifetime of the
    # gold_builder.py process that created them -- gold_tests.py runs as a
    # separate spark-submit invocation, so re-create the two views the
    # business-rule checks need directly from Silver.
    spark.sql("""
        SELECT order_id, customer_id, order_status, total_amount, created_at, updated_at
        FROM nessie.silver.orders WHERE is_deleted = false
    """).createOrReplaceTempView("stg_orders")
    spark.sql("""
        SELECT shipment_id, order_id, shipment_status, carrier, shipped_at, delivered_at, updated_at
        FROM nessie.silver.shipments WHERE is_deleted = false
    """).createOrReplaceTempView("stg_shipments")
    spark.sql("""
        SELECT payment_id, order_id, payment_status, payment_method, amount, created_at, updated_at
        FROM nessie.silver.payments WHERE is_deleted = false
    """).createOrReplaceTempView("stg_payments")

    results = [_grain_check(spark, table, cols) for table, cols in _GRAIN_CHECKS]
    results.append(_cancelled_orders_no_successful_fulfillment(spark))
    results.append(_payment_total_not_exceeding_order_total(spark))
    return results


def print_report(results: list[TestResult]) -> None:
    print(f"\n{'CHECK':<60}{'STATUS':<7}VIOLATIONS")
    print("-" * 80)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.name:<60}{status:<7}{r.violation_count}")
        if not r.passed and r.sample:
            for row in r.sample:
                print(f"    sample: {row}")
    failed = [r for r in results if not r.passed]
    print("-" * 80)
    print(f"Total checks: {len(results)}  Failed: {len(failed)}")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("gold-tests")
    results = run_all(spark)
    print_report(results)
    failed = [r for r in results if not r.passed]
    if failed:
        logger.error("%d Gold test(s) failed -- see report above", len(failed))
        return 1
    logger.info("All %d Gold tests passed.", len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
