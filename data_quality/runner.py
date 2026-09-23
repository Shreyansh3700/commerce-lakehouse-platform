"""CLI entrypoint for the Phase 3 data-quality gate.

    python runner.py --suite all --fail-on critical

Runs the requested suite's checks (see suites/registry.py) against the
Silver Iceberg tables, prints a human-readable report, writes every result
to the nessie.audit.dq_results Iceberg table, and exits 0 iff nothing
blocking failed -- see severity.py's CheckResult.is_blocking for exactly
what "blocking" means. This exit-code contract is deliberately
orchestrator-agnostic: `make gold-build` chains this with `gold_builder.py`/
`gold_tests.py` via Make prerequisite ordering today, and the same contract
maps directly onto an Airflow task dependency in Phase 5 with no changes to
this script.
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from spark_session import build_spark_session
from suites.registry import get_suite
from severity import CheckResult, Severity

logger = logging.getLogger("dq_runner")

_AUDIT_NAMESPACE_DDL = "CREATE NAMESPACE IF NOT EXISTS nessie.audit"

_AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS nessie.audit.dq_results (
    run_id          STRING,
    run_at          TIMESTAMP,
    table_name      STRING,
    check_name      STRING,
    category        STRING,
    severity        STRING,
    passed          BOOLEAN,
    is_blocking     BOOLEAN,
    element_count   BIGINT,
    violation_count BIGINT,
    failure_rate    DOUBLE,
    threshold       DOUBLE,
    sample_pks      STRING
)
USING iceberg
PARTITIONED BY (days(run_at))
"""


def ensure_audit_table(spark: SparkSession) -> None:
    spark.sql(_AUDIT_NAMESPACE_DDL)
    spark.sql(_AUDIT_TABLE_DDL)


def run_suite(spark: SparkSession, suite_name: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    for check_module_run in get_suite(suite_name):
        results.extend(check_module_run(spark))
    return results


def write_audit_rows(spark: SparkSession, run_id: str, run_at: datetime, results: list[CheckResult]) -> None:
    if not results:
        return
    columns = [
        "run_id", "run_at", "table_name", "check_name", "category", "severity",
        "passed", "is_blocking", "element_count", "violation_count",
        "failure_rate", "threshold", "sample_pks",
    ]
    rows = [
        (
            run_id,
            run_at,
            r.table,
            r.check_name,
            r.category.value,
            r.effective_severity.value,
            r.passed,
            r.is_blocking,
            r.element_count,
            r.violation_count,
            r.failure_rate,
            r.mostly_threshold,
            ",".join(r.sample_pks),
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, schema=columns)
    df.writeTo("nessie.audit.dq_results").append()


def print_report(results: list[CheckResult]) -> None:
    print(f"\n{'TABLE':<14}{'CHECK':<48}{'SEVERITY':<12}{'STATUS':<7}VIOLATIONS")
    print("-" * 93)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(
            f"{r.table:<14}{r.check_name:<48}{r.effective_severity.value:<12}"
            f"{status:<7}{r.violation_count}/{r.element_count}"
        )

    blocking = [r for r in results if r.is_blocking]
    recoverable = [r for r in results if r.effective_severity is Severity.RECOVERABLE]

    print("-" * 93)
    print(
        f"Total checks: {len(results)}  "
        f"Blocking failures: {len(blocking)}  "
        f"Recoverable stragglers: {len(recoverable)}"
    )

    if blocking:
        print("\nBLOCKING FAILURES:")
        for r in blocking:
            print(
                f"  - [{r.effective_severity.value}] {r.table}.{r.check_name}: "
                f"{r.violation_count}/{r.element_count} rows ({r.failure_rate:.4%}) "
                f"sample_pks={r.sample_pks[:5]}"
            )

    if recoverable:
        print("\nRECOVERABLE (logged, not blocking):")
        for r in recoverable:
            print(
                f"  - {r.table}.{r.check_name}: "
                f"{r.violation_count}/{r.element_count} rows ({r.failure_rate:.4%}) "
                f"sample_pks={r.sample_pks[:5]}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 3 data-quality gate")
    parser.add_argument("--suite", default="all", help="Suite name (default: all). See suites/registry.py.")
    parser.add_argument(
        "--fail-on",
        default="critical",
        choices=["critical"],
        help="Exit non-zero iff any check's effective severity reaches this level "
        "(only 'critical' is currently supported -- WARNING checks already "
        "self-escalate to CRITICAL when their mostly threshold is exceeded; "
        "see severity.py's CheckResult.is_blocking).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = build_spark_session("dq-runner")
    ensure_audit_table(spark)

    run_id = str(uuid.uuid4())
    run_at = datetime.now(timezone.utc)

    results = run_suite(spark, args.suite)
    write_audit_rows(spark, run_id, run_at, results)
    print_report(results)

    blocking = [r for r in results if r.is_blocking]
    if blocking:
        logger.error("%d blocking (CRITICAL) failure(s) -- see report above", len(blocking))
        return 1
    logger.info("No blocking failures (%d checks run).", len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
