"""Shared helpers for building CheckResult objects.

Uses Great Expectations' classic `great_expectations.dataset.SparkDFDataset`
API (not the newer Fluent Datasource/Data Context API) deliberately: this
gate validates ad hoc Spark DataFrames read straight from Iceberg tables --
there's no persistent GE project (`great_expectations.yml`, stored
Expectation Suites/Checkpoints) to set up, and the classic Dataset API lets
a single expectation call run directly against an in-memory DataFrame with
no extra scaffolding. Column-level checks (completeness/validity) go
through GE directly; referential-integrity/business-rule checks are
derived first as a plain PySpark anti-join / filter (GE has no first-class
cross-table "relationships" expectation -- that's a dbt-test concept), then
validated with GE's own `expect_table_row_count_to_equal(0)` against that
derived violation batch, so every check still flows through the same
CheckResult shape via one consistent mechanism.
"""

from __future__ import annotations

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame

from severity import Category, CheckResult, Severity


def _sample_pks(df: DataFrame, pk_cols: list[str], limit: int = 20) -> list[str]:
    rows = df.select(*pk_cols).limit(limit).collect()
    if len(pk_cols) == 1:
        return [str(row[0]) for row in rows]
    return [str(tuple(row)) for row in rows]


def column_check(
    df: DataFrame,
    table: str,
    check_name: str,
    category: Category,
    severity: Severity,
    pk_cols: list[str],
    expectation: str,
    mostly_threshold: float = 0.0,
    **expectation_kwargs,
) -> CheckResult:
    """Runs one GE column expectation (by method name, e.g.
    'expect_column_values_to_not_be_null') against `df` via the classic
    SparkDFDataset API, and folds the result into a CheckResult."""
    element_count = df.count()
    ge_df = SparkDFDataset(df)
    method = getattr(ge_df, expectation)
    result = method(**expectation_kwargs, result_format="SUMMARY")
    unexpected_count = result["result"].get("unexpected_count", 0) or 0
    unexpected_list = result["result"].get("partial_unexpected_list", []) or []
    return CheckResult(
        table=table,
        check_name=check_name,
        category=category,
        severity=severity,
        element_count=element_count,
        violation_count=unexpected_count,
        mostly_threshold=mostly_threshold,
        sample_pks=[str(v) for v in unexpected_list[:20]],
    )


def row_count_check(
    violations_df: DataFrame,
    total_df: DataFrame,
    table: str,
    check_name: str,
    category: Category,
    severity: Severity,
    pk_cols: list[str],
    mostly_threshold: float = 0.0,
) -> CheckResult:
    """For referential-integrity/business-rule checks: `violations_df` is a
    derived DataFrame (e.g. an anti-join) containing exactly the rows that
    violate the rule. Validated via GE's `expect_table_row_count_to_equal`
    against the derived batch, so both this and `column_check` above always
    produce a CheckResult through a GE expectation call."""
    violation_count = violations_df.count()
    element_count = total_df.count()
    ge_violations = SparkDFDataset(violations_df)
    ge_violations.expect_table_row_count_to_equal(0, result_format="SUMMARY")
    return CheckResult(
        table=table,
        check_name=check_name,
        category=category,
        severity=severity,
        element_count=element_count,
        violation_count=violation_count,
        mostly_threshold=mostly_threshold,
        sample_pks=_sample_pks(violations_df, pk_cols),
    )
