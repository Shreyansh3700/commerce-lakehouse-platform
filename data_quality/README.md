# Data quality gate (Great Expectations)

Validates the Silver Iceberg tables (`nessie.silver.*`) before
`batch/pyspark/gold_builder.py` is allowed to build Gold from them. See
`severity.py` for the CRITICAL /
WARNING / RECOVERABLE model, and the module docstrings in `checks/*.py` for
exactly which checks run against which tables and why.

## Layout

- `severity.py` -- `Severity`/`Category` enums and the `CheckResult`
  dataclass (pass/fail, blocking, and effective-severity logic all live
  here).
- `ge_helpers.py` -- thin wrappers around Great Expectations' classic
  `SparkDFDataset` API that produce a `CheckResult`.
- `checks/completeness.py`, `checks/validity.py`,
  `checks/referential_integrity.py`, `checks/business_rules.py` -- the
  actual expectation definitions, one `run(spark) -> list[CheckResult]`
  per module.
- `suites/registry.py` -- maps a `--suite` name to the modules to run.
- `runner.py` -- CLI entrypoint; also writes every run's results to the
  `nessie.audit.dq_results` Iceberg table and exits non-zero iff anything
  CRITICAL (including WARNING checks that escalated) failed.

## Running

Standalone (non-blocking visibility -- inspect the report, doesn't stop
anything):

```
make dq-check
```

As the actual gate in front of the Gold build (halts before
`gold_builder.py` runs on any CRITICAL failure):

```
make gold-build
```

Both wrap `docker compose run --rm --profile tools dq ...` -- see the
Makefile. `runner.py`'s exit-code contract (0 iff zero blocking failures)
is orchestrator-agnostic on purpose: it's what makes the Phase 3
prerequisite chain in `make gold-build` (`dq-check -> gold-build-run ->
gold-test`) today map directly onto an Airflow task dependency in Phase 5
with no changes to this script.
