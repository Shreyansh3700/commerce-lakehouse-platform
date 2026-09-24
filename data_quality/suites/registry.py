"""Maps a --suite name to the check-runner functions to execute."""

from __future__ import annotations

from typing import Callable

from pyspark.sql import SparkSession

from checks import business_rules, completeness, referential_integrity, validity
from severity import CheckResult

_ALL_MODULES: dict[str, Callable[[SparkSession], list[CheckResult]]] = {
    "completeness": completeness.run,
    "validity": validity.run,
    "referential_integrity": referential_integrity.run,
    "business_rules": business_rules.run,
}

# Coarser, table-oriented suites some callers may want (e.g. re-checking
# only one entity after a targeted fix). Documented here for traceability;
# not currently used to filter which checks run -- `get_suite` today only
# understands the category names above plus "all".
SUITE_TABLES = {
    "orders": ["orders", "order_items"],
    "payments": ["payments"],
    "shipments": ["shipments"],
    "customers": ["customers"],
    "products": ["products", "inventory"],
}


def get_suite(name: str) -> list[Callable[[SparkSession], list[CheckResult]]]:
    if name == "all":
        return list(_ALL_MODULES.values())
    if name in _ALL_MODULES:
        return [_ALL_MODULES[name]]
    raise ValueError(f"Unknown suite '{name}'. Known suites: all, {', '.join(_ALL_MODULES)}")
