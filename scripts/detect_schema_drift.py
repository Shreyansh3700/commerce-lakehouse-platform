"""Schema-drift diagnostic: compares the JSON keys actually present in
recent Bronze `after` payloads for one entity against what Silver currently
knows how to parse (ENTITY_SCHEMAS), and reports any keys Silver would
silently ignore.

This is what makes spec section 18's "detect source schema changes"
concrete rather than just "the developer happened to notice" -- see the
Phase 4 plan's Part C. Deliberately NOT a blocking gate like data_quality's
`dq-check`: a schema-drift finding needs a human to decide how (or whether)
to handle it, so this is run on demand, not wired into any automated
pipeline step.

Meant to be run the same way scripts/spark_query.py is -- inside a container
that already has streaming/pyspark/spark_session.py AND
streaming/pyspark/silver_schemas.py available (for ENTITY_SCHEMAS), with the
Iceberg/Nessie/S3A JARs baked in. data_quality/Dockerfile COPYs both this
script and silver_schemas.py into the `dq` image; `make detect-schema-drift
TABLE=orders` wraps the command below:

    docker compose run --rm --profile tools dq \\
        /opt/spark-app/detect_schema_drift.py --table orders
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql.functions import col, explode, map_keys
from silver_schemas import ENTITY_SCHEMAS
from spark_session import build_spark_session

# entity name -> Bronze Iceberg table. Mirrors silver_tables.py's
# TableConfig.bronze_table mapping (not imported directly, to keep this
# standalone diagnostic's only cross-module dependency limited to
# silver_schemas.py's ENTITY_SCHEMAS).
BRONZE_TABLES: dict[str, str] = {
    "customers": "nessie.bronze.customers_cdc",
    "products": "nessie.bronze.products_cdc",
    "orders": "nessie.bronze.orders_cdc",
    "order_items": "nessie.bronze.order_items_cdc",
    "payments": "nessie.bronze.payments_cdc",
    "inventory": "nessie.bronze.inventory_cdc",
    "shipments": "nessie.bronze.shipments_cdc",
}

# How far back to look for "recent" activity. A narrow window keeps this
# diagnostic cheap and focused on current source behavior rather than all of
# history (which could include keys from a change that's since been
# reverted). See main()'s fallback below for the fresh-environment case.
RECENT_WINDOW = "INTERVAL 1 HOUR"


def _distinct_after_keys(spark, bronze_table: str, where_clause: str):
    """Returns the set of distinct top-level JSON keys present across
    `after` payloads matching where_clause. `after` is NULL for delete
    events (ADR 0005's raw-JSON-string Bronze shape), so those rows
    contribute no keys -- expected, not a bug: a delete's payload has
    nothing to say about the current source schema."""
    df = spark.sql(f"""
        SELECT DISTINCT key
        FROM (
            SELECT explode(map_keys(from_json(after, 'map<string,string>'))) AS key
            FROM {bronze_table}
            WHERE after IS NOT NULL AND {where_clause}
        )
    """)
    return {row["key"] for row in df.collect()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect Bronze JSON keys that Silver's ENTITY_SCHEMAS doesn't yet know about."
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(BRONZE_TABLES),
        help="Entity name, e.g. 'orders' (maps to nessie.bronze.orders_cdc + ENTITY_SCHEMAS['orders']).",
    )
    args = parser.parse_args()

    bronze_table = BRONZE_TABLES[args.table]
    known_fields = {f.name for f in ENTITY_SCHEMAS[args.table].fields}

    spark = build_spark_session("detect-schema-drift")

    recent_where = f"ingestion_timestamp > current_timestamp() - {RECENT_WINDOW}"
    observed_keys = _distinct_after_keys(spark, bronze_table, recent_where)
    window_desc = f"last {RECENT_WINDOW.replace('INTERVAL ', '').lower()}"

    if not observed_keys:
        # No rows in the recent window -- e.g. right after a fresh `make
        # seed` with the generator not yet running long. Fall back to all
        # rows rather than reporting a false "clean" result off zero data.
        observed_keys = _distinct_after_keys(spark, bronze_table, "1=1")
        window_desc = "all available rows (no rows in the recent window)"

    unrecognized = sorted(observed_keys - known_fields)

    print(f"detect_schema_drift: table={args.table} bronze_table={bronze_table} window={window_desc}")
    print(f"  known Silver fields:  {sorted(known_fields)}")
    print(f"  observed Bronze keys: {sorted(observed_keys)}")

    if unrecognized:
        print(f"  DRIFT DETECTED -- {len(unrecognized)} key(s) Silver does not yet parse: {unrecognized}")
        return 0
    print("  clean -- no unrecognized keys observed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
