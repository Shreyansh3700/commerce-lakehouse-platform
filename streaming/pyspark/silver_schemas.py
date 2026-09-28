from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Debezium's Postgres connector is configured with decimal.handling.mode=string
# (see docs/cdc.md), so NUMERIC/DECIMAL source columns arrive in the before/
# after JSON as quoted strings (e.g. "19.99"), not JSON numbers. The schemas
# below therefore parse those columns as StringType via from_json, and
# silver_transform.py CASTs them to the target DECIMAL type afterwards --
# from_json silently nulls out a numeric-typed field when fed a JSON string,
# so parsing directly as DecimalType here would corrupt every money column.
#
# created_at/updated_at/shipped_at/delivered_at are Postgres TIMESTAMPTZ
# columns; Debezium (no custom converters configured) encodes these as
# io.debezium.time.ZonedTimestamp ISO-8601 strings, which Spark's default
# JSON timestamp parsing handles as TimestampType directly.

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", LongType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("price", StringType()),  # DECIMAL(10,2) source -- see note above
        StructField("stock_quantity", IntegerType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("order_status", StringType()),
        StructField("total_amount", StringType()),  # DECIMAL(12,2) source
        # Added in Phase 4's schema-evolution demo (see
        # infrastructure/postgres/migrations/001_add_orders_discount_amount.sql
        # and docs/decisions/0008-schema-evolution-strategy.md). Absent from
        # pre-migration Bronze envelopes -- from_json silently leaves it null
        # for those, which is exactly the additive-column safety property
        # being demonstrated.
        StructField("discount_amount", StringType()),  # DECIMAL(10,2) source
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", LongType()),
        StructField("order_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", StringType()),  # DECIMAL(10,2) source
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", LongType()),
        StructField("order_id", LongType()),
        StructField("payment_status", StringType()),
        StructField("payment_method", StringType()),
        StructField("amount", StringType()),  # DECIMAL(12,2) source
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("inventory_id", LongType()),
        StructField("product_id", LongType()),
        StructField("warehouse_id", IntegerType()),
        StructField("available_quantity", IntegerType()),
        StructField("reserved_quantity", IntegerType()),
        StructField("updated_at", TimestampType()),
    ]
)

SHIPMENTS_SCHEMA = StructType(
    [
        StructField("shipment_id", LongType()),
        StructField("order_id", LongType()),
        StructField("shipment_status", StringType()),
        StructField("carrier", StringType()),
        StructField("shipped_at", TimestampType()),
        StructField("delivered_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)

# entity name -> the StructType used to from_json() Bronze's before/after
# JSON string for that entity. Keyed by TableConfig.entity in silver_tables.py.
ENTITY_SCHEMAS: dict[str, StructType] = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "inventory": INVENTORY_SCHEMA,
    "shipments": SHIPMENTS_SCHEMA,
}

# entity -> {field_name: target Spark SQL DECIMAL type string}, applied via a
# CAST after from_json (see decimal.handling.mode=string note above).
DECIMAL_CAST_FIELDS: dict[str, dict[str, str]] = {
    "products": {"price": "DECIMAL(10,2)"},
    "orders": {"total_amount": "DECIMAL(12,2)", "discount_amount": "DECIMAL(10,2)"},
    "order_items": {"unit_price": "DECIMAL(10,2)"},
    "payments": {"amount": "DECIMAL(12,2)"},
}

# ---------------------------------------------------------------------------
# Rename handling (shipments.carrier -> carrier_name): the expand-contract
# read side of Part C's schema-evolution demo. See docs/decisions/0008-
# schema-evolution-strategy.md for the general rule; this is its concrete
# application to the one rename this project demonstrates
# (infrastructure/postgres/migrations/002_rename_shipments_carrier_to_carrier_name.sql).
#
# SHIPMENTS_SCHEMA above is left with ONLY `carrier` -- it doubles as both
# (a) the from_json parse schema and (b) (via entity_fields in
# silver_transform.py) the literal list of Silver output columns, and the
# Silver output column must stay singular (`carrier`): downstream (Gold
# staging views, DDL, docs) all reference `carrier`, and none of that should
# need to change just because the *source* JSON key changed. So the extra
# `carrier_name` key Debezium starts emitting post-rename is handled
# entirely below, without ever becoming a second Silver column.
# ---------------------------------------------------------------------------

# entity -> extra StructFields that must be recognized when parsing Bronze's
# before/after JSON (so the new post-rename key doesn't just get silently
# dropped by from_json), but that are NOT added to entity_fields / the
# Silver output column list. silver_transform.py's validate() merges these
# into the actual from_json parse schema; ENTITY_SCHEMAS itself is left
# untouched so every other place that derives Silver's column list from it
# (build_merge_sql, etc.) is unaffected.
EXTRA_PARSE_FIELDS: dict[str, list[StructField]] = {
    "shipments": [StructField("carrier_name", StringType())],
}

# entity -> {silver_output_column: [source JSON keys, in priority order]}.
# silver_transform.py's validate() uses this in place of a plain
# `col("_typed.<field>")` reference when building that Silver output
# column's value: COALESCE(source[0], source[1], ...) picks the first
# non-null key present on the parsed event. For shipments.carrier, that
# means events emitted after the rename (which carry `carrier_name` and a
# null/absent `carrier`) and pre-rename or replayed historical events
# (which carry `carrier` and no `carrier_name`) both resolve to the correct
# value in the single `carrier` Silver column, with no crash and no NULL
# gap either way.
FIELD_COALESCE: dict[str, dict[str, list[str]]] = {
    "shipments": {"carrier": ["carrier_name", "carrier"]},
}
