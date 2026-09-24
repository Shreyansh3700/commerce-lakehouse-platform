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
    "orders": {"total_amount": "DECIMAL(12,2)"},
    "order_items": {"unit_price": "DECIMAL(10,2)"},
    "payments": {"amount": "DECIMAL(12,2)"},
}
