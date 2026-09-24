"""Builds the six Gold analytical tables from Silver via plain Spark SQL --
no dbt. This replaced an earlier dbt-based implementation (see
docs/decisions/0007-plain-pyspark-gold-not-dbt.md for why): same logic,
same required output columns (spec section 15), just expressed as Python +
Spark SQL instead of a dbt project, since dbt was an unfamiliar tool for
this project's maintainer.

Run via `make gold-build-run` (docker compose run --rm --profile tools
gold-builder). Five of the six tables are a plain full-refresh
(`INSERT OVERWRITE`-equivalent `writeTo(...).overwritePartitions()`) on
every run -- cheap enough at this project's scale (<=10k products/customers,
~1M orders). `daily_sales` is the one incremental table -- see
build_daily_sales()'s docstring.

Staging/intermediate views (STG_VIEWS, INT_ORDERS_ENRICHED_SQL) mirror what
were transformations/dbt/models/staging/*.sql and
models/intermediate/int_orders_enriched.sql -- kept as named temp views for
the same reason dbt kept them as separate models: int_orders_enriched is
reused by three Gold tables (daily_sales, customer_lifetime_value,
order_fulfillment_metrics), so its join/aggregation logic is written once."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from spark_session import build_spark_session

logger = logging.getLogger("gold_builder")

_DDL_PATH = "/opt/spark-app/gold_ddl.sql"

# Order->delivery threshold used by int_orders_enriched.is_delayed (and thus
# order_fulfillment_metrics.delayed_orders). Was a dbt `--vars` override
# point (`fulfillment_sla_hours`); now just a module constant -- edit here
# and re-run `make gold-build-run` for a different SLA.
FULFILLMENT_SLA_HOURS = 120

# name -> SQL selecting Silver's live/current rows with Gold-relevant
# columns only. Mirrors the retired dbt project's staging models exactly
# (including the is_deleted filter on the five soft-delete entities;
# order_items/inventory are hard-delete, so no filter needed on those).
STG_VIEWS: dict[str, str] = {
    "stg_customers": """
        SELECT customer_id, name, email, city, country, created_at, updated_at
        FROM nessie.silver.customers WHERE is_deleted = false
    """,
    "stg_products": """
        SELECT product_id, product_name, category, price, stock_quantity, created_at, updated_at
        FROM nessie.silver.products WHERE is_deleted = false
    """,
    "stg_orders": """
        SELECT order_id, customer_id, order_status, total_amount, created_at, updated_at
        FROM nessie.silver.orders WHERE is_deleted = false
    """,
    "stg_order_items": """
        SELECT order_item_id, order_id, product_id, quantity, unit_price, created_at, updated_at
        FROM nessie.silver.order_items
    """,
    "stg_payments": """
        SELECT payment_id, order_id, payment_status, payment_method, amount, created_at, updated_at
        FROM nessie.silver.payments WHERE is_deleted = false
    """,
    "stg_inventory": """
        SELECT inventory_id, product_id, warehouse_id, available_quantity, reserved_quantity, updated_at
        FROM nessie.silver.inventory
    """,
    "stg_shipments": """
        SELECT shipment_id, order_id, shipment_status, carrier, shipped_at, delivered_at, updated_at
        FROM nessie.silver.shipments WHERE is_deleted = false
    """,
}

# Order-grain model: one row per order_id, enriched with per-order
# aggregates from order_items/payments and the order's most-recent
# shipment. Reused by daily_sales, customer_lifetime_value, and
# order_fulfillment_metrics.
_INT_ORDERS_ENRICHED_SQL_TEMPLATE = """
    WITH orders AS (
        SELECT * FROM stg_orders
    ),
    item_agg AS (
        SELECT order_id, SUM(quantity * unit_price) AS item_revenue, SUM(quantity) AS item_quantity
        FROM stg_order_items
        GROUP BY order_id
    ),
    payment_agg AS (
        SELECT
            order_id,
            SUM(CASE WHEN payment_status = 'SUCCESS' THEN amount ELSE 0 END) AS successful_payment_amount,
            SUM(CASE WHEN payment_status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_payment_amount
        FROM stg_payments
        GROUP BY order_id
    ),
    -- An order could in principle have more than one shipment row (e.g. a
    -- re-ship); pick the most-recently-updated one so this stays order grain.
    shipment_ranked AS (
        SELECT
            order_id, shipment_status, carrier, shipped_at, delivered_at,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
        FROM stg_shipments
    ),
    shipments AS (
        SELECT order_id, shipment_status, carrier, shipped_at, delivered_at
        FROM shipment_ranked WHERE rn = 1
    )
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.total_amount,
        orders.created_at,
        orders.updated_at,
        COALESCE(item_agg.item_revenue, 0) AS item_revenue,
        COALESCE(item_agg.item_quantity, 0) AS item_quantity,
        COALESCE(payment_agg.successful_payment_amount, 0) AS successful_payment_amount,
        COALESCE(payment_agg.refunded_payment_amount, 0) AS refunded_payment_amount,
        shipments.shipment_status,
        shipments.carrier,
        shipments.shipped_at,
        shipments.delivered_at,
        -- SLA measured from order creation to delivery; undelivered orders
        -- are never flagged delayed here (order_fulfillment_metrics only
        -- counts delayed_orders among delivered orders).
        CASE
            WHEN shipments.delivered_at IS NOT NULL
                THEN DATEDIFF(shipments.delivered_at, orders.created_at) > ({sla_hours} / 24.0)
            ELSE false
        END AS is_delayed
    FROM orders
    LEFT JOIN item_agg ON orders.order_id = item_agg.order_id
    LEFT JOIN payment_agg ON orders.order_id = payment_agg.order_id
    LEFT JOIN shipments ON orders.order_id = shipments.order_id
"""


def _apply_ddl(spark: SparkSession) -> None:
    with open(_DDL_PATH) as f:
        raw = f.read()
    lines = [line for line in raw.splitlines() if not line.strip().startswith("--")]
    script = "\n".join(lines)
    statements = [s.strip() for s in script.split(";") if s.strip()]
    for statement in statements:
        spark.sql(statement)
    logger.info("Gold DDL applied (%d statements)", len(statements))


def _create_views(spark: SparkSession) -> None:
    for name, sql in STG_VIEWS.items():
        spark.sql(sql).createOrReplaceTempView(name)
    spark.sql(_INT_ORDERS_ENRICHED_SQL_TEMPLATE.format(sla_hours=FULFILLMENT_SLA_HOURS)).createOrReplaceTempView(
        "int_orders_enriched"
    )


def _table_is_empty(spark: SparkSession, table: str) -> bool:
    return spark.table(table).limit(1).count() == 0


def build_daily_sales(spark: SparkSession) -> None:
    """Silver is mutable current-state (not an append log): an order
    created yesterday can be cancelled today, retroactively changing
    yesterday's aggregate row. A naive "only append new dates" incremental
    would miss that. Instead, on every run after the first we recompute
    aggregates only for orders whose underlying row CHANGED in the last 3
    days (new orders, cancellations, status changes) -- regardless of how
    old that order's own created_at date is -- then MERGE those recomputed
    date-rows into the target on `date`, replacing stale rows for touched
    dates while leaving untouched history alone. The very first run has no
    prior state to preserve, so it computes and overwrites the full history
    instead of applying the 3-day filter.

    Documented limitation: an order that changes more than 3 days after
    this job's last run (e.g. a very late cancellation) won't be reflected
    until a manual full rebuild (call this function with an empty target
    table, or DROP/recreate nessie.gold.daily_sales first).
    """
    target = "nessie.gold.daily_sales"
    first_run = _table_is_empty(spark, target)
    orders_source = "int_orders_enriched" if first_run else "_daily_sales_incremental_source"
    if not first_run:
        spark.sql(
            "SELECT * FROM int_orders_enriched WHERE updated_at >= date_sub(current_timestamp(), 3)"
        ).createOrReplaceTempView(orders_source)

    agg = spark.sql(f"""
        SELECT
            DATE(created_at) AS date,
            CAST(COUNT(*) AS BIGINT) AS orders,
            CAST(SUM(total_amount) AS DECIMAL(18,2)) AS gross_revenue,
            -- REFUNDED payments only occur on CANCELLED orders per the
            -- order/payment state machine (docs/data_model.md), so
            -- excluding CANCELLED orders and subtracting refunds are
            -- currently overlapping safety nets, not double-counted
            -- correction -- kept as two explicit terms so net_revenue
            -- stays correct if that invariant ever loosens.
            CAST(
                SUM(CASE WHEN order_status != 'CANCELLED' THEN total_amount ELSE 0 END)
                - SUM(refunded_payment_amount) AS DECIMAL(18,2)
            ) AS net_revenue,
            CAST(SUM(total_amount) / COUNT(*) AS DECIMAL(12,2)) AS average_order_value,
            CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS unique_customers,
            CAST(SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS BIGINT) AS cancelled_orders
        FROM {orders_source}
        GROUP BY DATE(created_at)
    """)

    if first_run:
        agg.writeTo(target).overwritePartitions()
        logger.info("gold.daily_sales: first run, full build (%d dates)", agg.count())
    else:
        agg.createOrReplaceTempView("_daily_sales_touched")
        spark.sql(f"""
            MERGE INTO {target} AS target
            USING _daily_sales_touched AS source
            ON target.date = source.date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info("gold.daily_sales: incremental merge (%d touched dates)", agg.count())


def build_product_performance(spark: SparkSession) -> None:
    df = spark.sql("""
        WITH refunded_orders AS (
            SELECT DISTINCT order_id FROM stg_payments WHERE payment_status = 'REFUNDED'
        ),
        joined AS (
            SELECT
                oi.product_id, oi.quantity, oi.unit_price, o.order_status,
                CASE WHEN ro.order_id IS NOT NULL THEN true ELSE false END AS is_refunded
            FROM stg_order_items oi
            INNER JOIN stg_orders o ON oi.order_id = o.order_id
            LEFT JOIN refunded_orders ro ON oi.order_id = ro.order_id
        )
        SELECT
            product_id,
            CAST(SUM(CASE WHEN order_status != 'CANCELLED' THEN quantity ELSE 0 END) AS BIGINT) AS units_sold,
            CAST(SUM(CASE WHEN order_status != 'CANCELLED' THEN quantity * unit_price ELSE 0 END)
                AS DECIMAL(18,2)) AS revenue,
            CAST(SUM(CASE WHEN is_refunded THEN quantity * unit_price ELSE 0 END)
                AS DECIMAL(18,2)) AS refunds,
            CAST(
                SUM(CASE WHEN order_status != 'CANCELLED' THEN quantity * unit_price ELSE 0 END)
                / NULLIF(SUM(CASE WHEN order_status != 'CANCELLED' THEN quantity ELSE 0 END), 0)
                AS DECIMAL(12,2)
            ) AS average_selling_price
        FROM joined
        GROUP BY product_id
    """)
    df.writeTo("nessie.gold.product_performance").overwritePartitions()
    logger.info("gold.product_performance: %d products", df.count())


def build_customer_lifetime_value(spark: SparkSession) -> None:
    df = spark.sql("""
        WITH orders AS (
            SELECT * FROM int_orders_enriched WHERE order_status != 'CANCELLED'
        )
        SELECT
            customer_id,
            CAST(COUNT(*) AS BIGINT) AS total_orders,
            CAST(SUM(total_amount) AS DECIMAL(18,2)) AS total_spend,
            CAST(SUM(total_amount) / COUNT(*) AS DECIMAL(12,2)) AS average_order_value,
            MIN(created_at) AS first_order_date,
            MAX(created_at) AS last_order_date,
            -- Placeholder: a true predictive/discounted LTV model is out of
            -- scope here. Kept as its own column (not an alias of
            -- total_spend) so a future model swap doesn't force a
            -- downstream rename.
            CAST(SUM(total_amount) AS DECIMAL(18,2)) AS lifetime_value
        FROM orders
        GROUP BY customer_id
    """)
    df.writeTo("nessie.gold.customer_lifetime_value").overwritePartitions()
    logger.info("gold.customer_lifetime_value: %d customers", df.count())


def build_inventory_health(spark: SparkSession) -> None:
    df = spark.sql("""
        SELECT
            i.product_id,
            i.warehouse_id,
            i.available_quantity,
            i.reserved_quantity,
            i.available_quantity <= 0 AS stockout_flag,
            CAST(i.available_quantity * p.price AS DECIMAL(18,2)) AS inventory_value
        FROM stg_inventory i
        LEFT JOIN stg_products p ON i.product_id = p.product_id
    """)
    df.writeTo("nessie.gold.inventory_health").overwritePartitions()
    logger.info("gold.inventory_health: %d product/warehouse rows", df.count())


def build_order_fulfillment_metrics(spark: SparkSession) -> None:
    # average_fulfillment_time / average_delivery_time are in HOURS.
    df = spark.sql("""
        SELECT
            DATE(created_at) AS date,
            CAST(COUNT(*) AS BIGINT) AS orders,
            AVG(
                CASE WHEN shipped_at IS NOT NULL
                    THEN (UNIX_TIMESTAMP(shipped_at) - UNIX_TIMESTAMP(created_at)) / 3600.0
                END
            ) AS average_fulfillment_time,
            AVG(
                CASE WHEN delivered_at IS NOT NULL AND shipped_at IS NOT NULL
                    THEN (UNIX_TIMESTAMP(delivered_at) - UNIX_TIMESTAMP(shipped_at)) / 3600.0
                END
            ) AS average_delivery_time,
            CAST(SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS BIGINT) AS cancelled_orders,
            CAST(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS BIGINT) AS delayed_orders
        FROM int_orders_enriched
        GROUP BY DATE(created_at)
    """)
    df.writeTo("nessie.gold.order_fulfillment_metrics").overwritePartitions()
    logger.info("gold.order_fulfillment_metrics: %d dates", df.count())


def build_payment_metrics(spark: SparkSession) -> None:
    # Grouped by payment created_at date (not order date) -- a single order
    # can have multiple payment attempts on different days.
    df = spark.sql("""
        SELECT
            DATE(created_at) AS date,
            CAST(SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END) AS BIGINT) AS successful_payments,
            CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS BIGINT) AS failed_payments,
            -- PENDING/REFUNDED excluded from the denominator -- failure_rate
            -- measures the outcome of attempts that actually resolved.
            SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END)
                / NULLIF(
                    SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END)
                    + SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END),
                    0
                ) AS failure_rate,
            CAST(SUM(CASE WHEN payment_status = 'SUCCESS' THEN amount ELSE 0 END) AS DECIMAL(18,2))
                AS total_payment_value
        FROM stg_payments
        GROUP BY DATE(created_at)
    """)
    df.writeTo("nessie.gold.payment_metrics").overwritePartitions()
    logger.info("gold.payment_metrics: %d dates", df.count())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_spark_session("gold-builder")
    _apply_ddl(spark)
    _create_views(spark)

    build_daily_sales(spark)
    build_product_performance(spark)
    build_customer_lifetime_value(spark)
    build_inventory_health(spark)
    build_order_fulfillment_metrics(spark)
    build_payment_metrics(spark)

    logger.info("Gold build complete.")


if __name__ == "__main__":
    main()
