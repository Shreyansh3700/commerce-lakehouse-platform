-- Gold layer namespace + all nessie.gold.* tables.
--
-- dim_customer_history is built by the Silver pipeline itself
-- (streaming/pyspark/scd2.py), not by batch/pyspark/gold_builder.py -- SCD2
-- needs the ordered stream of individual Bronze CDC changes, which only
-- exists at the point Silver dedupes/orders them; silver.customers
-- (current-state) has already discarded that history. See the Phase 3
-- plan's SCD2 section.
--
-- The other six tables (daily_sales, product_performance,
-- customer_lifetime_value, inventory_health, order_fulfillment_metrics,
-- payment_metrics) are built by batch/pyspark/gold_builder.py against
-- Silver -- see that file for the aggregation logic. Their column order
-- here MUST match each builder function's SELECT output order exactly,
-- since gold_builder.py writes via positional INSERT OVERWRITE / MERGE.
--
-- Executed idempotently (CREATE ... IF NOT EXISTS) by both the Silver
-- writer at startup (streaming/pyspark/silver_writer.py, for
-- dim_customer_history) and gold_builder.py at startup (for everything).
-- Unpartitioned throughout -- every Gold table here is a small aggregate
-- (one row per day, or per product/customer/warehouse), not a scan target
-- large enough to need partition pruning.

CREATE NAMESPACE IF NOT EXISTS nessie.gold;

CREATE TABLE IF NOT EXISTS nessie.gold.dim_customer_history (
    customer_history_id STRING,
    customer_id          BIGINT,
    name                  STRING,
    city                  STRING,
    country               STRING,
    valid_from            TIMESTAMP,
    valid_to              TIMESTAMP,
    is_current            BOOLEAN,
    _source_lsn           BIGINT,
    _event_id             STRING
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- Built incrementally (MERGE on `date`, 3-day updated_at lookback) by
-- gold_builder.py's build_daily_sales() -- the one Gold table that isn't a
-- plain full-refresh overwrite. See that function's docstring for why.
CREATE TABLE IF NOT EXISTS nessie.gold.daily_sales (
    date                 DATE,
    orders                BIGINT,
    gross_revenue          DECIMAL(18, 2),
    net_revenue             DECIMAL(18, 2),
    average_order_value      DECIMAL(12, 2),
    unique_customers           BIGINT,
    cancelled_orders             BIGINT
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.gold.product_performance (
    product_id            BIGINT,
    units_sold              BIGINT,
    revenue                   DECIMAL(18, 2),
    refunds                     DECIMAL(18, 2),
    average_selling_price         DECIMAL(12, 2)
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- lifetime_value is currently just an alias for total_spend -- a
-- placeholder for a future predictive/discounted LTV model, kept as its
-- own column so that model swap won't force a downstream rename.
CREATE TABLE IF NOT EXISTS nessie.gold.customer_lifetime_value (
    customer_id           BIGINT,
    total_orders            BIGINT,
    total_spend                DECIMAL(18, 2),
    average_order_value          DECIMAL(12, 2),
    first_order_date                TIMESTAMP,
    last_order_date                   TIMESTAMP,
    lifetime_value                      DECIMAL(18, 2)
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.gold.inventory_health (
    product_id            BIGINT,
    warehouse_id             INT,
    available_quantity          INT,
    reserved_quantity              INT,
    stockout_flag                     BOOLEAN,
    inventory_value                     DECIMAL(18, 2)
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- average_fulfillment_time / average_delivery_time are in HOURS.
CREATE TABLE IF NOT EXISTS nessie.gold.order_fulfillment_metrics (
    date                  DATE,
    orders                  BIGINT,
    average_fulfillment_time   DOUBLE,
    average_delivery_time        DOUBLE,
    cancelled_orders                BIGINT,
    delayed_orders                    BIGINT
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- Grouped by payment created_at date (not order date) -- a single order can
-- have multiple payment attempts on different days.
CREATE TABLE IF NOT EXISTS nessie.gold.payment_metrics (
    date                  DATE,
    successful_payments      BIGINT,
    failed_payments             BIGINT,
    failure_rate                   DOUBLE,
    total_payment_value               DECIMAL(18, 2)
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');
