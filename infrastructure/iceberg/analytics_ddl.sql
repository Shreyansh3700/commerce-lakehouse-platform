-- Analytics layer (Phase 4, spec sections 16-17): real-time streaming
-- metrics and late-arriving-event bookkeeping, read directly off the
-- `cdc.*` Kafka topics rather than off Bronze/Silver -- see
-- streaming/pyspark/analytics_writer.py's module docstring.
--
-- Executed idempotently (CREATE ... IF NOT EXISTS) by the Analytics writer
-- at startup -- see streaming/pyspark/analytics_writer.py.

CREATE NAMESPACE IF NOT EXISTS nessie.analytics;

-- Windowed aggregation sinks: one row per (window, *extra group keys),
-- upserted by analytics_transform.py's run_windowed_query() every trigger.
-- Small, point/range-lookup access pattern (one dashboard query per
-- window range) -- unpartitioned, same reasoning as silver.customers.

CREATE TABLE IF NOT EXISTS nessie.analytics.orders_per_minute (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    order_count  BIGINT,
    revenue      DECIMAL(18, 2),
    _updated_at  TIMESTAMP
)
USING iceberg;

CREATE TABLE IF NOT EXISTS nessie.analytics.revenue_5min (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    revenue      DECIMAL(18, 2),
    _updated_at  TIMESTAMP
)
USING iceberg;

-- failure_rate itself (failed_count / resolved_count) is computed at query
-- time (make query-analytics), same idiom as gold.payment_metrics -- not
-- stored here.
CREATE TABLE IF NOT EXISTS nessie.analytics.payment_failure_rate (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    failed_count   BIGINT,
    resolved_count BIGINT,
    _updated_at    TIMESTAMP
)
USING iceberg;

CREATE TABLE IF NOT EXISTS nessie.analytics.product_activity (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    product_id   BIGINT,
    quantity     BIGINT,
    revenue      DECIMAL(18, 2),
    _updated_at  TIMESTAMP
)
USING iceberg;

-- inventory_alerts: unbounded append log (one row per detected low-stock
-- condition, never updated in place) -- partitioned by days(alert_at), same
-- reasoning as bronze.dlq_events.
CREATE TABLE IF NOT EXISTS nessie.analytics.inventory_alerts (
    product_id          BIGINT,
    warehouse_id        INT,
    available_quantity  INT,
    reserved_quantity   INT,
    alert_at            TIMESTAMP,
    source_lsn          BIGINT
)
USING iceberg
PARTITIONED BY (days(alert_at));

-- late_events: unbounded append log of rows analytics_transform.py's
-- run_late_event_query() classified as late-beyond-threshold (a row Spark's
-- own watermark would have silently dropped from a windowed aggregation) --
-- partitioned by days(detected_at), same reasoning as bronze.dlq_events.
CREATE TABLE IF NOT EXISTS nessie.analytics.late_events (
    entity                STRING,
    event_id              STRING,
    event_time            TIMESTAMP,
    arrival_delay_seconds BIGINT,
    source_lsn            BIGINT,
    detected_at           TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(detected_at));

-- watermark_state: one row per opted-in late-event-detection query, holding
-- the watermark bound as of the end of that query's last completed batch.
-- Tiny (at most a handful of rows ever) -- unpartitioned.
CREATE TABLE IF NOT EXISTS nessie.analytics.watermark_state (
    query_name     STRING,
    max_event_time TIMESTAMP
)
USING iceberg;
