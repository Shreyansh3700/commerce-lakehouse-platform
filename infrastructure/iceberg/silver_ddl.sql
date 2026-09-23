-- Silver layer: current-state Iceberg tables, kept up to date by
-- streaming/pyspark/silver_writer.py's per-entity MERGE INTO from Bronze CDC
-- (dedup by event_id, ordered by source_lsn, guarded by
-- source.source_lsn > target._source_lsn on every MERGE match -- see the
-- Phase 3 plan for the full idempotency argument).
--
-- Every table carries a bookkeeping block (_event_id, _source_lsn,
-- _source_transaction_id, _bronze_operation, _bronze_ingestion_timestamp,
-- _silver_committed_at) recording the last Bronze CDC event applied to that
-- row -- this is what the MERGE guard compares against.
--
-- Executed idempotently (CREATE ... IF NOT EXISTS) by the Silver writer at
-- startup -- see streaming/pyspark/silver_writer.py.

CREATE NAMESPACE IF NOT EXISTS nessie.silver;

-- customers: never hard-deleted at source (docs/data_model.md) -- is_deleted
-- /deleted_at exist defensively in case a delete event ever arrives anyway.
-- Unpartitioned: small dimension table, point-lookup access pattern.
CREATE TABLE IF NOT EXISTS nessie.silver.customers (
    customer_id                 BIGINT,
    name                        STRING,
    email                       STRING,
    city                        STRING,
    country                     STRING,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP,
    is_deleted                  BOOLEAN,
    deleted_at                  TIMESTAMP,
    _event_id                   STRING,
    _source_lsn                 BIGINT,
    _source_transaction_id      STRING,
    _bronze_operation           STRING,
    _bronze_ingestion_timestamp TIMESTAMP,
    _silver_committed_at        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- products: never hard-deleted at source -- same defensive soft-delete
-- shape. Unpartitioned: small table, category is low-cardinality (Iceberg
-- column stats prune fine without physically partitioning on it).
CREATE TABLE IF NOT EXISTS nessie.silver.products (
    product_id                  BIGINT,
    product_name                STRING,
    category                    STRING,
    price                       DECIMAL(10, 2),
    stock_quantity              INT,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP,
    is_deleted                  BOOLEAN,
    deleted_at                  TIMESTAMP,
    _event_id                   STRING,
    _source_lsn                 BIGINT,
    _source_transaction_id      STRING,
    _bronze_operation           STRING,
    _bronze_ingestion_timestamp TIMESTAMP,
    _silver_committed_at        TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- orders: partitioned by days(created_at) -- created_at is immutable for the
-- row's lifetime (unlike updated_at, which changes on every status
-- transition), avoiding cross-partition file churn on every MERGE. This is
-- the designated partition-evolution demo table (spec section 20): starts
-- at days(created_at), later evolves to months(created_at).
CREATE TABLE IF NOT EXISTS nessie.silver.orders (
    order_id                    BIGINT,
    customer_id                 BIGINT,
    order_status                STRING,
    total_amount                DECIMAL(12, 2),
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP,
    is_deleted                  BOOLEAN,
    deleted_at                  TIMESTAMP,
    _event_id                   STRING,
    _source_lsn                 BIGINT,
    _source_transaction_id      STRING,
    _bronze_operation           STRING,
    _bronze_ingestion_timestamp TIMESTAMP,
    _silver_committed_at        TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- order_items: genuinely hard-deleted at source (line-item removed
-- pre-payment) -- no is_deleted/deleted_at columns. Partitioned to mirror
-- orders, so join-pruning works symmetrically on both sides of Gold's
-- date-ranged order/order_item joins.
CREATE TABLE IF NOT EXISTS nessie.silver.order_items (
    order_item_id                BIGINT,
    order_id                     BIGINT,
    product_id                   BIGINT,
    quantity                     INT,
    unit_price                   DECIMAL(10, 2),
    created_at                   TIMESTAMP,
    updated_at                   TIMESTAMP,
    _event_id                    STRING,
    _source_lsn                  BIGINT,
    _source_transaction_id       STRING,
    _bronze_operation            STRING,
    _bronze_ingestion_timestamp  TIMESTAMP,
    _silver_committed_at         TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- payments: never hard-deleted at source -- defensive soft-delete shape.
-- Partitioned by days(created_at): Gold's payment_metrics is date-bucketed,
-- and payments join to orders over the same date range.
CREATE TABLE IF NOT EXISTS nessie.silver.payments (
    payment_id                   BIGINT,
    order_id                     BIGINT,
    payment_status                STRING,
    payment_method                 STRING,
    amount                        DECIMAL(12, 2),
    created_at                    TIMESTAMP,
    updated_at                    TIMESTAMP,
    is_deleted                    BOOLEAN,
    deleted_at                    TIMESTAMP,
    _event_id                     STRING,
    _source_lsn                   BIGINT,
    _source_transaction_id        STRING,
    _bronze_operation             STRING,
    _bronze_ingestion_timestamp   TIMESTAMP,
    _silver_committed_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- inventory: genuinely hard-deleted at source (rare housekeeping correction)
-- -- no is_deleted/deleted_at columns. No immutable date column exists at
-- source, and access is a near-full scan (product x warehouse grain), so
-- unpartitioned.
CREATE TABLE IF NOT EXISTS nessie.silver.inventory (
    inventory_id                  BIGINT,
    product_id                    BIGINT,
    warehouse_id                  INT,
    available_quantity            INT,
    reserved_quantity             INT,
    updated_at                    TIMESTAMP,
    _event_id                     STRING,
    _source_lsn                   BIGINT,
    _source_transaction_id        STRING,
    _bronze_operation             STRING,
    _bronze_ingestion_timestamp   TIMESTAMP,
    _silver_committed_at          TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- shipments: never hard-deleted at source -- defensive soft-delete shape. No
-- immutable date column exists at source (shipped_at/updated_at both mutate
-- after row creation, so neither is safe as a partition key -- see orders'
-- created_at reasoning above), so unpartitioned for v1. A Silver-only
-- _first_seen_at column (set once at INSERT, never touched by UPDATE) is the
-- documented evolution path if this table's volume ever warrants it.
CREATE TABLE IF NOT EXISTS nessie.silver.shipments (
    shipment_id                   BIGINT,
    order_id                      BIGINT,
    shipment_status                STRING,
    carrier                        STRING,
    shipped_at                     TIMESTAMP,
    delivered_at                   TIMESTAMP,
    updated_at                     TIMESTAMP,
    is_deleted                     BOOLEAN,
    deleted_at                     TIMESTAMP,
    _event_id                      STRING,
    _source_lsn                    BIGINT,
    _source_transaction_id         STRING,
    _bronze_operation              STRING,
    _bronze_ingestion_timestamp    TIMESTAMP,
    _silver_committed_at           TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- Dead-letter table: rows silver_writer.py couldn't validate (bad operation
-- code, missing/unparseable before/after payload, null primary key, or null
-- source_lsn). Mirrors bronze.dlq_events' shape/partitioning.
CREATE TABLE IF NOT EXISTS nessie.silver.dlq_events (
    bronze_event_id STRING,
    source_table    STRING,
    error_type      STRING,
    error_message   STRING,
    raw_before      STRING,
    raw_after       STRING,
    failed_at       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(failed_at));
