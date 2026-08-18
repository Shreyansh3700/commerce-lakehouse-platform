-- Bronze layer: immutable, append-only raw CDC history.
--
-- before/after are raw JSON STRING (not typed STRUCT) -- deliberately, so a
-- source schema change (Phase 4) never forces an Iceberg schema-evolution on
-- Bronze itself. See docs/decisions/0005-bronze-before-after-as-raw-json.md.
--
-- Executed idempotently (CREATE ... IF NOT EXISTS) by the Bronze writer at
-- startup -- see streaming/pyspark/bronze_writer.py.

CREATE NAMESPACE IF NOT EXISTS nessie.bronze;

CREATE TABLE IF NOT EXISTS nessie.bronze.customers_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.products_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.orders_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.order_items_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.payments_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.inventory_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

CREATE TABLE IF NOT EXISTS nessie.bronze.shipments_cdc (
    event_id            STRING,
    operation           STRING,
    before              STRING,
    after               STRING,
    source_timestamp    TIMESTAMP,
    source_lsn          BIGINT,
    transaction_id      STRING,
    kafka_topic         STRING,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_timestamp))
TBLPROPERTIES ('write.target-file-size-bytes' = '134217728');

-- Dead-letter table: rows the Bronze writer couldn't parse. See
-- docs/decisions/0006-dlq-lives-in-bronze-writer-not-kafka-connect.md.
CREATE TABLE IF NOT EXISTS nessie.bronze.dlq_events (
    original_event  STRING,
    error_message   STRING,
    error_type      STRING,
    source_topic    STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    failed_at       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(failed_at));
