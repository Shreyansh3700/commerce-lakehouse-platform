from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from debezium_envelope import build_dlq_record, compute_event_id, extract_fields
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from spark_session import build_spark_session

logger = logging.getLogger("bronze_writer")

# (kafka_topic, iceberg_table) -- one generic app drives all 7, not 7
# near-duplicate scripts, since the parse/write logic is identical.
TABLES = [
    ("cdc.customers", "nessie.bronze.customers_cdc"),
    ("cdc.products", "nessie.bronze.products_cdc"),
    ("cdc.orders", "nessie.bronze.orders_cdc"),
    ("cdc.order_items", "nessie.bronze.order_items_cdc"),
    ("cdc.payments", "nessie.bronze.payments_cdc"),
    ("cdc.inventory", "nessie.bronze.inventory_cdc"),
    ("cdc.shipments", "nessie.bronze.shipments_cdc"),
]

_PARSE_SCHEMA = StructType(
    [
        StructField("operation", StringType()),
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("source_timestamp", TimestampType()),
        StructField("source_lsn", LongType()),
        StructField("transaction_id", StringType()),
        StructField("parse_error_type", StringType()),
        StructField("parse_error_message", StringType()),
    ]
)


def _parse_safe(raw_value: str | None) -> dict:
    """UDF body: never raises -- a parse failure is reported via the
    parse_error_* fields instead, so one bad row can't fail the whole batch.
    Type and message are kept as separate fields (rather than one combined
    string) so the DLQ record's error_type reflects the real exception class
    instead of a generic wrapper -- a UDF can only return plain columns
    across the JVM/Python boundary, not exception objects."""
    try:
        fields = extract_fields(raw_value)
        fields["parse_error_type"] = None
        fields["parse_error_message"] = None
        return fields
    except Exception as exc:  # noqa: BLE001 -- must not raise inside a UDF
        return {
            "operation": None,
            "before": None,
            "after": None,
            "source_timestamp": None,
            "source_lsn": None,
            "transaction_id": None,
            "parse_error_type": type(exc).__name__,
            "parse_error_message": str(exc),
        }


parse_envelope_udf = udf(_parse_safe, _PARSE_SCHEMA)
event_id_udf = udf(compute_event_id, StringType())


def process_batch(
    batch_df: DataFrame, batch_id: int, iceberg_table: str, dlq_topic: str, kafka_bootstrap_servers: str
) -> None:
    if batch_df.rdd.isEmpty():
        return

    decoded = (
        batch_df.withColumn("value_str", col("value").cast("string"))
        .withColumn("parsed", parse_envelope_udf(col("value_str")))
        .select(
            "topic",
            "partition",
            "offset",
            "value_str",
            col("parsed.operation").alias("operation"),
            col("parsed.before").alias("before"),
            col("parsed.after").alias("after"),
            col("parsed.source_timestamp").alias("source_timestamp"),
            col("parsed.source_lsn").alias("source_lsn"),
            col("parsed.transaction_id").alias("transaction_id"),
            col("parsed.parse_error_type").alias("parse_error_type"),
            col("parsed.parse_error_message").alias("parse_error_message"),
        )
    )
    decoded.persist()
    try:
        good = decoded.filter(col("parse_error_type").isNull())
        bad = decoded.filter(col("parse_error_type").isNotNull())

        good_count = good.count()
        bad_count = bad.count()

        if good_count:
            enriched = (
                good.withColumn("event_id", event_id_udf(col("topic"), col("partition"), col("offset")))
                .withColumn("kafka_topic", col("topic"))
                .withColumn("kafka_partition", col("partition"))
                .withColumn("kafka_offset", col("offset"))
                .withColumn("ingestion_timestamp", lit(datetime.now(timezone.utc)))
                .select(
                    "event_id",
                    "operation",
                    "before",
                    "after",
                    "source_timestamp",
                    "source_lsn",
                    "transaction_id",
                    "kafka_topic",
                    "kafka_partition",
                    "kafka_offset",
                    "ingestion_timestamp",
                )
            )
            enriched.writeTo(iceberg_table).append()
            logger.info("batch=%s table=%s wrote=%d", batch_id, iceberg_table, good_count)

        if bad_count:
            now = datetime.now(timezone.utc)
            dlq_rows = bad.select(
                "value_str", "topic", "partition", "offset", "parse_error_type", "parse_error_message"
            ).collect()
            dlq_records = [
                build_dlq_record(
                    row["value_str"],
                    row["parse_error_type"],
                    row["parse_error_message"],
                    row["topic"],
                    row["partition"],
                    row["offset"],
                    now,
                )
                for row in dlq_rows
            ]
            spark = batch_df.sparkSession
            dlq_df = spark.createDataFrame(dlq_records)
            dlq_df.writeTo("nessie.bronze.dlq_events").append()
            (
                dlq_df.selectExpr("original_event AS value")
                .write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                .option("topic", dlq_topic)
                .save()
            )
            logger.warning("batch=%s table=%s quarantined=%d to %s", batch_id, iceberg_table, bad_count, dlq_topic)
    finally:
        decoded.unpersist()


def ensure_bronze_tables(spark: SparkSession, ddl_path: str = "/opt/spark-app/bronze_ddl.sql") -> None:
    with open(ddl_path) as f:
        raw = f.read()
    # Strip full-line comments before splitting on ';' -- checking whether
    # each semicolon-delimited chunk *starts* with '--' would wrongly drop
    # any statement preceded by a comment header in the same chunk (e.g.
    # CREATE NAMESPACE, which has one).
    lines = [line for line in raw.splitlines() if not line.strip().startswith("--")]
    script = "\n".join(lines)
    statements = [s.strip() for s in script.split(";") if s.strip()]
    for statement in statements:
        spark.sql(statement)
    logger.info("Bronze DDL applied (%d statements)", len(statements))


def start_query(spark: SparkSession, kafka_bootstrap_servers: str, kafka_topic: str, iceberg_table: str):
    table_short = kafka_topic.split(".", 1)[1]
    dlq_topic = f"cdc.{table_short}.dlq"
    checkpoint = f"s3a://lakehouse/checkpoints/bronze/{table_short}/"

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("kafka.group.id", f"bronze-writer-{table_short}")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    def _batch_fn(batch_df: DataFrame, batch_id: int) -> None:
        process_batch(batch_df, batch_id, iceberg_table, dlq_topic, kafka_bootstrap_servers)

    return (
        stream_df.writeStream.foreachBatch(_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    spark = build_spark_session("bronze-writer")
    ensure_bronze_tables(spark)

    queries = [start_query(spark, kafka_bootstrap_servers, topic, table) for topic, table in TABLES]
    logger.info("started %d Bronze streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
