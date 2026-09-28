from __future__ import annotations

import logging
from datetime import datetime, timezone

from analytics_config import AlertConfig, LateEventConfig, WindowedMetricConfig
from debezium_envelope import compute_event_id, extract_fields
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, from_json, lit, udf, unix_timestamp, window
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
from silver_schemas import DECIMAL_CAST_FIELDS, ENTITY_SCHEMAS

logger = logging.getLogger("analytics_transform")

# All three query shapes below share one trigger cadence: faster than
# Silver's 30s (these are lighter per-batch aggregations/filters, not a
# validate->dedupe->MERGE pipeline over a whole entity), slower than
# Bronze's 10s (windowed aggregation benefits from a little more data
# accumulating per micro-batch before a MERGE is worth paying for).
TRIGGER_SECONDS = 20

# Caps how many Kafka offsets (summed across all subscribed partitions) one
# micro-batch will consume -- same rationale as silver_writer.py's
# streaming-max-files-per-micro-batch: with startingOffsets=earliest and a
# pre-existing topic backlog, an unbounded first batch attempting the whole
# backlog at once (windowed aggregation + stateful watermark tracking +
# Iceberg MERGE, all more expensive per row than Bronze's plain append) was
# observed to destabilize the JVM before that batch ever committed -- which
# meant the checkpoint never advanced and every restart retried the same
# oversized batch and failed again. Capping trades a slower catch-up for
# actually making forward progress.
MAX_OFFSETS_PER_TRIGGER = "2000"

# Mirrors bronze_writer.py's _PARSE_SCHEMA/_parse_safe/parse_envelope_udf
# exactly -- same UDF-wrapping approach, reusing debezium_envelope.py's
# extract_fields() rather than reimplementing envelope parsing. Duplicated
# here (not imported from bronze_writer.py) because this app reads Kafka
# independently of Bronze -- see this module's and analytics_writer.py's
# docstrings for why -- and the wrapper itself is a few lines of glue, not
# parsing logic.
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
    parse_error_type field instead, so one bad Kafka message can't fail the
    whole micro-batch. See bronze_writer.py's identical helper."""
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


def _parse_after(stream_df: DataFrame, entity: str) -> DataFrame:
    """Parses each raw Kafka message's Debezium envelope + `after` JSON
    payload into typed entity columns -- the same from_json-then-CAST
    pattern silver_transform.py's validate() uses, but simpler: no
    before/after operation-branching, since windowed aggregations, alerts,
    and late-event detection all only care about inserts/updates, not
    deletes. Rows with no `after` (deletes, or envelopes that failed to
    parse) are dropped here rather than routed to a DLQ -- unlike Bronze/
    Silver, this app has no durable record of its own to protect; a
    dropped row here just doesn't contribute to an in-memory aggregate or
    alert, and Bronze already durably captured the raw event independently.

    Returns topic/partition/offset (for event_id, where callers need it),
    source_lsn, source_timestamp, plus one column per field of
    ENTITY_SCHEMAS[entity], decimal-cast per DECIMAL_CAST_FIELDS."""
    entity_schema = ENTITY_SCHEMAS[entity]
    decimal_casts = DECIMAL_CAST_FIELDS.get(entity, {})
    entity_fields = [f.name for f in entity_schema.fields]

    parsed = (
        stream_df.withColumn("value_str", col("value").cast("string"))
        .withColumn("envelope", parse_envelope_udf(col("value_str")))
        .withColumn("after_typed", from_json(col("envelope.after"), entity_schema))
        .filter(col("envelope.parse_error_type").isNull() & col("after_typed").isNotNull())
    )

    entity_col_exprs = []
    for field in entity_fields:
        field_col = col(f"after_typed.{field}")
        cast_type = decimal_casts.get(field)
        if cast_type:
            field_col = field_col.cast(cast_type)
        entity_col_exprs.append(field_col.alias(field))

    return parsed.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("envelope.operation").alias("operation"),
        col("envelope.source_lsn").alias("source_lsn"),
        col("envelope.source_timestamp").alias("source_timestamp"),
        *entity_col_exprs,
    )


def build_windowed_merge_sql(
    sink_table: str, key_cols: tuple[str, ...], value_cols: tuple[str, ...], source_view: str
) -> str:
    """Generic MERGE-by-key builder for a windowed-aggregation sink -- same
    spirit as silver_transform.py's build_merge_sql(), far simpler: these
    sinks only ever gain a new window/key or replace an existing one's
    values (Spark's outputMode("update") never emits a "delete this
    key" signal), so there's no delete-strategy branching and no
    bookkeeping beyond whatever the caller already included in
    value_cols (typically an `_updated_at` timestamp)."""
    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_cols)
    update_set = ", ".join(f"target.{v} = source.{v}" for v in value_cols)
    insert_cols = list(key_cols) + list(value_cols)
    insert_col_list = ", ".join(insert_cols)
    insert_val_list = ", ".join(f"source.{c}" for c in insert_cols)
    return f"""
        MERGE INTO {sink_table} AS target
        USING {source_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_col_list}) VALUES ({insert_val_list})
    """.strip()


def run_windowed_query(spark: SparkSession, cfg: WindowedMetricConfig, kafka_bootstrap_servers: str):
    """Tumbling-window aggregation over `cfg.topic`, MERGEd into
    `cfg.sink_table` on (window_start, window_end, *extra_group_cols) every
    micro-batch. See analytics_config.py's WindowedMetricConfig for the
    per-metric parameters this is driven by."""
    sink_short = cfg.sink_table.rsplit(".", 1)[-1]
    # Distinct consumer group per SINK, not per topic/entity -- cdc.orders
    # alone feeds both orders_per_minute and revenue_5min, and each needs
    # its own independent read position through the topic (Kafka supports
    # arbitrarily many independent consumer groups per topic).
    group_id = f"analytics-writer-{cfg.entity}-{sink_short}"
    checkpoint = f"s3a://lakehouse/checkpoints/analytics/{sink_short}/"

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed = _parse_after(stream_df, cfg.entity).filter(col(cfg.event_time_col).isNotNull())
    if cfg.operation_filter:
        # See analytics_config.py's WindowedMetricConfig.operation_filter
        # docstring -- restricts e.g. orders_per_minute to creation events
        # only, so a later status-change/total_amount UPDATE on the same
        # order (same created_at, same window) doesn't double-count it.
        parsed = parsed.filter(col("operation").isin(*cfg.operation_filter))
    watermarked = parsed.withWatermark(cfg.event_time_col, cfg.watermark)
    group_cols = [window(col(cfg.event_time_col), cfg.window_duration)]
    group_cols += [col(c) for c in cfg.extra_group_cols]
    aggregated = watermarked.groupBy(*group_cols).agg(*[expr(e) for e in cfg.agg_exprs])

    def _batch_fn(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        rows = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            *[col(c) for c in cfg.extra_group_cols],
            *[col(c) for c in cfg.value_cols],
        ).withColumn("_updated_at", current_timestamp())

        view_name = f"_analytics_src_{sink_short}"
        rows.createOrReplaceTempView(view_name)
        key_cols = ("window_start", "window_end") + cfg.extra_group_cols
        value_cols = cfg.value_cols + ("_updated_at",)
        spark.sql(build_windowed_merge_sql(cfg.sink_table, key_cols, value_cols, view_name))
        logger.info("batch=%s sink=%s windowed rows merged", batch_id, cfg.sink_table)

    return (
        aggregated.writeStream.outputMode("update")
        .foreachBatch(_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )


def run_late_event_query(spark: SparkSession, cfg: LateEventConfig, kafka_bootstrap_servers: str):
    """A SEPARATE streaming query against `cfg.topic` (own consumer group,
    own checkpoint) -- NOT derived from run_windowed_query()'s foreachBatch,
    because a windowed aggregation's foreachBatch only ever sees the
    aggregated (window, count, sum) rows, never the raw per-event rows a
    lateness check needs.

    Spark's built-in watermark never exposes *which* rows it dropped, so
    this is a small hand-rolled parallel check: `cfg.watermark_state_table`
    holds one row per opted-in query recording the watermark bound as of
    the end of the last completed batch. Each micro-batch: read that stored
    bound (BEFORE this batch touches it -- mirrors Spark's own semantics of
    only advancing a watermark after a batch completes) -> any row with
    event_time < stored_bound - cfg.lateness_threshold is late-beyond-
    threshold -> appended to cfg.late_events_table instead of being
    silently lost -> then the stored bound is raised to
    GREATEST(stored_bound, this batch's own max event_time)."""
    group_id = f"analytics-writer-{cfg.entity}-late-events"
    checkpoint = "s3a://lakehouse/checkpoints/analytics/late_events/"

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    def _batch_fn(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        parsed = (
            _parse_after(batch_df, cfg.entity)
            .filter(col(cfg.event_time_col).isNotNull())
            .withColumn("event_id", event_id_udf(col("topic"), col("partition"), col("offset")))
        )
        parsed.persist()
        try:
            if parsed.rdd.isEmpty():
                return

            stored_rows = spark.sql(
                f"SELECT max_event_time FROM {cfg.watermark_state_table} "
                f"WHERE query_name = '{cfg.query_name}'"
            ).collect()
            stored_max = stored_rows[0]["max_event_time"] if stored_rows else None

            if stored_max is not None:
                late_cutoff = lit(stored_max) - expr(f"INTERVAL {cfg.lateness_threshold}")
                late = parsed.filter(col(cfg.event_time_col) < late_cutoff)
            else:
                # No stored watermark yet (first batch since this query's
                # checkpoint was created) -- nothing to compare against, so
                # nothing is classified late-beyond-threshold this round;
                # the watermark row is still seeded by the MERGE below.
                late = parsed.filter(lit(False))

            now = datetime.now(timezone.utc)
            late_count = late.count()
            if late_count:
                late_rows = late.select(
                    lit(cfg.entity).alias("entity"),
                    col("event_id"),
                    col(cfg.event_time_col).alias("event_time"),
                    (unix_timestamp(lit(now)) - unix_timestamp(col(cfg.event_time_col)))
                    .cast("long")
                    .alias("arrival_delay_seconds"),
                    col("source_lsn"),
                    lit(now).alias("detected_at"),
                )
                late_rows.writeTo(cfg.late_events_table).append()
                logger.warning(
                    "batch=%s query=%s late_beyond_threshold=%d appended to %s",
                    batch_id,
                    cfg.query_name,
                    late_count,
                    cfg.late_events_table,
                )

            view_name = f"_analytics_late_src_{cfg.query_name}"
            parsed.createOrReplaceTempView(view_name)
            spark.sql(f"""
                MERGE INTO {cfg.watermark_state_table} AS target
                USING (
                    SELECT '{cfg.query_name}' AS query_name, MAX({cfg.event_time_col}) AS batch_max
                    FROM {view_name}
                ) AS source
                ON target.query_name = source.query_name
                WHEN MATCHED THEN UPDATE SET
                    target.max_event_time = GREATEST(target.max_event_time, source.batch_max)
                WHEN NOT MATCHED THEN INSERT (query_name, max_event_time)
                    VALUES (source.query_name, source.batch_max)
            """)
        finally:
            parsed.unpersist()

    return (
        stream_df.writeStream.foreachBatch(_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )


def run_alert_query(spark: SparkSession, cfg: AlertConfig, kafka_bootstrap_servers: str):
    """Stateless filter+append: no window, no watermark, no MERGE -- every
    matching row is a fresh alert appended once, so this is a plain
    append-only sink (see analytics_ddl.sql's inventory_alerts, partitioned
    by days(alert_at) as an unbounded append log, same reasoning as
    bronze.dlq_events)."""
    sink_short = cfg.sink_table.rsplit(".", 1)[-1]
    group_id = f"analytics-writer-{cfg.entity}-alerts"
    checkpoint = f"s3a://lakehouse/checkpoints/analytics/{sink_short}/"

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("kafka.group.id", group_id)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    def _batch_fn(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        parsed = _parse_after(batch_df, cfg.entity)
        alerts = parsed.filter(expr(cfg.filter_expr)).select(
            *[col(c) for c in cfg.select_cols],
            current_timestamp().alias("alert_at"),
            col("source_lsn"),
        )
        count = alerts.count()
        if count:
            alerts.writeTo(cfg.sink_table).append()
            logger.info("batch=%s sink=%s appended=%d alerts", batch_id, cfg.sink_table, count)

    return (
        stream_df.writeStream.foreachBatch(_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
