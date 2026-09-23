from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, from_json, lit, row_number, when
from pyspark.sql.window import Window
from silver_schemas import DECIMAL_CAST_FIELDS, ENTITY_SCHEMAS
from silver_tables import TableConfig

logger = logging.getLogger("silver_transform")

# Bookkeeping columns every Silver table carries, recording the last Bronze
# CDC event applied to that row -- _source_lsn is what the MERGE guard
# (source.source_lsn > target._source_lsn) compares against to make
# cross-batch duplicates and out-of-order replays safe. See the Phase 3 plan.
BOOKKEEPING_COLS = [
    "_event_id",
    "_source_lsn",
    "_source_transaction_id",
    "_bronze_operation",
    "_bronze_ingestion_timestamp",
    "_silver_committed_at",
]

# bookkeeping column -> the `source`-qualified expression that populates it,
# used identically in both INSERT VALUES and UPDATE SET clauses.
_BOOKKEEPING_SOURCE_EXPR = {
    "_event_id": "source.event_id",
    "_source_lsn": "source.source_lsn",
    "_source_transaction_id": "source.transaction_id",
    "_bronze_operation": "source.operation",
    "_bronze_ingestion_timestamp": "source.ingestion_timestamp",
    "_silver_committed_at": "current_timestamp()",
}


def _bookkeeping_update_assignments() -> list[str]:
    return [f"target.{c} = {expr}" for c, expr in _BOOKKEEPING_SOURCE_EXPR.items()]


def _bookkeeping_insert_exprs() -> list[str]:
    return [_BOOKKEEPING_SOURCE_EXPR[c] for c in BOOKKEEPING_COLS]


def validate(batch_df: DataFrame, table_config: TableConfig) -> tuple[DataFrame, DataFrame]:
    """Parses Bronze's before/after JSON strings into typed entity columns
    and splits the micro-batch into (good, bad) per the Phase 3 plan's
    validity rules: bad operation code, missing/unparseable payload, a null
    primary key, or a null source_lsn.

    `good`'s schema is: event_id, operation, source_timestamp, source_lsn,
    transaction_id, kafka_offset, ingestion_timestamp, plus the entity's own
    columns (populated from `before` for deletes -- which carry no `after` --
    and from `after` otherwise). This is exactly the shape MERGE INTO's
    `source` side expects (see build_merge_sql below).

    `bad` carries enough to build a nessie.silver.dlq_events row. One bad row
    never fails the batch -- same discipline as bronze_writer.py's DLQ
    handling."""
    entity_schema = ENTITY_SCHEMAS[table_config.entity]
    entity_fields = [f.name for f in entity_schema.fields]
    pk_cols = table_config.pk_cols

    payload_col = when(col("operation") == "d", col("before")).otherwise(col("after"))
    typed_col = when(col("operation") == "d", from_json(col("before"), entity_schema)).otherwise(
        from_json(col("after"), entity_schema)
    )

    df = batch_df.withColumn("_typed", typed_col)

    op_valid = col("operation").isin("r", "c", "u", "d")
    payload_present = payload_col.isNotNull()
    parse_ok = col("_typed").isNotNull()
    pk_present = reduce(and_, [col(f"_typed.{pk}").isNotNull() for pk in pk_cols])
    lsn_present = col("source_lsn").isNotNull()

    valid_cond = op_valid & payload_present & parse_ok & pk_present & lsn_present
    error_type_col = (
        when(~op_valid, lit("invalid_operation"))
        .when(~payload_present, lit("missing_payload"))
        .when(~parse_ok, lit("payload_parse_error"))
        .when(~pk_present, lit("null_primary_key"))
        .when(~lsn_present, lit("null_source_lsn"))
        .otherwise(lit(None).cast("string"))
    )

    df = df.withColumn("_valid", valid_cond).withColumn("_error_type", error_type_col)

    entity_col_exprs = []
    decimal_casts = DECIMAL_CAST_FIELDS.get(table_config.entity, {})
    for field in entity_fields:
        field_col = col(f"_typed.{field}")
        cast_type = decimal_casts.get(field)
        if cast_type:
            field_col = field_col.cast(cast_type)
        entity_col_exprs.append(field_col.alias(field))

    base_cols = [
        col("event_id"),
        col("operation"),
        col("source_timestamp"),
        col("source_lsn"),
        col("transaction_id"),
        col("kafka_offset"),
        col("ingestion_timestamp"),
    ]

    good = df.filter(col("_valid")).select(*base_cols, *entity_col_exprs)
    bad = df.filter(~col("_valid")).select(
        col("event_id").alias("bronze_event_id"),
        lit(table_config.bronze_table).alias("source_table"),
        col("_error_type").alias("error_type"),
        concat(lit("Silver validation failed: "), col("_error_type")).alias("error_message"),
        col("before").alias("raw_before"),
        col("after").alias("raw_after"),
    )
    return good, bad


def dedupe(good_df: DataFrame) -> DataFrame:
    """Removes true duplicate Kafka messages (identical event_id) that
    Bronze's own at-least-once write path can produce -- see
    debezium_envelope.py's compute_event_id() docstring."""
    return good_df.dropDuplicates(["event_id"])


def pick_latest_per_key(df: DataFrame, pk_cols: tuple[str, ...]) -> DataFrame:
    """Keeps only the latest change per primary key within this micro-batch.
    Ordered by source_lsn (Postgres WAL LSN, monotonic across the whole
    database -- spec section 12's required ordering key, never
    ingestion_timestamp), then kafka_offset (tie-breaks snapshot-phase rows,
    which all share one Debezium-assigned LSN), then event_id (a final
    defensive, fully deterministic tie-break)."""
    window = Window.partitionBy(*pk_cols).orderBy(
        col("source_lsn").desc_nulls_last(),
        col("kafka_offset").desc_nulls_last(),
        col("event_id").desc(),
    )
    return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def build_merge_sql(table_config: TableConfig, source_view: str) -> str:
    """Builds the Iceberg MERGE INTO statement applying `source_view` (the
    deduped, latest-per-key micro-batch from pick_latest_per_key) onto the
    Silver table, parameterized by delete_strategy.

    Every WHEN MATCHED clause is guarded by source.source_lsn >
    target._source_lsn -- this is the mechanism (not just a claim) that makes
    cross-batch duplicates and out-of-order replays no-ops: a duplicate or
    late-arriving event has source_lsn <= the value already recorded on the
    target row, so the clause simply doesn't fire and the MERGE commits a
    snapshot with zero effective row changes. See the Phase 3 plan.

    WHEN NOT MATCHED AND operation = 'd' intentionally has no clause -- a
    delete for a key Silver has never seen is a no-op (documented limitation:
    see the plan's note on cross-batch delete/insert inversion)."""
    entity_schema = ENTITY_SCHEMAS[table_config.entity]
    entity_fields = [f.name for f in entity_schema.fields]
    non_pk_fields = [f for f in entity_fields if f not in table_config.pk_cols]

    on_clause = " AND ".join(f"target.{pk} = source.{pk}" for pk in table_config.pk_cols)
    lsn_guard = "source.source_lsn > target._source_lsn"

    update_assignments = [f"target.{f} = source.{f}" for f in non_pk_fields]
    bookkeeping_assignments = _bookkeeping_update_assignments()

    if table_config.delete_strategy == "hard":
        update_set = ", ".join(update_assignments + bookkeeping_assignments)
        delete_clause = f"WHEN MATCHED AND source.operation = 'd' AND {lsn_guard} THEN DELETE"
        insert_cols = entity_fields + BOOKKEEPING_COLS
        insert_value_exprs = [f"source.{f}" for f in entity_fields] + _bookkeeping_insert_exprs()
    elif table_config.delete_strategy == "soft":
        update_set = ", ".join(
            update_assignments
            + ["target.is_deleted = false", "target.deleted_at = NULL"]
            + bookkeeping_assignments
        )
        # Delete events carry no `after`, so validate() already populated
        # entity_fields from `before` for op='d' rows -- the UPDATE SET below
        # therefore preserves the row's last-known field values instead of
        # nulling them, per spec section 13's is_deleted/deleted_at shape.
        soft_delete_set = ", ".join(
            update_assignments
            + ["target.is_deleted = true", "target.deleted_at = source.source_timestamp"]
            + bookkeeping_assignments
        )
        delete_clause = (
            f"WHEN MATCHED AND source.operation = 'd' AND {lsn_guard} THEN UPDATE SET {soft_delete_set}"
        )
        insert_cols = entity_fields + ["is_deleted", "deleted_at"] + BOOKKEEPING_COLS
        insert_value_exprs = (
            [f"source.{f}" for f in entity_fields] + ["false", "NULL"] + _bookkeeping_insert_exprs()
        )
    else:
        raise ValueError(f"unknown delete_strategy: {table_config.delete_strategy!r}")

    insert_col_list = ", ".join(insert_cols)
    insert_val_list = ", ".join(insert_value_exprs)

    return f"""
        MERGE INTO {table_config.silver_table} AS target
        USING {source_view} AS source
        ON {on_clause}
        WHEN MATCHED AND source.operation != 'd' AND {lsn_guard} THEN
          UPDATE SET {update_set}
        {delete_clause}
        WHEN NOT MATCHED AND source.operation != 'd' THEN
          INSERT ({insert_col_list}) VALUES ({insert_val_list})
    """.strip()


def apply_merge(spark: SparkSession, latest_df: DataFrame, table_config: TableConfig) -> None:
    view_name = f"_silver_src_{table_config.entity}"
    latest_df.createOrReplaceTempView(view_name)
    spark.sql(build_merge_sql(table_config, view_name))


def write_dlq(spark: SparkSession, bad_df: DataFrame, dlq_table: str = "nessie.silver.dlq_events") -> None:
    now = datetime.now(timezone.utc)
    bad_df.withColumn("failed_at", lit(now)).writeTo(dlq_table).append()


def process_batch(
    spark: SparkSession, batch_df: DataFrame, batch_id: int, table_config: TableConfig
) -> DataFrame | None:
    """Runs the full validate -> dedupe -> order -> MERGE pipeline for one
    micro-batch of one entity's incremental Bronze read.

    Returns the persisted, deduped/latest-per-key DataFrame that was merged
    into Silver (or None if the batch had no valid rows), so the caller
    (silver_writer.py) can pass it into scd2.py for the customers table
    without recomputing dedup/ordering. The CALLER owns unpersisting the
    returned DataFrame once it's done with it."""
    if batch_df.rdd.isEmpty():
        return None

    good, bad = validate(batch_df, table_config)
    good.persist()
    bad.persist()
    try:
        bad_count = bad.count()
        if bad_count:
            write_dlq(spark, bad)
            logger.warning(
                "batch=%s table=%s quarantined=%d to nessie.silver.dlq_events",
                batch_id,
                table_config.silver_table,
                bad_count,
            )

        deduped = dedupe(good)
        latest = pick_latest_per_key(deduped, table_config.pk_cols).persist()
        row_count = latest.count()
        if row_count == 0:
            latest.unpersist()
            return None

        apply_merge(spark, latest, table_config)
        logger.info("batch=%s table=%s merged=%d", batch_id, table_config.silver_table, row_count)
        return latest
    finally:
        good.unpersist()
        bad.unpersist()
