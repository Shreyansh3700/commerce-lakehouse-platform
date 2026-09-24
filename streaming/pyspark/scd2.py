from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sha2

logger = logging.getLogger("scd2")

DIM_TABLE = "nessie.gold.dim_customer_history"
_TRACKED_ATTRS = ("name", "city", "country")


def apply_scd2(spark: SparkSession, latest_customers: DataFrame, batch_id: int) -> None:
    """Maintains gold.dim_customer_history from the same deduped, ordered
    per-batch `latest_customers` DataFrame silver_transform.process_batch()
    already computed for the silver.customers MERGE.

    This can't be derived later from silver.customers: SCD2 needs the
    ordered stream of individual changes (Mumbai -> Delhi -> Bangalore),
    which only exists at the point Bronze events are deduped/ordered --
    silver.customers (current-state) has already discarded that history. See
    the Phase 3 plan's SCD2 section.

    Per-batch algorithm (two Iceberg writes from one frozen classification):
      1. Classify each incoming customer row against the current
         is_current=true dim row (if any): new / changed / stale-unchanged.
      2. Step A -- MERGE closes out `changed` rows (valid_to = source_timestamp,
         is_current = false).
      3. Step B -- append() new current rows for new + changed customers.

    valid_from/valid_to use source_timestamp (Debezium's Postgres commit
    time), not ingestion time, so a full backfill reproduces identical values
    regardless of when it's run."""
    incoming = latest_customers.select(
        "customer_id", "name", "city", "country", "source_lsn", "source_timestamp", "event_id"
    )
    incoming.createOrReplaceTempView("_scd2_incoming")

    current = spark.sql(
        f"""
        SELECT customer_id, name AS _name, city AS _city, country AS _country, _source_lsn
        FROM {DIM_TABLE}
        WHERE is_current = true
          AND customer_id IN (SELECT customer_id FROM _scd2_incoming)
        """
    )
    current.createOrReplaceTempView("_scd2_current")

    # Classify once and cache -- Step B must read the SAME classification
    # Step A was built from, not a lazy re-evaluation against DIM_TABLE after
    # Step A's MERGE has already flipped is_current for the changed rows.
    classified = spark.sql(
        """
        SELECT
            i.customer_id, i.name, i.city, i.country, i.source_lsn, i.source_timestamp, i.event_id,
            c.customer_id IS NULL AS is_new,
            (
                c.customer_id IS NOT NULL
                AND i.source_lsn > c._source_lsn
                AND NOT (
                    (i.name <=> c._name) AND (i.city <=> c._city) AND (i.country <=> c._country)
                )
            ) AS is_changed
        FROM _scd2_incoming i
        LEFT JOIN _scd2_current c ON i.customer_id = c.customer_id
        """
    ).cache()

    try:
        to_write = classified.filter(col("is_new") | col("is_changed"))
        to_write_count = to_write.count()
        if to_write_count == 0:
            return

        changed = classified.filter(col("is_changed"))
        changed.createOrReplaceTempView("_scd2_changed")
        changed_count = changed.count()
        if changed_count:
            spark.sql(
                f"""
                MERGE INTO {DIM_TABLE} AS target
                USING _scd2_changed AS source
                ON target.customer_id = source.customer_id AND target.is_current = true
                WHEN MATCHED THEN
                  UPDATE SET target.valid_to = source.source_timestamp, target.is_current = false
                """
            )

        insert_df = to_write.select(
            sha2(
                concat_ws("|", col("customer_id").cast("string"), col("source_timestamp").cast("string")), 256
            ).alias("customer_history_id"),
            col("customer_id"),
            col("name"),
            col("city"),
            col("country"),
            col("source_timestamp").alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
            col("source_lsn").alias("_source_lsn"),
            col("event_id").alias("_event_id"),
        )
        insert_df.writeTo(DIM_TABLE).append()
        logger.info(
            "batch=%s dim_customer_history: closed=%d inserted=%d", batch_id, changed_count, to_write_count
        )
    finally:
        classified.unpersist()
