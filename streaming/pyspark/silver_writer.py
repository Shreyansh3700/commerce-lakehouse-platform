from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from scd2 import apply_scd2
from schema_evolution import apply_schema_evolutions
from silver_tables import TABLE_CONFIGS, TableConfig
from silver_transform import process_batch
from spark_session import build_spark_session

logger = logging.getLogger("silver_writer")

_DDL_FILES = ("/opt/spark-app/silver_ddl.sql", "/opt/spark-app/gold_ddl.sql")


def _apply_ddl_file(spark: SparkSession, ddl_path: str) -> int:
    with open(ddl_path) as f:
        raw = f.read()
    # Strip full-line comments before splitting on ';' -- mirrors
    # bronze_writer.py's ensure_bronze_tables() exactly (see its comment for
    # why this can't just check whether each chunk *starts* with '--').
    lines = [line for line in raw.splitlines() if not line.strip().startswith("--")]
    script = "\n".join(lines)
    statements = [s.strip() for s in script.split(";") if s.strip()]
    for statement in statements:
        spark.sql(statement)
    return len(statements)


def ensure_silver_tables(spark: SparkSession) -> None:
    total = sum(_apply_ddl_file(spark, ddl_path) for ddl_path in _DDL_FILES)
    logger.info("Silver/Gold DDL applied (%d statements)", total)


def start_query(spark: SparkSession, table_config: TableConfig, trigger_seconds: int):
    checkpoint = f"s3a://lakehouse/checkpoints/silver/{table_config.entity}/"

    # Bronze is strictly append-only (ADR 0004/0005); these options are
    # defensive documentation of that assumption for Silver's incremental
    # Iceberg streaming read, not a behavior change in steady state.
    #
    # streaming-max-files-per-micro-batch caps how much of a pre-existing
    # Bronze backlog one micro-batch will try to consume. Without it, a
    # cold-started Silver (fresh checkpoint, or a Bronze table that's
    # accumulated a large backlog since Silver last ran) attempts the ENTIRE
    # backlog in a single micro-batch -- observed in practice to be
    # expensive enough (dedup + windowing + Iceberg MERGE over the whole
    # backlog at once) to destabilize the JVM before that batch ever
    # commits, which means the checkpoint never advances and every restart
    # retries the same oversized batch and fails again. Capping batch size
    # trades a slower catch-up (many small micro-batches instead of one
    # giant one) for actually making forward progress.
    stream_df = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .option("streaming-max-files-per-micro-batch", "10")
        .load(table_config.bronze_table)
    )

    def _batch_fn(batch_df: DataFrame, batch_id: int) -> None:
        latest = process_batch(spark, batch_df, batch_id, table_config)
        if latest is None:
            return
        try:
            if table_config.has_scd2:
                apply_scd2(spark, latest, batch_id)
        finally:
            latest.unpersist()

    return (
        stream_df.writeStream.foreachBatch(_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    trigger_seconds = int(os.environ.get("SILVER_TRIGGER_SECONDS", "30"))

    spark = build_spark_session("silver-writer")
    ensure_silver_tables(spark)
    # Idempotent/restart-safe ALTER TABLE ... ADD COLUMN registry -- see
    # schema_evolution.py. Must run after ensure_silver_tables() (tables must
    # exist first) and before any streaming query starts (so every batch
    # sees the fully evolved schema, never a race against it).
    apply_schema_evolutions(spark)

    queries = [start_query(spark, table_config, trigger_seconds) for table_config in TABLE_CONFIGS]
    logger.info("started %d Silver streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
