"""Third long-running Structured Streaming app (spark-analytics-writer),
reading DIRECTLY from the `cdc.*` Kafka topics -- deliberately not from
Bronze/Silver Iceberg like silver_writer.py -- to demonstrate genuine
Kafka-native stream processing (spec section 16: "real-time metrics from
Kafka using PySpark Structured Streaming"). Runs as its own independent
consumer group per sink (see analytics_transform.py's run_*_query()
functions), coexisting with Bronze's own independent consumption of the
same topics -- Kafka supports arbitrarily many independent consumer groups
per topic, no conflict.

Mirrors bronze_writer.py's shape: apply DDL once at startup, start one
query per config entry, then block on spark.streams.awaitAnyTermination()."""

from __future__ import annotations

import logging
import os

from analytics_config import ALERT_CONFIGS, LATE_EVENT_CONFIGS, WINDOWED_METRIC_CONFIGS
from analytics_transform import run_alert_query, run_late_event_query, run_windowed_query
from pyspark.sql import SparkSession
from spark_session import build_spark_session

logger = logging.getLogger("analytics_writer")


def ensure_analytics_tables(spark: SparkSession, ddl_path: str = "/opt/spark-app/analytics_ddl.sql") -> None:
    """Applies analytics_ddl.sql idempotently at startup -- same split-on-
    semicolon-skip-comment-lines approach as bronze_writer.py's
    ensure_bronze_tables() / silver_writer.py's _apply_ddl_file(). Copied
    rather than imported from either, keeping each writer's startup
    self-contained the same way Bronze and Silver already don't share a
    common DDL-runner module."""
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
    logger.info("Analytics DDL applied (%d statements)", len(statements))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    spark = build_spark_session("analytics-writer")
    ensure_analytics_tables(spark)

    queries = [
        run_windowed_query(spark, cfg, kafka_bootstrap_servers) for cfg in WINDOWED_METRIC_CONFIGS
    ]
    queries += [
        run_late_event_query(spark, cfg, kafka_bootstrap_servers) for cfg in LATE_EVENT_CONFIGS
    ]
    queries += [run_alert_query(spark, cfg, kafka_bootstrap_servers) for cfg in ALERT_CONFIGS]

    logger.info("started %d Analytics streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
