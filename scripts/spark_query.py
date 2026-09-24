"""Ad hoc Spark SQL verification helper.

Meant to be run inside the `dq` (or `gold-builder`) container, where
streaming/pyspark/spark_session.py's build_spark_session() and the
Iceberg/Nessie/S3A JARs are already available -- see
data_quality/Dockerfile, which copies this script in alongside
spark_session.py at /opt/spark-app/.

    docker compose run --rm --profile tools dq \\
        /opt/spark-app/spark_query.py --sql "SELECT * FROM nessie.gold.daily_sales ORDER BY date"
"""

from __future__ import annotations

import argparse
import sys

from spark_session import build_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an ad hoc Spark SQL query against the lakehouse.")
    parser.add_argument("--sql", required=True, help="SQL to execute, e.g. 'SELECT * FROM nessie.gold.daily_sales'")
    parser.add_argument("--limit", type=int, default=100, help="Max rows to display (default: 100)")
    args = parser.parse_args()

    spark = build_spark_session("spark-query")
    df = spark.sql(args.sql)
    df.show(n=args.limit, truncate=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
