from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    """Builds a SparkSession wired to the Nessie Iceberg catalog backed by
    MinIO (S3A). Config values come from env vars set on the
    spark-bronze-writer compose service."""
    nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")
    nessie_ref = os.environ.get("NESSIE_DEFAULT_BRANCH", "main")
    bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ["MINIO_ROOT_USER"]
    minio_secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", nessie_ref)
        .config("spark.sql.catalog.nessie.warehouse", f"s3a://{bucket}/")
        # No io-impl override -- Iceberg's default resolving FileIO uses
        # HadoopFileIO for s3a:// paths, which goes through Hadoop's already
        # -configured S3AFileSystem (hadoop-aws) below. Avoids also needing
        # the separate AWS SDK v2 / iceberg-aws-bundle jars that Iceberg's
        # native S3FileIO would require.
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "nessie")
    )
    return builder.getOrCreate()
