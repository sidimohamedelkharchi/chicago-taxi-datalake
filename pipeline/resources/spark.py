"""Spark resource: builds a SparkSession wired to MinIO (s3a) and Delta Lake."""

from __future__ import annotations

import socket
from collections.abc import Iterator
from contextlib import contextmanager

from dagster import ConfigurableResource
from pyspark.sql import SparkSession

from pipeline.config import settings


def build_spark_session(app_name: str, master: str | None = None) -> SparkSession:
    master = master or settings.spark_master
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # --- Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # --- MinIO through s3a ---
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.s3_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # --- Sizing / behaviour ---
        .config("spark.sql.session.timeZone", "UTC")  # source timestamps are Chicago wall-clock, kept as-is
        .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # unparsable dates -> null
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master.startswith("spark://"):
        # Client mode: the driver runs inside the Dagster container, executors on
        # the Spark worker must be able to call it back -> advertise our IP.
        driver_ip = socket.gethostbyname(socket.gethostname())
        builder = (
            builder.config("spark.driver.host", driver_ip)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.memory", settings.spark_executor_memory)
            .config("spark.executor.cores", settings.spark_executor_cores)
            .config("spark.cores.max", settings.spark_cores_max)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class SparkResource(ConfigurableResource):
    """Gives each asset a SparkSession and guarantees it is stopped afterwards."""

    master: str = settings.spark_master

    @contextmanager
    def session(self, app_name: str) -> Iterator[SparkSession]:
        spark = build_spark_session(app_name=app_name, master=self.master)
        try:
            yield spark
        finally:
            spark.stop()
