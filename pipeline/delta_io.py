"""Small helpers around Delta Lake reads/writes (idempotent by construction)."""

from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).schema  # noqa: B018 - triggers metadata read only
        return True
    except AnalysisException:
        return False


def overwrite_partition(df: DataFrame, path: str, partition_col: str, value: str) -> None:
    """Replace exactly one partition: re-running a month never duplicates data."""
    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite").partitionBy(partition_col)
    if delta_table_exists(spark, path):
        writer = writer.option("replaceWhere", f"{partition_col} = '{value}'")
    writer.save(path)


def overwrite_table(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
