"""Bronze -> Silver: parsing, typing, data-quality rules, dedup, enrichment.

All functions are pure DataFrame -> DataFrame so they can be unit-tested with a
local SparkSession, independently of the orchestrator and of the storage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

TIMESTAMP_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss.SSS",  # SODA API (/resource endpoint)
    "yyyy-MM-dd'T'HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",  # portal "Export" format
)

# Business thresholds (documented assumptions, see README).
MAX_TRIP_SECONDS = 24 * 3600
MAX_TRIP_MILES = 500.0
MAX_TRIP_TOTAL = 2_000.0
AIRPORT_COMMUNITY_AREAS = (76, 56)  # O'Hare, Garfield Ridge (Midway)

STRING_COLUMNS = ("trip_id", "taxi_id", "payment_type", "company", "pickup_census_tract", "dropoff_census_tract")
MONEY_COLUMNS = ("fare", "tips", "tolls", "extras", "trip_total")
DOUBLE_COLUMNS = (
    "trip_miles",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
)
INT_COLUMNS = ("trip_seconds", "pickup_community_area", "dropoff_community_area")
TIMESTAMP_COLUMNS = ("trip_start_timestamp", "trip_end_timestamp")
EXPECTED_COLUMNS = STRING_COLUMNS + MONEY_COLUMNS + DOUBLE_COLUMNS + INT_COLUMNS + TIMESTAMP_COLUMNS


def _normalize_name(name: str) -> str:
    """'Trip Start Timestamp' -> 'trip_start_timestamp'."""
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def _parse_timestamp(col: Column) -> Column:
    return F.coalesce(*[F.to_timestamp(col, fmt) for fmt in TIMESTAMP_FORMATS])


def _clean_str(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "", None).otherwise(trimmed)


def parse_raw_trips(raw: DataFrame) -> DataFrame:
    """Rename + cast the all-string raw CSV into a typed DataFrame.

    Missing source columns are added as nulls so the schema is always the same.
    """
    df = raw.toDF(*[_normalize_name(c) for c in raw.columns])
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))

    typed = [_clean_str(F.col(c)).alias(c) for c in STRING_COLUMNS]
    typed += [F.regexp_replace(_clean_str(F.col(c)), r"[$,]", "").cast("double").alias(c) for c in MONEY_COLUMNS]
    typed += [F.regexp_replace(_clean_str(F.col(c)), ",", "").cast("double").alias(c) for c in DOUBLE_COLUMNS]
    typed += [F.regexp_replace(_clean_str(F.col(c)), ",", "").cast("double").cast("int").alias(c) for c in INT_COLUMNS]
    typed += [_parse_timestamp(_clean_str(F.col(c))).alias(c) for c in TIMESTAMP_COLUMNS]
    if "_source_file" in df.columns:
        typed.append(F.col("_source_file"))
    return df.select(*typed)


def quality_rules(trip_month: str) -> dict[str, Column]:
    """Each rule is a boolean Column that must be TRUE for a valid row."""
    start, end = F.col("trip_start_timestamp"), F.col("trip_end_timestamp")
    return {
        "trip_id_missing": F.col("trip_id").isNotNull(),
        "start_timestamp_invalid": start.isNotNull(),
        "start_outside_partition": F.date_format(start, "yyyy-MM") == F.lit(trip_month),
        "end_before_start": end.isNotNull() & (end >= start),
        "duration_out_of_range": F.col("trip_seconds").between(0, MAX_TRIP_SECONDS),
        "distance_out_of_range": F.col("trip_miles").isNull() | F.col("trip_miles").between(0, MAX_TRIP_MILES),
        "negative_fare": F.col("fare").isNull() | (F.col("fare") >= 0),
        "trip_total_out_of_range": F.col("trip_total").between(0, MAX_TRIP_TOTAL),
    }


def add_quality_errors(df: DataFrame, trip_month: str) -> DataFrame:
    """Adds `dq_errors` (array<string>) listing every rule a row violates."""
    checks = [F.when(~F.coalesce(rule, F.lit(False)), F.lit(name)) for name, rule in quality_rules(trip_month).items()]
    return df.withColumn("dq_errors", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per trip_id (most complete / latest end timestamp first)."""
    w = Window.partitionBy("trip_id").orderBy(
        F.col("trip_end_timestamp").desc_nulls_last(), F.col("trip_total").desc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich(df: DataFrame) -> DataFrame:
    start = F.col("trip_start_timestamp")
    hours = F.col("trip_seconds") / 3600.0
    return (
        df.withColumn("trip_date", F.to_date(start))
        .withColumn("pickup_hour", F.hour(start))
        .withColumn("weekday", ((F.dayofweek(start) + 5) % 7))  # 0 = Monday
        .withColumn("weekday_name", F.date_format(start, "EEEE"))
        .withColumn("duration_min", F.round(F.col("trip_seconds") / 60.0, 2))
        .withColumn(
            "avg_speed_mph",
            F.when(hours > 0, F.round(F.col("trip_miles") / hours, 2)),
        )
        .withColumn(
            "tip_rate",
            F.when(F.col("fare") > 0, F.round(F.coalesce(F.col("tips"), F.lit(0.0)) / F.col("fare"), 4)),
        )
        .withColumn("payment_type", F.initcap(F.col("payment_type")))
        .withColumn(
            "is_airport_trip",
            F.col("pickup_community_area").isin(*AIRPORT_COMMUNITY_AREAS)
            | F.col("dropoff_community_area").isin(*AIRPORT_COMMUNITY_AREAS),
        )
        .withColumn("is_zero_distance", F.coalesce(F.col("trip_miles"), F.lit(0.0)) == 0)
    )


@dataclass
class SilverResult:
    valid: DataFrame
    rejected: DataFrame


def build_silver_trips(raw: DataFrame, trip_month: str) -> SilverResult:
    """Full bronze -> silver transformation for one monthly partition."""
    checked = add_quality_errors(parse_raw_trips(raw), trip_month)
    processed_at = F.current_timestamp()

    rejected = (
        checked.filter(F.size("dq_errors") > 0)
        .withColumn("trip_month", F.lit(trip_month))
        .withColumn("_processed_at", processed_at)
    )
    valid = (
        enrich(deduplicate(checked.filter(F.size("dq_errors") == 0).drop("dq_errors")))
        .withColumn("trip_month", F.lit(trip_month))
        .withColumn("_processed_at", processed_at)
    )
    return SilverResult(valid=valid, rejected=rejected)


def parse_community_areas(raw: DataFrame) -> DataFrame:
    df = raw.toDF(*[_normalize_name(c) for c in raw.columns])
    return df.select(
        F.col("community_area").cast("int").alias("community_area"),
        F.trim("community_area_name").alias("community_area_name"),
        F.trim("side").alias("side"),
    ).dropDuplicates(["community_area"])
