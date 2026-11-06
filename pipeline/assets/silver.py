"""SILVER: typed, cleaned, deduplicated, quality-controlled Delta tables."""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pyspark.sql import functions as F

from pipeline.config import settings
from pipeline.delta_io import overwrite_partition, overwrite_table
from pipeline.partitions import monthly_partitions, partition_to_month
from pipeline.resources import S3Resource, SparkResource
from pipeline.transformations.silver import build_silver_trips, parse_community_areas

MAX_REJECTION_RATE = 0.10


@asset(
    group_name="silver",
    partitions_def=monthly_partitions,
    deps=["bronze_taxi_trips"],
    kinds={"spark", "delta"},
    description="Cleaned taxi trips (Delta, partitioned by trip_month). Invalid rows go to "
    "silver/taxi_trips_rejected with the list of violated rules.",
    check_specs=[
        AssetCheckSpec("silver_not_empty", asset="silver_taxi_trips"),
        AssetCheckSpec("trip_id_unique", asset="silver_taxi_trips"),
        AssetCheckSpec("rejection_rate_below_threshold", asset="silver_taxi_trips"),
    ],
)
def silver_taxi_trips(context: AssetExecutionContext, spark: SparkResource, s3: S3Resource) -> MaterializeResult:
    month = partition_to_month(context.partition_key)
    s3.ensure_layer_buckets()
    source = settings.s3a(settings.bronze_bucket, f"{settings.bronze_trips_prefix}/trip_month={month}/*.csv")

    with spark.session(f"silver_taxi_trips_{month}") as session:
        raw = (
            session.read.option("header", True)
            .option("escape", '"')
            .option("inferSchema", False)  # everything stays string, typing is explicit
            .csv(source)
            .withColumn("_source_file", F.input_file_name())
            .cache()
        )
        raw_rows = raw.count()

        result = build_silver_trips(raw, month)
        overwrite_partition(result.valid, settings.silver_trips_path, "trip_month", month)
        overwrite_partition(result.rejected, settings.silver_rejected_path, "trip_month", month)

        written = session.read.format("delta").load(settings.silver_trips_path).filter(F.col("trip_month") == month)
        stats = written.agg(F.count("*").alias("rows"), F.countDistinct("trip_id").alias("ids")).first()
        rejected = session.read.format("delta").load(settings.silver_rejected_path).filter(F.col("trip_month") == month)
        rejected_rows = rejected.count()
        reasons = {
            r["reason"]: r["n"]
            for r in rejected.select(F.explode("dq_errors").alias("reason"))
            .groupBy("reason")
            .agg(F.count("*").alias("n"))
            .collect()
        }

    valid_rows = stats["rows"]
    duplicates = raw_rows - valid_rows - rejected_rows
    rejection_rate = rejected_rows / raw_rows if raw_rows else 0.0
    context.log.info(f"{month}: raw={raw_rows} valid={valid_rows} rejected={rejected_rows} dup={duplicates}")

    return MaterializeResult(
        metadata={
            "raw_rows": raw_rows,
            "valid_rows": valid_rows,
            "rejected_rows": rejected_rows,
            "duplicates_removed": duplicates,
            "rejection_rate": round(rejection_rate, 4),
            "rejection_reasons": MetadataValue.json(reasons),
            "location": MetadataValue.path(settings.silver_trips_path),
        },
        check_results=[
            AssetCheckResult(check_name="silver_not_empty", passed=valid_rows > 0, severity=AssetCheckSeverity.ERROR),
            AssetCheckResult(
                check_name="trip_id_unique",
                passed=stats["rows"] == stats["ids"],
                severity=AssetCheckSeverity.ERROR,
                metadata={"rows": stats["rows"], "distinct_trip_ids": stats["ids"]},
            ),
            AssetCheckResult(
                check_name="rejection_rate_below_threshold",
                passed=rejection_rate <= MAX_REJECTION_RATE,
                severity=AssetCheckSeverity.WARN,
                metadata={"rejection_rate": round(rejection_rate, 4), "threshold": MAX_REJECTION_RATE},
            ),
        ],
    )


@asset(
    group_name="silver",
    deps=["bronze_community_areas"],
    kinds={"spark", "delta"},
    description="Typed community areas reference (id, name, side of the city).",
)
def silver_community_areas(context: AssetExecutionContext, spark: SparkResource) -> MaterializeResult:
    source = settings.s3a(settings.bronze_bucket, settings.bronze_reference_key)
    with spark.session("silver_community_areas") as session:
        df = parse_community_areas(session.read.option("header", True).csv(source))
        overwrite_table(df, settings.silver_community_areas_path)
        rows = df.count()
    return MaterializeResult(
        metadata={"rows": rows, "location": MetadataValue.path(settings.silver_community_areas_path)}
    )
