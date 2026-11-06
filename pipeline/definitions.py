"""Dagster code location: assets, resources, jobs, schedule and sensor."""

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    build_schedule_from_partitioned_job,
    define_asset_job,
    load_assets_from_modules,
    run_status_sensor,
)

from pipeline.assets import bronze, gold, silver
from pipeline.partitions import monthly_partitions
from pipeline.resources import S3Resource, SparkResource

all_assets = load_assets_from_modules([bronze, silver, gold])

# Monthly ingestion (bronze -> silver), one run per month partition.
monthly_trips_job = define_asset_job(
    "monthly_trips_job",
    selection=AssetSelection.assets("bronze_taxi_trips", "silver_taxi_trips"),
    partitions_def=monthly_partitions,
)
reference_job = define_asset_job(
    "reference_job", selection=AssetSelection.assets("bronze_community_areas", "silver_community_areas")
)
gold_job = define_asset_job("gold_job", selection=AssetSelection.groups("gold"))

# Production-like automation (disabled by default for the local test):
# - a schedule materialises the latest month partition every month,
# - a sensor refreshes the gold layer whenever a monthly run succeeds.
monthly_schedule = build_schedule_from_partitioned_job(
    monthly_trips_job, hour_of_day=6, default_status=DefaultScheduleStatus.STOPPED
)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[monthly_trips_job],
    request_job=gold_job,
    default_status=DefaultSensorStatus.STOPPED,
)
def refresh_gold_after_ingestion(context):
    return RunRequest(run_key=f"gold_after_{context.dagster_run.run_id}")


defs = Definitions(
    assets=all_assets,
    jobs=[monthly_trips_job, reference_job, gold_job],
    schedules=[monthly_schedule],
    sensors=[refresh_gold_after_ingestion],
    resources={"spark": SparkResource(), "s3": S3Resource()},
)
