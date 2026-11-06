"""GOLD: business tables / KPIs for the BI team (one Spark session for all tables)."""

from collections.abc import Iterator

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)
from pyspark.sql import functions as F

from pipeline.config import settings
from pipeline.delta_io import overwrite_table
from pipeline.resources import SparkResource
from pipeline.transformations.gold import GOLD_TABLES, build_gold_tables

DESCRIPTIONS = {
    "daily_kpis": "Per day: trips, revenue (total/fare/tips), avg & median duration, distance, active taxis.",
    "hourly_demand": "Weekday x hour heatmap: average trips per day, duration and ticket.",
    "pickup_zones": "Pickup community areas ranked by trips, per month and for the full period (period='ALL').",
    "payment_mix": "Trips, revenue and tip rate by payment type.",
}


@multi_asset(
    group_name="gold",
    specs=[
        AssetSpec(
            f"gold_{t}",
            deps=["silver_taxi_trips", "silver_community_areas"],
            description=DESCRIPTIONS[t],
            kinds={"spark", "delta"},
        )
        for t in GOLD_TABLES
    ],
    check_specs=[
        AssetCheckSpec("trips_reconcile_with_silver", asset="gold_daily_kpis"),
        AssetCheckSpec("revenue_non_negative", asset="gold_daily_kpis"),
        AssetCheckSpec("zones_cover_all_trips", asset="gold_pickup_zones"),
    ],
)
def gold_tables(context: AssetExecutionContext, spark: SparkResource) -> Iterator[MaterializeResult]:
    with spark.session("gold_tables") as session:
        trips = session.read.format("delta").load(settings.silver_trips_path).cache()
        areas = session.read.format("delta").load(settings.silver_community_areas_path)
        silver_rows = trips.count()

        tables = build_gold_tables(trips, areas)
        row_counts = {}
        for name, df in tables.items():
            overwrite_table(df, settings.gold_path(name))
            row_counts[name] = session.read.format("delta").load(settings.gold_path(name)).count()

        daily = session.read.format("delta").load(settings.gold_path("daily_kpis"))
        daily_stats = daily.agg(F.sum("trips").alias("trips"), F.min("revenue_total").alias("min_rev")).first()
        zones_trips = (
            session.read.format("delta")
            .load(settings.gold_path("pickup_zones"))
            .filter(F.col("period") == "ALL")
            .agg(F.sum("trips"))
            .first()[0]
        )
        top5 = (
            session.read.format("delta")
            .load(settings.gold_path("pickup_zones"))
            .filter("period = 'ALL' AND rank <= 5")
            .select("rank", "pickup_zone", "trips")
            .orderBy("rank")
            .collect()
        )

    for name in GOLD_TABLES:
        key = f"gold_{name}"
        metadata = {"rows": row_counts[name], "location": MetadataValue.path(settings.gold_path(name))}
        checks: list[AssetCheckResult] = []
        if name == "daily_kpis":
            checks = [
                AssetCheckResult(
                    asset_key=key,
                    check_name="trips_reconcile_with_silver",
                    passed=daily_stats["trips"] == silver_rows,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"gold_trips": daily_stats["trips"], "silver_rows": silver_rows},
                ),
                AssetCheckResult(
                    asset_key=key,
                    check_name="revenue_non_negative",
                    passed=(daily_stats["min_rev"] or 0) >= 0,
                    severity=AssetCheckSeverity.ERROR,
                ),
            ]
        if name == "pickup_zones":
            metadata["top_5_pickup_zones"] = MetadataValue.md(
                "| rank | zone | trips |\n|---|---|---|\n"
                + "\n".join(f"| {r['rank']} | {r['pickup_zone']} | {r['trips']:,} |" for r in top5)
            )
            checks = [
                AssetCheckResult(
                    asset_key=key,
                    check_name="zones_cover_all_trips",
                    passed=zones_trips == silver_rows,
                    severity=AssetCheckSeverity.ERROR,
                    metadata={"zone_trips": zones_trips, "silver_rows": silver_rows},
                )
            ]
        yield MaterializeResult(asset_key=key, metadata=metadata, check_results=checks)
