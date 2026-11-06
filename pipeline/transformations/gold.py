"""Silver -> Gold: business aggregates ready for BI."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

UNKNOWN_AREA_NAME = "Unknown / outside Chicago"


def daily_kpis(trips: DataFrame) -> DataFrame:
    """One row per day: volume, revenue, duration, distance."""
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue_total"),
            F.round(F.sum("fare"), 2).alias("revenue_fare"),
            F.round(F.sum("tips"), 2).alias("revenue_tips"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.percentile_approx("duration_min", 0.5), 2).alias("median_duration_min"),
            F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
            F.countDistinct("taxi_id").alias("active_taxis"),
            F.round(F.avg(F.col("is_airport_trip").cast("double")), 4).alias("airport_trip_share"),
        )
        .withColumn("trips_per_taxi", F.round(F.col("trips") / F.col("active_taxis"), 2))
        .orderBy("trip_date")
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    """Demand heatmap: weekday x hour, averaged over the number of such weekdays."""
    days_per_weekday = trips.groupBy("weekday").agg(F.countDistinct("trip_date").alias("n_days"))
    return (
        trips.groupBy("weekday", "weekday_name", "pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        )
        .join(days_per_weekday, "weekday")
        .withColumn("avg_trips_per_day", F.round(F.col("trips") / F.col("n_days"), 1))
        .drop("n_days")
        .orderBy("weekday", "pickup_hour")
    )


def pickup_zones(trips: DataFrame, community_areas: DataFrame) -> DataFrame:
    """Pickup community areas ranked by trips, per month and over the full period ('ALL')."""
    enriched = trips.join(
        community_areas.withColumnRenamed("community_area", "pickup_community_area"),
        on="pickup_community_area",
        how="left",
    ).withColumn("pickup_zone", F.coalesce("community_area_name", F.lit(UNKNOWN_AREA_NAME)))

    def _agg(df: DataFrame) -> DataFrame:
        return df.groupBy("period", "pickup_community_area", "pickup_zone", "side").agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue_total"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
            F.round(F.avg("tip_rate"), 4).alias("avg_tip_rate"),
        )

    monthly = _agg(enriched.withColumn("period", F.col("trip_month")))
    overall = _agg(enriched.withColumn("period", F.lit("ALL")))
    rank_window = Window.partitionBy("period").orderBy(F.col("trips").desc())
    period_total = Window.partitionBy("period")
    return (
        monthly.unionByName(overall)
        .withColumn("rank", F.dense_rank().over(rank_window))
        .withColumn("trip_share", F.round(F.col("trips") / F.sum("trips").over(period_total), 4))
        .orderBy("period", "rank")
    )


def payment_mix(trips: DataFrame) -> DataFrame:
    total = Window.partitionBy()
    return (
        trips.withColumn("payment_type", F.coalesce("payment_type", F.lit("Unknown")))
        .groupBy("payment_type")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue_total"),
            F.round(F.avg("tip_rate"), 4).alias("avg_tip_rate"),
        )
        .withColumn("trip_share", F.round(F.col("trips") / F.sum("trips").over(total), 4))
        .orderBy(F.col("trips").desc())
    )


GOLD_TABLES = ("daily_kpis", "hourly_demand", "pickup_zones", "payment_mix")


def build_gold_tables(trips: DataFrame, community_areas: DataFrame) -> dict[str, DataFrame]:
    return {
        "daily_kpis": daily_kpis(trips),
        "hourly_demand": hourly_demand(trips),
        "pickup_zones": pickup_zones(trips, community_areas),
        "payment_mix": payment_mix(trips),
    }
