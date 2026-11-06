import os
import time

import pytest
from pyspark.sql import SparkSession

# Spark returns naive datetimes in the driver's local timezone: pin it to UTC.
os.environ["TZ"] = "UTC"
time.tzset()

RAW_HEADER = [
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
]


def raw_row(
    trip_id="t1",
    start="2023-01-05T10:00:00.000",
    end="2023-01-05T10:15:00.000",
    seconds="900",
    miles="3.2",
    pickup="8",
    dropoff="32",
    fare="12.5",
    tips="2.5",
    total="15.0",
    payment="credit card",
    taxi="taxi_a",
):
    return [
        trip_id,
        taxi,
        start,
        end,
        seconds,
        miles,
        "",
        "",
        pickup,
        dropoff,
        fare,
        tips,
        "0",
        "0",
        total,
        payment,
        "Flash Cab",
        "41.89",
        "-87.63",
        "POINT (-87.63 41.89)",
        "41.88",
        "-87.62",
        "POINT (-87.62 41.88)",
    ]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def make_raw(spark):
    def _make(rows):
        return spark.createDataFrame(rows, schema=", ".join(f"`{c}` string" for c in RAW_HEADER))

    return _make
