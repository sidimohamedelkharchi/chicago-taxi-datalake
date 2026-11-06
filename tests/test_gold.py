from conftest import raw_row

from pipeline.transformations.gold import UNKNOWN_AREA_NAME, build_gold_tables
from pipeline.transformations.silver import build_silver_trips


def _silver(make_raw):
    rows = [
        raw_row("a", pickup="8", total="10", taxi="x"),
        raw_row("b", pickup="8", total="20", taxi="y"),
        raw_row("c", pickup="32", total="30", taxi="x", payment="cash", tips="0"),
        raw_row("d", pickup="", total="40", taxi="z", start="2023-01-06T23:00:00.000", end="2023-01-06T23:30:00.000"),
    ]
    return build_silver_trips(make_raw(rows), "2023-01").valid


def _areas(spark):
    return spark.createDataFrame(
        [(8, "Near North Side", "Central"), (32, "Loop", "Central")],
        "community_area int, community_area_name string, side string",
    )


def test_daily_kpis(spark, make_raw):
    daily = build_gold_tables(_silver(make_raw), _areas(spark))["daily_kpis"].collect()
    assert [r.trips for r in daily] == [3, 1]
    assert daily[0].revenue_total == 60.0
    assert daily[0].active_taxis == 2
    assert sum(r.trips for r in daily) == 4


def test_pickup_zones_ranking_and_unknown(spark, make_raw):
    zones = build_gold_tables(_silver(make_raw), _areas(spark))["pickup_zones"]
    overall = {r.pickup_zone: r for r in zones.filter("period = 'ALL'").collect()}
    assert overall["Near North Side"].rank == 1 and overall["Near North Side"].trips == 2
    assert UNKNOWN_AREA_NAME in overall
    assert sum(r.trips for r in overall.values()) == 4
    assert zones.filter("period = '2023-01'").count() == 3


def test_payment_mix_shares(spark, make_raw):
    mix = {r.payment_type: r for r in build_gold_tables(_silver(make_raw), _areas(spark))["payment_mix"].collect()}
    assert mix["Credit Card"].trips == 3 and mix["Cash"].trip_share == 0.25


def test_hourly_demand(spark, make_raw):
    hourly = build_gold_tables(_silver(make_raw), _areas(spark))["hourly_demand"].collect()
    assert {(r.weekday_name, r.pickup_hour, r.trips) for r in hourly} == {("Thursday", 10, 3), ("Friday", 23, 1)}
