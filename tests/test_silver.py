import datetime as dt

from conftest import raw_row

from pipeline.transformations.silver import build_silver_trips, parse_raw_trips


def test_parse_types_and_money(make_raw):
    df = parse_raw_trips(make_raw([raw_row(fare="$1,012.50", total="$1,020.00")]))
    row = df.first()
    assert row.trip_start_timestamp == dt.datetime(2023, 1, 5, 10, 0)
    assert row.trip_seconds == 900
    assert row.pickup_community_area == 8
    assert row.fare == 1012.5 and row.trip_total == 1020.0
    assert row.payment_type == "credit card"


def test_portal_export_timestamp_format(make_raw):
    df = parse_raw_trips(make_raw([raw_row(start="01/05/2023 10:00:00 PM", end="01/05/2023 10:15:00 PM")]))
    assert df.first().trip_start_timestamp == dt.datetime(2023, 1, 5, 22, 0)


def test_quality_rules_reject_bad_rows(make_raw):
    rows = [
        raw_row("ok"),
        raw_row("neg_fare", fare="-3"),
        raw_row("end_before", end="2023-01-05T09:00:00.000"),
        raw_row("other_month", start="2023-02-01T00:00:00.000", end="2023-02-01T00:10:00.000"),
        raw_row("too_long", seconds="100000"),
        raw_row(None),
        raw_row("bad_ts", start="not-a-date"),
    ]
    result = build_silver_trips(make_raw(rows), "2023-01")
    assert [r.trip_id for r in result.valid.collect()] == ["ok"]
    reasons = {r.trip_id: set(r.dq_errors) for r in result.rejected.collect()}
    assert reasons["neg_fare"] == {"negative_fare"}
    assert reasons["end_before"] == {"end_before_start"}
    assert reasons["other_month"] == {"start_outside_partition"}
    assert reasons["too_long"] == {"duration_out_of_range"}
    assert "trip_id_missing" in reasons[None]
    assert "start_timestamp_invalid" in reasons["bad_ts"]


def test_deduplication_keeps_one_row_per_trip(make_raw):
    rows = [raw_row("dup"), raw_row("dup"), raw_row("dup", end="2023-01-05T10:20:00.000"), raw_row("solo")]
    valid = build_silver_trips(make_raw(rows), "2023-01").valid
    assert valid.count() == 2
    assert valid.filter("trip_id = 'dup'").first().trip_end_timestamp == dt.datetime(2023, 1, 5, 10, 20)


def test_enrichment_columns(make_raw):
    row = build_silver_trips(make_raw([raw_row(pickup="76", seconds="1800", miles="15")]), "2023-01").valid.first()
    assert row.trip_date == dt.date(2023, 1, 5)
    assert row.pickup_hour == 10
    assert row.weekday == 3 and row.weekday_name == "Thursday"  # 2023-01-05 is a Thursday
    assert row.duration_min == 30.0
    assert row.avg_speed_mph == 30.0
    assert row.tip_rate == 0.2
    assert row.is_airport_trip is True
    assert row.payment_type == "Credit Card"
    assert row.trip_month == "2023-01"


def test_missing_source_columns_are_tolerated(spark):
    raw = spark.createDataFrame(
        [("t1", "2023-01-05T10:00:00.000", "2023-01-05T10:10:00.000", "600", "10", "12")],
        "`Trip ID` string, `Trip Start Timestamp` string, `Trip End Timestamp` string, "
        "`Trip Seconds` string, `Fare` string, `Trip Total` string",
    )
    valid = build_silver_trips(raw, "2023-01").valid
    assert valid.count() == 1
    assert valid.first().pickup_community_area is None
