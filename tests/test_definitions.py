from dagster import AssetKey

from pipeline.definitions import defs


def test_definitions_load_and_lineage():
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {"bronze_taxi_trips", "silver_taxi_trips", "gold_daily_kpis", "gold_pickup_zones"} <= keys
    assert AssetKey("silver_taxi_trips") in graph.get(AssetKey("gold_daily_kpis")).parent_keys
    assert AssetKey("bronze_taxi_trips") in graph.get(AssetKey("silver_taxi_trips")).parent_keys


def test_jobs_are_defined():
    for name in ("monthly_trips_job", "reference_job", "gold_job"):
        assert defs.resolve_job_def(name) is not None
