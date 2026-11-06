"""Read the gold Delta tables from object storage and expose them to DuckDB SQL."""

from __future__ import annotations

import os

import duckdb
from deltalake import DeltaTable

GOLD_TABLES = ("daily_kpis", "hourly_demand", "pickup_zones", "payment_mix")


def gold_base_uri() -> str:
    return os.getenv("GOLD_BASE_URI", f"s3://{os.getenv('GOLD_BUCKET', 'gold')}")


def storage_options() -> dict[str, str]:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("S3_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": str(endpoint.startswith("http://")).lower(),
    }


def load_table(name: str):
    uri = f"{gold_base_uri()}/{name}"
    options = storage_options() if uri.startswith("s3://") else None
    return DeltaTable(uri, storage_options=options).to_pyarrow_table()


def connect() -> tuple[duckdb.DuckDBPyConnection, list[str]]:
    """DuckDB in-memory connection with every available gold table registered as a view."""
    con = duckdb.connect()
    loaded = []
    for name in GOLD_TABLES:
        try:
            con.register(name, load_table(name))
            loaded.append(name)
        except Exception:  # table not built yet
            continue
    return con, loaded
