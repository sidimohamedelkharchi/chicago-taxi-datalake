"""Central configuration, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # --- Object storage (MinIO / S3) ---------------------------------------
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))
    s3_region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    bronze_bucket: str = field(default_factory=lambda: _env("BRONZE_BUCKET", "bronze"))
    silver_bucket: str = field(default_factory=lambda: _env("SILVER_BUCKET", "silver"))
    gold_bucket: str = field(default_factory=lambda: _env("GOLD_BUCKET", "gold"))

    # --- Spark ---------------------------------------------------------------
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "spark://spark-master:7077"))
    spark_executor_memory: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_MEMORY", "1536m"))
    spark_executor_cores: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_CORES", "2"))
    spark_cores_max: str = field(default_factory=lambda: _env("SPARK_CORES_MAX", "2"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "1g"))
    spark_shuffle_partitions: str = field(default_factory=lambda: _env("SPARK_SHUFFLE_PARTITIONS", "16"))

    # --- Source (Chicago SODA API) ------------------------------------------
    soda_base_url: str = field(
        default_factory=lambda: _env("SODA_BASE_URL", "https://data.cityofchicago.org/resource/wrvz-psew.csv")
    )
    soda_app_token: str = field(default_factory=lambda: _env("SODA_APP_TOKEN", ""))
    soda_page_size: int = field(default_factory=lambda: int(_env("SODA_PAGE_SIZE", "50000")))
    # Optional local cache of pre-downloaded raw CSV files (see `make download`).
    raw_local_dir: str = field(default_factory=lambda: _env("RAW_LOCAL_DIR", "/opt/data/raw"))
    reference_dir: str = field(default_factory=lambda: _env("REFERENCE_DIR", "/opt/project/data/reference"))

    # --- Period processed (monthly partitions, end exclusive) ---------------
    start_month: str = field(default_factory=lambda: _env("PIPELINE_START_MONTH", "2023-01"))
    end_month: str = field(default_factory=lambda: _env("PIPELINE_END_MONTH", "2023-04"))

    # --- Paths in the lake --------------------------------------------------
    @property
    def bronze_trips_prefix(self) -> str:
        return "chicago_taxi_trips"

    @property
    def bronze_reference_key(self) -> str:
        return "reference/community_areas/community_areas.csv"

    def s3a(self, bucket: str, path: str) -> str:
        return f"s3a://{bucket}/{path.strip('/')}"

    @property
    def silver_trips_path(self) -> str:
        return self.s3a(self.silver_bucket, "taxi_trips")

    @property
    def silver_rejected_path(self) -> str:
        return self.s3a(self.silver_bucket, "taxi_trips_rejected")

    @property
    def silver_community_areas_path(self) -> str:
        return self.s3a(self.silver_bucket, "community_areas")

    def gold_path(self, table: str) -> str:
        return self.s3a(self.gold_bucket, table)


settings = Settings()
