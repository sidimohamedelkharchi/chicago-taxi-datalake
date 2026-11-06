"""BRONZE: raw files landed as-is in object storage, partitioned by month."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from pipeline.config import settings
from pipeline.ingestion import soda
from pipeline.partitions import monthly_partitions, partition_to_month
from pipeline.resources import S3Resource


class BronzeIngestionConfig(Config):
    # auto  -> use the local cache in data/raw if present, otherwise call the API
    # api   -> always download from the SODA API
    # local -> only use the local cache (fails if missing)
    source: str = "auto"


@asset(
    group_name="bronze",
    partitions_def=monthly_partitions,
    kinds={"python", "s3"},
    description="Chicago taxi trips, raw CSV pages from the SODA API stored untouched in "
    "s3://bronze/chicago_taxi_trips/trip_month=YYYY-MM/ (+ a _manifest.json for lineage).",
    check_specs=[AssetCheckSpec("bronze_not_empty", asset="bronze_taxi_trips")],
)
def bronze_taxi_trips(
    context: AssetExecutionContext, config: BronzeIngestionConfig, s3: S3Resource
) -> MaterializeResult:
    month = partition_to_month(context.partition_key)
    s3.ensure_layer_buckets()

    use_local = config.source == "local" or (config.source == "auto" and soda.has_local_cache(month))
    if config.source == "local" and not soda.has_local_cache(month):
        raise FileNotFoundError(f"No local raw files for {month} in {settings.raw_local_dir}")
    pages = soda.iter_local_pages(month) if use_local else soda.iter_api_pages(month)
    source = "local_cache" if use_local else settings.soda_base_url
    context.log.info(f"Ingesting {month} from {source}")

    prefix = f"{settings.bronze_trips_prefix}/trip_month={month}/"
    removed = s3.delete_prefix(settings.bronze_bucket, prefix)  # idempotent re-run
    if removed:
        context.log.info(f"Removed {removed} previous objects under {prefix}")

    files, total_rows, total_bytes = [], 0, 0
    for page in pages:
        key = f"{prefix}page_{page.index:05d}.csv"
        s3.put_bytes(settings.bronze_bucket, key, page.content)
        files.append({"key": key, "rows": page.rows, "sha256": hashlib.sha256(page.content).hexdigest()})
        total_rows += page.rows
        total_bytes += len(page.content)

    manifest = {
        "dataset": "chicago_taxi_trips",
        "trip_month": month,
        "source": source,
        "query": soda.build_params(month, settings.soda_page_size, 0),
        "ingested_at": datetime.now(UTC).isoformat(),
        "rows": total_rows,
        "files": files,
    }
    s3.put_bytes(
        settings.bronze_bucket, f"{prefix}_manifest.json", json.dumps(manifest, indent=2).encode(), "application/json"
    )

    return MaterializeResult(
        metadata={
            "rows": total_rows,
            "files": len(files),
            "size_mb": round(total_bytes / 1e6, 1),
            "source": source,
            "location": MetadataValue.path(f"s3://{settings.bronze_bucket}/{prefix}"),
        },
        check_results=[
            AssetCheckResult(
                check_name="bronze_not_empty",
                passed=total_rows > 0,
                severity=AssetCheckSeverity.ERROR,
                metadata={"rows": total_rows},
            )
        ],
    )


@asset(
    group_name="bronze",
    kinds={"python", "s3"},
    description="Reference table of Chicago's 77 community areas (seed CSV versioned in the repo).",
)
def bronze_community_areas(context: AssetExecutionContext, s3: S3Resource) -> MaterializeResult:
    s3.ensure_layer_buckets()
    seed = Path(settings.reference_dir) / "community_areas.csv"
    content = seed.read_bytes()
    s3.put_bytes(settings.bronze_bucket, settings.bronze_reference_key, content)
    return MaterializeResult(
        metadata={
            "rows": soda.count_csv_rows(content),
            "location": MetadataValue.path(f"s3://{settings.bronze_bucket}/{settings.bronze_reference_key}"),
        }
    )
