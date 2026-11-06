"""Create the bronze / silver / gold buckets (run once by the `init` service)."""

import time

from pipeline.config import settings
from pipeline.resources.s3 import ensure_buckets, make_s3_client


def main(retries: int = 30) -> None:
    buckets = [settings.bronze_bucket, settings.silver_bucket, settings.gold_bucket]
    for attempt in range(retries):
        try:
            ensure_buckets(make_s3_client(), buckets)
            print(f"Buckets ready: {buckets}")
            return
        except Exception as exc:  # MinIO may still be starting
            print(f"Waiting for object storage ({attempt + 1}/{retries}): {exc}")
            time.sleep(2)
    raise SystemExit("Object storage not reachable")


if __name__ == "__main__":
    main()
