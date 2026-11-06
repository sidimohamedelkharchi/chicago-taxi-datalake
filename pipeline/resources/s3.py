"""Thin boto3 wrapper around the object storage (MinIO locally, S3 in the cloud)."""

from __future__ import annotations

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dagster import ConfigurableResource

from pipeline.config import settings


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_buckets(client, buckets: list[str]) -> None:
    for bucket in buckets:
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError:
            client.create_bucket(Bucket=bucket)


class S3Resource(ConfigurableResource):
    def client(self):
        return make_s3_client()

    def ensure_layer_buckets(self) -> None:
        ensure_buckets(
            self.client(),
            [settings.bronze_bucket, settings.silver_bucket, settings.gold_bucket],
        )

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Delete every object under a prefix (used to make bronze writes idempotent)."""
        client = self.client()
        deleted = 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
                deleted += len(objects)
        return deleted

    def put_bytes(self, bucket: str, key: str, data: bytes, content_type: str = "text/csv") -> None:
        self.client().put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
