from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .env import Runtime
from .paths import WorkPaths

MISSING_CODES = {"NoSuchKey", "404"}


class StoreError(RuntimeError):
    pass


class Store:
    """R2 object store through boto3's S3 client."""

    def __init__(
        self,
        runtime: Runtime,
        paths: WorkPaths,
        *,
        client: Any | None = None,
    ) -> None:
        if not runtime.r2_bucket:
            raise StoreError("required secret is unset: MIRROR_R2_BUCKET")
        self.bucket = runtime.r2_bucket
        self.client = client if client is not None else self._build_client(runtime)

    @staticmethod
    def _build_client(runtime: Runtime) -> Any:
        if not runtime.aws_access_key_id:
            raise StoreError("required secret is unset: AWS_ACCESS_KEY_ID")
        if not runtime.aws_secret_access_key:
            raise StoreError("required secret is unset: AWS_SECRET_ACCESS_KEY")
        if not runtime.aws_endpoint_url:
            raise StoreError("required secret is unset: AWS_ENDPOINT_URL_S3")
        # boto3 reads the endpoint, the keys and the two checksum settings from the
        # environment. The image sets the checksum settings, because R2 rejects the
        # default checksum headers of the SDK.
        return boto3.client(
            "s3", config=Config(retries={"max_attempts": 5, "mode": "standard"})
        )

    def get(self, key: str, target: Path) -> bool:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(self.bucket, key, str(target))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in MISSING_CODES:
                return False
            raise StoreError(f"R2 download failed: {exc}") from exc
        except (BotoCoreError, Boto3Error) as exc:
            raise StoreError(f"R2 download failed: {exc}") from exc
        return True

    def put(self, source: Path, key: str) -> None:
        try:
            self.client.upload_file(str(source), self.bucket, key)
        except (ClientError, BotoCoreError, Boto3Error) as exc:
            raise StoreError(f"R2 upload failed: {exc}") from exc

    def copy(self, source_key: str, target_key: str) -> None:
        try:
            self.client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                Key=target_key,
            )
        except (ClientError, BotoCoreError, Boto3Error) as exc:
            raise StoreError(f"R2 server-side copy failed: {exc}") from exc

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys.extend(entry["Key"] for entry in page.get("Contents", []))
        except (ClientError, BotoCoreError, Boto3Error) as exc:
            raise StoreError(f"R2 listing failed: {exc}") from exc
        return sorted(keys)

    def probe(self) -> None:
        """Fail unless the bucket answers a listing. A wrong bucket name or a rejected
        credential must never read as an empty mirror."""
        try:
            self.client.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        except (ClientError, BotoCoreError, Boto3Error) as exc:
            raise StoreError(f"R2 bucket is not reachable: {exc}") from exc
