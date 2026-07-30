from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

from market_loader.errors import S3ConflictError, S3IntegrityError


@dataclass(frozen=True, slots=True)
class S3ObjectIdentity:
    bucket: str
    object_key: str
    version_id: str
    content_sha256: str
    byte_size: int


def file_hashes(path: Path) -> tuple[str, str]:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest(), base64.b64encode(digest.digest()).decode("ascii")


class ImmutableS3:
    def __init__(self, session: Any) -> None:
        self._client = session.client(
            "s3", config=Config(retries={"mode": "adaptive", "max_attempts": 5})
        )

    def upload(
        self,
        *,
        bucket: str,
        object_key: str,
        path: Path,
        schema_version: str,
        processing_version: str,
        manifest_id: str,
        allow_identical_reuse: bool = False,
    ) -> S3ObjectIdentity:
        hex_sha256, base64_sha256 = file_hashes(path)
        byte_size = path.stat().st_size
        try:
            with path.open("rb") as stream:
                response = self._client.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=stream,
                    ContentLength=byte_size,
                    ContentType="application/vnd.apache.parquet",
                    ServerSideEncryption="AES256",
                    ChecksumAlgorithm="SHA256",
                    ChecksumSHA256=base64_sha256,
                    IfNoneMatch="*",
                    Metadata={
                        "content-sha256": hex_sha256,
                        "schema-version": schema_version,
                        "processing-version": processing_version,
                        "manifest-id": manifest_id,
                    },
                )
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 412 and allow_identical_reuse:
                existing = self._client.head_object(Bucket=bucket, Key=object_key)
                if existing.get("Metadata", {}).get("content-sha256") == hex_sha256:
                    version_id = existing.get("VersionId")
                    if not version_id:
                        raise S3IntegrityError("existing object has no VersionId") from exc
                    return self._verify(
                        bucket, object_key, version_id, hex_sha256, byte_size, base64_sha256
                    )
            if status == 412:
                raise S3ConflictError(f"immutable object key already exists: {object_key}") from exc
            raise
        version_id = response.get("VersionId")
        if not version_id:
            raise S3IntegrityError("S3 upload returned no VersionId")
        return self._verify(bucket, object_key, version_id, hex_sha256, byte_size, base64_sha256)

    def _verify(
        self,
        bucket: str,
        object_key: str,
        version_id: str,
        hex_sha256: str,
        byte_size: int,
        base64_sha256: str,
    ) -> S3ObjectIdentity:
        head = self._client.head_object(
            Bucket=bucket, Key=object_key, VersionId=version_id, ChecksumMode="ENABLED"
        )
        if head.get("ContentLength") != byte_size:
            raise S3IntegrityError("S3 byte size mismatch")
        if head.get("ServerSideEncryption") != "AES256":
            raise S3IntegrityError("S3 object is not encrypted with SSE-S3")
        metadata_hash = head.get("Metadata", {}).get("content-sha256")
        checksum = head.get("ChecksumSHA256")
        if metadata_hash != hex_sha256 or (checksum is not None and checksum != base64_sha256):
            raise S3IntegrityError("S3 SHA-256 mismatch")
        return S3ObjectIdentity(bucket, object_key, version_id, hex_sha256, byte_size)
