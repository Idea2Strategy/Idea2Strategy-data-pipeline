from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from market_loader.errors import S3ConflictError, S3IntegrityError
from market_loader.storage.local_staging import LocalStaging
from market_loader.storage.s3 import ImmutableS3, file_hashes


class FakeS3Client:
    def __init__(self, *, conflict: bool = False, version: str | None = "version-1") -> None:
        self.conflict = conflict
        self.version = version
        self.metadata: dict[str, str] = {}
        self.body = b""

    def put_object(self, **kwargs: object) -> dict[str, str]:
        stream = kwargs["Body"]
        self.body = stream.read()
        self.metadata = kwargs["Metadata"]
        if self.conflict:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        return {"VersionId": self.version} if self.version else {}

    def head_object(self, **_: object) -> dict[str, object]:
        digest = hashlib.sha256(self.body).digest()
        return {
            "VersionId": self.version,
            "ContentLength": len(self.body),
            "ServerSideEncryption": "AES256",
            "Metadata": self.metadata,
            "ChecksumSHA256": base64.b64encode(digest).decode(),
        }


class FakeSession:
    def __init__(self, client: FakeS3Client) -> None:
        self.value = client

    def client(self, *_: object, **__: object) -> FakeS3Client:
        return self.value


def test_local_staging_and_file_hashes(tmp_path: Path) -> None:
    identifier = UUID("11111111-1111-1111-1111-111111111111")
    path = LocalStaging(tmp_path).path_for(identifier, "raw", "30m", 2024, 3)
    assert tmp_path.resolve() in path.parents
    source = tmp_path / "bytes.bin"
    source.write_bytes(b"market-data")
    hexadecimal, encoded = file_hashes(source)
    assert hexadecimal == hashlib.sha256(b"market-data").hexdigest()
    assert base64.b64decode(encoded) == hashlib.sha256(b"market-data").digest()


def test_immutable_upload_verifies_version_size_hash_and_sse(tmp_path: Path) -> None:
    source = tmp_path / "part.parquet"
    source.write_bytes(b"parquet-bytes")
    client = FakeS3Client()
    result = ImmutableS3(FakeSession(client)).upload(
        bucket="private-bucket",
        object_key="historical/key",
        path=source,
        schema_version="market-bars/1",
        processing_version="market-loader/1.0.0",
        manifest_id="11111111-1111-1111-1111-111111111111",
    )
    assert result.version_id == "version-1"
    assert result.byte_size == len(b"parquet-bytes")
    assert client.metadata["content-sha256"] == result.content_sha256


def test_immutable_upload_rejects_missing_version_and_conflict(tmp_path: Path) -> None:
    source = tmp_path / "part.parquet"
    source.write_bytes(b"parquet-bytes")
    with pytest.raises(S3IntegrityError, match="VersionId"):
        ImmutableS3(FakeSession(FakeS3Client(version=None))).upload(
            bucket="bucket",
            object_key="key",
            path=source,
            schema_version="schema",
            processing_version="processing",
            manifest_id="manifest",
        )
    with pytest.raises(S3ConflictError):
        ImmutableS3(FakeSession(FakeS3Client(conflict=True))).upload(
            bucket="bucket",
            object_key="key",
            path=source,
            schema_version="schema",
            processing_version="processing",
            manifest_id="manifest",
        )
