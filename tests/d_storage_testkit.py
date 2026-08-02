"""Independent D storage test kit for small Parquet and fake S3 scenarios."""

from __future__ import annotations

import base64
import hashlib
import io
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from market_pipeline_lib.contracts import bar_schema


class FakeS3Error(Exception):
    def __init__(self, code: str, status: int, message: str) -> None:
        super().__init__(message)
        self.response = {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class FakeS3Client:
    """Small in-memory S3 surface with conditional immutable writes.

    It models the parts of the real service the adapter depends on: rejection
    of `IfNoneMatch="*"` against an existing key with HTTP 412, per-version
    addressing, SSE-S3 echo, and `ChecksumSHA256` returned only when the head
    request asks for it. It is a *fake*, not a proof: the same conditional-write
    path is exercised against a real S3 implementation in
    `tests/test_storage_adapter_localstack.py`.
    """

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.versions: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.put_kwargs: list[dict[str, Any]] = []
        self.put_calls = 0
        self.head_error: Exception | None = None

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        identity = (bucket, key)
        if kwargs.get("IfNoneMatch") == "*" and identity in self.objects:
            raise FakeS3Error("PreconditionFailed", 412, "object exists")
        body = kwargs["Body"]
        payload = body.read() if hasattr(body, "read") else bytes(body)
        self.put_calls += 1
        self.put_kwargs.append({key: value for key, value in kwargs.items() if key != "Body"})
        version_id = f"v{self.put_calls}"
        record = {
            "Body": payload,
            "Metadata": dict(kwargs.get("Metadata", {})),
            "VersionId": version_id,
            "ETag": hashlib.md5(payload, usedforsecurity=False).hexdigest(),
            "ServerSideEncryption": kwargs.get("ServerSideEncryption"),
            "ChecksumSHA256": kwargs.get("ChecksumSHA256"),
        }
        self.objects[identity] = record
        self.versions[(bucket, key, version_id)] = record
        return {"VersionId": version_id, "ETag": record["ETag"]}

    def head_object(
        self,
        *,
        Bucket: str,
        Key: str,
        VersionId: str | None = None,
        ChecksumMode: str | None = None,
    ) -> dict[str, Any]:
        if self.head_error is not None:
            raise self.head_error
        try:
            if VersionId is None:
                value = self.objects[(Bucket, Key)]
            else:
                value = self.versions[(Bucket, Key, VersionId)]
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey", 404, "object missing") from exc
        head: dict[str, Any] = {
            "ContentLength": len(value["Body"]),
            "Metadata": dict(value["Metadata"]),
            "VersionId": value["VersionId"],
            "ETag": f'"{value["ETag"]}"',
        }
        if value["ServerSideEncryption"] is not None:
            head["ServerSideEncryption"] = value["ServerSideEncryption"]
        # Real S3 returns the checksum only when it is asked for.
        if ChecksumMode == "ENABLED" and value["ChecksumSHA256"] is not None:
            head["ChecksumSHA256"] = value["ChecksumSHA256"]
        return head

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        try:
            payload = self.objects[(Bucket, Key)]["Body"]
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey", 404, "object missing") from exc
        return {"Body": io.BytesIO(payload)}


def base64_sha256(payload: bytes) -> str:
    return base64.b64encode(hashlib.sha256(payload).digest()).decode("ascii")


def required_bar_fields() -> list[pa.Field]:
    """The non-nullable half of `bar_schema`, which is what fixtures carry."""
    return [field for field in bar_schema(False) if not field.nullable]


def small_bar_table(price_offset: float = 0.0) -> pa.Table:
    """Build the two-row fixture table from `bar_schema`, not from a copy of it.

    Column names and Arrow types are taken from `contracts.bar_schema` so the
    fixture cannot drift away from the schema it is supposed to represent. If
    the canonical schema gains, drops or renames a required field this raises
    instead of silently writing a differently shaped object — the committed
    fixtures under `tests/fixtures/datasets/` are byte-compared, so a change
    here must be a deliberate regeneration.

    The schema is rebuilt from the field list rather than reused wholesale so
    that no key/value metadata is attached, and the fields are relaxed to
    nullable, because that is how the committed fixture objects under
    `tests/fixtures/datasets/` were written: Parquet encodes a required column
    without definition levels, so keeping `bar_schema`'s non-nullable flags
    would change every fixture's bytes and their recorded content hashes.
    Names, order and Arrow types — the parts that actually drift — still come
    straight from `bar_schema`.
    """
    instrument_id = "11111111-1111-4111-8111-111111111111"
    values: dict[str, list[Any]] = {
        "instrument_id": [instrument_id, instrument_id],
        "provider_symbol": ["AAPL", "AAPL"],
        "bar_start_at": [
            datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
            datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        ],
        "session_date_et": [date(2024, 1, 2), date(2024, 1, 2)],
        "open": [100.0 + price_offset, 101.0 + price_offset],
        "high": [102.0 + price_offset, 103.0 + price_offset],
        "low": [99.0 + price_offset, 100.0 + price_offset],
        "close": [101.0 + price_offset, 102.0 + price_offset],
        "volume": [1000, 1200],
    }
    fields = required_bar_fields()
    names = [field.name for field in fields]
    missing = [name for name in names if name not in values]
    extra = [name for name in values if name not in names]
    if missing or extra:
        raise AssertionError(
            "bar_schema의 필수 열이 fixture와 어긋났습니다. "
            f"누락={missing} 초과={extra}. 픽스처를 의도적으로 재생성하세요."
        )
    schema = pa.schema([field.with_nullable(True) for field in fields])
    return pa.table(
        {field.name: pa.array(values[field.name], type=field.type) for field in fields},
        schema=schema,
    )


def write_small_parquet(path: Path, *, price_offset: float = 0.0) -> Path:
    """Write a deterministic two-row market-bars object for adapter tests."""
    pq.write_table(small_bar_table(price_offset), path, compression="zstd", version="2.6")
    return path
