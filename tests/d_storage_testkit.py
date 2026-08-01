"""Independent D storage test kit for small Parquet and fake S3 scenarios."""

from __future__ import annotations

import hashlib
import io
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class FakeS3Error(Exception):
    def __init__(self, code: str, status: int, message: str) -> None:
        super().__init__(message)
        self.response = {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class FakeS3Client:
    """Small in-memory S3 surface with conditional immutable writes."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
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
        version_id = f"v{self.put_calls}"
        self.objects[identity] = {
            "Body": payload,
            "Metadata": dict(kwargs.get("Metadata", {})),
            "VersionId": version_id,
            "ETag": hashlib.md5(payload, usedforsecurity=False).hexdigest(),
        }
        return {"VersionId": version_id, "ETag": self.objects[identity]["ETag"]}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if self.head_error is not None:
            raise self.head_error
        try:
            value = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey", 404, "object missing") from exc
        return {
            "ContentLength": len(value["Body"]),
            "Metadata": dict(value["Metadata"]),
            "VersionId": value["VersionId"],
            "ETag": f'"{value["ETag"]}"',
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        try:
            payload = self.objects[(Bucket, Key)]["Body"]
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey", 404, "object missing") from exc
        return {"Body": io.BytesIO(payload)}


def write_small_parquet(path: Path) -> Path:
    """Write a deterministic two-row market-bars object for adapter tests."""

    timestamps = pa.array(
        [
            datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc),
        ],
        type=pa.timestamp("us", tz="UTC"),
    )
    table = pa.table(
        {
            "instrument_id": pa.array(
                [
                    "11111111-1111-4111-8111-111111111111",
                    "11111111-1111-4111-8111-111111111111",
                ]
            ),
            "provider_symbol": pa.array(["AAPL", "AAPL"]),
            "bar_start_at": timestamps,
            "session_date_et": pa.array(
                [date(2024, 1, 2), date(2024, 1, 2)], type=pa.date32()
            ),
            "open": pa.array([100.0, 101.0], type=pa.float64()),
            "high": pa.array([102.0, 103.0], type=pa.float64()),
            "low": pa.array([99.0, 100.0], type=pa.float64()),
            "close": pa.array([101.0, 102.0], type=pa.float64()),
            "volume": pa.array([1000, 1200], type=pa.int64()),
        }
    )
    pq.write_table(table, path, compression="zstd", version="2.6")
    return path
