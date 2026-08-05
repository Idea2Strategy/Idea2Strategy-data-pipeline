from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine

from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy


class _InsertedResult:
    def scalar_one_or_none(self) -> str:
        return "66666666-6666-4666-8666-666666666666"


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: Any) -> _InsertedResult:
        self.statements.append(str(statement.compile(dialect=postgresql.dialect())))
        return _InsertedResult()


class _RecordingEngine:
    def __init__(self) -> None:
        self.connection = _RecordingConnection()

    @contextmanager
    def begin(self) -> Iterator[_RecordingConnection]:
        yield self.connection


def _storage_object() -> dict[str, Any]:
    return {
        "id": "66666666-6666-4666-8666-666666666666",
        "status": "AVAILABLE",
        "storage_provider": "S3",
        "bucket_name": "idea2strategy-market-data",
        "object_key": "market-data/aapl.parquet",
        "provider_version_id": "version-1",
        "content_hash": "a" * 64,
        "byte_size": 4096,
        "file_format": "PARQUET",
        "compression_codec": "UNCOMPRESSED",
        "media_type": "application/vnd.apache.parquet",
        "schema_version": "market-bars-v2",
        "row_count": 64,
        "period_start": "2026-08-04T08:00:00Z",
        "period_end": "2026-08-05T00:00:00Z",
        "encryption_key_ref": None,
        "retention_policy_version": "UNSPECIFIED",
        "retention_until": None,
        "legal_hold": False,
        "created_at": "2026-08-05T10:03:59Z",
        "verified_at": "2026-08-05T10:03:59Z",
        "quarantined_at": None,
        "superseded_at": None,
        "deleted_at": None,
    }


def test_storage_object_registration_never_requires_update_privilege(tmp_path: Path) -> None:
    """D owns immutable registration, so its runtime role intentionally has no UPDATE."""

    engine = _RecordingEngine()
    catalog = PostgresCatalog(
        cast(Engine, engine),
        artifact_root=tmp_path,
        storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
    )

    catalog.upsert("storage.objects", _storage_object())

    assert len(engine.connection.statements) == 1
    sql = engine.connection.statements[0]
    assert "ON CONFLICT (id) DO NOTHING" in sql
    assert "DO UPDATE" not in sql
