from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from typing import Any

import pytest

from market_pipeline_lib.cli import build_parser
from market_pipeline_lib.legacy_bootstrap import (
    BootstrapConflict,
    ObjectVerificationError,
    S3LegacyObjectVerifier,
    materialize_legacy_catalog,
    same_database,
)

PROVIDER_ID = "10000000-0000-4000-8000-000000000001"
OBJECT_ID = "20000000-0000-4000-8000-000000000001"


def test_cli_requires_explicit_inventory_gates() -> None:
    args = build_parser().parse_args(
        [
            "bootstrap-legacy-catalog",
            "--artifact-root",
            "evidence",
            "--bucket",
            "market-data",
            "--expected-object-count",
            "768",
            "--expected-manifest-count",
            "96",
        ]
    )

    assert args.expected_object_count == 768
    assert args.expected_manifest_count == 96
    assert not args.execute


def test_source_and_target_identity_ignores_credentials() -> None:
    assert same_database(
        "postgresql+psycopg://legacy:one@db.internal/idea2strategy",
        "postgresql+psycopg://pipeline:two@DB.INTERNAL:5432/idea2strategy",
    )
    assert not same_database(
        "postgresql+psycopg://legacy:one@legacy.internal/idea2strategy",
        "postgresql+psycopg://pipeline:two@canonical.internal/idea2strategy",
    )


def provider() -> dict[str, Any]:
    return {
        "id": PROVIDER_ID,
        "code": "ALPACA",
        "display_name": "Alpaca",
        "rights_version": "approved-v1",
        "status": "ACTIVE",
        "created_at": "2026-07-31T00:00:00Z",
    }


def storage_object() -> dict[str, Any]:
    return {
        "id": OBJECT_ID,
        "status": "AVAILABLE",
        "storage_provider": "S3_COMPATIBLE",
        "bucket_name": "market-data",
        "object_key": "historical/year=2026/part-00001.parquet",
        "provider_version_id": "version-1",
        "content_hash": "a" * 64,
        "byte_size": 123,
        "file_format": "PARQUET",
        "compression_codec": "SNAPPY",
        "media_type": "application/vnd.apache.parquet",
        "schema_version": "market-bars/1",
        "row_count": 10,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2027-01-01T00:00:00Z",
        "encryption_key_ref": "SSE-S3",
        "retention_policy_version": "market-data-v1",
        "retention_until": None,
        "legal_hold": False,
        "created_at": "2026-07-31T00:00:00Z",
        "verified_at": "2026-07-31T00:00:01Z",
        "quarantined_at": None,
        "superseded_at": None,
        "deleted_at": None,
    }


class FakeCatalog:
    def __init__(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        self.rows = deepcopy(rows)
        self.transaction_count = 0

    def records(self, table: str, *, where: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        assert where is None
        return deepcopy(self.rows.get(table, []))

    @contextmanager
    def transaction(self):
        before = deepcopy(self.rows)
        self.transaction_count += 1
        try:
            yield self
        except Exception:
            self.rows = before
            raise

    def append_unique(
        self,
        table: str,
        record: dict[str, Any],
        key_fields: tuple[str, ...],
    ) -> None:
        key = tuple(record[field] for field in key_fields)
        if any(tuple(row[field] for field in key_fields) == key for row in self.rows.get(table, [])):
            return
        self.rows.setdefault(table, []).append(deepcopy(record))


class RecordingVerifier:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def verify_all(self, rows: list[dict[str, Any]]) -> int:
        self.rows = deepcopy(rows)
        return len(rows)


def test_dry_run_verifies_objects_but_does_not_write() -> None:
    source = FakeCatalog(
        {"market_data.providers": [provider()], "storage.objects": [storage_object()]}
    )
    target = FakeCatalog({})
    verifier = RecordingVerifier()

    report = materialize_legacy_catalog(
        source,
        target,
        object_verifier=verifier,
        expected_object_count=1,
        execute=False,
    )

    assert report["status"] == "DRY_RUN"
    assert report["source_table_counts"]["storage.objects"] == 1
    assert report["missing_table_counts"]["market_data.providers"] == 1
    assert verifier.rows == [storage_object()]
    assert target.rows == {}
    assert target.transaction_count == 0


def test_execute_is_atomic_and_an_exact_replay_is_a_noop() -> None:
    source = FakeCatalog(
        {"market_data.providers": [provider()], "storage.objects": [storage_object()]}
    )
    target = FakeCatalog({})
    verifier = RecordingVerifier()

    first = materialize_legacy_catalog(
        source,
        target,
        object_verifier=verifier,
        expected_object_count=1,
        execute=True,
    )
    second = materialize_legacy_catalog(
        source,
        target,
        object_verifier=verifier,
        expected_object_count=1,
        execute=True,
    )

    assert first["status"] == "APPLIED"
    assert first["inserted_row_count"] == 2
    assert second["status"] == "ALREADY_APPLIED"
    assert second["inserted_row_count"] == 0
    assert target.rows["market_data.providers"] == [provider()]
    assert target.rows["storage.objects"] == [storage_object()]


def test_divergent_target_row_fails_closed_without_a_write() -> None:
    source = FakeCatalog({"market_data.providers": [provider()]})
    changed = provider()
    changed["rights_version"] = "different"
    target = FakeCatalog({"market_data.providers": [changed]})

    with pytest.raises(BootstrapConflict, match="market_data.providers"):
        materialize_legacy_catalog(
            source,
            target,
            object_verifier=RecordingVerifier(),
            expected_object_count=0,
            execute=True,
        )

    assert target.transaction_count == 0


def test_unexpected_source_object_count_fails_before_target_write() -> None:
    source = FakeCatalog({"storage.objects": [storage_object()]})
    target = FakeCatalog({})

    with pytest.raises(BootstrapConflict, match="expected 768"):
        materialize_legacy_catalog(
            source,
            target,
            object_verifier=RecordingVerifier(),
            expected_object_count=768,
            execute=True,
        )

    assert target.transaction_count == 0


class HeadClient:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return deepcopy(self.response)


def test_s3_verifier_pins_the_exact_version_and_accepts_legacy_hash_metadata() -> None:
    client = HeadClient(
        {
            "VersionId": "version-1",
            "ContentLength": 123,
            "ServerSideEncryption": "AES256",
            "Metadata": {"content-sha256": "a" * 64},
        }
    )

    verified = S3LegacyObjectVerifier(client).verify_all([storage_object()])

    assert verified == 1
    assert client.calls == [
        {
            "Bucket": "market-data",
            "Key": "historical/year=2026/part-00001.parquet",
            "VersionId": "version-1",
            "ChecksumMode": "ENABLED",
        }
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("VersionId", "version-2", "version"),
        ("ContentLength", 122, "byte size"),
        ("ServerSideEncryption", None, "encryption"),
        ("Metadata", {"content-sha256": "b" * 64}, "content hash"),
    ],
)
def test_s3_verifier_rejects_an_object_that_differs_from_the_legacy_receipt(
    field: str,
    value: Any,
    message: str,
) -> None:
    response = {
        "VersionId": "version-1",
        "ContentLength": 123,
        "ServerSideEncryption": "AES256",
        "Metadata": {"content-sha256": "a" * 64},
    }
    response[field] = value

    with pytest.raises(ObjectVerificationError, match=message):
        S3LegacyObjectVerifier(HeadClient(response)).verify_all([storage_object()])
