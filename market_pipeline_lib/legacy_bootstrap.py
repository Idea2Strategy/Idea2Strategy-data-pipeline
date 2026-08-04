"""Read-only legacy catalog audit and retry-safe canonical DB bootstrap.

The legacy RDS instance remains an immutable source.  A bootstrap accepts only an
empty target or an exact subset of the source, verifies every version-pinned S3
receipt, and inserts only missing rows in one transaction.  An exact rerun is a
no-op; any divergent target row fails closed instead of being overwritten.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy.engine import make_url

from .catalog import MarketDataCatalog, PostgresCatalog, StorageObjectsPolicy
from .db.codec import table_for
from .db.engine import create_market_data_engine

__all__ = [
    "BOOTSTRAP_TABLE_ORDER",
    "BootstrapConflict",
    "ObjectVerificationError",
    "S3LegacyObjectVerifier",
    "connect_read_only_catalog",
    "materialize_legacy_catalog",
    "same_database",
]


# Parents precede children.  This includes all canonical rows D may own, not only
# the original nine-table export, so a later retry cannot silently omit reference,
# watermark, corporate-action, or feature metadata present in the source RDS.
BOOTSTRAP_TABLE_ORDER: tuple[str, ...] = (
    "market_data.providers",
    "market_data.feeds",
    "market_data.instruments",
    "market_data.instrument_symbols",
    "market_data.trading_sessions",
    "market_data.pipeline_runs",
    "storage.objects",
    "market_data.dataset_manifests",
    "market_data.dataset_objects",
    "market_data.dataset_lineage",
    "market_data.dataset_object_lineage",
    "market_data.quality_incidents",
    "market_data.stream_watermarks",
    "market_data.corporate_actions",
    "market_data.feature_definitions",
    "market_data.feature_materializations",
    "market_data.feature_snapshot_batches",
)


class BootstrapConflict(RuntimeError):
    """The source or target cannot be bootstrapped without changing meaning."""


class ObjectVerificationError(RuntimeError):
    """A version-pinned S3 object differs from its legacy DB receipt."""


class ObjectVerifier(Protocol):
    def verify_all(self, rows: list[dict[str, Any]]) -> int: ...


class S3HeadClient(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...


class S3LegacyObjectVerifier:
    """Verify legacy receipts with HEAD only; never downloads or mutates objects."""

    def __init__(self, client: S3HeadClient, *, expected_bucket: str | None = None) -> None:
        self._client = client
        self._expected_bucket = expected_bucket

    def verify_all(self, rows: list[dict[str, Any]]) -> int:
        for row in rows:
            self._verify(row)
        return len(rows)

    def _verify(self, row: Mapping[str, Any]) -> None:
        object_id = str(row["id"])
        bucket = str(row["bucket_name"])
        key = str(row["object_key"])
        version = str(row["provider_version_id"])
        if row["status"] != "AVAILABLE":
            raise ObjectVerificationError(f"object {object_id} is not AVAILABLE")
        if row["storage_provider"] not in {"S3", "S3_COMPATIBLE"}:
            raise ObjectVerificationError(
                f"object {object_id} is not an S3 receipt: {row['storage_provider']}"
            )
        if self._expected_bucket is not None and bucket != self._expected_bucket:
            raise ObjectVerificationError(
                f"object {object_id} names bucket {bucket!r}, expected {self._expected_bucket!r}"
            )
        try:
            head = self._client.head_object(
                Bucket=bucket,
                Key=key,
                VersionId=version,
                ChecksumMode="ENABLED",
            )
        except Exception as exc:
            raise ObjectVerificationError(
                f"cannot HEAD version-pinned object {object_id} at s3://{bucket}/{key}: {exc}"
            ) from exc

        if str(head.get("VersionId", "")) != version:
            raise ObjectVerificationError(f"object {object_id} version does not match the legacy receipt")
        if int(head.get("ContentLength", -1)) != int(row["byte_size"]):
            raise ObjectVerificationError(f"object {object_id} byte size does not match the legacy receipt")
        if head.get("ServerSideEncryption") not in {"AES256", "aws:kms"}:
            raise ObjectVerificationError(f"object {object_id} encryption is absent or unsupported")
        metadata = head.get("Metadata") or {}
        actual_hash = metadata.get("sha256") or metadata.get("content-sha256")
        if actual_hash != row["content_hash"]:
            raise ObjectVerificationError(f"object {object_id} content hash does not match the legacy receipt")


def same_database(left: str, right: str) -> bool:
    """Compare database identity while deliberately ignoring credentials."""

    first = make_url(left)
    second = make_url(right)
    return (
        (first.host or "").lower(),
        first.port or 5432,
        first.database,
    ) == (
        (second.host or "").lower(),
        second.port or 5432,
        second.database,
    )


def connect_read_only_catalog(database_url: str, *, artifact_root: Path) -> PostgresCatalog:
    """Open PostgreSQL with both client-side and server-side write denial."""

    engine = create_market_data_engine(
        database_url,
        writable_schemas=("__read_only__",),
        application_name="idea2strategy-legacy-catalog-read-only",
        connect_args={"options": "-c default_transaction_read_only=on"},
    )
    return PostgresCatalog(
        engine,
        artifact_root=artifact_root,
        storage_objects=StorageObjectsPolicy.READ_ONLY,
        owns_engine=True,
    )


def _snapshot(catalog: MarketDataCatalog) -> dict[str, list[dict[str, Any]]]:
    return {table: catalog.records(table) for table in BOOTSTRAP_TABLE_ORDER}


def _digest(snapshot: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    document = {table: list(snapshot[table]) for table in BOOTSTRAP_TABLE_ORDER}
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _primary_key(table: str, row: Mapping[str, Any]) -> tuple[Any, ...]:
    fields = tuple(column.name for column in table_for(table).primary_key.columns)
    return tuple(row[field] for field in fields)


def _missing_rows(
    source: Mapping[str, Sequence[dict[str, Any]]],
    target: Mapping[str, Sequence[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    missing: dict[str, list[dict[str, Any]]] = {}
    for table in BOOTSTRAP_TABLE_ORDER:
        source_by_key = {_primary_key(table, row): row for row in source[table]}
        target_by_key = {_primary_key(table, row): row for row in target[table]}
        for key, target_row in target_by_key.items():
            source_row = source_by_key.get(key)
            if source_row is None:
                raise BootstrapConflict(
                    f"target {table} contains row {key!r} that is absent from the legacy source"
                )
            if source_row != target_row:
                raise BootstrapConflict(
                    f"target {table} row {key!r} differs from the legacy source"
                )
        missing[table] = [row for key, row in source_by_key.items() if key not in target_by_key]
    return missing


def _parent_first(
    rows: Sequence[dict[str, Any]],
    *,
    id_field: str,
    parent_field: str,
) -> list[dict[str, Any]]:
    """Topologically order self-referencing rows without changing their content."""

    remaining = list(rows)
    ordered: list[dict[str, Any]] = []
    known: set[Any] = set()
    source_ids = {row[id_field] for row in remaining}
    while remaining:
        ready = [
            row
            for row in remaining
            if row.get(parent_field) is None
            or row[parent_field] in known
            or row[parent_field] not in source_ids
        ]
        if not ready:
            raise BootstrapConflict(f"cycle detected through {parent_field}")
        for row in ready:
            ordered.append(row)
            known.add(row[id_field])
            remaining.remove(row)
    return ordered


def _insertion_rows(table: str, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    if table == "market_data.dataset_manifests":
        return _parent_first(rows, id_field="id", parent_field="supersedes_manifest_id")
    if table == "market_data.corporate_actions":
        return _parent_first(rows, id_field="id", parent_field="supersedes_action_id")
    return list(rows)


def materialize_legacy_catalog(
    source: MarketDataCatalog,
    target: MarketDataCatalog,
    *,
    object_verifier: ObjectVerifier,
    expected_object_count: int,
    expected_manifest_count: int | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    """Audit and optionally append the exact legacy snapshot to a canonical DB."""

    source_snapshot = _snapshot(source)
    object_count = len(source_snapshot["storage.objects"])
    manifest_count = len(source_snapshot["market_data.dataset_manifests"])
    if object_count != expected_object_count:
        raise BootstrapConflict(
            f"legacy source has {object_count} storage objects; expected {expected_object_count}"
        )
    if expected_manifest_count is not None and manifest_count != expected_manifest_count:
        raise BootstrapConflict(
            f"legacy source has {manifest_count} manifests; expected {expected_manifest_count}"
        )
    verified = object_verifier.verify_all(source_snapshot["storage.objects"])
    if verified != object_count:
        raise ObjectVerificationError(f"verified {verified} of {object_count} legacy objects")

    target_snapshot = _snapshot(target)
    missing = _missing_rows(source_snapshot, target_snapshot)
    source_counts = {table: len(source_snapshot[table]) for table in BOOTSTRAP_TABLE_ORDER}
    target_counts = {table: len(target_snapshot[table]) for table in BOOTSTRAP_TABLE_ORDER}
    missing_counts = {table: len(missing[table]) for table in BOOTSTRAP_TABLE_ORDER}
    inserted_count = sum(missing_counts.values())
    report: dict[str, Any] = {
        "status": "DRY_RUN" if not execute else "PLANNED",
        "source_digest": _digest(source_snapshot),
        "source_table_counts": source_counts,
        "target_table_counts": target_counts,
        "missing_table_counts": missing_counts,
        "verified_object_count": verified,
        "manifest_count": manifest_count,
        "inserted_row_count": 0,
    }
    if not execute:
        return report
    if inserted_count == 0:
        return {**report, "status": "ALREADY_APPLIED"}

    with target.transaction() as unit:
        for table in BOOTSTRAP_TABLE_ORDER:
            key_fields = tuple(column.name for column in table_for(table).primary_key.columns)
            for row in _insertion_rows(table, missing[table]):
                unit.append_unique(table, row, key_fields)
        applied_snapshot = _snapshot(unit)
        if _digest(applied_snapshot) != _digest(source_snapshot):
            raise BootstrapConflict("target snapshot differs after insert; rolling back bootstrap")
    return {**report, "status": "APPLIED", "inserted_row_count": inserted_count}
