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

from sqlalchemy import UniqueConstraint, func, inspect, select
from sqlalchemy.engine import Connection, make_url

from .catalog import MarketDataCatalog, PostgresCatalog, StorageObjectsPolicy
from .db.codec import table_for
from .db.engine import create_market_data_engine
from .db.schema_guard import describe_schema_drift

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


_V001_CHANGED_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "market_data.dataset_objects": frozenset(
        {
            "id",
            "dataset_manifest_id",
            "object_id",
            "object_kind",
            "partition_key",
            "row_count",
            "min_bar_start_at",
            "max_bar_start_at",
            "created_at",
        }
    ),
    "market_data.dataset_lineage": frozenset(
        {
            "id",
            "dataset_manifest_id",
            "source_manifest_id",
            "relationship_type",
            "created_at",
        }
    ),
    "market_data.quality_incidents": frozenset(
        {
            "id",
            "dataset_manifest_id",
            "instrument_id",
            "incident_type",
            "severity",
            "period_start",
            "period_end",
            "status",
            "detail",
            "detected_at",
            "resolved_at",
        }
    ),
}

_V001_MISSING_TABLES = frozenset(
    {
        "market_data.dataset_object_lineage",
        "market_data.corporate_actions",
        "market_data.feature_materializations",
    }
)

# These are the complete differences reported by the canonical schema guard for
# tests/fixtures/legacy-market-schema.sql.  Matching only a subset would turn an
# unknown drift into an implicitly supported source shape.
_V001_CANONICAL_DRIFT = frozenset(
    {
        "market_data.corporate_actions: table is missing",
        "market_data.dataset_lineage.derived_manifest_id: column is missing",
        "market_data.dataset_lineage.relation_type: column is missing",
        "market_data.dataset_lineage: unique constraint on "
        "['derived_manifest_id', 'relation_type', 'source_manifest_id'] is missing",
        "market_data.dataset_objects.object_kind: type is varchar(30), expected varchar(40)",
        "market_data.dataset_objects.partition_granularity: column is missing",
        "market_data.dataset_objects.partition_start: column is missing",
        "market_data.dataset_objects.partition_end: column is missing",
        "market_data.dataset_objects.period_start: column is missing",
        "market_data.dataset_objects.period_end: column is missing",
        "market_data.dataset_objects.shard_key: column is missing",
        "market_data.dataset_objects.part_number: column is missing",
        "market_data.dataset_objects.min_instrument_id: column is missing",
        "market_data.dataset_objects.max_instrument_id: column is missing",
        "market_data.dataset_objects: unique constraint on "
        "['dataset_manifest_id', 'object_kind', 'part_number', 'partition_end', "
        "'partition_granularity', 'partition_start', 'shard_key'] is missing",
        "market_data.feature_materializations: table is missing",
        "market_data.quality_incidents.incident_code: column is missing",
        "market_data.quality_incidents.period_start: nullable is True, expected False",
        "market_data.quality_incidents.evidence_object_id: column is missing",
        "market_data.dataset_object_lineage: table is missing",
    }
)

_V001_CHANGED_TABLE_UNIQUES: dict[str, frozenset[frozenset[str]]] = {
    "market_data.dataset_objects": frozenset(
        {
            frozenset({"id"}),
            frozenset({"dataset_manifest_id", "object_id"}),
            frozenset({"dataset_manifest_id", "partition_key"}),
        }
    ),
    "market_data.dataset_lineage": frozenset(
        {
            frozenset({"id"}),
            frozenset(
                {"dataset_manifest_id", "source_manifest_id", "relationship_type"}
            ),
        }
    ),
    "market_data.quality_incidents": frozenset({frozenset({"id"})}),
}


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


def _canonical_columns(table: str) -> frozenset[str]:
    return frozenset(column.name for column in table_for(table).columns)


def _canonical_uniques(table: str) -> frozenset[frozenset[str]]:
    target = table_for(table)
    unique_sets: set[frozenset[str]] = {
        frozenset(column.name for column in target.primary_key.columns)
    }
    for constraint in target.constraints:
        if isinstance(constraint, UniqueConstraint):
            unique_sets.add(frozenset(column.name for column in constraint.columns))
    for index in target.indexes:
        if index.unique:
            unique_sets.add(frozenset(column.name for column in index.columns))
    return frozenset(unique_sets)


def _actual_uniques(connection: Connection, table: str) -> frozenset[frozenset[str]]:
    schema, name = table.split(".", 1)
    inspector = inspect(connection)
    unique_sets: set[frozenset[str]] = set()
    for index in inspector.get_indexes(name, schema=schema):
        if index.get("unique"):
            unique_sets.add(frozenset(value for value in index["column_names"] if value))
    for constraint in inspector.get_unique_constraints(name, schema=schema):
        unique_sets.add(frozenset(constraint["column_names"]))
    primary_key = inspector.get_pk_constraint(name, schema=schema).get("constrained_columns")
    if primary_key:
        unique_sets.add(frozenset(primary_key))
    return frozenset(unique_sets)


class _ImmutableLegacyCatalog(PostgresCatalog):
    """Canonical reader with one strict, empty-only retired V001 compatibility mode."""

    _schema_mode: str | None = None

    def verify_schema(self) -> None:
        if self._schema_mode is not None:
            return

        with self.engine.connect() as connection:
            drift = frozenset(describe_schema_drift(connection))
            mode = "canonical" if not drift else "legacy-v001"
            if drift and drift != _V001_CANONICAL_DRIFT:
                raise BootstrapConflict(
                    "legacy source is not the exact retired V001 schema; canonical drift was: "
                    + "; ".join(sorted(drift))
                )

            inspector = inspect(connection)
            for table in BOOTSTRAP_TABLE_ORDER:
                schema, name = table.split(".", 1)
                exists = inspector.has_table(name, schema=schema)
                if mode == "legacy-v001" and table in _V001_MISSING_TABLES:
                    if exists:
                        raise BootstrapConflict(
                            f"legacy source is not the exact retired V001 schema: unexpected {table}"
                        )
                    continue
                if not exists:
                    raise BootstrapConflict(
                        f"legacy source is not the exact retired V001 schema: missing {table}"
                    )

                expected_columns = (
                    _V001_CHANGED_TABLE_COLUMNS[table]
                    if mode == "legacy-v001" and table in _V001_CHANGED_TABLE_COLUMNS
                    else _canonical_columns(table)
                )
                actual_columns = frozenset(
                    column["name"] for column in inspector.get_columns(name, schema=schema)
                )
                if actual_columns != expected_columns:
                    raise BootstrapConflict(
                        f"legacy source is not the exact retired V001 schema: {table} columns differ"
                    )

                expected_uniques = (
                    _V001_CHANGED_TABLE_UNIQUES[table]
                    if mode == "legacy-v001" and table in _V001_CHANGED_TABLE_UNIQUES
                    else _canonical_uniques(table)
                )
                if _actual_uniques(connection, table) != expected_uniques:
                    raise BootstrapConflict(
                        f"legacy source is not the exact retired V001 schema: {table} uniqueness differs"
                    )

            if mode == "legacy-v001":
                for table in _V001_CHANGED_TABLE_COLUMNS:
                    target = table_for(table)
                    count = connection.execute(
                        select(func.count()).select_from(
                            # The canonical Table object names the same physical table;
                            # selecting only COUNT(*) never references drifted columns.
                            target
                        )
                    ).scalar_one()
                    if count:
                        raise BootstrapConflict(
                            f"{table} contains {count} legacy row(s); immutable V001 rows "
                            "cannot be translated without changing meaning"
                        )

        self._schema_mode = mode

    def records(
        self,
        table: str,
        *,
        where: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._schema_mode is None:
            self.verify_schema()
        if self._schema_mode == "legacy-v001" and (
            table in _V001_CHANGED_TABLE_COLUMNS or table in _V001_MISSING_TABLES
        ):
            if where:
                return []
            return []
        return super().records(table, where=where)


def connect_read_only_catalog(database_url: str, *, artifact_root: Path) -> PostgresCatalog:
    """Open PostgreSQL with both client-side and server-side write denial."""

    engine = create_market_data_engine(
        database_url,
        writable_schemas=("__read_only__",),
        application_name="idea2strategy-legacy-catalog-read-only",
        connect_args={"options": "-c default_transaction_read_only=on"},
    )
    return _ImmutableLegacyCatalog(
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
