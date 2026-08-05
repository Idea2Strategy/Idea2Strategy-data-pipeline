"""Read-only legacy catalog audit and retry-safe canonical DB bootstrap.

The legacy RDS instance remains an immutable source.  A bootstrap accepts only an
empty target or an exact subset of the source, verifies every version-pinned S3
receipt, and inserts only missing rows in one transaction.  An exact rerun is a
no-op; any divergent target row fails closed instead of being overwritten.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import UniqueConstraint, inspect, text
from sqlalchemy.engine import Connection, make_url

from .catalog import MarketDataCatalog, PostgresCatalog, StorageObjectsPolicy, canonical_filter
from .db.codec import normalise_record, table_for
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


_V001_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "market_data.providers": frozenset(
        {"id", "code", "name", "rights_version", "status", "created_at"}
    ),
    "market_data.feeds": frozenset(
        {
            "id",
            "provider_id",
            "code",
            "data_kind",
            "resolution",
            "session_scope",
            "status",
            "created_at",
        }
    ),
    "market_data.instruments": frozenset(
        {
            "id",
            "asset_type",
            "primary_exchange_mic",
            "currency",
            "support_status",
            "listed_from",
            "listed_to",
            "created_at",
        }
    ),
    "market_data.instrument_symbols": frozenset(
        {
            "id",
            "instrument_id",
            "symbol",
            "exchange_mic",
            "effective_from",
            "effective_to",
            "created_at",
        }
    ),
    "market_data.trading_sessions": frozenset(
        {
            "id",
            "exchange_mic",
            "session_date",
            "opens_at",
            "closes_at",
            "session_type",
            "calendar_version",
            "created_at",
        }
    ),
    "market_data.pipeline_runs": frozenset(
        {
            "id",
            "pipeline_type",
            "processing_version",
            "status",
            "idempotency_key",
            "requested_at",
            "started_at",
            "completed_at",
            "input_config",
            "summary_result",
            "failure_code",
        }
    ),
    "storage.objects": frozenset(
        {
            "id",
            "storage_class",
            "bucket_code",
            "object_key",
            "provider_version_id",
            "content_sha256",
            "byte_size",
            "media_type",
            "format_version",
            "encryption_profile",
            "created_at",
            "verified_at",
        }
    ),
    "market_data.dataset_manifests": frozenset(
        {
            "id",
            "feed_id",
            "instrument_id",
            "data_layer",
            "resolution",
            "period_start",
            "period_end",
            "revision_number",
            "as_of_at",
            "processing_version",
            "quality_status",
            "status",
            "row_count",
            "manifest_hash",
            "created_at",
            "supersedes_manifest_id",
        }
    ),
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
        "market_data.stream_watermarks",
        "market_data.corporate_actions",
        "market_data.feature_definitions",
        "market_data.feature_materializations",
        "market_data.feature_snapshot_batches",
    }
)

_V001_PIPELINE_PARTITION_COLUMNS = frozenset(
    {
        "id",
        "pipeline_run_id",
        "partition_key",
        "status",
        "result_manifest_id",
        "error_code",
        "error_summary",
        "created_at",
        "updated_at",
    }
)

# PR #34 supported an empty intermediate V001-shaped schema.  Keep that exact
# compatibility path separate from the original market-loader V001 schema below.
_EMPTY_V001_CANONICAL_DRIFT = frozenset(
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

_V001_TABLE_UNIQUES: dict[str, frozenset[frozenset[str]]] = {
    "market_data.providers": frozenset({frozenset({"id"}), frozenset({"code"})}),
    "market_data.feeds": frozenset({frozenset({"id"}), frozenset({"code"})}),
    "market_data.instruments": frozenset({frozenset({"id"})}),
    "market_data.instrument_symbols": frozenset({frozenset({"id"})}),
    "market_data.trading_sessions": frozenset(
        {frozenset({"id"}), frozenset({"exchange_mic", "session_date"})}
    ),
    "market_data.pipeline_runs": frozenset(
        {frozenset({"id"}), frozenset({"idempotency_key"})}
    ),
    "storage.objects": frozenset(
        {
            frozenset({"id"}),
            frozenset({"bucket_code", "object_key", "provider_version_id"}),
        }
    ),
    "market_data.dataset_manifests": frozenset(
        {
            frozenset({"id"}),
            frozenset({"feed_id", "period_start", "period_end", "revision_number"}),
            frozenset({"feed_id", "period_start", "period_end"}),
        }
    ),
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

_V001_PIPELINE_PARTITION_UNIQUES = frozenset(
    {
        frozenset({"id"}),
        frozenset({"pipeline_run_id", "partition_key"}),
    }
)

_EMPTY_V001_CHANGED_TABLES = frozenset(
    {
        "market_data.dataset_objects",
        "market_data.dataset_lineage",
        "market_data.quality_incidents",
    }
)
_EMPTY_V001_MISSING_TABLES = frozenset(
    {
        "market_data.dataset_object_lineage",
        "market_data.corporate_actions",
        "market_data.feature_materializations",
    }
)

_LEGACY_PARTITION = re.compile(
    r"^adjustment=(raw|all)/resolution=(30m|1h|4h|1d)/year=(\d{4})/shard=(\d{2})$"
)
_LEGACY_OBJECT_KEY = re.compile(
    r"(?:^|/)adjustment=(raw|all)/.*?/resolution=(30m|1h|4h|1d)/"
    r"revision=(\d{8})/year=(\d{4})/shard=(\d{2})-of-(\d{2})/"
    r"manifest_id=([0-9a-fA-F-]{36})/part-(\d{5})\.parquet$"
)

# SHA-256 of the canonical JSON representation of all 110 columns, 62 constraints,
# and both enum definitions observed read-only from the deployed legacy RDS.  The
# representation is reproduced by `_legacy_schema_metadata` below.  This makes a
# same-named column with changed type/nullability/default/check/FK fail closed too.
_POPULATED_V001_SCHEMA_FINGERPRINT = (
    "efc1451bc381b00778b48048651a506577c6a85d4f4d7335c0166ee8cb88d424"
)


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


def _legacy_schema_metadata(connection: Connection) -> dict[str, Any]:
    statement = text(
        """
        SELECT jsonb_build_object(
          'columns', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
              'schema', table_schema, 'table', table_name,
              'ordinal', ordinal_position, 'column', column_name,
              'type', data_type, 'udt', udt_schema || '.' || udt_name,
              'length', character_maximum_length, 'nullable', is_nullable,
              'default', column_default
            ) ORDER BY table_schema, table_name, ordinal_position)
            FROM information_schema.columns
            WHERE table_schema IN ('market_data', 'storage')
          ), '[]'::jsonb),
          'constraints', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
              'schema', n.nspname, 'table', c.relname, 'name', con.conname,
              'type', con.contype, 'definition',
              replace(replace(replace(
                pg_get_constraintdef(con.oid, true),
                'market_data.', ''), 'storage.', ''), 'operations.', '')
            ) ORDER BY n.nspname, c.relname, con.conname)
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('market_data', 'storage')
          ), '[]'::jsonb),
          'enums', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
              'schema', n.nspname, 'name', t.typname,
              'label', e.enumlabel, 'order', e.enumsortorder
            ) ORDER BY n.nspname, t.typname, e.enumsortorder)
            FROM pg_type t
            JOIN pg_enum e ON e.enumtypid = t.oid
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname IN ('market_data', 'storage', 'operations')
          ), '[]'::jsonb)
        )
        """
    )
    value = connection.execute(statement).scalar_one()
    _require(isinstance(value, dict), "legacy schema metadata query did not return an object")
    return value


def _legacy_schema_fingerprint(connection: Connection) -> str:
    return _json_hash(_legacy_schema_metadata(connection))


def _utc_start(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _legacy_shard_number(instrument_id: Any, shard_count: int) -> int:
    digest = hashlib.sha256(str(instrument_id).lower().encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % shard_count


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BootstrapConflict(message)


def _canonical_row(table: str, values: Mapping[str, Any]) -> dict[str, Any]:
    row = normalise_record(table, values)
    expected = {column.name for column in table_for(table).columns}
    _require(
        set(row) == expected,
        f"legacy {table} translation omitted canonical columns {sorted(expected - set(row))}",
    )
    return row


def _ordered(table: str, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    target = table_for(table)
    keys = (["created_at"] if "created_at" in target.c else []) + [
        column.name for column in target.primary_key.columns
    ]
    return sorted(rows, key=lambda row: tuple(str(row.get(key) or "") for key in keys))


def _translate_legacy_v001(
    source: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    bucket_name: str | None,
) -> dict[str, list[dict[str, Any]]]:
    """Translate the immutable market-loader V001 snapshot without source writes."""

    storage_rows = list(source["storage.objects"])
    if storage_rows:
        _require(bool(bucket_name and bucket_name.strip()), "legacy S3 bucket name is required")
    actual_bucket = "" if bucket_name is None else bucket_name.strip()

    manifests = {str(row["id"]): row for row in source["market_data.dataset_manifests"]}
    instruments = {str(row["id"]): row for row in source["market_data.instruments"]}
    feeds = {str(row["id"]): row for row in source["market_data.feeds"]}
    relations = {str(row["id"]): row for row in source["market_data.dataset_objects"]}
    relation_by_object: dict[str, Mapping[str, Any]] = {}
    relations_by_manifest: dict[str, list[Mapping[str, Any]]] = {}
    for relation in relations.values():
        object_id = str(relation["object_id"])
        _require(object_id not in relation_by_object, f"legacy object {object_id} has multiple relations")
        relation_by_object[object_id] = relation
        relations_by_manifest.setdefault(str(relation["dataset_manifest_id"]), []).append(relation)

    storage_by_id = {str(row["id"]): row for row in storage_rows}
    _require(
        set(storage_by_id) == set(relation_by_object),
        "every legacy storage object must have exactly one dataset object relation",
    )

    output: dict[str, list[dict[str, Any]]] = {table: [] for table in BOOTSTRAP_TABLE_ORDER}

    for row in source["market_data.providers"]:
        _require(len(str(row["rights_version"])) <= 80, "legacy provider rights_version is too long")
        output["market_data.providers"].append(
            _canonical_row(
                "market_data.providers",
                {
                    "id": row["id"],
                    "code": row["code"],
                    "display_name": row["name"],
                    "rights_version": row["rights_version"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                },
            )
        )

    for row in source["market_data.feeds"]:
        _require(str(row["data_kind"]) == "BAR", f"legacy feed {row['id']} is not BAR")
        _require(str(row["session_scope"]) == "REGULAR", f"legacy feed {row['id']} is not REGULAR")
        _require(str(row["status"]) == "ACTIVE", f"legacy feed {row['id']} is not ACTIVE")
        output["market_data.feeds"].append(
            _canonical_row(
                "market_data.feeds",
                {
                    "id": row["id"],
                    "provider_id": row["provider_id"],
                    "code": row["code"],
                    "data_kind": "BARS",
                    "resolution": row["resolution"],
                    "timezone_name": "America/New_York",
                    "feed_version": "legacy-market-loader-v1",
                    "created_at": row["created_at"],
                    "retired_at": None,
                },
            )
        )

    for row in source["market_data.instruments"]:
        _require(str(row["currency"]).strip() == "USD", f"legacy instrument {row['id']} is not USD")
        output["market_data.instruments"].append(
            _canonical_row(
                "market_data.instruments",
                {
                    "id": row["id"],
                    "asset_type": row["asset_type"],
                    "primary_exchange_mic": str(row["primary_exchange_mic"]).strip(),
                    "currency_code": "USD",
                    "provider_reference": None,
                    "listed_at": row["listed_from"],
                    "delisted_at": row["listed_to"],
                    "created_at": row["created_at"],
                },
            )
        )

    for row in source["market_data.instrument_symbols"]:
        output["market_data.instrument_symbols"].append(
            _canonical_row(
                "market_data.instrument_symbols",
                {
                    "id": row["id"],
                    "instrument_id": row["instrument_id"],
                    "exchange_mic": str(row["exchange_mic"]).strip(),
                    "symbol": row["symbol"],
                    "effective_from": _utc_start(row["effective_from"]),
                    "effective_to": (
                        None if row["effective_to"] is None else _utc_start(row["effective_to"])
                    ),
                },
            )
        )

    for row in source["market_data.trading_sessions"]:
        output["market_data.trading_sessions"].append(
            _canonical_row(
                "market_data.trading_sessions",
                {
                    "id": row["id"],
                    "exchange_mic": str(row["exchange_mic"]).strip(),
                    "session_date": row["session_date"],
                    "opens_at": row["opens_at"],
                    "closes_at": row["closes_at"],
                    "session_type": row["session_type"],
                    "calendar_version": row["calendar_version"],
                },
            )
        )

    for row in source["market_data.pipeline_runs"]:
        _require(
            str(row["pipeline_type"]) == "HISTORICAL_BACKFILL",
            f"legacy pipeline run {row['id']} has an unsupported type",
        )
        _require(len(str(row["processing_version"])) <= 40, "legacy processing_version is too long")
        output["market_data.pipeline_runs"].append(
            _canonical_row(
                "market_data.pipeline_runs",
                {
                    "id": row["id"],
                    "pipeline_code": row["pipeline_type"],
                    "pipeline_version": row["processing_version"],
                    "idempotency_key": str(row["idempotency_key"]).strip(),
                    "status": row["status"],
                    "input_hash": _json_hash(row["input_config"]),
                    "output_hash": (
                        None if row["summary_result"] is None else _json_hash(row["summary_result"])
                    ),
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "failure_code": row["failure_code"],
                },
            )
        )

    for row in source["market_data.dataset_manifests"]:
        manifest_id = str(row["id"])
        linked = relations_by_manifest.get(manifest_id, [])
        formats = {str(storage_by_id[str(item["object_id"])]["format_version"]) for item in linked}
        _require(len(formats) == 1, f"legacy manifest {manifest_id} has no single schema version")
        _require(bool(str(row["manifest_hash"] or "").strip()), f"legacy manifest {manifest_id} has no hash")
        output["market_data.dataset_manifests"].append(
            _canonical_row(
                "market_data.dataset_manifests",
                {
                    "id": row["id"],
                    "feed_id": row["feed_id"],
                    "instrument_id": row["instrument_id"],
                    "data_layer": row["data_layer"],
                    "resolution": row["resolution"],
                    "revision_number": row["revision_number"],
                    "status": row["status"],
                    "period_start": _utc_start(row["period_start"]),
                    "period_end": _utc_start(row["period_end"]),
                    "schema_version": next(iter(formats)),
                    "dataset_hash": str(row["manifest_hash"]).strip(),
                    "supersedes_manifest_id": row["supersedes_manifest_id"],
                    "created_at": row["created_at"],
                    "available_at": (
                        row["as_of_at"]
                        if str(row["status"]) in {"AVAILABLE", "SUPERSEDED"}
                        else None
                    ),
                },
            )
        )

    for row in storage_rows:
        object_id = str(row["id"])
        relation = relation_by_object[object_id]
        manifest = manifests[str(relation["dataset_manifest_id"])]
        _require(str(row["storage_class"]) == "S3_STANDARD", f"legacy object {object_id} storage class differs")
        _require(str(row["bucket_code"]) == "DEVELOPMENT_MARKET_DATA", f"legacy object {object_id} bucket code differs")
        _require(str(row["encryption_profile"]) == "SSE-S3-AES256", f"legacy object {object_id} encryption differs")
        output["storage.objects"].append(
            _canonical_row(
                "storage.objects",
                {
                    "id": row["id"],
                    "status": "AVAILABLE",
                    "storage_provider": "S3",
                    "bucket_name": actual_bucket,
                    "object_key": row["object_key"],
                    "provider_version_id": row["provider_version_id"],
                    "content_hash": str(row["content_sha256"]).strip(),
                    "byte_size": row["byte_size"],
                    "file_format": "PARQUET",
                    "compression_codec": "ZSTD",
                    "media_type": row["media_type"],
                    "schema_version": row["format_version"],
                    "row_count": relation["row_count"],
                    "period_start": _utc_start(manifest["period_start"]),
                    "period_end": _utc_start(manifest["period_end"]),
                    "encryption_key_ref": None,
                    "retention_policy_version": "UNSPECIFIED",
                    "retention_until": None,
                    "legal_hold": False,
                    "created_at": row["created_at"],
                    "verified_at": row["verified_at"],
                    "quarantined_at": None,
                    "superseded_at": None,
                    "deleted_at": None,
                },
            )
        )

    for row in source["market_data.dataset_objects"]:
        relation_id = str(row["id"])
        manifest = manifests[str(row["dataset_manifest_id"])]
        feed = feeds[str(manifest["feed_id"])]
        storage = storage_by_id[str(row["object_id"])]
        partition = _LEGACY_PARTITION.fullmatch(str(row["partition_key"]))
        object_key = _LEGACY_OBJECT_KEY.search(str(storage["object_key"]))
        _require(partition is not None, f"legacy relation {relation_id} partition key is unknown")
        _require(object_key is not None, f"legacy relation {relation_id} object key is unknown")
        assert partition is not None and object_key is not None
        adjustment, resolution, year_text, shard_text = partition.groups()
        (
            key_adjustment,
            key_resolution,
            revision,
            key_year,
            key_shard,
            shard_count,
            key_manifest,
            part,
        ) = object_key.groups()
        _require(adjustment == key_adjustment, f"legacy relation {relation_id} adjustment differs")
        _require(
            resolution == key_resolution == str(manifest["resolution"]),
            f"legacy relation {relation_id} resolution differs",
        )
        _require(
            year_text == key_year == str(manifest["period_start"].year),
            f"legacy relation {relation_id} year differs",
        )
        _require(shard_text == key_shard, f"legacy relation {relation_id} shard differs")
        _require(int(revision) == int(manifest["revision_number"]), f"legacy relation {relation_id} revision differs")
        _require(key_manifest.lower() == str(manifest["id"]).lower(), f"legacy relation {relation_id} manifest differs")
        _require(adjustment.upper() in str(feed["code"]), f"legacy relation {relation_id} feed differs")
        shard_number = int(shard_text)
        total_shards = int(shard_count)
        candidates = sorted(
            str(item["id"])
            for item in instruments.values()
            if str(item["support_status"]) == "ACTIVE"
            and item["listed_from"] < manifest["period_end"]
            and (item["listed_to"] is None or item["listed_to"] >= manifest["period_start"])
            and _legacy_shard_number(item["id"], total_shards) == shard_number
        )
        output["market_data.dataset_objects"].append(
            _canonical_row(
                "market_data.dataset_objects",
                {
                    "id": row["id"],
                    "dataset_manifest_id": row["dataset_manifest_id"],
                    "object_id": row["object_id"],
                    "object_kind": "MARKET_BARS",
                    "partition_granularity": "YEAR",
                    "partition_start": manifest["period_start"],
                    "partition_end": manifest["period_end"],
                    "period_start": _utc_start(manifest["period_start"]),
                    "period_end": _utc_start(manifest["period_end"]),
                    "shard_key": f"s{shard_number:02d}-of-{total_shards}",
                    "part_number": int(part),
                    "row_count": row["row_count"],
                    "min_instrument_id": candidates[0] if candidates else None,
                    "max_instrument_id": candidates[-1] if candidates else None,
                },
            )
        )

    for row in source["market_data.dataset_lineage"]:
        output["market_data.dataset_lineage"].append(
            _canonical_row(
                "market_data.dataset_lineage",
                {
                    "derived_manifest_id": row["dataset_manifest_id"],
                    "source_manifest_id": row["source_manifest_id"],
                    "relation_type": row["relationship_type"],
                },
            )
        )

    for row in source["market_data.quality_incidents"]:
        output["market_data.quality_incidents"].append(
            _canonical_row(
                "market_data.quality_incidents",
                {
                    "id": row["id"],
                    "dataset_manifest_id": row["dataset_manifest_id"],
                    "instrument_id": row["instrument_id"],
                    "severity": row["severity"],
                    "incident_code": row["incident_type"],
                    "period_start": row["period_start"] or row["detected_at"],
                    "period_end": row["period_end"],
                    "status": row["status"],
                    "evidence_object_id": None,
                    "detected_at": row["detected_at"],
                    "resolved_at": row["resolved_at"],
                },
            )
        )

    run_ids = {str(row["id"]) for row in source["market_data.pipeline_runs"]}
    manifest_ids = set(manifests)
    for row in source["market_data.pipeline_partitions"]:
        _require(str(row["pipeline_run_id"]) in run_ids, "legacy pipeline partition has no run")
        if row["result_manifest_id"] is not None:
            _require(str(row["result_manifest_id"]) in manifest_ids, "legacy pipeline partition has no manifest")

    for table in BOOTSTRAP_TABLE_ORDER:
        output[table] = _ordered(table, output[table])
    return output


class _ImmutableLegacyCatalog(PostgresCatalog):
    """Canonical reader for canonical, empty-intermediate, or populated V001 data."""

    def __init__(self, *args: Any, legacy_bucket_name: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._legacy_bucket_name = legacy_bucket_name
        self._schema_mode: str | None = None
        self._legacy_snapshot: dict[str, list[dict[str, Any]]] | None = None

    @property
    def bootstrap_target_extra_tables(self) -> frozenset[str]:
        """Canonical-only tables that cannot exist in a V001 source."""

        if self._schema_mode == "legacy-populated-v001":
            return _V001_MISSING_TABLES
        return frozenset()

    def verify_schema(self) -> None:
        if self._schema_mode is not None:
            return

        with self.engine.connect() as connection:
            drift = frozenset(describe_schema_drift(connection))
            if not drift:
                mode = "canonical"
            elif drift == _EMPTY_V001_CANONICAL_DRIFT:
                mode = "legacy-empty-v001"
                self._verify_empty_v001(connection)
            else:
                mode = "legacy-populated-v001"
                fingerprint = _legacy_schema_fingerprint(connection)
                if fingerprint != _POPULATED_V001_SCHEMA_FINGERPRINT:
                    raise BootstrapConflict(
                        "legacy source is not the exact populated market-loader V001 schema; "
                        f"schema fingerprint was {fingerprint}"
                    )
                raw: dict[str, list[Mapping[str, Any]]] = {}
                for table in _V001_TABLE_COLUMNS:
                    raw[table] = self._raw_rows(connection, table)
                raw["market_data.pipeline_partitions"] = self._raw_rows(
                    connection, "market_data.pipeline_partitions"
                )
                self._legacy_snapshot = _translate_legacy_v001(
                    raw,
                    bucket_name=self._legacy_bucket_name,
                )

        self._schema_mode = mode

    @staticmethod
    def _raw_rows(connection: Connection, table: str) -> list[Mapping[str, Any]]:
        schema, name = table.split(".", 1)
        statement = text(f'SELECT * FROM "{schema}"."{name}"')
        return list(connection.execute(statement).mappings().all())

    @staticmethod
    def _verify_empty_v001(connection: Connection) -> None:
        inspector = inspect(connection)
        for table in BOOTSTRAP_TABLE_ORDER:
            schema, name = table.split(".", 1)
            exists = inspector.has_table(name, schema=schema)
            if table in _EMPTY_V001_MISSING_TABLES:
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
                _V001_TABLE_COLUMNS[table]
                if table in _EMPTY_V001_CHANGED_TABLES
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
                _V001_TABLE_UNIQUES[table]
                if table in _EMPTY_V001_CHANGED_TABLES
                else _canonical_uniques(table)
            )
            if _actual_uniques(connection, table) != expected_uniques:
                raise BootstrapConflict(
                    f"legacy source is not the exact retired V001 schema: {table} uniqueness differs"
                )
        for table in _EMPTY_V001_CHANGED_TABLES:
            schema, name = table.split(".", 1)
            count = connection.execute(
                text(f'SELECT count(*) FROM "{schema}"."{name}"')
            ).scalar_one()
            if count:
                raise BootstrapConflict(
                    f"{table} contains {count} legacy row(s); the empty V001 adapter "
                    "cannot translate them"
                )

    def records(
        self,
        table: str,
        *,
        where: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self._schema_mode is None:
            self.verify_schema()
        if self._schema_mode == "legacy-empty-v001" and (
            table in _EMPTY_V001_CHANGED_TABLES or table in _EMPTY_V001_MISSING_TABLES
        ):
            canonical_filter(table, where or {})
            return []
        if self._schema_mode == "legacy-populated-v001":
            if self._legacy_snapshot is None:
                raise RuntimeError("legacy snapshot was not loaded")
            criteria = canonical_filter(table, where or {})
            return [
                dict(row)
                for row in self._legacy_snapshot[table]
                if all(row[name] == value for name, value in criteria.items())
            ]
        return super().records(table, where=where)


def connect_read_only_catalog(
    database_url: str,
    *,
    artifact_root: Path,
    legacy_bucket_name: str | None = None,
) -> PostgresCatalog:
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
        legacy_bucket_name=legacy_bucket_name,
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
    *,
    allow_target_extras: frozenset[str] = frozenset(),
) -> dict[str, list[dict[str, Any]]]:
    missing: dict[str, list[dict[str, Any]]] = {}
    for table in BOOTSTRAP_TABLE_ORDER:
        source_by_key = {_primary_key(table, row): row for row in source[table]}
        target_by_key = {_primary_key(table, row): row for row in target[table]}
        for key, target_row in target_by_key.items():
            source_row = source_by_key.get(key)
            if source_row is None:
                if table in allow_target_extras:
                    continue
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
    allowed_target_extras = getattr(
        source,
        "bootstrap_target_extra_tables",
        frozenset(),
    )
    missing = _missing_rows(
        source_snapshot,
        target_snapshot,
        allow_target_extras=allowed_target_extras,
    )
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
        remaining = _missing_rows(
            source_snapshot,
            applied_snapshot,
            allow_target_extras=allowed_target_extras,
        )
        if any(remaining.values()):
            raise BootstrapConflict("target snapshot is incomplete after insert; rolling back bootstrap")
    return {**report, "status": "APPLIED", "inserted_row_count": inserted_count}
