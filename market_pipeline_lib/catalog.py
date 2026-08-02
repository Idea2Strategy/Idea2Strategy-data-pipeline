"""The market-data catalog boundary, in a local and a PostgreSQL implementation.

Both implementations satisfy `MarketDataCatalog` and are behaviourally interchangeable;
`tests/test_catalog_contract.py` runs one suite against both to keep them that way.
That interchangeability is what lets `engine.py` drop its
``isinstance(self.catalog, LocalCatalog)`` gates: those gates exist only because
`PostgresCatalog.records()` used to raise `NotImplementedError`.

Four things are deliberately explicit rather than defaulted:

`transaction()`
    A whole ``publish_dataset`` -- manifest BUILDING, N objects, incidents, manifest
    AVAILABLE, previous SUPERSEDED -- commits as one unit or not at all.  Outside a
    transaction each call is its own unit of work, which is fine for reads and for the
    one-row reference upserts, and is *not* enough for a publish.

`StorageObjectsPolicy`
    `DatabaseAccessPolicy.java:36` registers the `storage` schema as SHARED while the
    implementation checklist calls it D-owned.  `PostgresCatalog` therefore refuses to
    guess: the caller states which side of that unresolved contradiction it is acting
    on, and the choice narrows the connection's writable schema set.

`CatalogCapability`
    `market_data.pipeline_run_outputs` does not exist in `db/schema.dbml`.  The local
    catalog keeps run-to-output provenance in an explicitly non-canonical sidecar; the
    PostgreSQL catalog refuses, loudly, instead of writing a sidecar next to a database
    and pretending the provenance was persisted.

`records(where=...)`
    Equality-only, both sides.  `PostgresCatalog` pushes it into SQL; `LocalCatalog`
    applies the same canonicalised predicate in Python.  Anything richer would have to
    be written twice, and the second implementation is where the two drift apart.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import Connection, Engine, and_, delete, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db.codec import canonical_columns, from_db_row, normalise_record, table_for, to_db_params
from .db.engine import create_market_data_engine
from .db.errors import (
    CanonicalTableMissing,
    DuplicateAvailableManifest,
    PipelineRunNotFound,
    StorageOwnershipUnresolved,
    UnknownCatalogTable,
)
from .db.schema_guard import verify_schema
from .db.tables import MARKET_DATA_SCHEMA, STORAGE_SCHEMA, TABLES_BY_NAME
from .db.tables import dataset_manifests as manifests_table
from .db.tables import dataset_objects as dataset_objects_table
from .db.tables import pipeline_runs as pipeline_runs_table
from .db.tables import storage_objects as storage_objects_table


__all__ = [
    "ID_TABLES",
    "LOCAL_TABLE_FILES",
    "TABLE_FILES",
    "NATURAL_KEYS",
    "CatalogCapability",
    "LocalCatalog",
    "MarketDataCatalog",
    "PostgresCatalog",
    "StorageObjectsPolicy",
    "canonical_filter",
]


#: The tables `operations.export_db_plan` exports, in load order.  Kept as the historical
#: nine so an export stays a load plan for exactly the tables this pipeline populates.
TABLE_FILES = {
    "market_data.providers": "providers.jsonl",
    "market_data.feeds": "feeds.jsonl",
    "market_data.pipeline_runs": "pipeline-runs.jsonl",
    "market_data.dataset_manifests": "dataset-manifests.jsonl",
    "storage.objects": "storage-objects.jsonl",
    "market_data.dataset_objects": "dataset-objects.jsonl",
    "market_data.dataset_lineage": "dataset-lineage.jsonl",
    "market_data.dataset_object_lineage": "dataset-object-lineage.jsonl",
    "market_data.quality_incidents": "quality-incidents.jsonl",
}

#: Every canonical table `LocalCatalog` can hold, so the local and PostgreSQL catalogs
#: accept the same table names.  The extra files are only created when written to.
LOCAL_TABLE_FILES = {
    **TABLE_FILES,
    **{
        name: name.split(".", 1)[1].replace("_", "-") + ".jsonl"
        for name in sorted(TABLES_BY_NAME)
        if name not in TABLE_FILES
    },
}

#: Tables whose primary key is the single column `id`, derived from the canonical
#: metadata rather than restated, so it cannot drift from `db/tables.py`.
ID_TABLES = frozenset(
    name for name, table in TABLES_BY_NAME.items() if [column.name for column in table.primary_key.columns] == ["id"]
)

#: The non-canonical sidecar described in `CatalogCapability.PIPELINE_RUN_OUTPUTS`.
PIPELINE_OUTPUT_SIDECAR = "pipeline-run-outputs.local.jsonl"

#: Tables whose natural key is a unique index rather than the surrogate `id`, so an
#: upsert on `id` alone would not deduplicate.
#:
#: `market_data.corporate_actions` is keyed centrally by
#: ``uq_corporate_actions_source_manifest_event (source_manifest_id, provider_event_key)``.
#: Read-then-write deduplication is racy: two researchers can both read "absent" and
#: both insert, and only the unique index stops them -- with an unhandled
#: `IntegrityError` rather than a merge.  Conflict-targeting the natural key makes the
#: database do it, so concurrent writers converge instead of colliding.
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "market_data.corporate_actions": ("source_manifest_id", "provider_event_key"),
}

_PIPELINE_RUN_OUTPUTS_GAP = (
    "market_data.pipeline_run_outputs is absent from db/schema.dbml, so there is no "
    "canonical carrier for run-to-output provenance. LocalCatalog keeps it in "
    f"{PIPELINE_OUTPUT_SIDECAR}, which is explicitly not canonical state; PostgresCatalog "
    "refuses rather than writing a file beside a database. Closing this needs a central "
    "DBML change plus a Flyway migration owned by backend/db-migration."
)


class CatalogCapability(StrEnum):
    """Facts a catalog may or may not be able to record.

    A capability exists only where the *canonical model* differs, never where one
    implementation is merely less finished than the other.
    """

    PIPELINE_RUN_OUTPUTS = "PIPELINE_RUN_OUTPUTS"


class StorageObjectsPolicy(StrEnum):
    """Which side of the `storage` schema ownership contradiction the caller takes."""

    #: Read `storage.objects`; refuse to write it.  Matches `DatabaseAccessPolicy`.
    READ_ONLY = "READ_ONLY"
    #: Write it, because `dataset_objects.object_id` is a NOT NULL foreign key to it and
    #: the pipeline cannot publish without it.  Matches the implementation checklist.
    WRITE_PENDING_OWNERSHIP_DECISION = "WRITE_PENDING_OWNERSHIP_DECISION"


@runtime_checkable
class MarketDataCatalog(Protocol):
    """Everything `engine.MarketPipelineEngine` needs from a catalog."""

    @property
    def artifact_root(self) -> Path:
        """Where operator artifacts (run summary, validation report) are written.

        No `market_data` table holds them, so both implementations need somewhere on
        disk; naming it here keeps `operations` from reaching for `LocalCatalog.root`.
        """
        ...

    def transaction(self) -> Any: ...

    def supports(self, capability: CatalogCapability) -> bool: ...

    def unsupported_reason(self, capability: CatalogCapability) -> str | None: ...

    def records(self, table: str, *, where: Mapping[str, Any] | None = None) -> list[dict[str, Any]]: ...

    def upsert(self, table: str, record: Mapping[str, Any]) -> None: ...

    def append_unique(self, table: str, record: Mapping[str, Any], key_fields: tuple[str, ...]) -> None: ...

    def begin_pipeline_run(self, record: Mapping[str, Any]) -> None: ...

    def pipeline_run(self, pipeline_run_id: str) -> dict[str, Any] | None: ...

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None: ...

    def stage_object(
        self,
        storage_record: Mapping[str, Any],
        dataset_object_record: Mapping[str, Any],
    ) -> None: ...

    def publish_manifest(self, record: Mapping[str, Any]) -> None: ...

    def record_dataset_lineage(self, record: Mapping[str, Any]) -> None: ...

    def record_object_lineage(self, record: Mapping[str, Any]) -> None: ...

    def record_quality_incident(self, record: Mapping[str, Any]) -> None: ...

    def record_pipeline_output(
        self,
        *,
        pipeline_run_id: str,
        dataset_manifest_id: str,
        dataset_object_id: str,
    ) -> None: ...

    def pipeline_outputs(self) -> list[dict[str, Any]]: ...

    def latest_available_manifest(
        self,
        *,
        feed_id: str,
        data_layer: str,
        resolution: str,
        year: int,
    ) -> dict[str, Any] | None: ...

    def objects_for_manifest(self, manifest_id: str) -> list[dict[str, Any]]: ...

    def write_summary(self, payload: Mapping[str, Any]) -> Path: ...


#: The grouping the DBML's uniqueness intent applies to, and the grouping
#: `latest_available_manifest` selects within.
_AvailabilityKey = tuple[str, str | None, str, str, int]


def _availability_key(record: Mapping[str, Any]) -> _AvailabilityKey:
    return (
        str(record["feed_id"]),
        None if record.get("instrument_id") is None else str(record["instrument_id"]),
        str(record["data_layer"]),
        str(record["resolution"]),
        int(str(record["period_start"])[:4]),
    )


def canonical_filter(table: str, where: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalise a `records(where=...)` predicate.

    Equality only, ANDed, on canonical columns, with `None` meaning SQL ``IS NULL``.
    Both catalogs run every predicate through this, so an unknown column or an
    ambiguous timestamp is rejected identically -- and `"2026-01-05T14:30:00+00:00"`
    selects the row stored as `"2026-01-05T14:30:00Z"` on either implementation.

    Deliberately not a general query language.  Anything richer would have to be
    reimplemented twice, and the second implementation is where they drift apart.
    """

    if not where:
        return {}
    return normalise_record(table, where)


def _atomic_write(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(payload, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class LocalCatalog:
    """Atomic JSONL catalog whose stored fields are the canonical DBML columns.

    Records go through `db.codec.normalise_record`, so a column the canonical schema
    does not define, or a timestamp without a timezone, fails here rather than at the
    moment the pipeline is finally pointed at PostgreSQL.
    """

    def __init__(self, root: Path, *, create: bool = True) -> None:
        self.root = root.expanduser().resolve()
        if create:
            self.root.mkdir(parents=True, exist_ok=True)
        self._snapshot: dict[Path, bytes | None] | None = None
        self._touched: set[_AvailabilityKey] = set()

    @property
    def artifact_root(self) -> Path:
        """The catalog directory itself; JSONL rows and artifacts live side by side."""

        return self.root

    # -- unit of work ---------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[LocalCatalog]:
        """Commit every write in the block together, or restore the previous state.

        The local catalog has no transaction log, so this snapshots the bytes of every
        file it may touch and puts them back on failure.  That is genuinely atomic for
        a single-process pipeline, which is the only way `LocalCatalog` is ever used.
        """

        if self._snapshot is not None:
            raise RuntimeError("catalog transactions do not nest")
        self._snapshot = {path: (path.read_bytes() if path.is_file() else None) for path in self._all_paths()}
        self._touched = set()
        try:
            yield self
            self._assert_single_available_manifest()
        except BaseException:
            for path, payload in self._snapshot.items():
                if payload is None:
                    path.unlink(missing_ok=True)
                else:
                    _atomic_write(path, payload.decode("utf-8"))
            raise
        finally:
            self._snapshot = None
            self._touched = set()

    def _all_paths(self) -> list[Path]:
        return [self.root / name for name in LOCAL_TABLE_FILES.values()] + [self.root / PIPELINE_OUTPUT_SIDECAR]

    def _assert_single_available_manifest(self) -> None:
        rows = [row for row in self.records("market_data.dataset_manifests") if row["status"] == "AVAILABLE"]
        counts: dict[_AvailabilityKey, list[str]] = {}
        for row in rows:
            counts.setdefault(_availability_key(row), []).append(str(row["id"]))
        for key in sorted(self._touched):
            ids = counts.get(key, [])
            if len(ids) > 1:
                raise DuplicateAvailableManifest(
                    f"{len(ids)} AVAILABLE manifests for feed={key[0]} instrument={key[1]} "
                    f"layer={key[2]} resolution={key[3]} year={key[4]}: {sorted(ids)}"
                )

    # -- capabilities ---------------------------------------------------------------

    def supports(self, capability: CatalogCapability) -> bool:
        return capability is CatalogCapability.PIPELINE_RUN_OUTPUTS

    def unsupported_reason(self, capability: CatalogCapability) -> str | None:
        return None if self.supports(capability) else _PIPELINE_RUN_OUTPUTS_GAP

    # -- reads ----------------------------------------------------------------------

    def _path(self, table: str) -> Path:
        table_for(table)  # raises UnknownCatalogTable
        return self.root / LOCAL_TABLE_FILES[table]

    def records(self, table: str, *, where: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        path = self._path(table)
        # Validated before the early return, so an unknown column is rejected whether or
        # not this catalog happens to have the file yet -- as PostgreSQL would.
        criteria = canonical_filter(table, where or {})
        if not path.is_file():
            return []
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if table in ID_TABLES:
            latest: dict[str, dict[str, Any]] = {}
            order: list[str] = []
            for row in rows:
                key = str(row["id"])
                if key not in latest:
                    order.append(key)
                latest[key] = row
            rows = [latest[key] for key in order]
        if not criteria:
            return rows
        return [row for row in rows if all(row.get(name) == value for name, value in criteria.items())]

    # -- writes ---------------------------------------------------------------------

    def _write(self, table: str, rows: Iterable[Mapping[str, Any]]) -> None:
        _atomic_write(
            self._path(table),
            "".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        )

    def upsert(self, table: str, record: Mapping[str, Any]) -> None:
        if table not in ID_TABLES:
            raise ValueError(f"upsert는 id table에만 사용합니다: {table}")
        canonical = normalise_record(table, record)
        rows = self.records(table)
        natural_key = NATURAL_KEYS.get(table)
        if natural_key is not None:
            # Same identity rule as `PostgresCatalog.upsert`: merge on the unique index
            # the canonical schema enforces, and keep the stored row's `id` so anything
            # already referencing it stays valid.
            missing = [name for name in natural_key if name not in canonical]
            if missing:
                raise ValueError(f"{table} upsert needs its natural key column(s) {missing}")
            wanted = tuple(canonical[name] for name in natural_key)
            merged = []
            replaced = False
            for row in rows:
                if tuple(row.get(name) for name in natural_key) == wanted:
                    merged.append({**canonical, "id": row["id"]})
                    replaced = True
                else:
                    merged.append(row)
            if not replaced:
                merged.append(canonical)
            self._write(table, merged)
            return
        by_id = {str(row["id"]): row for row in rows}
        by_id[str(canonical["id"])] = canonical
        self._write(table, by_id.values())

    def append_unique(
        self,
        table: str,
        record: Mapping[str, Any],
        key_fields: tuple[str, ...],
    ) -> None:
        canonical = normalise_record(table, record)
        rows = self.records(table)
        key = tuple(canonical.get(field) for field in key_fields)
        if any(tuple(row.get(field) for field in key_fields) == key for row in rows):
            return
        rows.append(canonical)
        self._write(table, rows)

    def begin_pipeline_run(self, record: Mapping[str, Any]) -> None:
        self.upsert("market_data.pipeline_runs", record)

    def pipeline_run(self, pipeline_run_id: str) -> dict[str, Any] | None:
        return next(
            (row for row in self.records("market_data.pipeline_runs") if row["id"] == pipeline_run_id),
            None,
        )

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None:
        existing = self.pipeline_run(pipeline_run_id)
        if existing is None:
            raise PipelineRunNotFound(f"pipeline run이 없습니다: {pipeline_run_id}")
        record = dict(existing)
        record.update(
            {
                "status": status,
                "output_hash": output_hash,
                "completed_at": datetime.now(UTC).isoformat(),
                "failure_code": failure_code,
            }
        )
        self.upsert("market_data.pipeline_runs", record)

    def stage_object(
        self,
        storage_record: Mapping[str, Any],
        dataset_object_record: Mapping[str, Any],
    ) -> None:
        self.upsert("storage.objects", storage_record)
        self.upsert("market_data.dataset_objects", dataset_object_record)

    def publish_manifest(self, record: Mapping[str, Any]) -> None:
        canonical = normalise_record("market_data.dataset_manifests", record)
        if self._snapshot is not None:
            self._touched.add(_availability_key(canonical))
        self.upsert("market_data.dataset_manifests", canonical)

    def record_dataset_lineage(self, record: Mapping[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_lineage",
            record,
            ("derived_manifest_id", "source_manifest_id", "relation_type"),
        )

    def record_object_lineage(self, record: Mapping[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_object_lineage",
            record,
            ("derived_dataset_object_id", "source_dataset_object_id", "relation_type"),
        )

    def record_quality_incident(self, record: Mapping[str, Any]) -> None:
        self.upsert("market_data.quality_incidents", record)

    # -- queries the engine needs ---------------------------------------------------

    def latest_available_manifest(
        self,
        *,
        feed_id: str,
        data_layer: str,
        resolution: str,
        year: int,
    ) -> dict[str, Any] | None:
        matches = [
            row
            for row in self.records("market_data.dataset_manifests")
            if row["feed_id"] == feed_id
            and row["data_layer"] == data_layer
            and row["resolution"] == resolution
            and row["status"] == "AVAILABLE"
            and str(row["period_start"]).startswith(str(year))
        ]
        return max(matches, key=lambda row: row["revision_number"], default=None)

    def objects_for_manifest(self, manifest_id: str) -> list[dict[str, Any]]:
        storage = {row["id"]: row for row in self.records("storage.objects")}
        output = [
            {**relation, "storage": storage[relation["object_id"]]}
            for relation in self.records("market_data.dataset_objects")
            if relation["dataset_manifest_id"] == manifest_id
        ]
        # Ordered by `id`, not by insertion, so the sequence matches
        # `PostgresCatalog.objects_for_manifest`.  `engine.publish_dataset` carries this
        # list forward and recomputes the manifest hash and observed period from it.
        output.sort(key=lambda item: str(item["id"]))
        return output

    # -- artifacts ------------------------------------------------------------------

    def write_summary(self, payload: Mapping[str, Any]) -> Path:
        path = self.root / "summary.json"
        _atomic_write(path, json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n")
        return path

    def record_pipeline_output(
        self,
        *,
        pipeline_run_id: str,
        dataset_manifest_id: str,
        dataset_object_id: str,
    ) -> None:
        """Record run-to-output provenance in the non-canonical sidecar.

        See `CatalogCapability.PIPELINE_RUN_OUTPUTS`: this file is deliberately named
        `.local.jsonl` and is deliberately not part of `TABLE_FILES`, so nothing exports
        it as if it were a canonical table.
        """

        record = {
            "pipeline_run_id": pipeline_run_id,
            "dataset_manifest_id": dataset_manifest_id,
            "dataset_object_id": dataset_object_id,
        }
        rows = self.pipeline_outputs()
        if record in rows:
            return
        rows.append(record)
        _atomic_write(
            self.root / PIPELINE_OUTPUT_SIDECAR,
            "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        )

    def pipeline_outputs(self) -> list[dict[str, Any]]:
        path = self.root / PIPELINE_OUTPUT_SIDECAR
        if not path.is_file():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class PostgresCatalog:
    """The same catalog boundary on SQLAlchemy Core.

    Connection handling, commit and rollback are real: writes outside `transaction()`
    are committed by an implicit one-statement unit of work, and writes inside it commit
    together or not at all.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        artifact_root: Path,
        storage_objects: StorageObjectsPolicy,
        owns_engine: bool = False,
    ) -> None:
        self._engine = engine
        self._artifact_root = artifact_root.expanduser().resolve()
        self._storage_objects = storage_objects
        self._owns_engine = owns_engine
        self._connection: Connection | None = None
        self._touched: set[_AvailabilityKey] = set()

    @classmethod
    def connect(
        cls,
        database_url: str,
        *,
        artifact_root: Path,
        storage_objects: StorageObjectsPolicy,
        **engine_kwargs: Any,
    ) -> PostgresCatalog:
        """Build a guarded engine for `database_url` and wrap it.

        `storage_objects` decides the writable schema set, so the runtime guard and the
        declared ownership position can never disagree.
        """

        writable = [MARKET_DATA_SCHEMA]
        if storage_objects is StorageObjectsPolicy.WRITE_PENDING_OWNERSHIP_DECISION:
            writable.append(STORAGE_SCHEMA)
        engine = create_market_data_engine(database_url, writable_schemas=writable, **engine_kwargs)
        return cls(
            engine,
            artifact_root=artifact_root,
            storage_objects=storage_objects,
            owns_engine=True,
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def artifact_root(self) -> Path:
        return self._artifact_root

    @property
    def storage_objects_policy(self) -> StorageObjectsPolicy:
        return self._storage_objects

    def verify_schema(self) -> None:
        """Raise `SchemaDriftError` unless the live schema matches `db.tables`."""

        with self._engine.connect() as connection:
            verify_schema(connection)

    def close(self) -> None:
        if self._connection is not None:
            raise RuntimeError("cannot close a catalog with an open transaction")
        if self._owns_engine:
            self._engine.dispose()

    # -- unit of work ---------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[PostgresCatalog]:
        """One PostgreSQL transaction spanning every write in the block."""

        if self._connection is not None:
            raise RuntimeError("catalog transactions do not nest")
        self._touched = set()
        with self._engine.begin() as connection:
            self._connection = connection
            try:
                yield self
                self._assert_single_available_manifest(connection)
            finally:
                self._connection = None
                self._touched = set()

    @contextmanager
    def _write_connection(self) -> Iterator[Connection]:
        if self._connection is not None:
            yield self._connection
            return
        with self._engine.begin() as connection:
            yield connection

    @contextmanager
    def _read_connection(self) -> Iterator[Connection]:
        if self._connection is not None:
            yield self._connection
            return
        with self._engine.connect() as connection:
            yield connection

    def _assert_single_available_manifest(self, connection: Connection) -> None:
        for key in sorted(self._touched):
            feed_id, instrument_id, data_layer, resolution, year = key
            criteria = [
                manifests_table.c.feed_id == feed_id,
                manifests_table.c.data_layer == data_layer,
                manifests_table.c.resolution == resolution,
                manifests_table.c.status == "AVAILABLE",
                manifests_table.c.period_start >= datetime(year, 1, 1, tzinfo=UTC),
                manifests_table.c.period_start < datetime(year + 1, 1, 1, tzinfo=UTC),
                manifests_table.c.instrument_id.is_(None)
                if instrument_id is None
                else manifests_table.c.instrument_id == instrument_id,
            ]
            found = connection.execute(select(manifests_table.c.id).where(and_(*criteria))).scalars().all()
            if len(found) > 1:
                raise DuplicateAvailableManifest(
                    f"{len(found)} AVAILABLE manifests for feed={feed_id} "
                    f"instrument={instrument_id} layer={data_layer} resolution={resolution} "
                    f"year={year}: {sorted(str(value) for value in found)}"
                )

    # -- capabilities ---------------------------------------------------------------

    def supports(self, capability: CatalogCapability) -> bool:
        if capability is CatalogCapability.PIPELINE_RUN_OUTPUTS:
            return False
        raise ValueError(f"unknown capability: {capability}")

    def unsupported_reason(self, capability: CatalogCapability) -> str | None:
        return None if self.supports(capability) else _PIPELINE_RUN_OUTPUTS_GAP

    # -- reads ----------------------------------------------------------------------

    def records(self, table: str, *, where: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        target = table_for(table)
        order_by = [target.c.created_at] if "created_at" in target.c else []
        order_by.extend(target.primary_key.columns)
        statement = select(target)
        criteria = canonical_filter(table, where or {})
        if criteria:
            # Pushed into SQL, not filtered in Python: a full-table scan per lookup
            # does not survive a production-sized `corporate_actions`.
            bound = to_db_params(table, criteria)
            statement = statement.where(
                *(
                    target.c[name].is_(None) if value is None else target.c[name] == value
                    for name, value in bound.items()
                )
            )
        with self._read_connection() as connection:
            rows = connection.execute(statement.order_by(*order_by)).mappings().all()
        return [from_db_row(table, row) for row in rows]

    def latest_available_manifest(
        self,
        *,
        feed_id: str,
        data_layer: str,
        resolution: str,
        year: int,
    ) -> dict[str, Any] | None:
        statement = (
            select(manifests_table)
            .where(
                manifests_table.c.feed_id == feed_id,
                manifests_table.c.data_layer == data_layer,
                manifests_table.c.resolution == resolution,
                manifests_table.c.status == "AVAILABLE",
                manifests_table.c.period_start >= datetime(year, 1, 1, tzinfo=UTC),
                manifests_table.c.period_start < datetime(year + 1, 1, 1, tzinfo=UTC),
            )
            .order_by(manifests_table.c.revision_number.desc())
            .limit(1)
        )
        with self._read_connection() as connection:
            row = connection.execute(statement).mappings().first()
        return None if row is None else from_db_row("market_data.dataset_manifests", row)

    def objects_for_manifest(self, manifest_id: str) -> list[dict[str, Any]]:
        relation_columns = list(dataset_objects_table.c)
        storage_columns = list(storage_objects_table.c)
        statement = (
            select(*relation_columns, *storage_columns)
            .select_from(
                dataset_objects_table.join(
                    storage_objects_table,
                    dataset_objects_table.c.object_id == storage_objects_table.c.id,
                )
            )
            .where(dataset_objects_table.c.dataset_manifest_id == manifest_id)
            .order_by(dataset_objects_table.c.id)
        )
        width = len(relation_columns)
        with self._read_connection() as connection:
            rows = connection.execute(statement).all()
        output = []
        for row in rows:
            relation = dict(zip(canonical_columns("market_data.dataset_objects"), row[:width], strict=True))
            storage = dict(zip(canonical_columns("storage.objects"), row[width:], strict=True))
            output.append(
                {
                    **from_db_row("market_data.dataset_objects", relation),
                    "storage": from_db_row("storage.objects", storage),
                }
            )
        return output

    def existing_ids(self, table: str, ids: Iterable[str]) -> set[str]:
        """Which of `ids` exist in `table`. One round trip, no full table scan."""

        target = table_for(table)
        wanted = [to_db_params(table, {"id": value})["id"] for value in ids]
        if not wanted:
            return set()
        statement = select(target.c.id).where(target.c.id.in_(wanted))
        with self._read_connection() as connection:
            return {str(value) for value in connection.execute(statement).scalars()}

    def pipeline_outputs(self) -> list[dict[str, Any]]:
        raise CanonicalTableMissing(f"{MARKET_DATA_SCHEMA}.pipeline_run_outputs", _PIPELINE_RUN_OUTPUTS_GAP)

    # -- writes ---------------------------------------------------------------------

    def _guard_storage_write(self, table: str) -> None:
        if table != "storage.objects":
            return
        if self._storage_objects is StorageObjectsPolicy.WRITE_PENDING_OWNERSHIP_DECISION:
            return
        raise StorageOwnershipUnresolved(
            "this catalog was constructed with StorageObjectsPolicy.READ_ONLY, so it "
            "will not write storage.objects. DatabaseAccessPolicy.java:36 registers the "
            "storage schema as SHARED while the implementation checklist calls it "
            "D-owned; pass StorageObjectsPolicy.WRITE_PENDING_OWNERSHIP_DECISION to act "
            "on the checklist's side until that is settled centrally."
        )

    def upsert(self, table: str, record: Mapping[str, Any]) -> None:
        """Insert or merge one row, conflict-targeting the table's real identity.

        For most tables that is the primary key.  For the tables in `NATURAL_KEYS` the
        surrogate `id` is not the identity the schema enforces, so the conflict target
        is the unique index instead; see that constant for why read-then-write is not
        an acceptable substitute.
        """

        self._guard_storage_write(table)
        target = table_for(table)
        params = to_db_params(table, record)
        primary_key = [column.name for column in target.primary_key.columns]
        missing = [name for name in primary_key if name not in params]
        if missing:
            raise ValueError(f"{table} upsert needs its primary key column(s) {missing}")
        conflict_target = list(NATURAL_KEYS.get(table, tuple(primary_key)))
        absent = [name for name in conflict_target if name not in params]
        if absent:
            raise ValueError(f"{table} upsert needs its natural key column(s) {absent}")
        statement = pg_insert(target).values(**params)
        # `id` is never overwritten on conflict: the stored row keeps the identity it
        # was inserted with, so foreign keys pointing at it stay valid.
        frozen = set(conflict_target) | set(primary_key)
        updates = {name: statement.excluded[name] for name in params if name not in frozen}
        statement = (
            statement.on_conflict_do_update(index_elements=conflict_target, set_=updates)
            if updates
            else statement.on_conflict_do_nothing(index_elements=conflict_target)
        )
        with self._write_connection() as connection:
            connection.execute(statement)

    def append_unique(
        self,
        table: str,
        record: Mapping[str, Any],
        key_fields: tuple[str, ...],
    ) -> None:
        self._guard_storage_write(table)
        target = table_for(table)
        params = to_db_params(table, record)
        statement = pg_insert(target).values(**params).on_conflict_do_nothing(index_elements=list(key_fields))
        with self._write_connection() as connection:
            connection.execute(statement)

    def begin_pipeline_run(self, record: Mapping[str, Any]) -> None:
        self.upsert("market_data.pipeline_runs", record)

    def pipeline_run(self, pipeline_run_id: str) -> dict[str, Any] | None:
        statement = select(pipeline_runs_table).where(
            pipeline_runs_table.c.id == to_db_params("market_data.pipeline_runs", {"id": pipeline_run_id})["id"]
        )
        with self._read_connection() as connection:
            row = connection.execute(statement).mappings().first()
        return None if row is None else from_db_row("market_data.pipeline_runs", row)

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None:
        statement = (
            update(pipeline_runs_table)
            .where(pipeline_runs_table.c.id == pipeline_run_id)
            .values(
                status=status,
                output_hash=output_hash,
                completed_at=datetime.now(UTC),
                failure_code=failure_code,
            )
        )
        with self._write_connection() as connection:
            result = connection.execute(statement)
            if result.rowcount == 0:
                raise PipelineRunNotFound(f"pipeline run이 없습니다: {pipeline_run_id}")

    def stage_object(
        self,
        storage_record: Mapping[str, Any],
        dataset_object_record: Mapping[str, Any],
    ) -> None:
        self.upsert("storage.objects", storage_record)
        self.upsert("market_data.dataset_objects", dataset_object_record)

    def publish_manifest(self, record: Mapping[str, Any]) -> None:
        canonical = normalise_record("market_data.dataset_manifests", record)
        key = _availability_key(canonical)
        if self._connection is not None:
            self._touched.add(key)
            # Serialise concurrent publishers of the same dataset for the duration of
            # this transaction.  Without it two workers could each see one AVAILABLE
            # manifest, each pass the pre-commit check, and both commit.
            self._connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:token, 0))"),
                {"token": "|".join(str(part) for part in key)},
            )
        self.upsert("market_data.dataset_manifests", canonical)

    def record_dataset_lineage(self, record: Mapping[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_lineage",
            record,
            ("derived_manifest_id", "source_manifest_id", "relation_type"),
        )

    def record_object_lineage(self, record: Mapping[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_object_lineage",
            record,
            ("derived_dataset_object_id", "source_dataset_object_id", "relation_type"),
        )

    def record_quality_incident(self, record: Mapping[str, Any]) -> None:
        self.upsert("market_data.quality_incidents", record)

    def record_pipeline_output(
        self,
        *,
        pipeline_run_id: str,
        dataset_manifest_id: str,
        dataset_object_id: str,
    ) -> None:
        """Refuse: there is no canonical table for this fact.

        The local catalog's `.local.jsonl` sidecar is a development convenience.  Doing
        the same next to a database would look like provenance was persisted when it was
        not, so the PostgreSQL path fails instead.  Callers check
        `supports(CatalogCapability.PIPELINE_RUN_OUTPUTS)` first.
        """

        raise CanonicalTableMissing(f"{MARKET_DATA_SCHEMA}.pipeline_run_outputs", _PIPELINE_RUN_OUTPUTS_GAP)

    # -- artifacts ------------------------------------------------------------------

    def write_summary(self, payload: Mapping[str, Any]) -> Path:
        """Write the operator run summary.

        A run summary is an operator artifact, not canonical state: no `market_data`
        table holds it.  Both catalogs therefore write the same file, which is why
        `artifact_root` is a required constructor argument rather than something this
        class can quietly do without.
        """

        path = self.artifact_root / "summary.json"
        _atomic_write(path, json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n")
        return path

    # -- bulk load ------------------------------------------------------------------

    def replace_table(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        """Replace a table's contents. Used by the catalog-to-PostgreSQL apply path."""

        self._guard_storage_write(table)
        target = table_for(table)
        with self._write_connection() as connection:
            connection.execute(delete(target))
            if rows:
                connection.execute(target.insert(), [to_db_params(table, row) for row in rows])


def known_tables() -> tuple[str, ...]:
    """Every canonical table name this catalog boundary accepts."""

    return tuple(sorted(TABLES_BY_NAME))


def ensure_known_table(table: str) -> None:
    """Raise `UnknownCatalogTable` unless `table` is canonical."""

    if table not in TABLES_BY_NAME:
        raise UnknownCatalogTable(f"{table!r} is not a canonical catalog table")
