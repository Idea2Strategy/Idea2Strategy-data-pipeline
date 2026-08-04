"""Adjusted-dataset regeneration as a new immutable revision.

Two rules define this module.

**Never mutate.**  An approval does not rewrite the adjusted dataset in place.
It publishes a *new* `market_data.dataset_manifests` revision whose
`supersedes_manifest_id` points at the revision it replaces, and records
`market_data.dataset_lineage` edges to both the raw source it was built from and
the adjusted revision it supersedes.  The previous revision keeps its id, its
revision number, its dataset hash and its objects; only its status moves to
`SUPERSEDED`, which is the canonical lifecycle, so anything already reading it
can still resolve exactly what it read.

**Always rebuild from raw.**  The adjustment arithmetic is not idempotent -- see
:mod:`market_pipeline_lib.corporate_actions.adjustment` -- so regeneration never
adjusts an adjusted dataset.  It reads the raw revision and applies the whole
approved set from scratch.  Re-running with an unchanged approved set therefore
produces byte-identical content, which is detected by comparing the content hash
and published as *no new revision at all*.

The object store is reached only through :class:`RawBarReader` and
:class:`AdjustedBarWriter`.  Those live outside this module's ownership (storage
is DP-a/DP-b), so they are protocols here and tests supply in-memory fakes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from ..contracts import canonical_dataset_hash, deterministic_uuid
from .adjustment import ApprovedAction, Bar, adjusted_bars

__all__ = [
    "ADJUSTED_LAYER",
    "AdjustedBarWriter",
    "AdjustedDatasetRegenerator",
    "RawBarReader",
    "RegenerationCatalog",
    "RegenerationResult",
    "WrittenDataset",
]

DATASET_MANIFESTS_TABLE = "market_data.dataset_manifests"
ADJUSTED_LAYER = "ADJUSTED"

#: `dataset_lineage.relation_type` values this module writes.
ADJUSTMENT_SOURCE = "ADJUSTMENT_SOURCE"
SUPERSEDES = "SUPERSEDES"


@dataclass(frozen=True)
class WrittenDataset:
    """What the object-store writer reports back about one written revision."""

    object_key: str
    content_hash: str
    row_count: int
    byte_size: int
    storage_record: Mapping[str, Any] | None = None
    relation_record: Mapping[str, Any] | None = None


@runtime_checkable
class RawBarReader(Protocol):
    """Reads the raw bars of one manifest revision."""

    def read_bars(self, manifest_id: str) -> Sequence[Bar]: ...


@runtime_checkable
class AdjustedBarWriter(Protocol):
    """Writes an adjusted series and reports its content hash."""

    def write_bars(self, bars: Sequence[Bar], *, dataset_key: str) -> WrittenDataset: ...


@runtime_checkable
class RegenerationCatalog(Protocol):
    """The narrow slice of `MarketDataCatalog` regeneration needs."""

    def transaction(self) -> Any: ...

    def records(self, table: str) -> list[dict[str, Any]]: ...

    def upsert(self, table: str, record: Mapping[str, Any]) -> None: ...

    def publish_manifest(self, record: Mapping[str, Any]) -> None: ...

    def record_dataset_lineage(self, record: Mapping[str, Any]) -> None: ...

    def stage_object(
        self, storage_record: Mapping[str, Any], dataset_object_record: Mapping[str, Any]
    ) -> None: ...

    def latest_available_manifest(
        self, *, feed_id: str, data_layer: str, resolution: str, year: int
    ) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class RegenerationResult:
    """The revision the adjusted dataset now resolves to."""

    manifest_id: str
    revision_number: int
    supersedes_manifest_id: str | None
    dataset_hash: str
    row_count: int
    #: False when the approved set produced content identical to the current
    #: revision, so no new revision was published.
    created: bool


class AdjustedDatasetRegenerator:
    """Rebuilds the adjusted dataset from raw for the current approved set."""

    def __init__(
        self,
        *,
        catalog: RegenerationCatalog,
        reader: RawBarReader,
        writer: AdjustedBarWriter,
        require_feed_compatibility: bool = False,
    ) -> None:
        self._catalog = catalog
        self._reader = reader
        self._writer = writer
        self._require_feed_compatibility = require_feed_compatibility

    def with_catalog(self, catalog: RegenerationCatalog) -> AdjustedDatasetRegenerator:
        """Bind regeneration writes to the caller's active catalog transaction."""
        return AdjustedDatasetRegenerator(
            catalog=catalog,
            reader=self._reader,
            writer=self._writer,
            require_feed_compatibility=self._require_feed_compatibility,
        )

    def regenerate(
        self,
        *,
        raw_manifest_id: str,
        adjusted_feed_id: str,
        approved_actions: Sequence[ApprovedAction],
        now: datetime,
    ) -> RegenerationResult:
        """Regenerate as a standalone operation with its own DB transaction."""
        return self._regenerate(
            raw_manifest_id=raw_manifest_id,
            adjusted_feed_id=adjusted_feed_id,
            approved_actions=approved_actions,
            now=now,
            transaction_active=False,
        )

    def regenerate_in_transaction(
        self,
        *,
        raw_manifest_id: str,
        adjusted_feed_id: str,
        approved_actions: Sequence[ApprovedAction],
        now: datetime,
    ) -> RegenerationResult:
        """Regenerate using the caller's active catalog transaction.

        Object bytes are written before relational publication. If the database
        transaction rolls back, those bytes are deliberately left unreferenced;
        the object-store orphan collector may remove them later, while no
        manifest is ever allowed to expose a partially committed revision.
        """
        return self._regenerate(
            raw_manifest_id=raw_manifest_id,
            adjusted_feed_id=adjusted_feed_id,
            approved_actions=approved_actions,
            now=now,
            transaction_active=True,
        )

    def _regenerate(
        self,
        *,
        raw_manifest_id: str,
        adjusted_feed_id: str,
        approved_actions: Sequence[ApprovedAction],
        now: datetime,
        transaction_active: bool,
    ) -> RegenerationResult:
        raw = self._manifest(raw_manifest_id)
        if self._require_feed_compatibility:
            self._verify_feed_compatibility(raw, adjusted_feed_id)
        resolution = str(raw["resolution"])
        period_start = str(raw["period_start"])
        year = int(period_start[:4])

        current = self._catalog.latest_available_manifest(
            feed_id=adjusted_feed_id,
            data_layer=ADJUSTED_LAYER,
            resolution=resolution,
            year=year,
        )
        next_revision = 1 if current is None else int(current["revision_number"]) + 1

        bars = self._reader.read_bars(raw_manifest_id)
        rebuilt = adjusted_bars(bars, approved_actions)
        written = self._writer.write_bars(
            rebuilt,
            dataset_key=(
                f"feed={adjusted_feed_id}/layer={ADJUSTED_LAYER}/"
                f"resolution={resolution}/revision={next_revision}"
            ),
        )
        registered_hash = self._dataset_hash(written)

        if current is not None and str(current["dataset_hash"]) == registered_hash:
            # The approved set produced exactly what is already published. A new
            # revision here would be a lie about the data having changed.
            return RegenerationResult(
                manifest_id=str(current["id"]),
                revision_number=int(current["revision_number"]),
                supersedes_manifest_id=(
                    None
                    if current.get("supersedes_manifest_id") is None
                    else str(current["supersedes_manifest_id"])
                ),
                dataset_hash=registered_hash,
                row_count=written.row_count,
                created=False,
            )

        manifest_id = deterministic_uuid(
            DATASET_MANIFESTS_TABLE,
            adjusted_feed_id,
            str(raw.get("instrument_id")),
            ADJUSTED_LAYER,
            resolution,
            period_start,
            next_revision,
        )
        moment = now.astimezone(now.tzinfo).isoformat().replace("+00:00", "Z")
        record = {
            "id": manifest_id,
            "feed_id": adjusted_feed_id,
            "instrument_id": raw.get("instrument_id"),
            "data_layer": ADJUSTED_LAYER,
            "resolution": resolution,
            "revision_number": next_revision,
            "status": "AVAILABLE",
            "period_start": period_start,
            "period_end": str(raw["period_end"]),
            "schema_version": str(raw["schema_version"]),
            "dataset_hash": registered_hash,
            "supersedes_manifest_id": None if current is None else str(current["id"]),
            "created_at": moment,
            "available_at": moment,
        }

        if transaction_active:
            self._publish_revision(
                self._catalog, current, record, manifest_id, raw_manifest_id, written
            )
        else:
            with self._catalog.transaction() as catalog:
                self._publish_revision(catalog, current, record, manifest_id, raw_manifest_id, written)

        return RegenerationResult(
            manifest_id=manifest_id,
            revision_number=next_revision,
            supersedes_manifest_id=None if current is None else str(current["id"]),
            dataset_hash=registered_hash,
            row_count=written.row_count,
            created=True,
        )

    @staticmethod
    def _publish_revision(
        catalog: RegenerationCatalog,
        current: Mapping[str, Any] | None,
        record: Mapping[str, Any],
        manifest_id: str,
        raw_manifest_id: str,
        written: WrittenDataset,
    ) -> None:
        if current is not None:
            # Retire the prior revision *before* publishing, so the invariant
            # "one AVAILABLE revision per dataset" never briefly breaks.
            catalog.upsert(
                DATASET_MANIFESTS_TABLE, {**dict(current), "status": "SUPERSEDED"}
            )
        # The relation's FK needs the manifest row first. All writes remain in
        # the caller's transaction, so no observer can see a manifest without
        # its registered object.
        catalog.publish_manifest(record)
        if written.storage_record is not None and written.relation_record is not None:
            object_id = deterministic_uuid(
                "storage-object", written.content_hash, written.object_key
            )
            relation_id = deterministic_uuid("dataset-object", manifest_id, object_id)
            catalog.stage_object(
                {**dict(written.storage_record), "id": object_id},
                {
                    **dict(written.relation_record),
                    "id": relation_id,
                    "dataset_manifest_id": manifest_id,
                    "object_id": object_id,
                },
            )
        catalog.record_dataset_lineage(
            {
                "derived_manifest_id": manifest_id,
                "source_manifest_id": raw_manifest_id,
                "relation_type": ADJUSTMENT_SOURCE,
            }
        )
        if current is not None:
            catalog.record_dataset_lineage(
                {
                    "derived_manifest_id": manifest_id,
                    "source_manifest_id": str(current["id"]),
                    "relation_type": SUPERSEDES,
                }
            )

    @staticmethod
    def _dataset_hash(written: WrittenDataset) -> str:
        if written.storage_record is None or written.relation_record is None:
            # Legacy/in-memory adapters have no catalog registration contract.
            return written.content_hash
        canonical = {
            **dict(written.relation_record),
            "content_hash": written.content_hash,
            "schema_version": written.storage_record["schema_version"],
        }
        return canonical_dataset_hash([canonical])

    def _manifest(self, manifest_id: str) -> dict[str, Any]:
        for row in self._catalog.records(DATASET_MANIFESTS_TABLE):
            if str(row["id"]) == manifest_id:
                return row
        raise LookupError(
            f"no dataset manifest {manifest_id!r}; the adjusted dataset cannot be "
            "regenerated without the raw revision it derives from"
        )

    def _verify_feed_compatibility(
        self, raw_manifest: Mapping[str, Any], adjusted_feed_id: str
    ) -> None:
        feeds = self._catalog.records("market_data.feeds")
        raw_feed = next(
            (item for item in feeds if str(item["id"]) == str(raw_manifest["feed_id"])), None
        )
        adjusted_feed = next(
            (item for item in feeds if str(item["id"]) == adjusted_feed_id), None
        )
        if raw_feed is None or adjusted_feed is None:
            raise ValueError("source and adjusted feeds must both exist before regeneration")
        fields = ("provider_id", "data_kind", "resolution", "timezone_name")
        mismatches = [field for field in fields if raw_feed.get(field) != adjusted_feed.get(field)]
        if mismatches or str(raw_manifest["resolution"]) != str(adjusted_feed["resolution"]):
            raise ValueError(
                "adjusted feed is incompatible with the candidate source feed: "
                f"{sorted(set(mismatches + ['manifest.resolution']))}"
            )
