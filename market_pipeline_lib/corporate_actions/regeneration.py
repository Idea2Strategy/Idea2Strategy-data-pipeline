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

from ..contracts import deterministic_uuid
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
    ) -> None:
        self._catalog = catalog
        self._reader = reader
        self._writer = writer

    def with_catalog(self, catalog: RegenerationCatalog) -> AdjustedDatasetRegenerator:
        """Bind regeneration writes to the caller's active catalog transaction."""
        return AdjustedDatasetRegenerator(
            catalog=catalog,
            reader=self._reader,
            writer=self._writer,
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

        if current is not None and str(current["dataset_hash"]) == written.content_hash:
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
                dataset_hash=written.content_hash,
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
            "dataset_hash": written.content_hash,
            "supersedes_manifest_id": None if current is None else str(current["id"]),
            "created_at": moment,
            "available_at": moment,
        }

        if transaction_active:
            self._publish_revision(self._catalog, current, record, manifest_id, raw_manifest_id)
        else:
            with self._catalog.transaction() as catalog:
                self._publish_revision(catalog, current, record, manifest_id, raw_manifest_id)

        return RegenerationResult(
            manifest_id=manifest_id,
            revision_number=next_revision,
            supersedes_manifest_id=None if current is None else str(current["id"]),
            dataset_hash=written.content_hash,
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
    ) -> None:
        if current is not None:
            # Retire the prior revision *before* publishing, so the invariant
            # "one AVAILABLE revision per dataset" never briefly breaks.
            catalog.upsert(
                DATASET_MANIFESTS_TABLE, {**dict(current), "status": "SUPERSEDED"}
            )
        catalog.publish_manifest(record)
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

    def _manifest(self, manifest_id: str) -> dict[str, Any]:
        for row in self._catalog.records(DATASET_MANIFESTS_TABLE):
            if str(row["id"]) == manifest_id:
                return row
        raise LookupError(
            f"no dataset manifest {manifest_id!r}; the adjusted dataset cannot be "
            "regenerated without the raw revision it derives from"
        )
