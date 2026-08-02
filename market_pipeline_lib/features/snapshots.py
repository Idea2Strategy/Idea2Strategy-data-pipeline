"""Point-in-time feature snapshot batches (`market_data.feature_snapshot_batches`).

A backtest does not consume one feature; it consumes a *set* of features over a *set*
of instruments, as of one moment.  A snapshot batch is that grouping, and the single
property it exists to guarantee is:

    **A batch that is missing any planned member is not consumable.**

Which is enforced three times over, because "not consumable" has to survive a caller
that ignores a return value:

* `seal` refuses to compute a `batch_hash` while a planned member is absent or
  ``FAILED``, and raises `PartialSnapshotBatch` naming what is missing;
* the row keeps ``PENDING`` (nothing arrived yet) or moves to ``FAILED`` (a member
  failed), and never acquires `available_at`;
* `consume` refuses any batch that is not ``SUCCEEDED`` with a `batch_hash`, so a
  consumer that skipped `seal` entirely still cannot read a half-built snapshot.

Membership is *planned*, not discovered.  `SnapshotBatchPlan` states the definitions
and the market inputs up front, and completeness is measured against that plan --
discovering membership from whatever happens to be in the table would make "complete"
mean "nothing more has arrived yet", which is exactly the bug.

What sealing requires, and why
------------------------------
``feature_snapshot_batch_success_complete`` (`V1__initial_schema.sql:1104`) is a CHECK
the SQLAlchemy metadata does not restate: ``status = 'SUCCEEDED'`` requires
`snapshot_object_id`, `batch_hash`, `row_count` **and** `available_at`, and
``feature_snapshot_batch_row_count_positive`` requires ``row_count > 0``.  So `seal`
takes both the snapshot object the batch was written to and the `MaterializationResult`
objects that were written into it.  The results are not taken on trust: each one's
`result_hash` is checked against the stored `feature_materializations` row, so a caller
cannot inflate `row_count` with values the catalog never saw.  The canonical schema has
no per-materialization row count, which is exactly why the count has to arrive this way
rather than being read back.

`batch_hash` covers the members' result hashes and not `row_count` or
`snapshot_object_id`: the identity of a snapshot is what is in it, not where it was
stored or how many rows that came to.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import product
from typing import Any
from uuid import UUID

from ..contracts import deterministic_uuid
from .definitions import FeatureDefinitionRegistry
from .errors import PartialSnapshotBatch, SnapshotBatchNotConsumable
from .hashing import (
    batch_version,
    canonical_json,
    canonical_sha256,
    compact_utc,
    is_sha256_hex,
    iso_utc,
)
from .materialization import MaterializationResult
from .tables import FEATURE_MATERIALIZATIONS, FEATURE_SNAPSHOT_BATCHES, FeatureCatalog

__all__ = [
    "FEATURE_SET_SCHEMA_VERSION",
    "FeatureSnapshotBatchBuilder",
    "MarketInput",
    "SealedSnapshotBatch",
    "SnapshotBatchPlan",
]


FEATURE_SET_SCHEMA_VERSION = 1
MARKET_SET_SCHEMA_VERSION = 1
BATCH_SCHEMA_VERSION = 1

_UUID_PURPOSE = "feature-snapshot-batch"
#: Must stay identical to `materialization._UUID_PURPOSE`: a planned member is located
#: by recomputing the id the materializer would have written.
_MATERIALIZATION_PURPOSE = "feature-materialization"

#: `market_data.feature_snapshot_batches.idempotency_key` is VARCHAR(160).
_IDEMPOTENCY_KEY_LIMIT = 160


def _require_uuid(value: Any, label: str) -> str:
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{label}={value!r} is not a UUID") from exc


@dataclass(frozen=True)
class MarketInput:
    """One instrument and the exact input bundle the batch pins for it."""

    instrument_id: str
    input_dataset_set_hash: str

    def __post_init__(self) -> None:
        try:
            UUID(self.instrument_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"instrument_id={self.instrument_id!r} is not a UUID") from exc
        if not is_sha256_hex(self.input_dataset_set_hash):
            raise ValueError(
                f"input_dataset_set_hash must be 64 lowercase hex characters, "
                f"got {self.input_dataset_set_hash!r}"
            )

    def entry(self) -> dict[str, str]:
        return {
            "input_dataset_set_hash": self.input_dataset_set_hash,
            "instrument_id": self.instrument_id,
        }


@dataclass(frozen=True)
class SnapshotBatchPlan:
    """The declared contents of one point-in-time batch."""

    definition_hashes: tuple[str, ...]
    market_inputs: tuple[MarketInput, ...]
    period_start: datetime
    period_end: datetime
    source_start_watermark: str
    source_end_watermark: str

    def __post_init__(self) -> None:
        if not self.definition_hashes:
            raise ValueError("a snapshot batch plan needs at least one feature definition")
        for value in self.definition_hashes:
            if not is_sha256_hex(value):
                raise ValueError(f"definition hash must be 64 lowercase hex characters, got {value!r}")
        if not self.market_inputs:
            raise ValueError("a snapshot batch plan needs at least one market input")
        instruments = [item.instrument_id for item in self.market_inputs]
        if len(set(instruments)) != len(instruments):
            raise ValueError("a snapshot batch plan must not list the same instrument twice")
        if self.period_end <= self.period_start:
            raise ValueError("period_end must be after period_start")
        for label, watermark in (
            ("source_start_watermark", self.source_start_watermark),
            ("source_end_watermark", self.source_end_watermark),
        ):
            if not isinstance(watermark, str) or not watermark.strip():
                raise ValueError(f"{label} must be a non-empty string")
            if len(watermark) > 300:
                raise ValueError(f"{label} exceeds the canonical 300 characters")

    # -- identity ----------------------------------------------------------------------

    @property
    def unique_definition_hashes(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.definition_hashes)))

    @property
    def feature_set_hash(self) -> str:
        """What this batch contains, independent of the order it was listed in."""

        return canonical_sha256(
            {
                "definition_hashes": list(self.unique_definition_hashes),
                "feature_set_schema_version": FEATURE_SET_SCHEMA_VERSION,
            }
        )

    @property
    def input_market_set_hash(self) -> str:
        """Which market inputs it is pinned to.

        This is the value a COM06 backtest request carries as
        ``input_bundle_fingerprint`` when the request pins a whole batch rather than a
        single materialization.
        """

        entries = sorted((item.entry() for item in self.market_inputs), key=canonical_json)
        return canonical_sha256(
            {"market_inputs": entries, "market_set_schema_version": MARKET_SET_SCHEMA_VERSION}
        )

    @property
    def expected_members(self) -> tuple[tuple[str, MarketInput], ...]:
        ordered_inputs = sorted(self.market_inputs, key=lambda item: item.instrument_id)
        return tuple(product(self.unique_definition_hashes, ordered_inputs))

    @property
    def expected_member_count(self) -> int:
        return len(self.unique_definition_hashes) * len(self.market_inputs)

    @property
    def idempotency_key(self) -> str:
        key = ":".join(
            (
                "fsb1",
                self.feature_set_hash[:16],
                self.input_market_set_hash[:16],
                compact_utc(self.period_start),
                compact_utc(self.period_end),
            )
        )
        if len(key) > _IDEMPOTENCY_KEY_LIMIT:  # pragma: no cover - fixed-width by construction
            raise ValueError(f"idempotency key exceeds {_IDEMPOTENCY_KEY_LIMIT} characters: {key}")
        return key

    @property
    def id(self) -> str:
        return str(deterministic_uuid(_UUID_PURPOSE, self.idempotency_key))

    def materialization_id(self, definition_hash: str, market_input: MarketInput) -> str:
        """The `feature_materializations.id` a planned member must have."""

        return str(
            deterministic_uuid(
                _MATERIALIZATION_PURPOSE,
                definition_hash,
                market_input.instrument_id,
                market_input.input_dataset_set_hash,
                iso_utc(self.period_start),
                iso_utc(self.period_end),
            )
        )

    def base_record(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "feature_set_hash": self.feature_set_hash,
            "input_market_set_hash": self.input_market_set_hash,
            "source_start_watermark": self.source_start_watermark,
            "source_end_watermark": self.source_end_watermark,
            "period_start": iso_utc(self.period_start),
            "period_end": iso_utc(self.period_end),
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class SealedSnapshotBatch:
    """A ``SUCCEEDED`` batch, and the version a backtest request pins."""

    id: str
    feature_set_hash: str
    input_market_set_hash: str
    batch_hash: str
    period_start: datetime
    period_end: datetime
    source_start_watermark: str
    source_end_watermark: str
    member_count: int
    #: Both non-optional: `feature_snapshot_batch_success_complete` refuses a SUCCEEDED
    #: row without them, so a `SealedSnapshotBatch` that lacked them could not exist.
    row_count: int
    snapshot_object_id: str
    status: str = "SUCCEEDED"

    @property
    def feature_materialization_version(self) -> str:
        return batch_version(
            feature_set_hash=self.feature_set_hash,
            input_market_set_hash=self.input_market_set_hash,
            batch_hash=self.batch_hash,
        )

    @property
    def input_bundle_fingerprint(self) -> str:
        """The COM06 field of the same name, for a request pinning this batch."""

        return self.input_market_set_hash

    def to_document(self) -> dict[str, Any]:
        """The `feature-snapshot` document the lightweight-validation Lambda checks."""

        return {
            "feature_materialization_version": self.feature_materialization_version,
            "feature_set_hash": self.feature_set_hash,
            "input_market_set_hash": self.input_market_set_hash,
            "batch_hash": self.batch_hash,
            "period_start": iso_utc(self.period_start),
            "period_end": iso_utc(self.period_end),
            "source_start_watermark": self.source_start_watermark,
            "source_end_watermark": self.source_end_watermark,
            "member_count": self.member_count,
            "row_count": self.row_count,
            "snapshot_object_id": self.snapshot_object_id,
            "status": self.status,
        }


def _batch_hash(
    *,
    plan: SnapshotBatchPlan,
    members: Sequence[tuple[str, MarketInput, str]],
) -> str:
    entries = sorted(
        (
            {
                "definition_hash": definition_hash,
                "input_dataset_set_hash": market_input.input_dataset_set_hash,
                "instrument_id": market_input.instrument_id,
                "result_hash": result_hash,
            }
            for definition_hash, market_input, result_hash in members
        ),
        key=canonical_json,
    )
    return canonical_sha256(
        {
            "batch_schema_version": BATCH_SCHEMA_VERSION,
            "feature_set_hash": plan.feature_set_hash,
            "input_market_set_hash": plan.input_market_set_hash,
            "members": entries,
            "period_end": iso_utc(plan.period_end),
            "period_start": iso_utc(plan.period_start),
            "source_end_watermark": plan.source_end_watermark,
            "source_start_watermark": plan.source_start_watermark,
        }
    )


class FeatureSnapshotBatchBuilder:
    """Opens, seals and hands out snapshot batches."""

    def __init__(self, catalog: FeatureCatalog, registry: FeatureDefinitionRegistry) -> None:
        self._catalog = catalog
        self._registry = registry

    # -- lifecycle ---------------------------------------------------------------------

    def open(self, plan: SnapshotBatchPlan) -> str:
        """Record the batch as ``PENDING``.  Idempotent; never reopens a sealed batch."""

        for definition_hash in plan.unique_definition_hashes:
            # Raises FeatureDefinitionNotPublished: a batch may only plan features whose
            # definitions a consumer can actually read back.
            self._registry.get(definition_hash)
        existing = self._row(plan)
        if existing is not None:
            return str(existing["id"])
        record = plan.base_record()
        record.update(
            {
                "snapshot_object_id": None,
                "batch_hash": None,
                "row_count": None,
                "status": "PENDING",
                "available_at": None,
                "created_at": iso_utc(datetime.now(UTC)),
            }
        )
        self._catalog.upsert(FEATURE_SNAPSHOT_BATCHES, record)
        return plan.id

    def seal(
        self,
        plan: SnapshotBatchPlan,
        *,
        results: Iterable[MaterializationResult],
        snapshot_object_id: str,
    ) -> SealedSnapshotBatch:
        """Close a complete batch, or refuse and say what is missing.

        `snapshot_object_id` and `results` are both required by the canonical
        `feature_snapshot_batch_success_complete` CHECK; see the module docstring.
        """

        if not snapshot_object_id:
            raise ValueError(
                "snapshot_object_id is required: the canonical CHECK "
                "feature_snapshot_batch_success_complete refuses a SUCCEEDED batch that "
                "does not name the object the snapshot was written to. Write the "
                "snapshot object first, then seal the batch against it."
            )
        _require_uuid(snapshot_object_id, "snapshot_object_id")
        row = self._row(plan)
        if row is None:
            self.open(plan)
            row = self._row(plan)
        if row is None:  # pragma: no cover - open() has just written it
            raise SnapshotBatchNotConsumable(f"snapshot batch {plan.id} could not be opened")
        if row["status"] == "SUCCEEDED" and row["batch_hash"] is not None:
            return self._sealed_from_row(plan, row)

        stored = {str(item["id"]): item for item in self._catalog.records(FEATURE_MATERIALIZATIONS)}
        members: list[tuple[str, MarketInput, str]] = []
        missing: list[str] = []
        failed = False
        for definition_hash, market_input in plan.expected_members:
            member = stored.get(plan.materialization_id(definition_hash, market_input))
            label = f"{definition_hash}/{market_input.instrument_id}"
            if member is None:
                missing.append(f"{label} (absent)")
                continue
            status = str(member["status"])
            if status != "SUCCEEDED" or member["result_hash"] is None:
                failed = failed or status == "FAILED"
                missing.append(f"{label} ({status})")
                continue
            members.append((definition_hash, market_input, str(member["result_hash"])))

        if missing:
            self._mark(plan, row, status="FAILED" if failed else "PENDING")
            raise PartialSnapshotBatch(
                f"snapshot batch {plan.id} plans {plan.expected_member_count} members and "
                f"{len(members)} are usable; it will not be sealed. Missing: {missing}"
            )

        row_count = self._row_count(plan, members, results)
        digest = _batch_hash(plan=plan, members=members)
        now = datetime.now(UTC)
        record = plan.base_record()
        record.update(
            {
                "snapshot_object_id": snapshot_object_id,
                "batch_hash": digest,
                "row_count": row_count,
                "status": "SUCCEEDED",
                "available_at": iso_utc(now),
                "created_at": str(row.get("created_at") or iso_utc(now)),
            }
        )
        self._catalog.upsert(FEATURE_SNAPSHOT_BATCHES, record)
        return SealedSnapshotBatch(
            id=plan.id,
            feature_set_hash=plan.feature_set_hash,
            input_market_set_hash=plan.input_market_set_hash,
            batch_hash=digest,
            period_start=plan.period_start,
            period_end=plan.period_end,
            source_start_watermark=plan.source_start_watermark,
            source_end_watermark=plan.source_end_watermark,
            member_count=len(members),
            row_count=row_count,
            snapshot_object_id=snapshot_object_id,
        )

    def consume(self, plan: SnapshotBatchPlan) -> SealedSnapshotBatch:
        """The sealed batch, or `SnapshotBatchNotConsumable`.

        The last line of defence: a consumer that never called `seal` still cannot read
        a batch that is not finished.
        """

        row = self._row(plan)
        if row is None:
            raise SnapshotBatchNotConsumable(
                f"snapshot batch {plan.id} ({plan.idempotency_key}) has never been opened"
            )
        if str(row["status"]) != "SUCCEEDED" or row["batch_hash"] is None:
            raise SnapshotBatchNotConsumable(
                f"snapshot batch {plan.id} is {row['status']} with batch_hash="
                f"{row['batch_hash']!r}; only a SUCCEEDED batch with a batch_hash is a "
                "consistent point-in-time snapshot"
            )
        return self._sealed_from_row(plan, row)

    # -- internals ---------------------------------------------------------------------

    def _row(self, plan: SnapshotBatchPlan) -> dict[str, Any] | None:
        for row in self._catalog.records(FEATURE_SNAPSHOT_BATCHES):
            if str(row.get("id")) == plan.id:
                return row
        return None

    def _mark(self, plan: SnapshotBatchPlan, row: Mapping[str, Any], *, status: str) -> None:
        if str(row.get("status")) == status:
            return
        record = dict(row)
        record["status"] = status
        record["batch_hash"] = None
        record["row_count"] = None
        record["available_at"] = None
        self._catalog.upsert(FEATURE_SNAPSHOT_BATCHES, record)

    def _row_count(
        self,
        plan: SnapshotBatchPlan,
        members: Sequence[tuple[str, MarketInput, str]],
        results: Iterable[MaterializationResult],
    ) -> int:
        """Sum the caller's value counts, after checking they are the stored ones."""

        supplied = list(results)
        expected = {
            (definition_hash, market_input.instrument_id): result_hash
            for definition_hash, market_input, result_hash in members
        }
        seen: dict[tuple[str, str], int] = {}
        for result in supplied:
            key = (result.definition_hash, result.instrument_id)
            if expected.get(key) != result.result_hash:
                raise PartialSnapshotBatch(
                    f"result for {key} is not the one recorded for batch {plan.id}; "
                    "row_count is only counted from results the catalog agrees with"
                )
            seen[key] = result.row_count
        if set(seen) != set(expected):
            raise PartialSnapshotBatch(
                f"row_count needs a result for every member of batch {plan.id}; "
                f"got {len(seen)} of {len(expected)}"
            )
        total = sum(seen.values())
        if total <= 0:
            raise PartialSnapshotBatch(
                f"batch {plan.id} would seal with {total} rows; the canonical CHECK "
                "feature_snapshot_batch_row_count_positive refuses an empty snapshot, and "
                "a batch whose every member produced nothing is a data gap, not a snapshot"
            )
        return total

    def _sealed_from_row(self, plan: SnapshotBatchPlan, row: Mapping[str, Any]) -> SealedSnapshotBatch:
        return SealedSnapshotBatch(
            id=str(row["id"]),
            feature_set_hash=str(row["feature_set_hash"]),
            input_market_set_hash=str(row["input_market_set_hash"]),
            batch_hash=str(row["batch_hash"]),
            period_start=plan.period_start,
            period_end=plan.period_end,
            source_start_watermark=str(row["source_start_watermark"]),
            source_end_watermark=str(row["source_end_watermark"]),
            member_count=plan.expected_member_count,
            row_count=int(row["row_count"]),
            snapshot_object_id=str(row["snapshot_object_id"]),
        )
