"""D90 -- the pre-start gate: 누락 시 실행을 차단.

What this is for
----------------
C's runtime warms a bot up from a D-published bundle and refuses to start if the
manifest is not ``AVAILABLE`` (``StartupWarmupCoordinator.java:67-69``).  That check
is real, but it can only see what D wrote.  If D publishes an ``AVAILABLE`` manifest
for a session whose feed went silent at lunchtime, or whose feature batch never
sealed, C starts on it -- because nothing told C otherwise.

This module is the thing that tells C otherwise.  It evaluates, in one place and in
a fixed order, the three facts a consumer's start depends on:

1. **The data.**  Is there an ``AVAILABLE`` manifest for the required session, with a
   published object for *every* required shard?  A session covered on one shard and
   missing on another is not a covered session.
2. **The freshness.**  Does ``market_data.stream_watermarks`` say the feed reached the
   session, and that ingestion is still running?  Those are two different questions
   and they get two different reason codes.
3. **The features.**  Is the feature snapshot batch the consumer pins actually
   consumable?

Each failure yields its own :class:`~market_pipeline_lib.realtime_warmup.WarmupBlockReason`.
The verdict is then written to the two projections a gate can actually be read from:

* ``market_data.quality_incidents`` and ``market_data.dataset_manifests.status`` --
  the pair the canonical DBML names in the ``stream_watermarks`` ``Note``
  ("평가 전 실행 게이트가 quality_incidents와 함께 읽어");
* the warm-up ``manifest.json``, which is the only artefact C's Java actually opens.

A block that exists only inside this process is not a block, so ``evaluate`` never
mutates anything and ``record`` is the step that makes it visible.

What C's Java side must assert for D90 to be jointly complete
-------------------------------------------------------------
D cannot close D90 alone: half of "누락 시 실행을 차단" is C refusing to start.  What
follows is the exact C-side contract this module is built against.  Nothing here is
speculative -- each item names the Java that already exists or the Java that does
not.

**1. Already satisfied by C, and load-bearing.**
``StartupWarmupCoordinator.java:67-69`` refuses any manifest whose ``status`` is not
``AVAILABLE``.  Every block this gate records writes ``QUARANTINED`` into both the
``manifest.json`` C reads and ``market_data.dataset_manifests``, so C's existing
check already turns a D-side block into a refused start.  C must **not** relax that
check to a warning, and must not treat a missing bundle as "start anyway": today an
absent manifest yields ``SNAPSHOT_NOT_FOUND`` (``:47-48``), which is correct.

**2. C must read and surface ``readiness.reason_code``.**
The manifest now carries a ``readiness`` object (see
:meth:`~market_pipeline_lib.realtime_warmup.WarmupReadiness.as_document`) with
``state``, ``reason_code``, ``detail``, ``feed_id``, ``session_date_et``,
``evaluated_at`` and ``observed``.  ``ManifestBoundWarmupDataSource`` currently
ignores it, so C reports ``MANIFEST_UNAVAILABLE`` and loses *why*.  C must propagate
D's ``reason_code`` verbatim into its ``WarmupFailure`` message; the seven codes are
:class:`~market_pipeline_lib.realtime_warmup.WarmupBlockReason`, all prefixed
``D90_``.  Inventing a C-side synonym would give one cause two names.

**3. C must add the pre-evaluation freshness gate, which does not exist.**
The canonical schema's ``Note`` on ``market_data.stream_watermarks`` designates it,
with ``market_data.quality_incidents``, as the pre-evaluation execution gate.  No
Java in ``trading-engine`` reads either table.  ``MarketDataAvailabilityGate`` is the
in-memory stand-in and has no production caller, and ``strategy-runtime`` has no JDBC
dependency at all, so it cannot query them even in principle.  The query C must run
before evaluating a bot, over the central schema and with no D code in the path, is
pinned by ``tests/test_d90_c_integration.py::
TestBlockIsVisibleInThePostgresProjection``.  It must assert all four of:

* a ``market_data.stream_watermarks`` row exists for the feed  -- else block;
* ``now() - last_ingested_at`` is within C's own freshness budget -- else block;
* ``last_source_event_at`` has reached the session's completion target -- else block;
* no ``ACTIVE``/``ERROR`` ``market_data.quality_incidents`` row whose
  ``dataset_manifest_id`` points at the manifest C is about to start from.

**4. C must add a wall-clock staleness check on the manifest.**
``MANIFEST_UNAVAILABLE`` fires only on an explicit non-``AVAILABLE`` status, and the
only time-based check C has (``StartupWarmupCoordinator.java:116-119``) rejects
observations that are too *new*.  A three-day-old but structurally valid
``AVAILABLE`` manifest passes C's gate today.  Item 3 closes this from the DB side;
C should also reject a ``readiness.evaluated_at`` older than its budget, so a stale
bundle read from object storage alone still fails closed.

Known limitation, stated rather than worked around
--------------------------------------------------
``market_data.stream_watermarks`` is keyed by ``feed_id`` alone.  The freshness this
gate can prove is therefore the feed's **completion floor** -- the position every
declared shard has passed -- not per-shard freshness.  For *blocking* that is the
conservative direction: the floor is the slowest shard, so a stalled shard blocks the
whole feed and nothing is under-blocked.  What it cannot do is tell an operator
*which* shard stalled, and it cannot let a healthy shard proceed while a sibling is
behind.  Fixing that needs ``(feed_id, shard_key)`` as the primary key, which is a
central migration and a DBML decision.  See :mod:`market_pipeline_lib.watermarks`.

For C's gate specifically this is **not** a correctness gap: item 3 asks a
feed-level question ("is this feed's market data complete and still arriving?"), and
the floor answers exactly that, conservatively.  It is an *operability* gap -- the
block cannot name the stalled shard -- and it becomes a correctness gap only if a
future requirement lets one shard's bots run while another shard is behind.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from .contracts import DatasetContract, Granularity, iso_utc, partition_bounds
from .features.errors import PartialSnapshotBatch, SnapshotBatchNotConsumable
from .quality import ImpactScope, QualityIncident, ScopeBreadth
from .realtime_warmup import BLOCKED, READY, WarmupBlockReason, WarmupReadiness
from .watermarks import WatermarkRepository

__all__ = [
    "ConsumableFeatureBatch",
    "WarmupCoverage",
    "WarmupReadinessGate",
]

LOGGER = logging.getLogger("market_pipeline_lib.warmup_gate")

DATASET_MANIFESTS = "market_data.dataset_manifests"

#: `market_data.quality_incidents.severity`; a block is an ERROR, never a WARNING.
BLOCK_SEVERITY = "ERROR"

#: The batch statuses a never-opened batch reports.  `FeatureSnapshotBatchBuilder`
#: distinguishes the two by message, so the gate does too rather than collapsing
#: "you never built it" into "it is not finished".
_NEVER_OPENED_MARKER = "has never been opened"


@runtime_checkable
class ConsumableFeatureBatch(Protocol):
    """Anything that can answer "may a consumer read this batch right now?".

    Deliberately narrow: the gate does not need to know how a batch is planned, only
    whether reading it raises.  `FeatureSnapshotBatchBuilder.consume` bound to its plan
    satisfies this, and so does a caller's own projection.
    """

    def consume(self) -> Any:
        """Return the sealed batch, or raise a `features.errors` failure."""


@dataclass(frozen=True)
class WarmupCoverage:
    """Exactly what a consumer's start requires, with nothing implied.

    ``required_watermark_at`` is the instant the feed must have consumed past for the
    session to count as delivered.  It is an input rather than something derived from
    the session date because there is no single right answer: a 30-minute regular-hours
    stream is complete at its 15:30 ET bar, an after-hours feed is not complete until
    20:00 ET, and a half-day close moves both.  Deriving it would be exactly the kind
    of hidden default policy that makes a gate quietly wrong on the one day it matters.
    """

    contract: DatasetContract
    session: date
    granularity: Granularity
    required_shards: tuple[str, ...]
    required_watermark_at: datetime

    def __post_init__(self) -> None:
        if not self.required_shards:
            raise ValueError(
                "required_shards must name every shard a consumer needs; an empty set "
                "would make a session with no data at all look fully covered"
            )
        if len(set(self.required_shards)) != len(self.required_shards):
            raise ValueError(f"required_shards must be unique, got {list(self.required_shards)}")
        moment = self.required_watermark_at
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise ValueError("required_watermark_at must be timezone-aware")
        object.__setattr__(self, "required_watermark_at", moment.astimezone(UTC))
        start, end = self.bounds
        from .contracts import ET

        lower = datetime.combine(start, datetime.min.time(), ET).astimezone(UTC)
        upper = datetime.combine(end, datetime.min.time(), ET).astimezone(UTC)
        if not lower <= self.required_watermark_at < upper:
            raise ValueError(
                f"required_watermark_at {iso_utc(self.required_watermark_at)} is outside the "
                f"{self.granularity} partition {start.isoformat()}..{end.isoformat()}"
            )

    @property
    def session_date_et(self) -> str:
        return self.session.isoformat()

    @property
    def bounds(self) -> tuple[date, date]:
        return partition_bounds(self.session, self.granularity)

    def period(self) -> tuple[datetime, datetime]:
        """The partition expressed in UTC, for the incident row's period columns."""

        from .contracts import ET

        start, end = self.bounds
        return (
            datetime.combine(start, datetime.min.time(), ET).astimezone(UTC),
            datetime.combine(end, datetime.min.time(), ET).astimezone(UTC),
        )

class WarmupReadinessGate:
    """Evaluates the pre-start gate and publishes its verdict."""

    def __init__(
        self,
        catalog: Any,
        *,
        feed_id: str,
        watermarks: WatermarkRepository,
        freshness_budget: timedelta,
        now: Callable[[], datetime],
    ) -> None:
        if freshness_budget <= timedelta(0):
            raise ValueError(
                "freshness_budget must be positive; a zero or negative budget would "
                "block every session the instant it was published"
            )
        self._catalog = catalog
        self._feed_id = feed_id
        self._watermarks = watermarks
        self._freshness_budget = freshness_budget
        self._now = now

    @property
    def feed_id(self) -> str:
        return self._feed_id

    @property
    def freshness_budget(self) -> timedelta:
        return self._freshness_budget

    # -- evaluation -------------------------------------------------------------------

    def evaluate(
        self,
        coverage: WarmupCoverage,
        *,
        feature_batch: ConsumableFeatureBatch | None = None,
    ) -> WarmupReadiness:
        """Decide, without writing anything, whether a consumer may start.

        Order matters and is fixed: data, then freshness, then features.  A missing
        object is reported as a missing object even when the watermark is also stale,
        because the first cause is the one an operator has to fix first.
        """

        evaluated_at = self._now()
        observed: dict[str, Any] = {
            "feed_id": self._feed_id,
            "session_date_et": coverage.session_date_et,
            "required_shards": list(coverage.required_shards),
            "freshness_budget": _iso_duration(self._freshness_budget),
        }

        manifest, data_block = self._check_data(coverage, observed)
        manifest_id = None if manifest is None else str(manifest["id"])
        if data_block is not None:
            return self._blocked(coverage, evaluated_at, manifest_id, observed, *data_block)

        watermark_block = self._check_watermark(coverage, evaluated_at, observed)
        if watermark_block is not None:
            return self._blocked(coverage, evaluated_at, manifest_id, observed, *watermark_block)

        feature_block = self._check_features(feature_batch, observed)
        if feature_block is not None:
            return self._blocked(coverage, evaluated_at, manifest_id, observed, *feature_block)

        return WarmupReadiness(
            state=READY,
            session_date_et=coverage.session_date_et,
            feed_id=self._feed_id,
            evaluated_at=evaluated_at,
            manifest_id=manifest_id,
            observed=observed,
        )

    def _blocked(
        self,
        coverage: WarmupCoverage,
        evaluated_at: datetime,
        manifest_id: str | None,
        observed: Mapping[str, Any],
        reason: WarmupBlockReason,
        detail: str,
    ) -> WarmupReadiness:
        LOGGER.warning(
            "d90.warmup.blocked",
            extra={
                "feed_id": self._feed_id,
                "session_date_et": coverage.session_date_et,
                "reason_code": str(reason),
            },
        )
        return WarmupReadiness(
            state=BLOCKED,
            session_date_et=coverage.session_date_et,
            feed_id=self._feed_id,
            evaluated_at=evaluated_at,
            reason_code=str(reason),
            detail=detail,
            manifest_id=manifest_id,
            observed=observed,
        )

    # -- the three checks -------------------------------------------------------------

    def _check_data(
        self, coverage: WarmupCoverage, observed: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, tuple[WarmupBlockReason, str] | None]:
        contract = coverage.contract
        candidates = [
            row
            for row in self._catalog.records(DATASET_MANIFESTS)
            if str(row["feed_id"]) == self._feed_id
            and str(row["data_layer"]) == contract.data_layer
            and str(row["resolution"]) == contract.resolution
        ]
        if not candidates:
            observed["manifest_count"] = 0
            return None, (
                WarmupBlockReason.DAILY_OBJECT_MISSING,
                f"no dataset manifest exists for feed {self._feed_id} "
                f"({contract.data_layer}/{contract.resolution}) covering {coverage.session_date_et}",
            )

        available = [row for row in candidates if str(row["status"]) == "AVAILABLE"]
        observed["manifest_count"] = len(candidates)
        if not available:
            statuses = sorted({str(row["status"]) for row in candidates})
            return None, (
                WarmupBlockReason.MANIFEST_NOT_AVAILABLE,
                f"the only manifests for {coverage.session_date_et} are {statuses}; "
                "C starts only from AVAILABLE",
            )

        manifest = max(available, key=lambda row: int(row["revision_number"]))
        observed["manifest_id"] = str(manifest["id"])
        observed["manifest_revision"] = int(manifest["revision_number"])

        start, end = coverage.bounds
        covered = {
            str(item["shard_key"])
            for item in self._catalog.objects_for_manifest(str(manifest["id"]))
            if _as_date(item["partition_start"]) <= coverage.session < _as_date(item["partition_end"])
            and str(item["partition_granularity"]) == coverage.granularity
        }
        observed["covered_shards"] = sorted(covered)
        missing = [shard for shard in coverage.required_shards if shard not in covered]
        if missing:
            return manifest, (
                WarmupBlockReason.DAILY_OBJECT_MISSING,
                f"manifest {manifest['id']} has no {coverage.granularity} object covering "
                f"{coverage.session_date_et} ({start.isoformat()}..{end.isoformat()}) for "
                f"shard(s) {missing}",
            )
        return manifest, None

    def _check_watermark(
        self, coverage: WarmupCoverage, evaluated_at: datetime, observed: dict[str, Any]
    ) -> tuple[WarmupBlockReason, str] | None:
        stored = self._watermarks.load(self._feed_id)
        if stored is None:
            return (
                WarmupBlockReason.WATERMARK_MISSING,
                f"market_data.stream_watermarks has no row for feed {self._feed_id}; "
                "nothing has ever been ingested, so freshness is unknown rather than good",
            )
        observed["watermark_source_event_at"] = stored.position.isoformat()
        observed["watermark_sequence"] = stored.position.sequence
        observed["watermark_ingested_at"] = iso_utc(stored.ingested_at)

        age = evaluated_at - stored.ingested_at
        if age > self._freshness_budget:
            return (
                WarmupBlockReason.WATERMARK_STALE,
                f"feed {self._feed_id} last ingested at {iso_utc(stored.ingested_at)}, "
                f"{_iso_duration(age)} before {iso_utc(evaluated_at)}, which exceeds the "
                f"{_iso_duration(self._freshness_budget)} budget",
            )

        target = coverage.required_watermark_at
        observed["required_watermark_at"] = iso_utc(target)
        if stored.position.source_event_at < target:
            return (
                WarmupBlockReason.WATERMARK_BEHIND_SESSION,
                f"feed {self._feed_id} has consumed only up to "
                f"{stored.position.isoformat()}, short of the {coverage.session_date_et} "
                f"completion target {iso_utc(target)}",
            )
        return None

    @staticmethod
    def _check_features(
        feature_batch: ConsumableFeatureBatch | None, observed: dict[str, Any]
    ) -> tuple[WarmupBlockReason, str] | None:
        if feature_batch is None:
            observed["feature_batch_checked"] = False
            return None
        observed["feature_batch_checked"] = True
        try:
            feature_batch.consume()
        except SnapshotBatchNotConsumable as error:
            message = str(error)
            reason = (
                WarmupBlockReason.FEATURE_BATCH_MISSING
                if _NEVER_OPENED_MARKER in message
                else WarmupBlockReason.FEATURE_BATCH_INCOMPLETE
            )
            return reason, message
        except PartialSnapshotBatch as error:
            return WarmupBlockReason.FEATURE_BATCH_INCOMPLETE, str(error)
        return None

    # -- making the verdict observable ------------------------------------------------

    def record(self, readiness: WarmupReadiness, coverage: WarmupCoverage) -> str | None:
        """Write the verdict to the database projection a gate is read from.

        Returns the ``market_data.quality_incidents`` row id when the verdict is a
        block, and ``None`` when it is ``READY`` -- which means "no incident was
        written", never "written successfully".  A ready verdict changes nothing on
        purpose: this method's job is to make a block visible, not to re-publish a
        manifest that is already correct.
        """

        if not readiness.blocked:
            return None

        period_start, period_end = coverage.period()
        start, end = coverage.bounds
        shards = _blocked_shards(readiness, coverage)
        if shards is None:
            scope = ImpactScope(
                breadth=ScopeBreadth.MANIFEST,
                period_start=period_start,
                period_end=period_end,
                manifest_wide_reason=(
                    f"{readiness.reason_code} is a feed-wide fact: market_data."
                    "stream_watermarks is keyed by feed_id alone and the feature batch is "
                    "planned per feed, so neither can be attributed to one shard"
                ),
            )
        else:
            scope = ImpactScope(
                breadth=ScopeBreadth.PARTITION,
                period_start=period_start,
                period_end=period_end,
                shard_key=shards[0],
                partition_start=start,
                partition_end=end,
            )

        incident = QualityIncident(
            incident_code=str(readiness.reason_code),
            severity=BLOCK_SEVERITY,
            scope=scope,
            detected_at=readiness.evaluated_at,
            message=readiness.detail or "",
        )
        row = incident.to_db_row(dataset_manifest_id=readiness.manifest_id)
        self._catalog.record_quality_incident(row)
        self._quarantine(readiness.manifest_id)
        return str(row["id"])

    def _quarantine(self, manifest_id: str | None) -> None:
        """Flip the manifest C would otherwise read to ``QUARANTINED``.

        This is the half of the block C already honours: its status check refuses any
        manifest that is not ``AVAILABLE``.  Nothing is written when there is no
        manifest -- an absent manifest already blocks, and inventing a row for a
        session that produced nothing would misreport it as having been built.
        """

        if manifest_id is None:
            return
        for row in self._catalog.records(DATASET_MANIFESTS):
            if str(row["id"]) != manifest_id:
                continue
            if str(row["status"]) == "QUARANTINED":
                return
            self._catalog.upsert(DATASET_MANIFESTS, {**row, "status": "QUARANTINED"})
            return
        raise LookupError(
            f"{DATASET_MANIFESTS} row {manifest_id} vanished between evaluation and recording"
        )


def _blocked_shards(readiness: WarmupReadiness, coverage: WarmupCoverage) -> Sequence[str] | None:
    """The shards a block is about, or ``None`` when it is feed-wide."""

    if readiness.reason_code != str(WarmupBlockReason.DAILY_OBJECT_MISSING):
        return None
    covered = set(readiness.observed.get("covered_shards", ()))
    missing = [shard for shard in coverage.required_shards if shard not in covered]
    return missing or None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _iso_duration(delta: timedelta) -> str:
    """An ISO-8601 duration, so a budget in a log line is unambiguous."""

    seconds = int(delta.total_seconds())
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    body = "".join(
        part
        for part in (
            f"{hours}H" if hours else "",
            f"{minutes}M" if minutes else "",
            f"{seconds}S" if seconds or not (hours or minutes) else "",
        )
    )
    return f"{sign}PT{body}"
