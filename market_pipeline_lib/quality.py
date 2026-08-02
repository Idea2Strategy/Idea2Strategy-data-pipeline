"""Quality incidents that carry their real impact scope.

Card **D10**: "누락 bar, 중복, 역순, 비정상 가격·거래량, 세션 불일치와 checksum
실패를 *영향 범위와 함께* 기록한다."

The six check families are detected here and rendered as :class:`QualityIncident`
values whose fields map one-to-one onto ``market_data.quality_incidents`` in
``db/schema.dbml``.  The point of this module is the *scope*: before it existed
every incident was written with ``instrument_id = NULL`` and the whole manifest
year as its period, so "one bad bar in one shard of one day" and "the year is
broken" were the same row.

Scope model
-----------
:class:`ImpactScope` refuses to represent a claim wider than the evidence:

* ``BAR_RANGE`` / ``INSTRUMENT_PARTITION`` must name an instrument, a shard and a
  partition, and their period must lie **inside** that partition's UTC window.
  Constructing a single-bar finding with a year-long period raises.
* ``OBJECT`` must carry ``evidence_object_id`` -- that is what makes a checksum
  failure actionable.
* ``PARTITION`` may not name an instrument (it is a claim about every row).
* ``MANIFEST`` is the only breadth that may span a whole manifest, it may not
  carry a shard or partition, and it must state, in
  ``manifest_wide_reason``, why nothing narrower is knowable.  It is reachable
  only through :meth:`ImpactScope.manifest_wide`, so "somewhere in this year"
  can never be produced by accident.

Policies
--------
Every threshold is a named, versioned value (``PRICE_OUTLIER_POLICY``,
``VOLUME_POLICY``, ``MISSING_BAR_POLICY``, ``ORDERING_POLICY``), and the version
travels with each finding in ``Finding.policy_version`` so a recorded incident
can be read back against the policy that produced it.

Wiring
------
This module is pure and side-effect free.  ``processing.quality_issues`` /
``processing.quality_findings`` call the detectors; ``engine.py`` and
``operations.py`` are owned by other stages and must be changed to call
:func:`scoped_incidents` and :meth:`QualityIncident.to_db_row`.  The
``market_pipeline_lib.db`` SQLAlchemy Core table is the persistence target:
``to_db_row`` emits exactly :data:`QUALITY_INCIDENT_COLUMNS`, in that order.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from functools import lru_cache
from typing import Any, Literal, Protocol, runtime_checkable

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from .contracts import CALENDAR_NAME, ET, deterministic_uuid, iso_utc

__all__ = [
    "BAR_SPAN_MINUTES",
    "MISSING_BAR_POLICY",
    "ORDERING_POLICY",
    "PRICE_OUTLIER_POLICY",
    "QUALITY_INCIDENT_COLUMNS",
    "QUALITY_POLICY_VERSION",
    "VOLUME_POLICY",
    "Finding",
    "ImpactScope",
    "MissingBarPolicy",
    "MissingInterval",
    "OrderingPolicy",
    "PriceOutlierPolicy",
    "QualityIncident",
    "QualityIncidentRecorder",
    "ScopeBreadth",
    "Severity",
    "VolumePolicy",
    "bar_span",
    "content_hash_mismatch_incident",
    "count_missing_bar_intervals",
    "detect_duplicate_bars",
    "detect_invalid_values",
    "detect_missing_bars",
    "detect_out_of_order_bars",
    "detect_partition_boundary_violation",
    "detect_price_outliers",
    "detect_session_date_mismatch",
    "detect_volume_anomalies",
    "expected_session_bar_starts",
    "incident_from_issue",
    "incident_report_from_issue",
    "incident_row_from_issue",
    "missing_bar_intervals",
    "partition_utc_window",
    "record_issue_incidents",
    "record_quality_incidents",
    "scoped_incidents",
]

Severity = Literal["ERROR", "WARNING", "INFO"]

_SEVERITIES: frozenset[str] = frozenset({"ERROR", "WARNING", "INFO"})

#: Umbrella version for the structural checks that have no numeric threshold
#: (schema, duplicate, OHLC relation, session-date agreement, partition bounds).
QUALITY_POLICY_VERSION = "market-data-quality:1.0.0"

#: Canonical column list of ``market_data.quality_incidents`` (schema.dbml).
#: ``period_start`` and ``detected_at`` are NOT NULL there; the value object
#: therefore always produces them.
QUALITY_INCIDENT_COLUMNS: tuple[str, ...] = (
    "id",
    "dataset_manifest_id",
    "instrument_id",
    "severity",
    "incident_code",
    "period_start",
    "period_end",
    "status",
    "evidence_object_id",
    "detected_at",
    "resolved_at",
)

INCIDENT_STATUS_ACTIVE = "ACTIVE"

#: Wall-clock span one bar of each resolution covers.  ``1d`` is 390 minutes --
#: the standard 6.5-hour XNYS regular session, matching the derived-bar ceiling
#: already used by the ``source_minutes`` checks -- rather than 24 hours, so a
#: daily bar's recorded period stays inside its own day partition.
BAR_SPAN_MINUTES: dict[str, int] = {"30m": 30, "1h": 60, "4h": 240, "1d": 390}


def bar_span(resolution: str) -> pd.Timedelta:
    try:
        return pd.Timedelta(minutes=BAR_SPAN_MINUTES[resolution])
    except KeyError as exc:  # pragma: no cover - contracts restrict the values
        raise ValueError(f"지원하지 않는 해상도: {resolution}") from exc


# ---------------------------------------------------------------------------
# Policies.  Every threshold below is named, versioned and justified in place.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceOutlierPolicy:
    """When a *valid* price is nonetheless *abnormal*.

    ``max_abs_log_return`` -- absolute natural-log return between two
    consecutive bars **of the same session**.  0.30 is roughly a 35% move inside
    one bar.  LULD (Reg NMS Plan to Address Extraordinary Market Volatility)
    caps a tier-1 NMS stock at 5%/10% bands during regular hours, so a
    single-bar move of this size cannot happen without a series of limit states
    or a halt; an unapplied split or a bad print is by far the likelier cause.
    Cross-session pairs are exempt: overnight and weekend gaps are legitimately
    large and belong to corporate actions, not to tick hygiene.

    ``max_intrabar_range_ratio`` -- ``(high - low) / low`` within one bar.  0.20
    is the widest LULD band applied to any security during regular hours, so a
    single bar spanning more than that has crossed a band that should have
    paused trading first.

    Both are WARNING, never ERROR: an abnormal-but-valid bar is evidence to
    review, and quarantining on it would discard data the pipeline can neither
    reconstruct nor prove wrong.
    """

    version: str
    max_abs_log_return: float
    max_intrabar_range_ratio: float


PRICE_OUTLIER_POLICY = PriceOutlierPolicy(
    version="price-outlier:1.0.0",
    max_abs_log_return=0.30,
    max_intrabar_range_ratio=0.20,
)


@dataclass(frozen=True)
class VolumePolicy:
    """Zero-volume and volume-spike detection alongside the negative check.

    ``spike_multiple`` -- a bar is a spike when its volume exceeds
    ``spike_multiple x median volume`` of the same instrument in the same
    partition.  The **median** is the reference, not the mean, so the very bar
    being tested cannot inflate its own threshold.  50x is deliberately
    conservative: the open and close humps of a normal US equity session, and
    earnings or index-rebalance days, stay far below it, so what remains is the
    mis-scaled or duplicated aggregate this check exists to catch.

    ``spike_reference_min_bars`` -- below 20 bars in a partition the median is
    not a usable reference, so no spike is claimed rather than a guess made.
    """

    version: str
    spike_multiple: float
    spike_reference_min_bars: int


VOLUME_POLICY = VolumePolicy(
    version="volume-anomaly:1.0.0",
    spike_multiple=50.0,
    spike_reference_min_bars=20,
)


@dataclass(frozen=True)
class MissingBarPolicy:
    """Session-calendar gaps in the base RAW/ADJUSTED layer.

    WARNING, not ERROR: a gap is a fact about the provider's coverage.  The
    pipeline must record it with its exact interval, not throw away the bars it
    did receive.  Expectation is bounded by the first and last observed bar of
    each instrument, so a partially collected partition is not reported as one
    enormous hole.
    """

    version: str
    severity: Severity


MISSING_BAR_POLICY = MissingBarPolicy(version="missing-bar:1.0.0", severity="WARNING")


@dataclass(frozen=True)
class OrderingPolicy:
    """Bars delivered out of ascending time order, per instrument.

    WARNING: sorting repairs it losslessly, so it must not quarantine data.  It
    is still recorded, because it is a provider contract violation and it breaks
    any consumer that streams the input without sorting first.
    """

    version: str
    severity: Severity


ORDERING_POLICY = OrderingPolicy(version="bar-ordering:1.0.0", severity="WARNING")


# ---------------------------------------------------------------------------
# Impact scope
# ---------------------------------------------------------------------------


class ScopeBreadth(StrEnum):
    """How much data one incident actually claims to be about."""

    BAR_RANGE = "BAR_RANGE"
    INSTRUMENT_PARTITION = "INSTRUMENT_PARTITION"
    OBJECT = "OBJECT"
    PARTITION = "PARTITION"
    MANIFEST = "MANIFEST"


_INSTRUMENT_REQUIRED = frozenset({ScopeBreadth.BAR_RANGE, ScopeBreadth.INSTRUMENT_PARTITION})
_INSTRUMENT_FORBIDDEN = frozenset({ScopeBreadth.PARTITION, ScopeBreadth.MANIFEST})


def partition_utc_window(partition_start: date, partition_end: date) -> tuple[datetime, datetime]:
    """The half-open UTC window a `[start, end)` ET partition covers."""
    return (
        datetime.combine(partition_start, time.min, ET).astimezone(UTC),
        datetime.combine(partition_end, time.min, ET).astimezone(UTC),
    )


@dataclass(frozen=True)
class ImpactScope:
    """Exactly which rows an incident is about. Never wider than the evidence."""

    breadth: ScopeBreadth
    period_start: datetime
    period_end: datetime
    instrument_id: str | None = None
    shard_key: str | None = None
    partition_start: date | None = None
    partition_end: date | None = None
    affected_bar_count: int = 0
    evidence_object_id: str | None = None
    manifest_wide_reason: str = ""

    def __post_init__(self) -> None:
        if self.period_start.tzinfo is None or self.period_end.tzinfo is None:
            raise ValueError("impact scope의 period는 timezone-aware여야 합니다.")
        object.__setattr__(self, "period_start", self.period_start.astimezone(UTC))
        object.__setattr__(self, "period_end", self.period_end.astimezone(UTC))
        if self.period_end <= self.period_start:
            raise ValueError(
                "impact scope의 period_end는 period_start보다 뒤여야 합니다: "
                f"{self.period_start.isoformat()}~{self.period_end.isoformat()}"
            )
        if self.affected_bar_count < 0:
            raise ValueError("affected_bar_count는 음수일 수 없습니다.")

        breadth = self.breadth
        if breadth in _INSTRUMENT_REQUIRED:
            if not self.instrument_id:
                raise ValueError(f"{breadth} scope에는 instrument_id가 필요합니다.")
            if self.affected_bar_count < 1:
                raise ValueError(f"{breadth} scope에는 affected_bar_count가 1 이상이어야 합니다.")
        if breadth in _INSTRUMENT_FORBIDDEN and self.instrument_id:
            raise ValueError(f"{breadth} scope는 단일 instrument를 지목할 수 없습니다.")
        if breadth is ScopeBreadth.OBJECT and not self.evidence_object_id:
            raise ValueError("OBJECT scope에는 evidence_object_id가 필요합니다.")

        if breadth is ScopeBreadth.MANIFEST:
            if self.shard_key or self.partition_start is not None or self.partition_end is not None:
                raise ValueError("MANIFEST scope는 shard_key/partition 경계를 가질 수 없습니다.")
            if not self.manifest_wide_reason.strip():
                raise ValueError(
                    "MANIFEST scope는 더 좁은 범위를 쓸 수 없는 이유를 "
                    "manifest_wide_reason에 명시해야 합니다."
                )
            return

        if self.manifest_wide_reason:
            raise ValueError("manifest_wide_reason은 MANIFEST scope에서만 쓸 수 있습니다.")
        if not self.shard_key:
            raise ValueError(f"{breadth} scope에는 shard_key가 필요합니다.")
        if self.partition_start is None or self.partition_end is None:
            raise ValueError(f"{breadth} scope에는 partition 경계가 필요합니다.")
        if self.partition_end <= self.partition_start:
            raise ValueError("partition_end는 partition_start보다 뒤여야 합니다.")
        lower, upper = partition_utc_window(self.partition_start, self.partition_end)
        if self.period_start < lower or self.period_end > upper:
            raise ValueError(
                "impact scope의 period가 partition 범위를 벗어났습니다: "
                f"period={self.period_start.isoformat()}~{self.period_end.isoformat()}, "
                f"partition={lower.isoformat()}~{upper.isoformat()}"
            )

    @classmethod
    def manifest_wide(
        cls,
        *,
        period_start: datetime,
        period_end: datetime,
        reason: str,
        evidence_object_id: str | None = None,
        affected_bar_count: int = 0,
    ) -> ImpactScope:
        """The deliberate escape hatch. Requires a written reason."""
        return cls(
            breadth=ScopeBreadth.MANIFEST,
            period_start=period_start,
            period_end=period_end,
            evidence_object_id=evidence_object_id,
            affected_bar_count=affected_bar_count,
            manifest_wide_reason=reason,
        )


# ---------------------------------------------------------------------------
# The incident value object
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QualityIncident:
    """One ``market_data.quality_incidents`` row, with its scope attached."""

    incident_code: str
    severity: Severity
    scope: ImpactScope
    detected_at: datetime
    message: str = ""
    policy_version: str = QUALITY_POLICY_VERSION
    status: str = INCIDENT_STATUS_ACTIVE
    resolved_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.incident_code.strip():
            raise ValueError("incident_code는 비어 있을 수 없습니다.")
        if self.severity not in _SEVERITIES:
            raise ValueError(f"알 수 없는 severity: {self.severity}")
        if self.detected_at.tzinfo is None:
            raise ValueError("detected_at은 timezone-aware여야 합니다.")
        if not self.status.strip():
            raise ValueError("status는 비어 있을 수 없습니다.")
        if self.resolved_at is not None:
            if self.resolved_at.tzinfo is None:
                raise ValueError("resolved_at은 timezone-aware여야 합니다.")
            if self.status == INCIDENT_STATUS_ACTIVE:
                raise ValueError("ACTIVE 인시던트는 resolved_at을 가질 수 없습니다.")

    def row_id(self, dataset_manifest_id: str | None) -> str:
        """Deterministic identity.

        Every scope dimension is part of the salt.  Two findings that differ in
        instrument, shard, partition, period or evidence object are two rows, so
        an upsert can no longer collapse "AAPL lost 13:30" and "MSFT lost the
        whole afternoon" into a single record.
        """
        return deterministic_uuid(
            "quality-incident",
            dataset_manifest_id,
            self.incident_code,
            self.scope.breadth.value,
            self.scope.instrument_id,
            self.scope.shard_key,
            self.scope.partition_start,
            self.scope.partition_end,
            iso_utc(self.scope.period_start),
            iso_utc(self.scope.period_end),
            self.scope.evidence_object_id,
        )

    def to_db_row(self, *, dataset_manifest_id: str | None = None) -> dict[str, Any]:
        """Exactly :data:`QUALITY_INCIDENT_COLUMNS`, in that order, JSON-safe."""
        return {
            "id": self.row_id(dataset_manifest_id),
            "dataset_manifest_id": dataset_manifest_id,
            "instrument_id": self.scope.instrument_id,
            "severity": self.severity,
            "incident_code": self.incident_code,
            "period_start": iso_utc(self.scope.period_start),
            "period_end": iso_utc(self.scope.period_end),
            "status": self.status,
            "evidence_object_id": self.scope.evidence_object_id,
            "detected_at": iso_utc(self.detected_at),
            "resolved_at": iso_utc(self.resolved_at) if self.resolved_at else None,
        }

    def to_report_entry(self, *, dataset_manifest_id: str | None = None) -> dict[str, Any]:
        """The DB row plus the non-column context a human report needs."""
        return {
            **self.to_db_row(dataset_manifest_id=dataset_manifest_id),
            "message": self.message,
            "policy_version": self.policy_version,
            "scope_breadth": self.scope.breadth.value,
            "shard_key": self.scope.shard_key,
            "partition_start": (
                self.scope.partition_start.isoformat() if self.scope.partition_start else None
            ),
            "partition_end": (
                self.scope.partition_end.isoformat() if self.scope.partition_end else None
            ),
            "affected_bar_count": self.scope.affected_bar_count,
            "manifest_wide_reason": self.scope.manifest_wide_reason or None,
        }


def content_hash_mismatch_incident(
    *,
    object_id: str,
    object_key: str,
    expected_content_hash: str,
    actual_content_hash: str,
    shard_key: str,
    partition_start: date,
    partition_end: date,
    period_start: datetime,
    period_end: datetime,
    detected_at: datetime,
    instrument_id: str | None = None,
) -> QualityIncident:
    """A checksum failure, modelled as a persistable incident.

    ``operations.validate_catalog`` currently appends ``CONTENT_HASH_MISMATCH``
    to a local ``validation-report.json`` and nothing else; the row never
    reaches ``market_data.quality_incidents``.  This constructor gives that
    finding the same shape as every other incident, with the failing object as
    ``evidence_object_id`` -- the one column that makes it actionable.
    """
    return QualityIncident(
        incident_code="CONTENT_HASH_MISMATCH",
        severity="ERROR",
        scope=ImpactScope(
            breadth=ScopeBreadth.OBJECT,
            period_start=period_start,
            period_end=period_end,
            instrument_id=instrument_id,
            shard_key=shard_key,
            partition_start=partition_start,
            partition_end=partition_end,
            evidence_object_id=object_id,
        ),
        detected_at=detected_at,
        message=(
            f"{object_key} 체크섬 불일치: expected={expected_content_hash}, "
            f"actual={actual_content_hash}"
        ),
    )


# ---------------------------------------------------------------------------
# Findings: a detected defect before a shard/partition is attached
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    """A detected defect. Carries every scope dimension the data itself knows.

    ``period_start``/``period_end`` are ``None`` only when the defect has no
    row-level extent (a schema mismatch); :meth:`with_scope` then falls back to
    the partition window rather than to the manifest.
    """

    code: str
    severity: Severity
    message: str
    policy_version: str
    breadth: ScopeBreadth = ScopeBreadth.PARTITION
    instrument_id: str | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None
    affected_bar_count: int = 0

    def as_issue(self) -> dict[str, Any]:
        """Flat JSON-safe dict, a superset of the legacy issue shape.

        ``severity``/``code``/``message`` keep their old names and meaning so
        ``engine.py`` and ``operations.py`` keep working unchanged; the scope
        keys are additive.
        """
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "scope_breadth": self.breadth.value,
            "instrument_id": self.instrument_id,
            "period_start": iso_utc(self.period_start) if self.period_start else None,
            "period_end": iso_utc(self.period_end) if self.period_end else None,
            "affected_bar_count": self.affected_bar_count,
            "policy_version": self.policy_version,
        }

    def with_scope(
        self,
        *,
        shard_key: str,
        partition_start: date,
        partition_end: date,
        detected_at: datetime,
        evidence_object_id: str | None = None,
    ) -> QualityIncident:
        """Attach the shard/partition the finding was produced in.

        The recorded period is clamped to the partition window, so attaching a
        scope can only ever narrow a claim.  A finding whose period lies wholly
        outside its partition (the ``PARTITION_BOUNDARY_VIOLATION`` case) is
        recorded against the partition itself rather than being dropped.
        """
        lower, upper = partition_utc_window(partition_start, partition_end)
        start = self.period_start or lower
        end = self.period_end or upper
        clamped_start = max(start, lower)
        clamped_end = min(end, upper)

        breadth = self.breadth
        instrument_id = self.instrument_id
        message = self.message
        if clamped_end <= clamped_start:
            clamped_start, clamped_end = lower, upper
            breadth = ScopeBreadth.PARTITION
            instrument_id = None
            message = (
                f"{message} (원래 period "
                f"{start.isoformat()}~{end.isoformat()}가 partition 밖이라 "
                "partition 범위로 기록)"
            )
        if breadth in _INSTRUMENT_REQUIRED and not instrument_id:
            breadth = ScopeBreadth.PARTITION
        if breadth in _INSTRUMENT_FORBIDDEN:
            instrument_id = None
        if breadth is ScopeBreadth.OBJECT and not evidence_object_id:
            breadth = ScopeBreadth.PARTITION
            instrument_id = None

        affected = self.affected_bar_count
        if breadth in _INSTRUMENT_REQUIRED:
            affected = max(1, affected)

        return QualityIncident(
            incident_code=self.code,
            severity=self.severity,
            scope=ImpactScope(
                breadth=breadth,
                period_start=clamped_start,
                period_end=clamped_end,
                instrument_id=instrument_id,
                shard_key=shard_key,
                partition_start=partition_start,
                partition_end=partition_end,
                affected_bar_count=affected,
                evidence_object_id=evidence_object_id,
            ),
            detected_at=detected_at,
            message=message,
            policy_version=self.policy_version,
        )


def scoped_incidents(
    findings: Iterable[Finding],
    *,
    shard_key: str,
    partition_start: date,
    partition_end: date,
    detected_at: datetime,
    evidence_object_id: str | None = None,
) -> list[QualityIncident]:
    """Attach one shard/partition to a batch of findings."""
    return [
        finding.with_scope(
            shard_key=shard_key,
            partition_start=partition_start,
            partition_end=partition_end,
            detected_at=detected_at,
            evidence_object_id=evidence_object_id,
        )
        for finding in findings
    ]


# ---------------------------------------------------------------------------
# Persistence
#
# The incidents table is reached through the narrowest interface that can do
# the job.  ``LocalCatalog`` and ``PostgresCatalog`` (owned by another stage)
# already expose exactly this one method, so both satisfy the protocol without
# any change, and neither this module nor its tests need to know which one is
# in play.
# ---------------------------------------------------------------------------


@runtime_checkable
class QualityIncidentRecorder(Protocol):
    """Anything that can persist one ``market_data.quality_incidents`` row."""

    def record_quality_incident(self, record: Mapping[str, Any]) -> None: ...


def _issue_datetime(value: Any) -> datetime | None:
    """Parse a period bound as written by :meth:`Finding.as_issue`."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("issue의 period는 timezone-aware여야 합니다.")
        return value.astimezone(UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"issue의 period에 시간대가 없습니다: {value!r}")
    return parsed.astimezone(UTC)


def _issue_date(value: Any, *, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not value:
        raise ValueError(
            f"issue에 {field}가 없어 impact scope를 좁힐 수 없습니다. "
            "매니페스트 전체로 넓히는 대신 거부합니다."
        )
    return date.fromisoformat(str(value))


def incident_from_issue(
    issue: Mapping[str, Any],
    *,
    detected_at: datetime,
    evidence_object_id: str | None = None,
) -> QualityIncident:
    """Rebuild a scoped incident from the flat issue dict the engine carries.

    ``engine.py`` collects :meth:`Finding.as_issue` dicts and merges
    ``shard_key``/``partition_start``/``partition_end`` onto each one before
    recording it.  That dict already holds every scope dimension; this function
    is the inverse of ``as_issue`` so the scope survives instead of being
    replaced by ``instrument_id=None`` plus the manifest's whole period.

    An issue with no shard or partition raises: silently widening it back to the
    manifest is the exact defect card D10 exists to remove.
    """
    finding = Finding(
        code=str(issue["code"]),
        severity=issue["severity"],
        message=str(issue.get("message", "")),
        policy_version=str(issue.get("policy_version") or QUALITY_POLICY_VERSION),
        breadth=ScopeBreadth(str(issue.get("scope_breadth") or ScopeBreadth.PARTITION)),
        instrument_id=issue.get("instrument_id"),
        period_start=_issue_datetime(issue.get("period_start")),
        period_end=_issue_datetime(issue.get("period_end")),
        affected_bar_count=int(issue.get("affected_bar_count") or 0),
    )
    shard_key = issue.get("shard_key")
    if not shard_key:
        raise ValueError(
            f"issue에 shard_key가 없어 impact scope를 좁힐 수 없습니다: {finding.code}"
        )
    return finding.with_scope(
        shard_key=str(shard_key),
        partition_start=_issue_date(issue.get("partition_start"), field="partition_start"),
        partition_end=_issue_date(issue.get("partition_end"), field="partition_end"),
        detected_at=detected_at,
        evidence_object_id=evidence_object_id or issue.get("object_id"),
    )


def incident_row_from_issue(
    issue: Mapping[str, Any],
    *,
    dataset_manifest_id: str | None,
    detected_at: datetime,
    evidence_object_id: str | None = None,
) -> dict[str, Any]:
    """One ``quality_incidents`` row, ready for ``record_quality_incident``."""
    return incident_from_issue(
        issue,
        detected_at=detected_at,
        evidence_object_id=evidence_object_id,
    ).to_db_row(dataset_manifest_id=dataset_manifest_id)


def incident_report_from_issue(
    issue: Mapping[str, Any],
    *,
    dataset_manifest_id: str | None,
    detected_at: datetime,
    evidence_object_id: str | None = None,
) -> dict[str, Any]:
    """The row plus the shard/partition context a human report needs."""
    return incident_from_issue(
        issue,
        detected_at=detected_at,
        evidence_object_id=evidence_object_id,
    ).to_report_entry(dataset_manifest_id=dataset_manifest_id)


def record_quality_incidents(
    recorder: QualityIncidentRecorder,
    incidents: Iterable[QualityIncident],
    *,
    dataset_manifest_id: str | None,
) -> int:
    """Persist incidents, de-duplicated by their deterministic row id.

    Returns the number of rows written.  Two findings that differ in any scope
    dimension have different ids and are therefore two rows; re-running the same
    validation produces the same ids and so upserts in place.
    """
    written = 0
    seen: set[str] = set()
    for incident in incidents:
        row = incident.to_db_row(dataset_manifest_id=dataset_manifest_id)
        if row["id"] in seen:
            continue
        seen.add(str(row["id"]))
        recorder.record_quality_incident(row)
        written += 1
    return written


def record_issue_incidents(
    recorder: QualityIncidentRecorder,
    issues: Iterable[Mapping[str, Any]],
    *,
    dataset_manifest_id: str | None,
    detected_at: datetime,
) -> int:
    """Scope a batch of engine issue dicts and persist them.

    This is the whole integration surface ``engine.py`` needs: it replaces the
    hand-built dict literal that hard-coded ``instrument_id=None`` and the
    manifest period.  Every issue is scoped before anything is written, so a
    batch containing one unscopable issue records nothing rather than half.
    """
    incidents = [
        incident_from_issue(issue, detected_at=detected_at) for issue in issues
    ]
    return record_quality_incidents(
        recorder, incidents, dataset_manifest_id=dataset_manifest_id
    )


# ---------------------------------------------------------------------------
# Session calendar helpers.
#
# Ported from ``data_validation/audit_regular_session.py`` (lines 38-152), which
# held the repository's only real per-gap impact-scope logic: expectation bounded
# by the observed edges, and gap grouping that never joins two sessions.  That
# module now delegates here so the logic exists once.
# ---------------------------------------------------------------------------


@lru_cache(maxsize=256)
def _session_windows(
    start: date,
    end: date,
    calendar_name: str,
) -> tuple[tuple[pd.Timestamp, pd.Timestamp], ...]:
    schedule = mcal.get_calendar(calendar_name).schedule(
        start_date=start,
        end_date=end,
        tz="UTC",
    )
    return tuple(
        (pd.Timestamp(market_open), pd.Timestamp(market_close))
        for market_open, market_close in schedule[
            ["market_open", "market_close"]
        ].itertuples(index=False, name=None)
    )


@lru_cache(maxsize=16)
def _calendar_timezone(calendar_name: str) -> Any:
    return mcal.get_calendar(calendar_name).tz


def expected_session_bar_starts(
    first_timestamp: pd.Timestamp,
    last_timestamp: pd.Timestamp,
    *,
    bar_frequency: pd.Timedelta,
    calendar_name: str = CALENDAR_NAME,
) -> pd.DatetimeIndex:
    """Regular-session bar starts between the observed boundaries, inclusive.

    Bounding by the observed edges is deliberate: a partition that was only
    partly collected must not be reported as one gap the size of the partition.
    """
    first = pd.Timestamp(first_timestamp).tz_convert("UTC")
    last = pd.Timestamp(last_timestamp).tz_convert("UTC")
    local = pd.DatetimeIndex([first, last]).tz_convert(_calendar_timezone(calendar_name))
    windows = _session_windows(local.min().date(), local.max().date(), calendar_name)
    values = [
        timestamp
        for market_open, market_close in windows
        for timestamp in pd.date_range(
            market_open,
            market_close,
            freq=bar_frequency,
            inclusive="left",
        )
    ]
    if not values:
        return pd.DatetimeIndex([], tz="UTC")
    expected = pd.DatetimeIndex(values)
    return expected[(expected >= first) & (expected <= last)]


@dataclass(frozen=True)
class MissingInterval:
    """One contiguous run of missing bars, inside a single session."""

    #: First missing bar start.
    start: datetime
    #: Exclusive end of the gap: last missing bar start plus one bar span.
    end: datetime
    #: Last missing bar start, i.e. the inclusive end of the gap.
    last_start: datetime
    bar_count: int
    session_date: date
    previous_observed: datetime | None
    next_observed: datetime | None


def _gap_boundaries(
    missing: pd.DatetimeIndex,
    bar_frequency: pd.Timedelta,
    calendar_name: str,
) -> list[tuple[int, int]]:
    """Index runs of adjacent missing bars that never join two sessions."""
    local_dates = missing.tz_convert(_calendar_timezone(calendar_name)).normalize()
    intervals: list[tuple[int, int]] = []
    interval_start = 0
    for position in range(1, len(missing)):
        same_session = local_dates[position] == local_dates[position - 1]
        consecutive = missing[position] - missing[position - 1] == bar_frequency
        if not (same_session and consecutive):
            intervals.append((interval_start, position - 1))
            interval_start = position
    intervals.append((interval_start, len(missing) - 1))
    return intervals


def missing_bar_intervals(
    missing: pd.DatetimeIndex,
    expected: pd.DatetimeIndex,
    observed: pd.DatetimeIndex,
    *,
    bar_frequency: pd.Timedelta,
    calendar_name: str = CALENDAR_NAME,
) -> list[MissingInterval]:
    """Group adjacent missing timestamps without joining separate sessions."""
    if missing.empty:
        return []
    timezone_name = _calendar_timezone(calendar_name)
    expected_set = set(expected)
    observed_set = set(observed)
    intervals: list[MissingInterval] = []
    for start_position, end_position in _gap_boundaries(missing, bar_frequency, calendar_name):
        missing_start = missing[start_position]
        missing_end = missing[end_position]
        previous_timestamp = missing_start - bar_frequency
        next_timestamp = missing_end + bar_frequency
        intervals.append(
            MissingInterval(
                start=missing_start.to_pydatetime(),
                end=(missing_end + bar_frequency).to_pydatetime(),
                last_start=missing_end.to_pydatetime(),
                bar_count=end_position - start_position + 1,
                session_date=missing_start.tz_convert(timezone_name).date(),
                previous_observed=(
                    previous_timestamp.to_pydatetime()
                    if previous_timestamp in expected_set and previous_timestamp in observed_set
                    else None
                ),
                next_observed=(
                    next_timestamp.to_pydatetime()
                    if next_timestamp in expected_set and next_timestamp in observed_set
                    else None
                ),
            )
        )
    return intervals


def count_missing_bar_intervals(
    missing: pd.DatetimeIndex,
    *,
    bar_frequency: pd.Timedelta,
    calendar_name: str = CALENDAR_NAME,
) -> int:
    """Count contiguous gaps without materialising the interval objects."""
    if missing.empty:
        return 0
    local_dates = missing.tz_convert(_calendar_timezone(calendar_name)).normalize()
    breaks = (local_dates[1:] != local_dates[:-1]) | (
        missing[1:] - missing[:-1] != bar_frequency
    )
    return 1 + int(breaks.sum())


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------


def _contiguous_spans(
    stamps: pd.DatetimeIndex,
    span: pd.Timedelta,
) -> list[tuple[pd.Timestamp, pd.Timestamp, int]]:
    """`(first_start, last_start + span, row_count)` per contiguous run.

    Two offending rows belong to the same run when their starts are equal (a
    duplicate) or exactly one bar apart.  Anything else opens a new run, so a
    defect at the start of a partition and a defect at its end are never merged
    into one claim that spans the whole partition.
    """
    if len(stamps) == 0:
        return []
    runs: list[tuple[pd.Timestamp, pd.Timestamp, int]] = []
    run_start = stamps[0]
    run_last = stamps[0]
    count = 1
    for current in stamps[1:]:
        delta = current - run_last
        if delta == pd.Timedelta(0) or delta == span:
            run_last = current
            count += 1
            continue
        runs.append((run_start, run_last + span, count))
        run_start = current
        run_last = current
        count = 1
    runs.append((run_start, run_last + span, count))
    return runs


def _row_findings(
    frame: pd.DataFrame,
    mask: pd.Series[bool],
    *,
    code: str,
    severity: Severity,
    policy_version: str,
    describe: Callable[[str, int, pd.Timestamp, pd.Timestamp], str],
    span: pd.Timedelta,
) -> list[Finding]:
    """One finding per contiguous run of offending rows, per instrument."""
    if not bool(mask.any()):
        return []
    selected = frame.loc[mask, ["instrument_id", "bar_start_at"]]
    findings: list[Finding] = []
    for instrument_id, group in selected.groupby("instrument_id", sort=True):
        stamps = pd.DatetimeIndex(group["bar_start_at"]).sort_values()
        for start, end, count in _contiguous_spans(stamps, span):
            findings.append(
                Finding(
                    code=code,
                    severity=severity,
                    message=describe(str(instrument_id), count, start, end),
                    policy_version=policy_version,
                    breadth=ScopeBreadth.BAR_RANGE,
                    instrument_id=str(instrument_id),
                    period_start=start.to_pydatetime(),
                    period_end=end.to_pydatetime(),
                    affected_bar_count=count,
                )
            )
    return findings


def detect_duplicate_bars(frame: pd.DataFrame, *, span: pd.Timedelta) -> list[Finding]:
    """중복: the same `(instrument_id, bar_start_at)` delivered more than once."""
    mask = frame.duplicated(["instrument_id", "bar_start_at"], keep=False)
    return _row_findings(
        frame,
        mask,
        code="DUPLICATE_BAR",
        severity="ERROR",
        policy_version=QUALITY_POLICY_VERSION,
        describe=lambda instrument, count, start, end: (
            f"{instrument} 중복 {count}행 {start.isoformat()}~{end.isoformat()}"
        ),
        span=span,
    )


def detect_out_of_order_bars(frame: pd.DataFrame, *, span: pd.Timedelta) -> list[Finding]:
    """역순: bars delivered in descending time order.

    Must run on the **unsorted** input.  ``processing.sort_bar_table`` destroys
    the evidence, so any caller that sorts before validating can never report
    this.
    """
    findings: list[Finding] = []
    for instrument_id, group in frame.groupby("instrument_id", sort=True):
        stamps = pd.DatetimeIndex(group["bar_start_at"])
        for position in range(1, len(stamps)):
            previous = stamps[position - 1]
            current = stamps[position]
            if current >= previous:
                continue
            findings.append(
                Finding(
                    code="OUT_OF_ORDER_BARS",
                    severity=ORDERING_POLICY.severity,
                    message=(
                        f"{instrument_id} 역순 입력: {position - 1}행 "
                        f"{previous.isoformat()} 다음에 {position}행 {current.isoformat()}"
                    ),
                    policy_version=ORDERING_POLICY.version,
                    breadth=ScopeBreadth.BAR_RANGE,
                    instrument_id=str(instrument_id),
                    period_start=current.to_pydatetime(),
                    period_end=(previous + span).to_pydatetime(),
                    affected_bar_count=2,
                )
            )
    return findings


def detect_invalid_values(frame: pd.DataFrame, *, span: pd.Timedelta) -> list[Finding]:
    """비정상 가격·거래량 (invalid half): unusable values, always ERROR."""
    findings: list[Finding] = []
    prices = frame[["open", "high", "low", "close"]]
    finite_positive = np.isfinite(prices.to_numpy()).all(axis=1) & (prices > 0).all(axis=1)
    findings.extend(
        _row_findings(
            frame,
            pd.Series(~finite_positive, index=frame.index),
            code="INVALID_PRICE",
            severity="ERROR",
            policy_version=QUALITY_POLICY_VERSION,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 가격 오류 {count}행 {start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    ohlc_ok = frame["high"].ge(frame[["open", "close", "low"]].max(axis=1)) & frame["low"].le(
        frame[["open", "close", "high"]].min(axis=1)
    )
    findings.extend(
        _row_findings(
            frame,
            ~ohlc_ok,
            code="INVALID_OHLC",
            severity="ERROR",
            policy_version=QUALITY_POLICY_VERSION,
            describe=lambda instrument, count, start, end: (
                f"{instrument} OHLC 관계 오류 {count}행 {start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    negative = frame["volume"].lt(0) | (
        frame["trade_count"].notna() & frame["trade_count"].lt(0)
    )
    findings.extend(
        _row_findings(
            frame,
            negative,
            code="NEGATIVE_ACTIVITY",
            severity="ERROR",
            policy_version=QUALITY_POLICY_VERSION,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 음수 volume/trade_count {count}행 "
                f"{start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    return findings


def detect_price_outliers(
    frame: pd.DataFrame,
    *,
    span: pd.Timedelta,
    policy: PriceOutlierPolicy = PRICE_OUTLIER_POLICY,
) -> list[Finding]:
    """비정상 가격 (abnormal half): valid numbers that cannot plausibly be real."""
    findings: list[Finding] = []

    ordered = frame.sort_values(["instrument_id", "bar_start_at"], kind="mergesort")
    close = ordered["close"].to_numpy(dtype="float64")
    previous_close = np.roll(close, 1)
    same_instrument = ordered["instrument_id"].eq(ordered["instrument_id"].shift(1)).to_numpy()
    same_session = ordered["session_date_et"].eq(ordered["session_date_et"].shift(1)).to_numpy()
    comparable = (
        same_instrument
        & same_session
        & np.isfinite(close)
        & np.isfinite(previous_close)
        & (close > 0)
        & (previous_close > 0)
    )
    ratio = np.where(comparable, close / np.where(previous_close > 0, previous_close, 1.0), 1.0)
    offending = comparable & (np.abs(np.log(ratio)) > policy.max_abs_log_return)
    return_mask = pd.Series(False, index=frame.index)
    return_mask.loc[ordered.index[offending]] = True
    findings.extend(
        _row_findings(
            frame,
            return_mask,
            code="PRICE_OUTLIER_RETURN",
            severity="WARNING",
            policy_version=policy.version,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 직전 봉 대비 |log수익률| > {policy.max_abs_log_return} "
                f"{count}행 {start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )

    low = frame["low"].to_numpy(dtype="float64")
    high = frame["high"].to_numpy(dtype="float64")
    usable = np.isfinite(low) & np.isfinite(high) & (low > 0)
    range_ratio = np.where(usable, (high - low) / np.where(low > 0, low, 1.0), 0.0)
    findings.extend(
        _row_findings(
            frame,
            pd.Series(usable & (range_ratio > policy.max_intrabar_range_ratio), index=frame.index),
            code="PRICE_OUTLIER_RANGE",
            severity="WARNING",
            policy_version=policy.version,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 봉 내 (high-low)/low > {policy.max_intrabar_range_ratio} "
                f"{count}행 {start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    return findings


def detect_volume_anomalies(
    frame: pd.DataFrame,
    *,
    span: pd.Timedelta,
    policy: VolumePolicy = VOLUME_POLICY,
) -> list[Finding]:
    """비정상 거래량: zero-volume runs and median-relative spikes."""
    findings: list[Finding] = _row_findings(
        frame,
        frame["volume"].eq(0),
        code="ZERO_VOLUME_BAR",
        severity="WARNING",
        policy_version=policy.version,
        describe=lambda instrument, count, start, end: (
            f"{instrument} 거래량 0인 봉 {count}행 {start.isoformat()}~{end.isoformat()}"
        ),
        span=span,
    )
    spike_mask = pd.Series(False, index=frame.index)
    for _, group in frame.groupby("instrument_id", sort=False):
        if len(group) < policy.spike_reference_min_bars:
            continue
        median = float(group["volume"].median())
        if median <= 0:
            continue
        threshold = median * policy.spike_multiple
        spike_mask.loc[group.index[group["volume"] > threshold]] = True
    findings.extend(
        _row_findings(
            frame,
            spike_mask,
            code="VOLUME_SPIKE",
            severity="WARNING",
            policy_version=policy.version,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 거래량이 파티션 중앙값의 {policy.spike_multiple}배 초과 "
                f"{count}행 {start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    return findings


def detect_session_date_mismatch(frame: pd.DataFrame, *, span: pd.Timedelta) -> list[Finding]:
    """세션 불일치: `session_date_et` disagrees with `bar_start_at` in ET."""
    expected = pd.DatetimeIndex(frame["bar_start_at"]).tz_convert(ET).date
    mismatch = pd.Series(expected != frame["session_date_et"].to_numpy(), index=frame.index)
    return _row_findings(
        frame,
        mismatch,
        code="SESSION_DATE_ET_MISMATCH",
        severity="ERROR",
        policy_version=QUALITY_POLICY_VERSION,
        describe=lambda instrument, count, start, end: (
            f"{instrument} bar_start_at과 session_date_et 불일치 {count}행 "
            f"{start.isoformat()}~{end.isoformat()}"
        ),
        span=span,
    )


def detect_partition_boundary_violation(
    frame: pd.DataFrame,
    partition_start: date,
    partition_end: date,
) -> list[Finding]:
    """Rows whose session date falls outside the partition they were written to."""
    outside = frame["session_date_et"].lt(partition_start) | frame["session_date_et"].ge(
        partition_end
    )
    count = int(outside.sum())
    if not count:
        return []
    offending = frame.loc[outside, "session_date_et"]
    return [
        Finding(
            code="PARTITION_BOUNDARY_VIOLATION",
            severity="ERROR",
            message=(
                f"파티션 [{partition_start.isoformat()}, {partition_end.isoformat()}) 밖 "
                f"{count}행: session_date_et {offending.min()}~{offending.max()}"
            ),
            policy_version=QUALITY_POLICY_VERSION,
            breadth=ScopeBreadth.PARTITION,
            affected_bar_count=count,
        )
    ]


def detect_missing_bars(
    frame: pd.DataFrame,
    *,
    span: pd.Timedelta,
    calendar_name: str = CALENDAR_NAME,
    policy: MissingBarPolicy = MISSING_BAR_POLICY,
) -> list[Finding]:
    """누락 bar: session-calendar gaps in the base layer, one finding per gap.

    Runs per instrument, so a gap in one symbol is never attributed to another,
    and each gap is recorded with its exact interval and bar count.
    """
    findings: list[Finding] = []
    for instrument_id, group in frame.groupby("instrument_id", sort=True):
        observed = pd.DatetimeIndex(group["bar_start_at"].unique()).sort_values()
        if len(observed) < 2:
            continue
        expected = expected_session_bar_starts(
            observed.min(),
            observed.max(),
            bar_frequency=span,
            calendar_name=calendar_name,
        )
        missing = expected.difference(observed)
        for interval in missing_bar_intervals(
            missing,
            expected,
            observed,
            bar_frequency=span,
            calendar_name=calendar_name,
        ):
            findings.append(
                Finding(
                    code="MISSING_BARS",
                    severity=policy.severity,
                    message=(
                        f"{instrument_id} {interval.session_date.isoformat()} 세션 "
                        f"{interval.bar_count}봉 누락 "
                        f"{interval.start.isoformat()}~{interval.end.isoformat()}"
                    ),
                    policy_version=policy.version,
                    breadth=ScopeBreadth.BAR_RANGE,
                    instrument_id=str(instrument_id),
                    period_start=interval.start,
                    period_end=interval.end,
                    affected_bar_count=interval.bar_count,
                )
            )
    return findings


def detect_derived_session_issues(
    frame: pd.DataFrame,
    *,
    resolution: str,
    span: pd.Timedelta,
    calendar_name: str = CALENDAR_NAME,
) -> list[Finding]:
    """DERIVED-only checks over `source_minutes` and the XNYS regular session."""
    timezone_name = _calendar_timezone(calendar_name)
    bounds = {
        market_open.tz_convert(timezone_name).date(): (market_open, market_close)
        for market_open, market_close in _session_windows(
            frame["session_date_et"].min(),
            frame["session_date_et"].max(),
            calendar_name,
        )
    }
    maximum = {"1h": 60, "4h": 240, "1d": 390}[resolution]
    outside_positions: list[Any] = []
    bad_source_positions: list[Any] = []
    missing_source_positions: list[Any] = []
    for label, row in zip(frame.index, frame.itertuples(index=False), strict=True):
        session = bounds.get(row.session_date_et)
        if session is None or not (session[0] <= row.bar_start_at < session[1]):
            outside_positions.append(label)
        session_minutes = (
            int((session[1] - session[0]).total_seconds() // 60) if session is not None else maximum
        )
        allowed_max = min(maximum, session_minutes)
        if row.source_minutes <= 0 or row.source_minutes % 30 or row.source_minutes > allowed_max:
            bad_source_positions.append(label)
        if session is not None:
            if resolution == "1d":
                expected_minutes = session_minutes
            else:
                remaining = int((session[1] - row.bar_start_at).total_seconds() // 60)
                expected_minutes = min(maximum, max(0, remaining))
            if row.source_minutes < expected_minutes:
                missing_source_positions.append(label)

    def mask_of(labels: list[Any]) -> pd.Series[bool]:
        mask = pd.Series(False, index=frame.index)
        if labels:
            mask.loc[labels] = True
        return mask

    findings = _row_findings(
        frame,
        mask_of(outside_positions),
        code="XNYS_SESSION_VIOLATION",
        severity="ERROR",
        policy_version=QUALITY_POLICY_VERSION,
        describe=lambda instrument, count, start, end: (
            f"{instrument} 정규장 밖 {count}행 {start.isoformat()}~{end.isoformat()}"
        ),
        span=span,
    )
    findings.extend(
        _row_findings(
            frame,
            mask_of(bad_source_positions),
            code="INVALID_SOURCE_MINUTES",
            severity="ERROR",
            policy_version=QUALITY_POLICY_VERSION,
            describe=lambda instrument, count, start, end: (
                f"{instrument} source_minutes 오류 {count}행 "
                f"{start.isoformat()}~{end.isoformat()}"
            ),
            span=span,
        )
    )
    findings.extend(
        _row_findings(
            frame,
            mask_of(missing_source_positions),
            code="UNEXPECTED_MISSING_SOURCE_BARS",
            severity="WARNING",
            policy_version=MISSING_BAR_POLICY.version,
            describe=lambda instrument, count, start, end: (
                f"{instrument} 기대 분량보다 적은 파생봉 {count}행 "
                f"{start.isoformat()}~{end.isoformat()}; 누락 봉은 생성하지 않았습니다."
            ),
            span=span,
        )
    )
    return findings


def schema_mismatch_finding(expected: Any, actual: Any) -> Finding:
    """No row is interpretable, so the whole partition is the narrowest truth."""
    return Finding(
        code="SCHEMA_MISMATCH",
        severity="ERROR",
        message=f"expected={expected}, actual={actual}",
        policy_version=QUALITY_POLICY_VERSION,
        breadth=ScopeBreadth.PARTITION,
    )


def normalise_bar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """UTC-normalise `bar_start_at` and give the frame a unique positional index."""
    prepared = frame.reset_index(drop=True)
    prepared["bar_start_at"] = pd.to_datetime(prepared["bar_start_at"], utc=True)
    return prepared
