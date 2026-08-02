"""Deterministic D90 publication of C market events for startup warm-up.

Two D12/D90 defects are corrected here.

**No assumed resolution or field.**  This module used to reject any resolution
but ``PT1M`` and any feature but ``close``, and to recognise only ``BAR_1M``
events; the feature extractor read ``values["close"]`` literally.  A provider
publishing 30-minute bars, or a strategy warming up on ``vwap``, was unbuildable.
Now :class:`WarmupPublicationSpec` names the dataset contract, the event type and
the partition granularity, and each :class:`FeatureRequirement` names its own
resolution and the exact ``values`` key it reads.  None of them has a default.

**Canonical object keys.**  The market-events object used to land under
``warmup/session_date_et=…/market-events-….parquet``, which is not the key
:func:`market_pipeline_lib.contracts.object_key` defines, so the object could
never be found by ``MarketPipelineEngine.compact``.  It now lands under the
canonical key, at its full nested path inside the bundle, and the bundle's own
sidecars (the manifest and the warm-up feature document) stay namespaced under
``warmup/`` so they are never mistaken for market-data objects.

**A consumer.**  :func:`warmup_bundle_from_source` drains a
:class:`~market_pipeline_lib.realtime_ingest.RealtimeEventSource` -- in
production the LocalStack/SQS adapter -- and builds the bundle from what it
received, instead of from a JSON file someone placed on disk.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import (
    DatasetContract,
    Granularity,
    canonical_dataset_hash,
    deterministic_uuid,
    logical_dataset_id,
    object_key,
    partition_bounds,
    stable_shard_key,
)
from .fs_paths import long_path
from .realtime_ingest import PARTITION_GRANULARITIES, RealtimeEventSource


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
EVENT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
BAR_OBJECT_SCHEMA_VERSION = "warmup-bars-v1"
FEATURE_OBJECT_SCHEMA_VERSION = "feature-object-v1"

_ISO_DURATION = re.compile(r"^P(?!$)(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")


class RealtimeWarmupError(ValueError):
    """Raised when a daily warm-up bundle cannot be safely published."""


class WarmupBlockReason(StrEnum):
    """Why a consumer must not start.

    One code per distinguishable cause, because "not ready" is not actionable.  An
    operator seeing ``D90_WATERMARK_STALE`` restarts an ingest worker; one seeing
    ``D90_DAILY_OBJECT_MISSING`` goes looking for a session that was never published.
    A single generic code would send both to the same wrong place.
    """

    #: No published, AVAILABLE object covers a required shard of a required session.
    DAILY_OBJECT_MISSING = "D90_DAILY_OBJECT_MISSING"
    #: A manifest exists for the session but is not AVAILABLE (BUILDING/QUARANTINED/...).
    MANIFEST_NOT_AVAILABLE = "D90_MANIFEST_NOT_AVAILABLE"
    #: `market_data.stream_watermarks` has no row for this feed at all.
    WATERMARK_MISSING = "D90_WATERMARK_MISSING"
    #: The feed has a watermark but ingestion has not run recently enough.
    WATERMARK_STALE = "D90_WATERMARK_STALE"
    #: The feed is live but has not consumed as far as the session it must cover.
    WATERMARK_BEHIND_SESSION = "D90_WATERMARK_BEHIND_SESSION"
    #: The feature snapshot batch a consumer pins was never opened.
    FEATURE_BATCH_MISSING = "D90_FEATURE_BATCH_MISSING"
    #: The batch exists but has not reached a consumable point in time.
    FEATURE_BATCH_INCOMPLETE = "D90_FEATURE_BATCH_INCOMPLETE"


READY = "READY"
BLOCKED = "BLOCKED"

#: `market_data.dataset_status` values a warm-up manifest may carry.  C parses this
#: string into `DatasetManifestStatus` and refuses anything but AVAILABLE
#: (`StartupWarmupCoordinator.java:67-69`), which is what makes QUARANTINED a block
#: C already honours without a single line of new Java.
_READINESS_MANIFEST_STATUS = {READY: "AVAILABLE", BLOCKED: "QUARANTINED"}


@dataclass(frozen=True)
class WarmupReadiness:
    """The verdict on whether a consumer may start from this session.

    It is a required input to publication rather than something publication decides,
    so there is no path that writes an ``AVAILABLE`` manifest without someone having
    evaluated the gate.
    """

    state: str
    session_date_et: str
    feed_id: str
    evaluated_at: datetime
    reason_code: str | None = None
    detail: str | None = None
    manifest_id: str | None = None
    observed: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.state not in _READINESS_MANIFEST_STATUS:
            raise ValueError(
                f"state must be one of {sorted(_READINESS_MANIFEST_STATUS)}, got {self.state!r}"
            )
        if not self.session_date_et:
            raise ValueError("session_date_et must not be empty")
        date_type.fromisoformat(self.session_date_et)
        if not self.feed_id:
            raise ValueError("feed_id must not be empty")
        if self.evaluated_at.tzinfo is None or self.evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        object.__setattr__(self, "evaluated_at", self.evaluated_at.astimezone(UTC))
        if self.state == BLOCKED:
            if not self.reason_code:
                raise ValueError(
                    "a BLOCKED verdict must name its reason_code; an unexplained block "
                    "cannot be acted on and cannot be distinguished from a bug"
                )
            if not self.detail:
                raise ValueError("a BLOCKED verdict must carry a human-readable detail")
        elif self.reason_code is not None or self.detail is not None:
            raise ValueError("a READY verdict must not carry a reason_code or detail")
        object.__setattr__(self, "observed", dict(self.observed))

    @property
    def blocked(self) -> bool:
        return self.state == BLOCKED

    @property
    def manifest_status(self) -> str:
        return _READINESS_MANIFEST_STATUS[self.state]

    def as_document(self) -> dict[str, Any]:
        """The ``readiness`` object embedded in the manifest C reads."""

        return {
            "state": self.state,
            "reason_code": None if self.reason_code is None else str(self.reason_code),
            "detail": self.detail,
            "feed_id": self.feed_id,
            "session_date_et": self.session_date_et,
            "evaluated_at": self.evaluated_at.isoformat().replace("+00:00", "Z"),
            "observed": dict(sorted(self.observed.items())),
        }


@dataclass(frozen=True)
class WarmupPublicationSpec:
    """Which dataset, which event, which partition -- all stated, none assumed.

    ``revision`` and ``shard_count`` describe how the bundle's own market-events
    object is addressed.  A warm-up bundle carries one session for one consumer,
    so a single shard and revision 1 are the normal case; both are here so that a
    caller republishing a corrected bundle can say so rather than overwrite.
    """

    contract: DatasetContract
    event_type: str
    granularity: Granularity
    revision: int = 1
    shard_count: int = 1

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise RealtimeWarmupError("event_type must name the event carrying a bar")
        if self.granularity not in PARTITION_GRANULARITIES:
            raise RealtimeWarmupError(
                f"granularity must be one of {list(PARTITION_GRANULARITIES)}, got {self.granularity!r}"
            )
        if self.revision < 1:
            raise RealtimeWarmupError("revision must be at least 1")
        if self.shard_count < 1:
            raise RealtimeWarmupError("shard_count must be at least 1")


@dataclass(frozen=True)
class FeatureRequirement:
    """One warm-up series a consumer needs before it can start.

    ``value_field`` is the key inside the event's ``values`` object that supplies
    the observation.  It is separate from ``feature_id`` on purpose: the feature a
    strategy names and the field a provider publishes are not the same vocabulary,
    and collapsing them is how ``close`` became hardcoded.
    """

    requirement_id: str
    feature_id: str
    feature_version: str
    resolution: str
    value_field: str
    instruments: tuple[str, ...]
    required_observations: int

    def __post_init__(self) -> None:
        for name in ("requirement_id", "feature_id", "feature_version", "resolution", "value_field"):
            if not getattr(self, name):
                raise RealtimeWarmupError(f"{name} must not be empty")
        if not _ISO_DURATION.match(self.resolution):
            raise RealtimeWarmupError(
                f"resolution must be an ISO-8601 duration, got {self.resolution!r}"
            )
        if not self.instruments or len(set(self.instruments)) != len(self.instruments):
            raise RealtimeWarmupError("instruments must be unique and non-empty")
        if self.required_observations < 1:
            raise RealtimeWarmupError("required_observations must be positive")


@dataclass(frozen=True)
class RealtimeWarmupBundle:
    manifest: dict[str, Any]
    manifest_path: Path
    daily_object_path: Path
    feature_object_path: Path


def _required_text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise RealtimeWarmupError(f"{label}.{field} must be a non-empty string")
    return value


def _timestamp(document: Mapping[str, Any], field: str, label: str) -> datetime:
    value = _required_text(document, field, label)
    if not value.endswith("Z"):
        raise RealtimeWarmupError(f"{label}.{field} must be UTC")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RealtimeWarmupError(f"{label}.{field} must be ISO-8601") from exc
    if parsed.utcoffset() != timedelta(0):
        raise RealtimeWarmupError(f"{label}.{field} must be UTC")
    return parsed.astimezone(UTC)


def _canonical_json(document: Mapping[str, Any]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    # Canonical keys nest ten `key=value` directories, so a bundle under a normal
    # user profile is already past the Windows MAX_PATH limit before the file name.
    with open(long_path(path), "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _decimal_text(value: object, label: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise RealtimeWarmupError(f"{label} must be numeric")
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise RealtimeWarmupError(f"{label} must be numeric") from exc
    if not parsed.is_finite():
        raise RealtimeWarmupError(f"{label} must be finite")
    return format(parsed.normalize(), "f")


def _validated_bars(document: Mapping[str, Any], wanted_event_type: str) -> list[dict[str, Any]]:
    if document.get("schemaVersion") != EVENT_SCHEMA_VERSION:
        raise RealtimeWarmupError("document schema is incompatible")
    events = document.get("events")
    if not isinstance(events, list) or not events:
        raise RealtimeWarmupError("events must not be empty")

    selected: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for index, value in enumerate(events):
        label = f"events[{index}]"
        if not isinstance(value, Mapping):
            raise RealtimeWarmupError(f"{label} must be an object")
        if value.get("schemaVersion") != EVENT_SCHEMA_VERSION:
            raise RealtimeWarmupError(f"{label} schema is incompatible")
        event = dict(value)
        event_id = _required_text(event, "eventId", label)
        instrument = _required_text(event, "instrumentId", label)
        provider = _required_text(event, "provider", label)
        feed = _required_text(event, "feed", label)
        event_type = _required_text(event, "eventType", label)
        provider_event_id = _required_text(event, "providerEventId", label)
        occurred_at = _timestamp(event, "occurredAt", label)
        received_at = _timestamp(event, "receivedAt", label)
        sequence = event.get("sequence")
        revision = event.get("revision")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise RealtimeWarmupError(f"{label}.sequence must be non-negative")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise RealtimeWarmupError(f"{label}.revision must be non-negative")
        correction = event.get("correctionOfEventId")
        if revision == 0 and correction is not None:
            raise RealtimeWarmupError(f"{label} revision zero cannot be a correction")
        if revision > 0 and (not isinstance(correction, str) or not correction):
            raise RealtimeWarmupError(f"{label} correction must name the original event")
        values = event.get("values")
        if not isinstance(values, Mapping):
            raise RealtimeWarmupError(f"{label}.values must be an object")

        event["_occurred_at"] = occurred_at
        event["_received_at"] = received_at
        key = (instrument, provider, feed, event_type, provider_event_id)
        previous = selected.get(key)
        if previous is None or (revision, received_at, event_id) > (
            previous["revision"], previous["_received_at"], previous["eventId"]
        ):
            selected[key] = event
        elif revision == previous["revision"] and event != previous:
            raise RealtimeWarmupError(f"{label} conflicts with a duplicate revision")

    bars = [event for event in selected.values() if event["eventType"] == wanted_event_type]
    if not bars:
        raise RealtimeWarmupError(f"required {wanted_event_type} coverage is missing")
    bars.sort(key=lambda event: (
        event["_occurred_at"], event["instrumentId"], event["providerEventId"], event["revision"], event["eventId"]
    ))
    dates = {event["_occurred_at"].astimezone(ET).date().isoformat() for event in bars}
    if len(dates) != 1:
        raise RealtimeWarmupError("one publication must contain exactly one ET session date")
    return bars


def _bar_table(bars: Sequence[Mapping[str, Any]]) -> pa.Table:
    rows: list[dict[str, Any]] = []
    for index, event in enumerate(bars):
        values = event["values"]
        required = ("open", "high", "low", "close", "volume")
        missing = [field for field in required if field not in values]
        if missing:
            raise RealtimeWarmupError(f"{event['eventType']} values missing {missing[0]}")
        prices = {field: float(_decimal_text(values[field], f"bars[{index}].{field}")) for field in required[:-1]}
        volume = values["volume"]
        if isinstance(volume, bool) or not isinstance(volume, int) or volume < 0:
            raise RealtimeWarmupError(f"bars[{index}].volume must be non-negative integer")
        rows.append({
            "event_id": event["eventId"],
            "schema_version": event["schemaVersion"],
            "instrument_id": event["instrumentId"],
            "provider": event["provider"],
            "feed": event["feed"],
            "event_type": event["eventType"],
            "provider_event_id": event["providerEventId"],
            "occurred_at": event["_occurred_at"],
            "received_at": event["_received_at"],
            "sequence": event["sequence"],
            "revision": event["revision"],
            "correction_of_event_id": event["correctionOfEventId"],
            **prices,
            "volume": volume,
        })
    schema = pa.schema([
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("feed", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("provider_event_id", pa.string(), nullable=False),
        pa.field("occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("revision", pa.int32(), nullable=False),
        pa.field("correction_of_event_id", pa.string(), nullable=True),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
    ], metadata={b"schema_version": BAR_OBJECT_SCHEMA_VERSION.encode("ascii")})
    return pa.Table.from_pylist(rows, schema=schema)


def _object_metadata(
    path: Path,
    *,
    key: str,
    partition_start: date_type,
    partition_end: date_type,
    granularity: Granularity,
    shard_key: str,
    part_number: int,
    row_count: int,
    schema_version: str,
    role: str,
) -> dict[str, Any]:
    start = datetime.combine(partition_start, datetime.min.time(), ET).astimezone(UTC)
    end = datetime.combine(partition_end, datetime.min.time(), ET).astimezone(UTC)
    content_hash = _sha256(path)
    return {
        "storage_object_id": deterministic_uuid("d90-object", key, content_hash),
        "object_key": key,
        "content_hash": content_hash,
        "object_kind": "PARQUET" if path.suffix == ".parquet" else "JSON",
        "object_role": role,
        "partition_granularity": granularity,
        "partition_start": partition_start.isoformat(),
        "partition_end": partition_end.isoformat(),
        "period_start": start.isoformat().replace("+00:00", "Z"),
        "period_end": end.isoformat().replace("+00:00", "Z"),
        "shard_key": shard_key,
        "part_number": part_number,
        "row_count": row_count,
        "schema_version": schema_version,
    }


def _verify_readiness_binding(manifest: Mapping[str, Any]) -> str:
    """The manifest's ``status`` must be the one its own verdict implies.

    Without this, a blocked bundle could be edited to ``AVAILABLE`` while still
    carrying ``state: BLOCKED``, and C -- which reads only ``status`` -- would start
    on data D had already refused.
    """

    readiness = manifest.get("readiness")
    if not isinstance(readiness, Mapping):
        raise RealtimeWarmupError(
            "manifest carries no readiness verdict; a bundle with no recorded gate "
            "decision is indistinguishable from one that was never gated"
        )
    state = readiness.get("state")
    if state not in _READINESS_MANIFEST_STATUS:
        raise RealtimeWarmupError(f"manifest readiness.state is invalid: {state!r}")
    expected = _READINESS_MANIFEST_STATUS[str(state)]
    if manifest.get("status") != expected:
        raise RealtimeWarmupError(
            f"manifest status {manifest.get('status')!r} contradicts its readiness "
            f"verdict {state!r}, which requires {expected!r}"
        )
    if state == BLOCKED and not readiness.get("reason_code"):
        raise RealtimeWarmupError("a BLOCKED manifest readiness must name a reason_code")
    if state == READY and readiness.get("reason_code") is not None:
        raise RealtimeWarmupError("a READY manifest readiness must not carry a reason_code")
    return str(state)


def verify_realtime_warmup_bundle(root: Path) -> dict[str, Any]:
    """Verify every declared object and binding before a bundle is consumed."""
    root = root.resolve()
    manifest_path = root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RealtimeWarmupError("manifest is missing or invalid") from exc
    if (
        manifest.get("contract_id") != "d90.realtime-warmup-manifest"
        or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or manifest.get("status") not in set(_READINESS_MANIFEST_STATUS.values())
    ):
        raise RealtimeWarmupError("manifest schema or status is incompatible")
    state = _verify_readiness_binding(manifest)
    objects = manifest.get("objects")
    if not isinstance(objects, list):
        raise RealtimeWarmupError("manifest objects are missing")
    if not objects:
        # A block that had no data to describe.  There is nothing to hash-check, and
        # C will refuse it on `status` alone.
        if state != BLOCKED:
            raise RealtimeWarmupError("an AVAILABLE manifest must declare its objects")
        if canonical_dataset_hash(()) != manifest.get("dataset_hash"):
            raise RealtimeWarmupError("dataset hash mismatch")
        return dict(manifest)
    source_objects = []
    feature_path: Path | None = None
    for index, item in enumerate(objects):
        if not isinstance(item, Mapping):
            raise RealtimeWarmupError(f"manifest object {index} is invalid")
        key = _required_text(item, "object_key", f"objects[{index}]")
        # Objects live at their full key path inside the bundle, so the canonical
        # key is the layout rather than a label attached to a flat file name.
        path = Path(os.path.normpath(root / key))
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise RealtimeWarmupError("manifest object path escapes the bundle") from exc
        if not os.path.isfile(long_path(path)) or _sha256(path) != item.get("content_hash"):
            raise RealtimeWarmupError(f"object hash mismatch: {key}")
        if item.get("object_role") == "MARKET_EVENTS":
            source_objects.append(dict(item))
        elif item.get("object_role") == "WARMUP_FEATURES":
            feature_path = path
    if not source_objects or feature_path is None:
        raise RealtimeWarmupError("required market and feature objects are missing")
    if canonical_dataset_hash(source_objects) != manifest.get("dataset_hash"):
        raise RealtimeWarmupError("dataset hash mismatch")
    try:
        feature = json.loads(feature_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RealtimeWarmupError("feature object is invalid") from exc
    if (
        feature.get("manifest_id") != manifest.get("manifest_id")
        or feature.get("dataset_hash") != manifest.get("dataset_hash")
        or feature.get("object_schema_version") != FEATURE_OBJECT_SCHEMA_VERSION
    ):
        raise RealtimeWarmupError("feature binding does not match the manifest")
    return manifest


def publish_realtime_warmup_bundle(
    document: Mapping[str, Any],
    output: Path,
    requirements: Sequence[FeatureRequirement],
    *,
    spec: WarmupPublicationSpec,
    readiness: WarmupReadiness,
) -> RealtimeWarmupBundle:
    """Validate, materialize, and atomically publish one ET session bundle.

    The market-events object is written at its canonical key
    (:func:`market_pipeline_lib.contracts.object_key`), so the bundle's layout is
    the object identity rather than a flat file name with a key glued on.

    ``readiness`` is required.  A bundle whose data is all present can still be one a
    consumer must not start from -- a stale feed, an unconsumable feature batch -- and
    the only way to make that visible to C is to write it into the manifest C reads.
    """

    if not requirements:
        raise RealtimeWarmupError("at least one feature requirement is required")
    if len({item.requirement_id for item in requirements}) != len(requirements):
        raise RealtimeWarmupError("requirement_id values must be unique")
    bars = _validated_bars(document, spec.event_type)
    table = _bar_table(bars)
    session = bars[0]["_occurred_at"].astimezone(ET).date()
    session_date = session.isoformat()
    if readiness.session_date_et != session_date:
        raise RealtimeWarmupError(
            f"readiness was evaluated for {readiness.session_date_et} but this bundle "
            f"covers {session_date}; a verdict about one session says nothing about another"
        )
    partition_start, partition_end = partition_bounds(session, spec.granularity)

    instruments = {event["instrumentId"] for event in bars}
    shard_keys = {stable_shard_key(value, spec.shard_count) for value in instruments}
    if len(shard_keys) != 1:
        raise RealtimeWarmupError(
            f"a warm-up bundle publishes one object, but these instruments span "
            f"{sorted(shard_keys)}; raise shard_count or split the bundle"
        )
    shard_key = shard_keys.pop()
    dataset_id = logical_dataset_id(spec.contract, partition_start.year)
    events_key = object_key(
        spec.contract,
        dataset_id,
        spec.revision,
        spec.granularity,
        partition_start,
        partition_end,
        shard_key,
        1,
    )
    feature_key = f"warmup/session_date_et={session_date}/warmup-features.json"
    manifest_name = "manifest.json"

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise RealtimeWarmupError("output must be absent or an empty directory")
    staging = Path(tempfile.mkdtemp(prefix=".d90-", dir=output.parent))
    try:
        daily_path = staging / events_key
        os.makedirs(long_path(daily_path.parent), exist_ok=True)
        pq.write_table(
            table, long_path(daily_path), compression="zstd", version="2.6", data_page_version="2.0",
            use_dictionary=False, write_statistics=True,
        )
        daily = _object_metadata(
            daily_path,
            key=events_key,
            partition_start=partition_start,
            partition_end=partition_end,
            granularity=spec.granularity,
            shard_key=shard_key,
            part_number=1,
            row_count=table.num_rows,
            schema_version=BAR_OBJECT_SCHEMA_VERSION,
            role="MARKET_EVENTS",
        )
        dataset_hash = canonical_dataset_hash((daily,))
        requirements_key = json.dumps([
            {
                "requirement_id": item.requirement_id,
                "feature_id": item.feature_id,
                "feature_version": item.feature_version,
                "resolution": item.resolution,
                "value_field": item.value_field,
                "instruments": sorted(item.instruments),
                "required_observations": item.required_observations,
            }
            for item in sorted(requirements, key=lambda value: value.requirement_id)
        ], sort_keys=True, separators=(",", ":"))
        manifest_id = deterministic_uuid("d90-manifest", session_date, dataset_hash, requirements_key)

        series = []
        for requirement in sorted(requirements, key=lambda value: value.requirement_id):
            observations = [
                {
                    "instrument": event["instrumentId"],
                    "observed_at": event["_occurred_at"].isoformat().replace("+00:00", "Z"),
                    "value": _decimal_text(
                        _required_value(event["values"], requirement.value_field, event["eventId"]),
                        requirement.value_field,
                    ),
                }
                for event in bars if event["instrumentId"] in requirement.instruments
            ]
            counts = dict.fromkeys(requirement.instruments, 0)
            for observation in observations:
                counts[observation["instrument"]] += 1
            missing = [instrument for instrument, count in counts.items() if count < requirement.required_observations]
            if missing:
                raise RealtimeWarmupError(f"required feature coverage is missing for {missing[0]}")
            series.append({
                "requirement_id": requirement.requirement_id,
                "feature_id": requirement.feature_id,
                "feature_version": requirement.feature_version,
                "resolution": requirement.resolution,
                "value_field": requirement.value_field,
                "manifest_id": manifest_id,
                "dataset_hash": dataset_hash,
                "observations": observations,
            })
        feature_document = {
            "contract_id": "d90.warmup-features",
            "schema_version": 1,
            "object_schema_version": FEATURE_OBJECT_SCHEMA_VERSION,
            "manifest_id": manifest_id,
            "dataset_hash": dataset_hash,
            "series": series,
        }
        feature_path = staging / feature_key
        feature_path.parent.mkdir(parents=True, exist_ok=True)
        feature_path.write_bytes(_canonical_json(feature_document))
        feature = _object_metadata(
            feature_path,
            key=feature_key,
            partition_start=partition_start,
            partition_end=partition_end,
            granularity=spec.granularity,
            shard_key=shard_key,
            part_number=1,
            row_count=sum(len(item["observations"]) for item in series),
            schema_version=FEATURE_OBJECT_SCHEMA_VERSION,
            role="WARMUP_FEATURES",
        )
        manifest = {
            "contract_id": "d90.realtime-warmup-manifest",
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "manifest_id": manifest_id,
            "dataset_id": dataset_id,
            "revision": spec.revision,
            "status": readiness.manifest_status,
            "session_date_et": session_date,
            "dataset_hash": dataset_hash,
            "dataset_hash_scope": "MARKET_EVENTS",
            "readiness": readiness.as_document(),
            "objects": [daily, feature],
        }
        manifest_path = staging / manifest_name
        manifest_path.write_bytes(_canonical_json(manifest))

        if pq.read_table(long_path(daily_path)).num_rows != table.num_rows:
            raise RealtimeWarmupError("published Parquet row count is invalid")
        verify_realtime_warmup_bundle(staging)
        if output.exists():
            output.rmdir()
        os.replace(staging, output)
        return RealtimeWarmupBundle(
            manifest=manifest,
            manifest_path=output / manifest_name,
            daily_object_path=output / events_key,
            feature_object_path=output / feature_key,
        )
    except Exception:
        shutil.rmtree(long_path(staging), ignore_errors=True)
        raise


def publish_blocked_warmup_manifest(
    output: Path,
    *,
    spec: WarmupPublicationSpec,
    readiness: WarmupReadiness,
) -> Path:
    """Publish a manifest for a session that produced nothing usable.

    The case the previous D90 had no answer for: when the daily object itself is
    missing there is no bundle to publish, so D published nothing -- and "nothing" is
    indistinguishable, from C's side, from "D has not run yet".  Writing a manifest
    that is structurally valid but ``QUARANTINED`` turns silence into a statement,
    and C's existing status check (``StartupWarmupCoordinator.java:67-69``) already
    refuses to start on it.
    """

    if not readiness.blocked:
        raise RealtimeWarmupError(
            "publish_blocked_warmup_manifest writes a QUARANTINED manifest; a READY "
            "verdict must go through publish_realtime_warmup_bundle with its objects"
        )
    session = date_type.fromisoformat(readiness.session_date_et)
    partition_start, _partition_end = partition_bounds(session, spec.granularity)
    dataset_id = logical_dataset_id(spec.contract, partition_start.year)
    dataset_hash = canonical_dataset_hash(())
    manifest = {
        "contract_id": "d90.realtime-warmup-manifest",
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "manifest_id": deterministic_uuid(
            "d90-blocked-manifest",
            readiness.session_date_et,
            str(readiness.reason_code),
            dataset_id,
        ),
        "dataset_id": dataset_id,
        "revision": spec.revision,
        "status": readiness.manifest_status,
        "session_date_et": readiness.session_date_et,
        "dataset_hash": dataset_hash,
        "dataset_hash_scope": "MARKET_EVENTS",
        "readiness": readiness.as_document(),
        "objects": [],
    }

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise RealtimeWarmupError("output must be absent or an empty directory")
    staging = Path(tempfile.mkdtemp(prefix=".d90b-", dir=output.parent))
    try:
        (staging / "manifest.json").write_bytes(_canonical_json(manifest))
        verify_realtime_warmup_bundle(staging)
        if output.exists():
            output.rmdir()
        os.replace(staging, output)
    except Exception:
        shutil.rmtree(long_path(staging), ignore_errors=True)
        raise
    return output / "manifest.json"


def _required_value(values: Mapping[str, Any], field: str, event_id: str) -> object:
    if field not in values:
        raise RealtimeWarmupError(
            f"event {event_id} has no values.{field}; the requirement names a field this "
            "stream does not publish"
        )
    return values[field]


def warmup_bundle_from_source(
    source: RealtimeEventSource,
    output: Path,
    requirements: Sequence[FeatureRequirement],
    *,
    spec: WarmupPublicationSpec,
    readiness: WarmupReadiness,
    max_empty_cycles: int = 2,
    wait_seconds: float = 1.0,
    max_messages_per_poll: int = 10,
) -> RealtimeWarmupBundle:
    """Drain `source`, then publish the bundle the drained events describe.

    This is the consumer the module never had.  A drain that yields nothing
    raises rather than publishing an empty bundle: "no events" and "a warm-up
    bundle containing no observations" are different facts, and a consumer that
    started from the second would trade on nothing.
    """

    events: list[Mapping[str, Any]] = []
    empty = 0
    while empty < max_empty_cycles:
        deliveries = source.poll(max_messages=max_messages_per_poll, wait_seconds=wait_seconds)
        if not deliveries:
            empty += 1
            continue
        empty = 0
        for delivery in deliveries:
            body = delivery.body
            batch = body.get("events") if isinstance(body, Mapping) else None
            if not isinstance(batch, list) or not batch:
                raise RealtimeWarmupError(
                    f"message {delivery.message_id} does not carry a non-empty 'events' array"
                )
            events.extend(batch)
            source.acknowledge(delivery)
    if not events:
        raise RealtimeWarmupError(
            "the event source yielded no events; refusing to publish an empty warm-up bundle"
        )
    return publish_realtime_warmup_bundle(
        {"schemaVersion": EVENT_SCHEMA_VERSION, "events": events},
        output,
        requirements,
        spec=spec,
        readiness=readiness,
    )
