"""Deterministic D90 publication of C market events for startup warm-up."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import canonical_dataset_hash, deterministic_uuid


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
EVENT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
BAR_OBJECT_SCHEMA_VERSION = "warmup-bars-v1"
FEATURE_OBJECT_SCHEMA_VERSION = "feature-object-v1"


class RealtimeWarmupError(ValueError):
    """Raised when a daily warm-up bundle cannot be safely published."""


@dataclass(frozen=True)
class FeatureRequirement:
    requirement_id: str
    feature_id: str
    feature_version: str
    resolution: str
    instruments: tuple[str, ...]
    required_observations: int

    def __post_init__(self) -> None:
        for field in ("requirement_id", "feature_id", "feature_version", "resolution"):
            if not getattr(self, field):
                raise RealtimeWarmupError(f"{field} must not be empty")
        if self.resolution != "PT1M":
            raise RealtimeWarmupError("only PT1M warm-up requirements are supported")
        if self.feature_id != "close":
            raise RealtimeWarmupError("only the close feature is currently supported")
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
    with path.open("rb") as handle:
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


def _validated_bars(document: Mapping[str, Any]) -> list[dict[str, Any]]:
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

    bars = [event for event in selected.values() if event["eventType"] == "BAR_1M"]
    if not bars:
        raise RealtimeWarmupError("required BAR_1M coverage is missing")
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
            raise RealtimeWarmupError(f"BAR_1M values missing {missing[0]}")
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


def _object_metadata(path: Path, *, object_key: str, session_date: str, row_count: int, schema_version: str, role: str) -> dict[str, Any]:
    start = datetime.fromisoformat(session_date).replace(tzinfo=ET).astimezone(UTC)
    end = (datetime.fromisoformat(session_date) + timedelta(days=1)).replace(tzinfo=ET).astimezone(UTC)
    content_hash = _sha256(path)
    return {
        "storage_object_id": deterministic_uuid("d90-object", object_key, content_hash),
        "object_key": object_key,
        "content_hash": content_hash,
        "object_kind": "PARQUET" if path.suffix == ".parquet" else "JSON",
        "object_role": role,
        "partition_granularity": "DAY",
        "partition_start": session_date,
        "partition_end": (datetime.fromisoformat(session_date) + timedelta(days=1)).date().isoformat(),
        "period_start": start.isoformat().replace("+00:00", "Z"),
        "period_end": end.isoformat().replace("+00:00", "Z"),
        "shard_key": "s00-of-01",
        "part_number": 1,
        "row_count": row_count,
        "schema_version": schema_version,
    }


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
        or manifest.get("status") != "AVAILABLE"
    ):
        raise RealtimeWarmupError("manifest schema or status is incompatible")
    objects = manifest.get("objects")
    if not isinstance(objects, list) or not objects:
        raise RealtimeWarmupError("manifest objects are missing")
    source_objects = []
    feature_path: Path | None = None
    for index, item in enumerate(objects):
        if not isinstance(item, Mapping):
            raise RealtimeWarmupError(f"manifest object {index} is invalid")
        object_key = _required_text(item, "object_key", f"objects[{index}]")
        filename = Path(object_key).name
        path = (root / filename).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise RealtimeWarmupError("manifest object path escapes the bundle") from exc
        if not path.is_file() or _sha256(path) != item.get("content_hash"):
            raise RealtimeWarmupError(f"object hash mismatch: {object_key}")
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
    document: Mapping[str, Any], output: Path, requirements: Sequence[FeatureRequirement]
) -> RealtimeWarmupBundle:
    """Validate, materialize, and atomically publish one ET daily bundle."""
    if not requirements:
        raise RealtimeWarmupError("at least one feature requirement is required")
    if len({item.requirement_id for item in requirements}) != len(requirements):
        raise RealtimeWarmupError("requirement_id values must be unique")
    bars = _validated_bars(document)
    table = _bar_table(bars)
    session_date = bars[0]["_occurred_at"].astimezone(ET).date().isoformat()

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise RealtimeWarmupError("output must be absent or an empty directory")
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.d90-", dir=output.parent))
    daily_name = f"market-events-{session_date}.parquet"
    feature_name = f"warmup-features-{session_date}.json"
    manifest_name = "manifest.json"
    try:
        daily_path = staging / daily_name
        pq.write_table(
            table, daily_path, compression="zstd", version="2.6", data_page_version="2.0",
            use_dictionary=False, write_statistics=True,
        )
        daily = _object_metadata(
            daily_path, object_key=f"warmup/session_date_et={session_date}/{daily_name}",
            session_date=session_date, row_count=table.num_rows,
            schema_version=BAR_OBJECT_SCHEMA_VERSION, role="MARKET_EVENTS",
        )
        dataset_hash = canonical_dataset_hash((daily,))
        requirements_key = json.dumps([
            {
                "requirement_id": item.requirement_id,
                "feature_id": item.feature_id,
                "feature_version": item.feature_version,
                "resolution": item.resolution,
                "instruments": sorted(item.instruments),
                "required_observations": item.required_observations,
            }
            for item in sorted(requirements, key=lambda value: value.requirement_id)
        ], sort_keys=True, separators=(",", ":"))
        manifest_id = deterministic_uuid("d90-manifest", session_date, dataset_hash, requirements_key)
        dataset_id = deterministic_uuid("d90-dataset", session_date, dataset_hash)

        series = []
        for requirement in sorted(requirements, key=lambda value: value.requirement_id):
            observations = [
                {
                    "instrument": event["instrumentId"],
                    "observed_at": event["_occurred_at"].isoformat().replace("+00:00", "Z"),
                    "value": _decimal_text(event["values"]["close"], "close"),
                }
                for event in bars if event["instrumentId"] in requirement.instruments
            ]
            counts = {instrument: 0 for instrument in requirement.instruments}
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
        feature_path = staging / feature_name
        feature_path.write_bytes(_canonical_json(feature_document))
        feature = _object_metadata(
            feature_path, object_key=f"warmup/session_date_et={session_date}/{feature_name}",
            session_date=session_date,
            row_count=sum(len(item["observations"]) for item in series),
            schema_version=FEATURE_OBJECT_SCHEMA_VERSION, role="WARMUP_FEATURES",
        )
        manifest = {
            "contract_id": "d90.realtime-warmup-manifest",
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "manifest_id": manifest_id,
            "dataset_id": dataset_id,
            "revision": 1,
            "status": "AVAILABLE",
            "session_date_et": session_date,
            "dataset_hash": dataset_hash,
            "dataset_hash_scope": "MARKET_EVENTS",
            "objects": [daily, feature],
        }
        manifest_path = staging / manifest_name
        manifest_path.write_bytes(_canonical_json(manifest))

        if pq.read_table(daily_path).num_rows != table.num_rows:
            raise RealtimeWarmupError("published Parquet row count is invalid")
        verify_realtime_warmup_bundle(staging)
        if output.exists():
            output.rmdir()
        os.replace(staging, output)
        return RealtimeWarmupBundle(
            manifest=manifest,
            manifest_path=output / manifest_name,
            daily_object_path=output / daily_name,
            feature_object_path=output / feature_name,
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
