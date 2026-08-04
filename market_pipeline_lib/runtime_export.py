"""Deterministic trading runtime artifacts from canonical catalog evidence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, BinaryIO, Literal, Protocol

import pyarrow.parquet as pq

from .catalog import MarketDataCatalog
from .contracts import DATASET_CONTRACTS, ET
from .fs_paths import long_path
from .legacy_bootstrap import S3LegacyObjectVerifier

RevisionPolicy = Literal["latest-per-period", "all-available"]


class RuntimeExportError(RuntimeError):
    """Runtime configuration cannot be proven from immutable source evidence."""


class VersionedS3Client(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class HistoricalSelection:
    """Every semantic selector must be supplied by the caller."""

    layer_by_resolution: Mapping[str, str]
    adjustment: str
    start: date
    end_exclusive: date
    latest_revision_policy: RevisionPolicy
    symbol_effective_cutoff: datetime

    def __post_init__(self) -> None:
        layers = dict(self.layer_by_resolution)
        if not layers:
            raise ValueError("at least one resolution and layer are required")
        if self.adjustment not in {"raw", "adjusted"}:
            raise ValueError("adjustment must be raw or adjusted")
        if self.end_exclusive <= self.start:
            raise ValueError("end_exclusive must be after start")
        if self.latest_revision_policy not in {"latest-per-period", "all-available"}:
            raise ValueError("latest_revision_policy is unsupported")
        cutoff = self.symbol_effective_cutoff
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("symbol_effective_cutoff must be timezone-aware")
        for resolution, layer in layers.items():
            if (self.adjustment, layer, resolution) not in DATASET_CONTRACTS:
                raise ValueError(
                    f"no dataset contract for {self.adjustment}/{layer}/{resolution}"
                )
        object.__setattr__(self, "layer_by_resolution", layers)
        object.__setattr__(self, "symbol_effective_cutoff", cutoff.astimezone(UTC))


def _canonical_json(document: object) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _digest(document: object) -> str:
    return hashlib.sha256(_canonical_json(document)).hexdigest()


def _parse_timestamp(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeExportError(f"{label} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeExportError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _period_dates(manifest: Mapping[str, Any]) -> tuple[date, date]:
    start = _parse_timestamp(manifest["period_start"], "manifest.period_start")
    end = _parse_timestamp(manifest["period_end"], "manifest.period_end")
    return start.astimezone(ET).date(), end.astimezone(ET).date()


def _selected_manifests(
    catalog: MarketDataCatalog, selection: HistoricalSelection
) -> list[dict[str, Any]]:
    feed_ids: dict[str, str] = {
        str(row["code"]): str(row["id"])
        for row in catalog.records("market_data.feeds")
    }
    all_manifests = catalog.records("market_data.dataset_manifests")
    selected: list[dict[str, Any]] = []
    for resolution, layer in sorted(selection.layer_by_resolution.items()):
        contract = DATASET_CONTRACTS[(selection.adjustment, layer, resolution)]
        feed_id = feed_ids.get(contract.feed_code)
        if feed_id is None:
            raise RuntimeExportError(f"canonical feed is missing: {contract.feed_code}")
        candidates = [
            row
            for row in all_manifests
            if row["status"] == "AVAILABLE"
            and str(row["feed_id"]) == feed_id
            and row["data_layer"] == layer
            and row["resolution"] == resolution
            and _period_dates(row)[0] < selection.end_exclusive
            and _period_dates(row)[1] > selection.start
        ]
        if not candidates:
            raise RuntimeExportError(
                f"no AVAILABLE manifest for {selection.adjustment}/{layer}/{resolution}"
            )
        if selection.latest_revision_policy == "latest-per-period":
            by_period: dict[tuple[str, str], dict[str, Any]] = {}
            for row in candidates:
                key = (str(row["period_start"]), str(row["period_end"]))
                previous = by_period.get(key)
                if previous is None or int(row["revision_number"]) > int(
                    previous["revision_number"]
                ):
                    by_period[key] = row
            candidates = list(by_period.values())
        _assert_complete_coverage(candidates, selection, resolution)
        selected.extend(candidates)
    return sorted(
        selected,
        key=lambda row: (
            str(row["resolution"]),
            str(row["period_start"]),
            int(row["revision_number"]),
            str(row["id"]),
        ),
    )


def _assert_complete_coverage(
    manifests: list[dict[str, Any]],
    selection: HistoricalSelection,
    resolution: str,
) -> None:
    intervals = sorted(_period_dates(row) for row in manifests)
    cursor = selection.start
    for start, end in intervals:
        if end <= cursor:
            continue
        if start > cursor:
            raise RuntimeExportError(
                f"manifest coverage gap for {resolution}: {cursor} through {start}"
            )
        cursor = max(cursor, end)
        if cursor >= selection.end_exclusive:
            return
    raise RuntimeExportError(
        f"manifest coverage for {resolution} ends at {cursor}, before {selection.end_exclusive}"
    )


def _object_rows(
    catalog: MarketDataCatalog, manifests: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    manifest_ids = {str(row["id"]) for row in manifests}
    storage = {
        str(row["id"]): row for row in catalog.records("storage.objects")
    }
    grouped: dict[str, list[dict[str, Any]]] = {
        manifest_id: [] for manifest_id in manifest_ids
    }
    for relation in catalog.records("market_data.dataset_objects"):
        manifest_id = str(relation["dataset_manifest_id"])
        if manifest_id not in grouped:
            continue
        object_id = str(relation["object_id"])
        receipt = storage.get(object_id)
        if receipt is None:
            raise RuntimeExportError(f"dataset object references missing storage object {object_id}")
        version = receipt.get("provider_version_id")
        if not isinstance(version, str) or not version:
            raise RuntimeExportError(f"storage object {object_id} has no immutable version")
        grouped[manifest_id].append(receipt)
    for manifest_id, rows in grouped.items():
        if not rows:
            raise RuntimeExportError(f"manifest {manifest_id} has no storage objects")
        rows.sort(key=lambda row: (str(row["object_key"]), str(row["provider_version_id"])))
    return grouped


def _stream_to_file(body: BinaryIO, path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(long_path(path), "wb") as target:
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            target.write(chunk)
    return digest.hexdigest(), size


def _scan_object(
    client: VersionedS3Client,
    receipt: Mapping[str, Any],
    *,
    start: date,
    end_exclusive: date,
    temporary_root: Path,
) -> set[str]:
    bucket = str(receipt["bucket_name"])
    key = str(receipt["object_key"])
    version = str(receipt["provider_version_id"])
    response = client.get_object(Bucket=bucket, Key=key, VersionId=version)
    if str(response.get("VersionId", "")) != version:
        raise RuntimeExportError(f"GET returned a different version for s3://{bucket}/{key}")
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeExportError(f"GET returned no readable body for s3://{bucket}/{key}")
    path = temporary_root / f"{receipt['id']}.parquet"
    try:
        actual_hash, actual_size = _stream_to_file(body, path)
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()
    if actual_hash != receipt["content_hash"] or actual_size != int(receipt["byte_size"]):
        raise RuntimeExportError(f"downloaded bytes differ from receipt for s3://{bucket}/{key}")
    try:
        table = pq.read_table(
            long_path(path),
            columns=["instrument_id"],
            filters=[
                ("session_date_et", ">=", start),
                ("session_date_et", "<", end_exclusive),
            ],
        )
    except Exception as exc:
        raise RuntimeExportError(f"cannot scan canonical Parquet object {key}: {exc}") from exc
    return {str(value) for value in table.column("instrument_id").to_pylist()}


def _instrument_mapping(
    catalog: MarketDataCatalog,
    instrument_ids: set[str],
    cutoff: datetime,
) -> dict[str, str]:
    instruments = {
        str(row["id"]): row for row in catalog.records("market_data.instruments")
    }
    symbols = catalog.records("market_data.instrument_symbols")
    mapping: dict[str, str] = {}
    for instrument_id in sorted(instrument_ids):
        instrument = instruments.get(instrument_id)
        if instrument is None:
            raise RuntimeExportError(f"historical object names unknown instrument {instrument_id}")
        cutoff_date = cutoff.date()
        listed = instrument.get("listed_at")
        delisted = instrument.get("delisted_at")
        if listed is not None and date.fromisoformat(str(listed)) > cutoff_date:
            raise RuntimeExportError(f"instrument {instrument_id} was not listed at the cutoff")
        if delisted is not None and date.fromisoformat(str(delisted)) <= cutoff_date:
            raise RuntimeExportError(f"instrument {instrument_id} was delisted at the cutoff")
        active = [
            row
            for row in symbols
            if str(row["instrument_id"]) == instrument_id
            and row["exchange_mic"] == instrument["primary_exchange_mic"]
            and _parse_timestamp(row["effective_from"], "symbol.effective_from") <= cutoff
            and (
                row.get("effective_to") is None
                or cutoff < _parse_timestamp(row["effective_to"], "symbol.effective_to")
            )
        ]
        if len(active) != 1:
            raise RuntimeExportError(
                f"instrument {instrument_id} has {len(active)} primary symbols at the cutoff"
            )
        symbol = str(active[0]["symbol"])
        previous = mapping.get(symbol)
        if previous is not None and previous != instrument_id:
            raise RuntimeExportError(f"symbol {symbol} maps to multiple canonical instruments")
        mapping[symbol] = instrument_id
    if not mapping:
        raise RuntimeExportError("historical dataset intersection is empty")
    return dict(sorted(mapping.items()))


def _write_exact(path: Path, payload: bytes) -> str:
    path = path.expanduser().resolve()
    if path.exists():
        if path.is_file() and path.read_bytes() == payload:
            return "UNCHANGED"
        raise RuntimeExportError(f"runtime artifact already exists with different bytes: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        temporary_path.write_bytes(payload)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return "WRITTEN"


def export_trading_instruments(
    catalog: MarketDataCatalog,
    client: VersionedS3Client,
    selection: HistoricalSelection,
    *,
    output: Path,
    evidence_output: Path,
    expected_bucket: str,
    execute: bool,
) -> dict[str, Any]:
    """Intersect actual Parquet IDs and publish the gateway's symbol mapping."""

    manifests = _selected_manifests(catalog, selection)
    objects_by_manifest = _object_rows(catalog, manifests)
    all_objects = [
        row for manifest in manifests for row in objects_by_manifest[str(manifest["id"])]
    ]
    S3LegacyObjectVerifier(client, expected_bucket=expected_bucket).verify_all(all_objects)
    manifest_sets: list[set[str]] = []
    with tempfile.TemporaryDirectory(prefix="i2s-runtime-export-") as temporary:
        temporary_root = Path(temporary)
        for manifest in manifests:
            found: set[str] = set()
            for receipt in objects_by_manifest[str(manifest["id"])]:
                found.update(
                    _scan_object(
                        client,
                        receipt,
                        start=selection.start,
                        end_exclusive=selection.end_exclusive,
                        temporary_root=temporary_root,
                    )
                )
            if not found:
                raise RuntimeExportError(f"manifest {manifest['id']} has no rows in the selected range")
            manifest_sets.append(found)
    intersection = set.intersection(*manifest_sets)
    mapping = _instrument_mapping(catalog, intersection, selection.symbol_effective_cutoff)
    mapping_bytes = _canonical_json(mapping)
    source_objects = [
        {
            "dataset_manifest_id": str(manifest["id"]),
            "storage_object_id": str(row["id"]),
            "bucket_name": str(row["bucket_name"]),
            "object_key": str(row["object_key"]),
            "provider_version_id": str(row["provider_version_id"]),
            "content_hash": str(row["content_hash"]),
            "byte_size": int(row["byte_size"]),
        }
        for manifest in manifests
        for row in objects_by_manifest[str(manifest["id"])]
    ]
    source_manifests = [
        {
            "manifest_id": str(row["id"]),
            "dataset_hash": str(row["dataset_hash"]),
            "layer": str(row["data_layer"]),
            "resolution": str(row["resolution"]),
            "revision_number": int(row["revision_number"]),
            "period_start": str(row["period_start"]),
            "period_end": str(row["period_end"]),
        }
        for row in manifests
    ]
    evidence = {
        "contract_id": "idea2strategy.trading-instrument-runtime-export",
        "schema_version": 1,
        "selection": {
            "adjustment": selection.adjustment,
            "layer_by_resolution": dict(sorted(selection.layer_by_resolution.items())),
            "start": selection.start.isoformat(),
            "end_exclusive": selection.end_exclusive.isoformat(),
            "latest_revision_policy": selection.latest_revision_policy,
            "symbol_effective_cutoff": selection.symbol_effective_cutoff.isoformat().replace(
                "+00:00", "Z"
            ),
        },
        "source_manifests": source_manifests,
        "source_objects": source_objects,
        "source_digest": _digest(
            {"manifests": source_manifests, "objects": source_objects}
        ),
        "mapping_sha256": hashlib.sha256(mapping_bytes).hexdigest(),
        "instrument_count": len(mapping),
    }
    evidence_bytes = _canonical_json(evidence)
    report: dict[str, Any] = {
        "status": "DRY_RUN",
        "instrument_count": len(mapping),
        "manifest_count": len(manifests),
        "object_count": len(all_objects),
        "mapping_sha256": evidence["mapping_sha256"],
        "source_digest": evidence["source_digest"],
    }
    if not execute:
        return report
    mapping_status = _write_exact(output, mapping_bytes)
    evidence_status = _write_exact(evidence_output, evidence_bytes)
    return {
        **report,
        "status": (
            "ALREADY_APPLIED"
            if mapping_status == evidence_status == "UNCHANGED"
            else "EXPORTED"
        ),
        "output": str(output.expanduser().resolve()),
        "evidence_output": str(evidence_output.expanduser().resolve()),
    }
