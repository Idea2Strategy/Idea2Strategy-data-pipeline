"""Production object-store adapters for corporate-action regeneration."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from ..contracts import SCHEMA_VERSION, bar_schema
from ..storage import ObjectStore
from .adjustment import Bar
from .regeneration import WrittenDataset

__all__ = ["CatalogObjectBarReader", "ImmutableObjectBarWriter"]

_EASTERN = ZoneInfo("America/New_York")


class CatalogObjectBarReader:
    """Resolve manifest relations, verify immutable objects, and read canonical Parquet."""

    def __init__(self, *, catalog: Any, object_store: ObjectStore) -> None:
        self._catalog = catalog
        self._store = object_store

    def read_bars(self, manifest_id: str) -> Sequence[Bar]:
        objects = self._catalog.objects_for_manifest(manifest_id)
        if not objects:
            raise LookupError(f"manifest {manifest_id} has no registered dataset objects")
        bars: list[Bar] = []
        for item in objects:
            storage = item["storage"]
            if storage["status"] != "AVAILABLE":
                raise RuntimeError(f"manifest {manifest_id} references a non-AVAILABLE object")
            key = str(storage["object_key"])
            verification = self._store.verify(key, str(storage["content_hash"]))
            if not verification.ok:
                raise OSError(f"raw object verification failed for {key}: {verification.message}")
            with self._store.open(key) as body:
                # boto3 StreamingBody is not seekable, while Parquet needs
                # footer seeks. Spool to disk beyond 64 MiB so a yearly shard
                # cannot exhaust worker memory.
                with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024) as parquet:
                    shutil.copyfileobj(body, parquet)
                    parquet.seek(0)
                    table = pq.read_table(parquet)
            missing = set(bar_schema(False).names) - set(table.column_names)
            if missing:
                raise ValueError(f"raw object {key} is missing canonical bar columns: {sorted(missing)}")
            for row in table.to_pylist():
                bars.append(
                    Bar(
                        instrument_id=str(row["instrument_id"]),
                        bar_start_at=_utc(row["bar_start_at"]),
                        open=Decimal(str(row["open"])),
                        high=Decimal(str(row["high"])),
                        low=Decimal(str(row["low"])),
                        close=Decimal(str(row["close"])),
                        volume=int(row["volume"]),
                        provider_symbol=str(row["provider_symbol"]),
                        session_date_et=row["session_date_et"],
                        trade_count=(None if row["trade_count"] is None else int(row["trade_count"])),
                        vwap=(None if row["vwap"] is None else Decimal(str(row["vwap"]))),
                    )
                )
        return tuple(sorted(bars, key=lambda bar: (bar.instrument_id, bar.bar_start_at)))


class ImmutableObjectBarWriter:
    """Write one canonical immutable Parquet object and return its catalog registration."""

    def __init__(self, *, object_store: ObjectStore, staging_root: Path) -> None:
        self._store = object_store
        self._staging_root = staging_root

    def write_bars(self, bars: Sequence[Bar], *, dataset_key: str) -> WrittenDataset:
        if not bars:
            raise ValueError("an adjusted dataset cannot publish an empty object")
        if any(not bar.provider_symbol for bar in bars):
            raise ValueError("canonical adjusted bars require provider_symbol from the raw object")
        ordered = tuple(sorted(bars, key=lambda bar: (bar.instrument_id, bar.bar_start_at)))
        table = _bar_table(ordered)
        year = min(bar.bar_start_at for bar in ordered).year
        object_key = (
            f"market-data/{dataset_key.strip('/')}/granularity=YEAR/"
            f"partition_start={year}-01-01/partition_end={year + 1}-01-01/"
            "shard=s00-of-01/part-00001.parquet"
        )
        self._staging_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=self._staging_root) as temporary:
            path = Path(temporary) / "adjusted.parquet"
            pq.write_table(table, path, compression="zstd", version="2.6")
            receipt = self._store.put(path, object_key)
        verified = self._store.verify(object_key, receipt.content_hash)
        if not verified.ok or verified.byte_size != receipt.byte_size:
            raise OSError(f"adjusted object verification failed for {object_key}")

        starts = [bar.bar_start_at for bar in ordered]
        period_start = min(starts)
        period_end = max(starts) + _resolution_duration(dataset_key)
        now = datetime.now(UTC)
        iso_start = _iso(period_start)
        iso_end = _iso(period_end)
        storage = {
            "status": "AVAILABLE",
            "storage_provider": receipt.storage_provider,
            "bucket_name": receipt.bucket_name,
            "object_key": receipt.object_key,
            "provider_version_id": receipt.provider_version_id,
            "content_hash": receipt.content_hash,
            "byte_size": receipt.byte_size,
            "file_format": "PARQUET",
            "compression_codec": "ZSTD",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": SCHEMA_VERSION,
            "row_count": len(ordered),
            "period_start": iso_start,
            "period_end": iso_end,
            "encryption_key_ref": None,
            "retention_policy_version": "UNSPECIFIED",
            "retention_until": None,
            "legal_hold": False,
            "created_at": _iso(now),
            "verified_at": _iso(now),
            "quarantined_at": None,
            "superseded_at": None,
            "deleted_at": None,
        }
        instrument_ids = sorted({bar.instrument_id for bar in ordered})
        relation = {
            "object_kind": "MARKET_BARS",
            "partition_granularity": "YEAR",
            "partition_start": date(year, 1, 1).isoformat(),
            "partition_end": date(year + 1, 1, 1).isoformat(),
            "period_start": iso_start,
            "period_end": iso_end,
            "shard_key": "s00-of-01",
            "part_number": 1,
            "row_count": len(ordered),
            "min_instrument_id": instrument_ids[0],
            "max_instrument_id": instrument_ids[-1],
        }
        return WrittenDataset(
            object_key=receipt.object_key,
            content_hash=receipt.content_hash,
            row_count=len(ordered),
            byte_size=receipt.byte_size,
            storage_record=storage,
            relation_record=relation,
        )


def _bar_table(bars: Sequence[Bar]) -> pa.Table:
    values = {
        "instrument_id": [bar.instrument_id for bar in bars],
        "provider_symbol": [bar.provider_symbol for bar in bars],
        "bar_start_at": [bar.bar_start_at for bar in bars],
        "session_date_et": [
            bar.session_date_et or bar.bar_start_at.astimezone(_EASTERN).date() for bar in bars
        ],
        "open": [float(bar.open) for bar in bars],
        "high": [float(bar.high) for bar in bars],
        "low": [float(bar.low) for bar in bars],
        "close": [float(bar.close) for bar in bars],
        "volume": [bar.volume for bar in bars],
        "trade_count": [bar.trade_count for bar in bars],
        "vwap": [None if bar.vwap is None else float(bar.vwap) for bar in bars],
    }
    schema = bar_schema(False)
    return pa.Table.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in schema], schema=schema
    )


def _resolution_duration(dataset_key: str) -> timedelta:
    marker = "resolution="
    if marker not in dataset_key:
        raise ValueError("dataset key must bind a resolution")
    value = dataset_key.split(marker, 1)[1].split("/", 1)[0]
    unit = value[-1]
    amount = int(value[:-1])
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    raise ValueError(f"unsupported bar resolution {value!r}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("canonical bar timestamp is not timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
