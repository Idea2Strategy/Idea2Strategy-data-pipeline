"""Instrument-scoped canonical adjusted equity history for local development."""

from __future__ import annotations

import hashlib
import json
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from .catalog import MarketDataCatalog
from .contracts import (
    SCHEMA_VERSION,
    InstrumentMapping,
    canonical_dataset_hash,
    deterministic_uuid,
    iso_utc,
    sha256_file,
)
from .processing import (
    derive_regular_bars,
    filter_regular_session_bars,
    normalize_provider_frame,
    write_parquet,
)
from .storage import ObjectStore

TARGET_SYMBOLS = ("AAPL", "MSFT", "META", "AMZN", "NVDA")
FEEDS = {
    "30m": "ALPACA_SIP_ALL_30M",
    "1h": "ALPACA_SIP_ALL_1H",
    "4h": "ALPACA_SIP_ALL_4H",
    "1d": "ALPACA_SIP_ALL_1D",
}


def _legacy_object_key(
    resolution: str, year: int, revision: int, manifest_id: str
) -> str:
    return (
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        f"resolution={resolution}/revision={revision:08d}/year={year}/"
        f"shard=00-of-01/manifest_id={manifest_id}/part-00001.parquet"
    )


def _legacy_dataset_hash(
    *,
    feed_code: str,
    resolution: str,
    year: int,
    revision: int,
    content_hash: str,
    row_count: int,
) -> str:
    payload = {
        "provider": "ALPACA",
        "feed": feed_code,
        "adjustment": "all",
        "session": "XNYS_REGULAR",
        "resolution": resolution,
        "period_start": f"{year}-01-01",
        "period_end": f"{year + 1}-01-01",
        "revision": revision,
        "schema_version": "market-bars/1",
        "processing_version": "market-loader/1.0.0",
        "objects": [{
            "content_sha256": content_hash,
            "row_count": row_count,
            "period_start": f"{year}-01-01",
            "period_end": f"{year + 1}-01-01",
            "shard": 0,
            "part": 1,
        }],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def latest_permitted_sip_30m_end(observed_at: datetime) -> datetime:
    if observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    delayed = observed_at.astimezone(UTC) - timedelta(minutes=15)
    return delayed.replace(minute=(delayed.minute // 30) * 30, second=0, microsecond=0)


def fetch_adjusted_30m(
    api_key: str,
    api_secret: str,
    mappings: dict[str, InstrumentMapping],
    start: datetime,
    end: datetime,
) -> dict[str, pa.Table]:
    from alpaca.data.enums import Adjustment, DataFeed
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    if end <= start:
        raise ValueError("equity history end must be after start")
    client = StockHistoricalDataClient(api_key, api_secret)
    frames: dict[str, list[pd.DataFrame]] = {symbol: [] for symbol in mappings}
    cursor = start
    while cursor < end:
        upper = min(cursor + timedelta(days=180), end)
        request = StockBarsRequest(
            symbol_or_symbols=sorted(mappings),
            timeframe=TimeFrame(30, TimeFrameUnit.Minute),
            start=cursor,
            end=upper,
            adjustment=Adjustment.ALL,
            feed=DataFeed.SIP,
        )
        response = client.get_stock_bars(request).df
        if not response.empty:
            materialized = response.reset_index()
            for symbol in mappings:
                selected = materialized[materialized["symbol"] == symbol]
                if not selected.empty:
                    frames[symbol].append(selected)
        cursor = upper
        time.sleep(0.35)
    tables: dict[str, pa.Table] = {}
    for symbol, mapping in mappings.items():
        if not frames[symbol]:
            raise RuntimeError(f"Alpaca returned no adjusted 30m history for {symbol}")
        combined = pd.concat(frames[symbol], ignore_index=True)
        table = normalize_provider_frame(combined, mapping)
        if table.num_rows == 0:
            raise RuntimeError(f"Alpaca normalized history is empty for {symbol}")
        tables[symbol] = table
    return tables


def publish_instrument_year(
    catalog: MarketDataCatalog,
    object_store: ObjectStore,
    *,
    provider_code: str,
    feed_id: str,
    feed_code: str,
    symbol: str,
    instrument_id: str,
    resolution: str,
    data_layer: str,
    manifest_schema_version: str,
    year: int,
    table: pa.Table,
    observed_at: datetime,
) -> dict[str, Any]:
    session_years = pc.year(table.column("session_date_et"))
    selected = table.filter(pc.equal(session_years, pa.scalar(year, session_years.type)))
    if selected.num_rows == 0:
        raise ValueError(f"{symbol} {resolution} has no rows for {year}")
    selected = selected.sort_by([("bar_start_at", "ascending")])
    previous = catalog.latest_available_manifest(
        feed_id=feed_id,
        instrument_id=instrument_id,
        data_layer=data_layer,
        resolution=resolution,
        year=year,
    )
    revision = 1 if previous is None else int(previous["revision_number"]) + 1
    manifest_id = deterministic_uuid(
        "instrument-manifest", provider_code, feed_code, instrument_id, resolution, year, revision
    )
    if manifest_schema_version == "market-bars/1":
        if provider_code != "ALPACA" or not feed_code.startswith("ALPACA_SIP_ALL_"):
            raise ValueError("market-bars/1 metadata is supported only for adjusted Alpaca SIP feeds")
    elif manifest_schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported market-bar schema version: {manifest_schema_version}")

    def staged_table(staged_manifest_id: str) -> pa.Table:
        if manifest_schema_version != "market-bars/1":
            return selected
        legacy = selected
        if "source_minutes" in selected.schema.names:
            source_bar_count = pa.array(
                [int(value.as_py()) // 30 for value in selected.column("source_minutes")],
                type=pa.int16(),
            )
            legacy = selected.append_column(
                pa.field("source_bar_count", pa.int16(), nullable=False),
                source_bar_count,
            ).select([
                *(name for name in selected.schema.names if name != "source_minutes"),
                "source_bar_count",
                "source_minutes",
            ])
        return legacy.replace_schema_metadata({
            b"schema_version": b"market-bars/1",
            b"provider": b"alpaca",
            b"feed": b"sip",
            b"adjustment": b"all",
            b"session_scope": b"regular",
            b"resolution": resolution.encode("ascii"),
            b"manifest_id": staged_manifest_id.encode("ascii"),
        })
    actual_start = selected.column("bar_start_at")[0].as_py()
    actual_end = selected.column("bar_start_at")[-1].as_py()
    period_start = datetime(year, 1, 1, tzinfo=UTC)
    period_end = datetime(year + 1, 1, 1, tzinfo=UTC)
    object_key = (
        _legacy_object_key(resolution, year, revision, manifest_id)
        if manifest_schema_version == "market-bars/1"
        else (
            f"market-data/provider={provider_code}/feed={feed_code}/instrument={symbol}/"
            f"year={year}/revision={revision}/manifest_id={manifest_id}/part-00001.parquet"
        )
    )
    now = iso_utc(observed_at)
    with tempfile.TemporaryDirectory(prefix="i2s-equity-") as temporary:
        path = Path(temporary) / "bars.parquet"
        comparison_manifest_id = str(previous["id"]) if previous is not None else manifest_id
        write_parquet(staged_table(comparison_manifest_id), path)

        def staged_hashes(hash_revision: int) -> tuple[str, str]:
            candidate_hash = sha256_file(path)
            canonical = [{
                "content_hash": candidate_hash,
                "object_kind": "MARKET_BARS",
                "partition_granularity": "YEAR",
                "partition_start": f"{year}-01-01",
                "partition_end": f"{year + 1}-01-01",
                "period_start": iso_utc(period_start),
                "period_end": iso_utc(period_end),
                "shard_key": "s00-of-1",
                "part_number": 1,
                "row_count": selected.num_rows,
                "schema_version": manifest_schema_version,
            }]
            dataset_hash = (
                _legacy_dataset_hash(
                    feed_code=feed_code,
                    resolution=resolution,
                    year=year,
                    revision=hash_revision,
                    content_hash=candidate_hash,
                    row_count=selected.num_rows,
                )
                if manifest_schema_version == "market-bars/1"
                else canonical_dataset_hash(canonical)
            )
            return candidate_hash, dataset_hash

        comparison_revision = (
            int(previous["revision_number"]) if previous is not None else revision
        )
        candidate_hash, dataset_hash = staged_hashes(comparison_revision)
        if previous is not None and previous["dataset_hash"] == dataset_hash:
            previous_objects = catalog.objects_for_manifest(str(previous["id"]))
            expected_previous_key = _legacy_object_key(
                resolution, year, comparison_revision, str(previous["id"])
            )
            if len(previous_objects) == 1 and expected_previous_key == str(
                previous_objects[0].get("storage", {}).get("object_key", "")
            ):
                return {"status": "UNCHANGED", "manifest": previous, "rows": selected.num_rows}
        if comparison_manifest_id != manifest_id:
            write_parquet(staged_table(manifest_id), path)
            candidate_hash, dataset_hash = staged_hashes(revision)
        receipt = object_store.put(path, object_key)
        if receipt.content_hash != candidate_hash:
            raise RuntimeError("uploaded equity object hash differs from staged parquet bytes")
        verification = object_store.verify(object_key, receipt.content_hash)
        if not verification.ok:
            raise RuntimeError(f"published equity object failed verification: {verification.message}")
    object_id = deterministic_uuid("storage-object", receipt.content_hash, object_key)
    relation_id = deterministic_uuid("dataset-object", manifest_id, object_id)
    relation = {
        "id": relation_id,
        "dataset_manifest_id": manifest_id,
        "object_id": object_id,
        "object_kind": "MARKET_BARS",
        "partition_granularity": "YEAR",
        "partition_start": f"{year}-01-01",
        "partition_end": f"{year + 1}-01-01",
        "period_start": iso_utc(period_start),
        "period_end": iso_utc(period_end),
        "actual_start_at": iso_utc(actual_start),
        "actual_end_at": iso_utc(actual_end),
        "shard_key": "s00-of-1",
        "part_number": 1,
        "row_count": selected.num_rows,
        "min_instrument_id": instrument_id,
        "max_instrument_id": instrument_id,
    }
    storage = {
        "id": object_id,
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
        "schema_version": manifest_schema_version,
        "row_count": selected.num_rows,
        "period_start": iso_utc(period_start),
        "period_end": iso_utc(period_end),
        "encryption_key_ref": None,
        "retention_policy_version": "LOCAL_DEVELOPMENT_ONLY",
        "retention_until": None,
        "legal_hold": False,
        "created_at": now,
        "verified_at": now,
        "quarantined_at": None,
        "superseded_at": None,
        "deleted_at": None,
    }
    building = {
        "id": manifest_id,
        "feed_id": feed_id,
        "instrument_id": instrument_id,
        "data_layer": data_layer,
        "resolution": resolution,
        "revision_number": revision,
        "status": "BUILDING",
        "period_start": iso_utc(period_start),
        "period_end": iso_utc(period_end),
        "actual_start_at": None,
        "actual_end_at": None,
        "schema_version": manifest_schema_version,
        "dataset_hash": hashlib.sha256(f"BUILDING:{manifest_id}".encode()).hexdigest(),
        "supersedes_manifest_id": None if previous is None else previous["id"],
        "created_at": now,
        "available_at": None,
        "object_count": 0,
    }
    available = {
        **building,
        "status": "AVAILABLE",
        "actual_start_at": iso_utc(actual_start),
        "actual_end_at": iso_utc(actual_end),
        "dataset_hash": dataset_hash,
        "available_at": now,
        "object_count": 1,
    }
    try:
        with catalog.transaction():
            catalog.publish_manifest(building)
            catalog.stage_object(storage, relation)
            catalog.publish_manifest(available)
            if previous is not None:
                catalog.publish_manifest({**previous, "status": "SUPERSEDED"})
    except Exception:
        object_store.delete(receipt)
        raise
    return {"status": "PUBLISHED", "manifest": available, "rows": selected.num_rows}


def derive_required_resolutions(adjusted_30m: pa.Table) -> dict[str, pa.Table]:
    regular_30m = filter_regular_session_bars(adjusted_30m)
    return {
        "30m": regular_30m,
        "1h": derive_regular_bars(regular_30m, "1h"),
        "4h": derive_regular_bars(regular_30m, "4h"),
        "1d": derive_regular_bars(regular_30m, "1d"),
    }
