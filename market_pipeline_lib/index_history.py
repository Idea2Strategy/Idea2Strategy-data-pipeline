"""Canonical S&P 500 and NASDAQ-100 cash-index history publication.

Yahoo's chart endpoint is used only as the upstream source.  Every response is
validated, written as immutable Parquet, verified in the object store, and then
published through the same manifest catalog as equity history.  Redis is merely a
rebuildable read model and is intentionally outside this module.
"""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pyarrow as pa

from .catalog import MarketDataCatalog
from .contracts import (
    ET,
    SCHEMA_VERSION,
    bar_schema,
    canonical_dataset_hash,
    deterministic_uuid,
    iso_utc,
    sha256_file,
)
from .processing import write_parquet
from .storage import ObjectStore

PROVIDER_CODE = "YAHOO_FINANCE_INDEX"
PROVIDER_ID = deterministic_uuid("provider", PROVIDER_CODE)
FEED_VERSION = "yahoo-chart-v8-index-1d-v1"
BENCHMARKS: dict[str, dict[str, str]] = {
    "SPX": {
        "provider_symbol": "^GSPC",
        "instrument_id": "2d4bf3fb-4f1d-5a58-a6b1-96c00d0bc001",
        "symbol_id": "2d4bf3fb-4f1d-5a58-a6b1-96c00d0bd001",
        "exchange_mic": "XNYS",
        "listed_at": "1957-03-04",
        "feed": "SPX_DAILY",
    },
    "NDX": {
        "provider_symbol": "^NDX",
        "instrument_id": "2d4bf3fb-4f1d-5a58-a6b1-96c00d0bc002",
        "symbol_id": "2d4bf3fb-4f1d-5a58-a6b1-96c00d0bd002",
        "exchange_mic": "XNAS",
        "listed_at": "1985-01-31",
        "feed": "NDX_DAILY",
    },
}


def parse_yahoo_chart(
    payload: Mapping[str, Any], *, expected_symbol: str
) -> list[dict[str, Any]]:
    chart = payload.get("chart")
    if not isinstance(chart, Mapping) or chart.get("error") is not None:
        raise ValueError(f"Yahoo chart error: {None if not isinstance(chart, Mapping) else chart.get('error')}")
    results = chart.get("result")
    if not isinstance(results, list) or len(results) != 1:
        raise ValueError("Yahoo chart must contain exactly one result")
    result = results[0]
    if not isinstance(result, Mapping):
        raise ValueError("Yahoo chart result is malformed")
    meta = result.get("meta")
    actual_symbol = meta.get("symbol") if isinstance(meta, Mapping) else None
    if actual_symbol != expected_symbol:
        raise ValueError(f"unexpected Yahoo symbol: expected {expected_symbol}, got {actual_symbol}")
    timestamps = result.get("timestamp")
    indicators = result.get("indicators")
    quotes = indicators.get("quote") if isinstance(indicators, Mapping) else None
    quote_row = quotes[0] if isinstance(quotes, list) and len(quotes) == 1 else None
    if not isinstance(timestamps, list) or not isinstance(quote_row, Mapping):
        raise ValueError("Yahoo chart OHLCV columns are missing")
    columns = [quote_row.get(name) for name in ("open", "high", "low", "close", "volume")]
    if any(not isinstance(column, list) for column in columns):
        raise ValueError("Yahoo chart OHLCV columns are missing")
    if any(len(column) != len(timestamps) for column in columns):
        raise ValueError("Yahoo chart column lengths do not match")
    bars: list[dict[str, Any]] = []
    for index, raw_timestamp in enumerate(timestamps):
        values = [column[index] for column in columns]
        if any(value is None for value in values):
            raise ValueError(f"Yahoo chart contains missing OHLCV at row {index}")
        open_, high, low, close = (float(value) for value in values[:4])
        volume = int(values[4])
        if min(open_, high, low, close) <= 0 or volume < 0:
            raise ValueError(f"Yahoo chart contains non-positive price or volume at row {index}")
        if high < max(open_, close) or low > min(open_, close) or high < low:
            raise ValueError(f"Yahoo chart contains invalid OHLC at row {index}")
        bars.append({
            "bar_start_at": datetime.fromtimestamp(int(raw_timestamp), UTC),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })
    if not bars:
        raise ValueError("Yahoo chart returned no bars")
    if bars != sorted(bars, key=lambda row: row["bar_start_at"]):
        raise ValueError("Yahoo chart timestamps are not ordered")
    if len({row["bar_start_at"] for row in bars}) != len(bars):
        raise ValueError("Yahoo chart contains duplicate timestamps")
    return bars


def fetch_yahoo_chart(
    provider_symbol: str, start: date, end_exclusive: date, *, timeout_seconds: int = 60
) -> tuple[list[dict[str, Any]], str]:
    if end_exclusive <= start:
        raise ValueError("index history end must be after start")
    parameters = urlencode({
        "period1": int(datetime.combine(start, datetime.min.time(), UTC).timestamp()),
        "period2": int(datetime.combine(end_exclusive, datetime.min.time(), UTC).timestamp()),
        "interval": "1d",
        "events": "history",
    })
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(provider_symbol, safe='')}?{parameters}"
    request = Request(url, headers={"User-Agent": "Idea2Strategy-local-canonical-loader/1.0"})
    import json

    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read()
    payload = json.loads(body)
    return parse_yahoo_chart(payload, expected_symbol=provider_symbol), hashlib.sha256(body).hexdigest()


def ensure_benchmark_metadata(catalog: MarketDataCatalog, observed_at: datetime) -> None:
    now = iso_utc(observed_at)
    catalog.upsert("market_data.providers", {
        "id": PROVIDER_ID,
        "code": PROVIDER_CODE,
        "display_name": "Yahoo Finance cash-index history",
        "rights_version": "LOCAL_DEVELOPMENT_ONLY",
        "status": "LOCAL_DEVELOPMENT_ONLY",
        "created_at": now,
    })
    for symbol, benchmark in BENCHMARKS.items():
        feed_id = deterministic_uuid("feed", PROVIDER_CODE, benchmark["feed"])
        catalog.upsert("market_data.feeds", {
            "id": feed_id,
            "provider_id": PROVIDER_ID,
            "code": benchmark["feed"],
            "data_kind": "BARS",
            "resolution": "1d",
            "timezone_name": "America/New_York",
            "feed_version": FEED_VERSION,
            "created_at": now,
            "retired_at": None,
        })
        catalog.upsert("market_data.instruments", {
            "id": benchmark["instrument_id"],
            "asset_type": "INDEX",
            "primary_exchange_mic": benchmark["exchange_mic"],
            "currency_code": "USD",
            "provider_reference": benchmark["provider_symbol"],
            "listed_at": benchmark["listed_at"],
            "delisted_at": None,
            "created_at": now,
        })
        catalog.upsert("market_data.instrument_symbols", {
            "id": benchmark["symbol_id"],
            "instrument_id": benchmark["instrument_id"],
            "exchange_mic": benchmark["exchange_mic"],
            "symbol": symbol,
            "effective_from": f"{benchmark['listed_at']}T00:00:00Z",
            "effective_to": None,
        })


def _bar_table(benchmark: Mapping[str, str], bars: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist([
        {
            "instrument_id": benchmark["instrument_id"],
            "provider_symbol": benchmark["provider_symbol"],
            "bar_start_at": bar["bar_start_at"],
            "session_date_et": bar["bar_start_at"].astimezone(ET).date(),
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
            "trade_count": None,
            "vwap": None,
        }
        for bar in bars
    ], schema=bar_schema())


def publish_benchmark_year(
    catalog: MarketDataCatalog,
    object_store: ObjectStore,
    symbol: str,
    year: int,
    bars: list[dict[str, Any]],
    *,
    source_response_hash: str,
    observed_at: datetime,
) -> dict[str, Any]:
    if not source_response_hash or len(source_response_hash) != 64:
        raise ValueError("source response SHA-256 is required")
    benchmark = BENCHMARKS[symbol]
    selected = [bar for bar in bars if bar["bar_start_at"].astimezone(ET).year == year]
    if not selected:
        raise ValueError(f"{symbol} has no provider rows for {year}")
    feed_id = deterministic_uuid("feed", PROVIDER_CODE, benchmark["feed"])
    previous = catalog.latest_available_manifest(
        feed_id=feed_id,
        instrument_id=benchmark["instrument_id"],
        data_layer="DERIVED",
        resolution="1d",
        year=year,
    )
    revision = 1 if previous is None else int(previous["revision_number"]) + 1
    manifest_id = deterministic_uuid("index-manifest", symbol, year, revision)
    table = _bar_table(benchmark, selected)
    actual_start = selected[0]["bar_start_at"]
    actual_end = selected[-1]["bar_start_at"]
    period_start = datetime(year, 1, 1, tzinfo=UTC)
    period_end = datetime(year + 1, 1, 1, tzinfo=UTC)
    object_key = (
        f"market-data/provider={PROVIDER_CODE}/feed={benchmark['feed']}/"
        f"instrument={symbol}/year={year}/revision={revision}/part-00001.parquet"
    )
    now = iso_utc(observed_at)
    with tempfile.TemporaryDirectory(prefix="i2s-index-") as temporary:
        path = Path(temporary) / "bars.parquet"
        write_parquet(table, path)
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
            "row_count": len(selected),
            "schema_version": SCHEMA_VERSION,
        }]
        dataset_hash = canonical_dataset_hash(canonical)
        if previous is not None and previous["dataset_hash"] == dataset_hash:
            return {"status": "UNCHANGED", "manifest": previous, "rows": len(selected)}
        receipt = object_store.put(path, object_key)
        if receipt.content_hash != candidate_hash:
            raise RuntimeError("uploaded index object hash differs from staged parquet bytes")
        verification = object_store.verify(object_key, receipt.content_hash)
        if not verification.ok:
            raise RuntimeError(f"published index object failed verification: {verification.message}")
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
        "row_count": len(selected),
        "min_instrument_id": benchmark["instrument_id"],
        "max_instrument_id": benchmark["instrument_id"],
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
        "schema_version": SCHEMA_VERSION,
        "row_count": len(selected),
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
        "instrument_id": benchmark["instrument_id"],
        "data_layer": "DERIVED",
        "resolution": "1d",
        "revision_number": revision,
        "status": "BUILDING",
        "period_start": iso_utc(period_start),
        "period_end": iso_utc(period_end),
        "actual_start_at": None,
        "actual_end_at": None,
        "schema_version": SCHEMA_VERSION,
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
    with catalog.transaction():
        catalog.publish_manifest(building)
        catalog.stage_object(storage, relation)
        catalog.publish_manifest(available)
        if previous is not None:
            catalog.publish_manifest({**previous, "status": "SUPERSEDED"})
    return {"status": "PUBLISHED", "manifest": available, "rows": len(selected)}
