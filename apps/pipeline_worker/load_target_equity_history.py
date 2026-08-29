"""Load the canonical long-horizon local demo equities without shrinking the universe."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import boto3
import redis

from apps.pipeline_worker.sync_market_history import HistorySyncConfig, _project_history
from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.contracts import InstrumentMapping
from market_pipeline_lib.equity_history import (
    FEEDS,
    TARGET_SYMBOLS,
    derive_required_resolutions,
    fetch_adjusted_30m,
    publish_instrument_year,
)
from market_pipeline_lib.storage import S3ObjectStore


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def execute() -> dict[str, object]:
    config = HistorySyncConfig.from_environment()
    endpoint = _required("PIPELINE_WORKER_AWS_ENDPOINT_URL")
    observed_at = datetime.now(UTC)
    catalog = PostgresCatalog.connect(
        config.database_url,
        artifact_root=config.state_root / "target-equity-history" / "catalog-artifacts",
        storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
    )
    store = S3ObjectStore(
        config.bucket,
        client=boto3.client("s3", endpoint_url=endpoint),
    )
    cache = redis.Redis.from_url(config.redis_uri, decode_responses=True)
    try:
        catalog.verify_schema()
        instruments = {str(row["id"]): row for row in catalog.records("market_data.instruments")}
        symbols = {
            str(row["symbol"]): row
            for row in catalog.records("market_data.instrument_symbols")
            if row["effective_to"] is None and str(row["symbol"]) in TARGET_SYMBOLS
        }
        if set(symbols) != set(TARGET_SYMBOLS):
            raise RuntimeError(f"canonical target symbols are missing: {sorted(set(TARGET_SYMBOLS) - set(symbols))}")
        mappings = {}
        for symbol in TARGET_SYMBOLS:
            symbol_row = symbols[symbol]
            instrument = instruments[str(symbol_row["instrument_id"])]
            mappings[symbol] = InstrumentMapping(
                provider_symbol=symbol,
                instrument_id=str(instrument["id"]),
            )
        feeds = {str(row["code"]): str(row["id"]) for row in catalog.records("market_data.feeds")}
        missing_feeds = sorted(set(FEEDS.values()) - set(feeds))
        if missing_feeds:
            raise RuntimeError(f"canonical adjusted feeds are missing: {missing_feeds}")
        source = fetch_adjusted_30m(
            config.api_key,
            config.api_secret,
            mappings,
            datetime(2015, 1, 1, tzinfo=UTC),
            observed_at,
        )
        publication_count = 0
        rows: dict[str, dict[str, int]] = {}
        for symbol, base in source.items():
            rows[symbol] = {}
            for resolution, table in derive_required_resolutions(base).items():
                rows[symbol][resolution] = table.num_rows
                years = sorted({value.year for value in table.column("session_date_et").to_pylist()})
                for year in years:
                    result = publish_instrument_year(
                        catalog,
                        store,
                        provider_code="ALPACA",
                        feed_id=feeds[FEEDS[resolution]],
                        feed_code=FEEDS[resolution],
                        symbol=symbol,
                        instrument_id=mappings[symbol].instrument_id,
                        resolution=resolution,
                        data_layer="ADJUSTED",
                        manifest_schema_version="market-bars/1",
                        year=year,
                        table=table,
                        observed_at=observed_at,
                    )
                    publication_count += result["status"] == "PUBLISHED"

        feed_by_id = {str(row["id"]): str(row["code"]) for row in catalog.records("market_data.feeds")}

        def selector(active_catalog, timeframe: str, _layer: str):
            expected = FEEDS[timeframe]
            return sorted(
                (
                    row for row in active_catalog.records("market_data.dataset_manifests")
                    if row["status"] == "AVAILABLE"
                    and feed_by_id.get(str(row["feed_id"])) == expected
                    and row["data_layer"] == "ADJUSTED"
                    and row["resolution"] == timeframe
                ),
                key=lambda row: (str(row["period_start"]), int(row["revision_number"])),
            )

        projection = _project_history(
            catalog,
            store,
            cache,
            config,
            observed_at,
            manifest_selector=selector,
            require_storage_metadata=False,
        )
        return {
            "status": "SUCCEEDED",
            "publishedManifests": publication_count,
            "rows": rows,
            "projection": projection,
        }
    finally:
        cache.close()
        catalog.close()


def main() -> int:
    print(json.dumps(execute(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
