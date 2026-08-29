"""Load canonical SPX/NDX cash-index history and rebuild their Redis projections."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pyarrow.parquet as pq
import redis

from apps.pipeline_worker.sync_market_history import _verified_parquet, compact_history_payload
from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.index_history import (
    BENCHMARKS,
    PROVIDER_CODE,
    ensure_benchmark_metadata,
    fetch_yahoo_chart,
    publish_benchmark_year,
)
from market_pipeline_lib.storage import S3ObjectStore


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def execute(
    environment: Mapping[str, str] | None = None,
    *,
    start: date = date(2015, 1, 1),
    through: date | None = None,
) -> dict[str, Any]:
    values = os.environ if environment is None else environment
    database_url = _required(values, "PIPELINE_WORKER_DATABASE_URL")
    bucket = _required(values, "MARKET_DATA_BUCKET")
    redis_uri = _required(values, "PIPELINE_WORKER_MARKET_HISTORY_REDIS_URI")
    endpoint = values.get("PIPELINE_WORKER_AWS_ENDPOINT_URL", "").strip() or None
    prefix = values.get("PIPELINE_WORKER_MARKET_HISTORY_REDIS_KEY_PREFIX", "i2s").strip()
    if not prefix or "{" in prefix or "}" in prefix:
        raise ValueError("market history Redis key prefix must be plain and non-empty")
    observed_at = datetime.now(UTC)
    final_day = through or observed_at.date()
    if final_day < start:
        raise ValueError("through must not be before start")
    state_root = Path(values.get("PIPELINE_WORKER_OBJECT_STORE_ROOT", ".local/pipeline")).resolve()
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    store = S3ObjectStore(bucket, client=s3_client)
    catalog = PostgresCatalog.connect(
        database_url,
        artifact_root=state_root / "index-history" / "catalog-artifacts",
        storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
    )
    cache = redis.Redis.from_url(redis_uri, decode_responses=True)
    summary: dict[str, Any] = {"status": "SUCCEEDED", "benchmarks": []}
    try:
        catalog.verify_schema()
        ensure_benchmark_metadata(catalog, observed_at)
        for symbol, benchmark in BENCHMARKS.items():
            bars, response_hash = fetch_yahoo_chart(
                benchmark["provider_symbol"], start, final_day + timedelta(days=1)
            )
            years = sorted({bar["bar_start_at"].year for bar in bars})
            publications = [
                publish_benchmark_year(
                    catalog,
                    store,
                    symbol,
                    year,
                    bars,
                    source_response_hash=response_hash,
                    observed_at=observed_at,
                )
                for year in years
            ]
            manifests = sorted(
                (
                    row for row in catalog.records("market_data.dataset_manifests")
                    if row["status"] == "AVAILABLE"
                    and row["instrument_id"] == benchmark["instrument_id"]
                    and row["resolution"] == "1d"
                ),
                key=lambda row: row["actual_start_at"] or row["period_start"],
            )
            projected: list[dict[str, Any]] = []
            object_hashes: list[str] = []
            with tempfile.TemporaryDirectory(prefix="i2s-index-projection-") as temporary:
                for manifest in manifests:
                    for relation in catalog.objects_for_manifest(manifest["id"]):
                        path = _verified_parquet(store, relation, Path(temporary))
                        projected.extend(pq.read_table(path).to_pylist())
                        object_hashes.append(relation["storage"]["content_hash"])
                        path.unlink(missing_ok=True)
            projected.sort(key=lambda bar: bar["bar_start_at"])
            if not projected:
                raise RuntimeError(f"{symbol} canonical manifests contain no rows")
            encoded = compact_history_payload(
                benchmark["instrument_id"],
                "1d",
                projected,
                manifest_ids=tuple(row["id"] for row in manifests),
                dataset_hashes=tuple(row["dataset_hash"] for row in manifests),
                object_hashes=tuple(object_hashes),
                revision=max(int(row["revision_number"]) for row in manifests),
                provider=PROVIDER_CODE,
                feed=benchmark["feed"],
            )
            key = f"{{{prefix}:market}}:history:bars:{benchmark['instrument_id']}:1d"
            cache.set(key, encoded)
            summary["benchmarks"].append({
                "symbol": symbol,
                "instrumentId": benchmark["instrument_id"],
                "rows": len(projected),
                "actualFrom": projected[0]["bar_start_at"].isoformat(),
                "actualTo": projected[-1]["bar_start_at"].isoformat(),
                "publishedYears": sum(item["status"] == "PUBLISHED" for item in publications),
                "unchangedYears": sum(item["status"] == "UNCHANGED" for item in publications),
            })
    finally:
        cache.close()
        catalog.close()
    return summary


def main() -> int:
    print(json.dumps(execute(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
