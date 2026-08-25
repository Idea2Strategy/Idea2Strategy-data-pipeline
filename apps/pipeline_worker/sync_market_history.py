"""Scheduled canonical history publication and Redis read-model projection."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas_market_calendars as mcal
import pyarrow.parquet as pq

from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.contracts import ADJUSTED_FEED, ET, deterministic_uuid
from market_pipeline_lib.engine import AlpacaBarSource, MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.storage import S3ObjectStore

TIMEFRAMES: dict[str, tuple[str, int]] = {
    "30m": ("ADJUSTED", 120),
    "1h": ("DERIVED", 240),
    "4h": ("DERIVED", 600),
    "1d": ("DERIVED", 900),
}
PUBLICATION_SESSION_BATCH = 20


@dataclass(frozen=True)
class HistorySyncConfig:
    database_url: str
    bucket: str
    redis_uri: str
    redis_key_prefix: str
    limit: int
    api_key: str
    api_secret: str
    state_root: Path

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> HistorySyncConfig:
        values = os.environ if environment is None else environment
        required = {
            "PIPELINE_WORKER_DATABASE_URL": values.get("PIPELINE_WORKER_DATABASE_URL", ""),
            "MARKET_DATA_BUCKET": values.get("MARKET_DATA_BUCKET", ""),
            "PIPELINE_WORKER_MARKET_HISTORY_REDIS_URI": values.get(
                "PIPELINE_WORKER_MARKET_HISTORY_REDIS_URI", ""
            ),
            "ALPACA_API_KEY": values.get("ALPACA_API_KEY", ""),
            "ALPACA_SECRET_KEY": values.get("ALPACA_SECRET_KEY", ""),
        }
        missing = sorted(name for name, value in required.items() if not value.strip())
        if missing:
            raise ValueError(f"sync-market-history missing environment variables: {missing}")
        prefix = values.get("PIPELINE_WORKER_MARKET_HISTORY_REDIS_KEY_PREFIX", "i2s")
        if not prefix or "{" in prefix or "}" in prefix:
            raise ValueError("market history Redis key prefix must be plain and non-empty")
        try:
            limit = int(values.get("PIPELINE_WORKER_MARKET_HISTORY_LIMIT", "1000"))
        except ValueError as error:
            raise ValueError("PIPELINE_WORKER_MARKET_HISTORY_LIMIT must be an integer") from error
        if limit < 1 or limit > 1_000:
            raise ValueError("PIPELINE_WORKER_MARKET_HISTORY_LIMIT must be between 1 and 1000")
        state_root = Path(
            values.get("PIPELINE_WORKER_OBJECT_STORE_ROOT", "/var/lib/idea2strategy/pipeline")
        ).expanduser()
        return cls(
            database_url=required["PIPELINE_WORKER_DATABASE_URL"],
            bucket=required["MARKET_DATA_BUCKET"],
            redis_uri=required["PIPELINE_WORKER_MARKET_HISTORY_REDIS_URI"],
            redis_key_prefix=prefix,
            limit=limit,
            api_key=required["ALPACA_API_KEY"],
            api_secret=required["ALPACA_SECRET_KEY"],
            state_root=state_root,
        )


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must be timezone-aware: {value}")
    return parsed.astimezone(UTC)


def completed_sessions_after(
    latest_period_end: datetime, now: datetime, calendar_name: str = "XNYS"
) -> list[date]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    schedule = mcal.get_calendar(calendar_name).schedule(
        start_date=latest_period_end.astimezone(ET).date(),
        end_date=now.astimezone(ET).date(),
        tz="UTC",
    )
    return [
        index.date()
        for index, row in schedule.iterrows()
        if row["market_close"].to_pydatetime() > latest_period_end
        and row["market_close"].to_pydatetime() <= now
    ]


def _adjusted_manifests(catalog: Any, resolution: str, layer: str) -> list[dict[str, Any]]:
    feed_id = deterministic_uuid("feed", "ALPACA", ADJUSTED_FEED)
    return sorted(
        (
            row
            for row in catalog.records("market_data.dataset_manifests")
            if row["status"] == "AVAILABLE"
            and str(row["feed_id"]) == feed_id
            and row["data_layer"] == layer
            and row["resolution"] == resolution
        ),
        key=lambda row: (_timestamp(row["period_start"]), int(row["revision_number"])),
    )


def _write_instrument_map(catalog: Any, output: Path, now: datetime) -> int:
    instruments = {
        str(row["id"]): row for row in catalog.records("market_data.instruments")
    }
    cutoff = now.date()
    rows: list[tuple[str, str]] = []
    for symbol in catalog.records("market_data.instrument_symbols"):
        instrument = instruments.get(str(symbol["instrument_id"]))
        if instrument is None:
            continue
        if symbol["exchange_mic"] != instrument["primary_exchange_mic"]:
            continue
        effective_from = _timestamp(symbol["effective_from"])
        effective_to = (
            None if symbol.get("effective_to") is None else _timestamp(symbol["effective_to"])
        )
        if effective_from > now or (effective_to is not None and now >= effective_to):
            continue
        listed = instrument.get("listed_at")
        delisted = instrument.get("delisted_at")
        if listed is not None and date.fromisoformat(str(listed)) > cutoff:
            continue
        if delisted is not None and date.fromisoformat(str(delisted)) <= cutoff:
            continue
        rows.append((str(symbol["symbol"]), str(instrument["id"])))
    rows.sort()
    if not rows:
        raise RuntimeError("canonical catalog has no active primary instrument symbols")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("provider_symbol", "instrument_id"))
        writer.writerows(rows)
    return len(rows)


def _publish_missing_history(
    catalog: Any,
    object_store: S3ObjectStore,
    config: HistorySyncConfig,
    now: datetime,
) -> dict[str, Any]:
    manifests = _adjusted_manifests(catalog, "30m", "ADJUSTED")
    if not manifests:
        raise RuntimeError("no AVAILABLE adjusted 30m manifest exists to continue")
    latest_end = max(_timestamp(row["period_end"]) for row in manifests)
    sessions = completed_sessions_after(latest_end, now)
    if not sessions:
        return {"status": "CURRENT", "sessions": [], "latest_period_end": latest_end.isoformat()}
    instrument_map = config.state_root / "market-history" / "active-instruments.csv"
    instrument_count = _write_instrument_map(catalog, instrument_map, now)
    staging_root = config.state_root / "market-history" / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    engine = MarketPipelineEngine(
        PipelineConfig(
            local_root=config.state_root / "market-history" / "objects",
            staging_root=staging_root,
            instrument_map_path=instrument_map,
            resume=True,
        ),
        object_store=object_store,
        catalog=catalog,
        source=AlpacaBarSource(config.api_key, config.api_secret),
        ensure_provider_metadata=False,
    )
    run_ids: list[str] = []
    for offset in range(0, len(sessions), PUBLICATION_SESSION_BATCH):
        batch = sessions[offset : offset + PUBLICATION_SESSION_BATCH]
        result = engine.incremental(
            sessions=batch,
            price_types=("adjusted",),
            resolutions=tuple(TIMEFRAMES),
        )
        if result.get("status") != "SUCCEEDED":
            raise RuntimeError(
                f"adjusted incremental publication failed: {result.get('status')}"
            )
        if result.get("pipeline_run_id"):
            run_ids.append(str(result["pipeline_run_id"]))
    return {
        "status": "PUBLISHED",
        "instrument_count": instrument_count,
        "sessions": [session.isoformat() for session in sessions],
        "pipeline_run_ids": run_ids,
    }


def _verified_parquet(
    object_store: S3ObjectStore, relation: Mapping[str, Any], temporary_root: Path
) -> Path:
    storage = relation["storage"]
    verification = object_store.verify_version(
        str(storage["object_key"]),
        str(storage["provider_version_id"]),
        str(storage["content_hash"]),
        int(storage["byte_size"]),
    )
    if not verification.ok:
        raise RuntimeError(
            f"canonical S3 object failed verification: {storage['object_key']} ({verification.message})"
        )
    path = temporary_root / f"{storage['id']}.parquet"
    digest = hashlib.sha256()
    size = 0
    with object_store.open_version(
        str(storage["object_key"]), str(storage["provider_version_id"])
    ) as source, path.open("wb") as target:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            target.write(chunk)
    if digest.hexdigest() != storage["content_hash"] or size != int(storage["byte_size"]):
        raise RuntimeError(f"downloaded canonical bytes differ: {storage['object_key']}")
    return path


def compact_history_payload(
    instrument_id: str, timeframe: str, bars: list[Mapping[str, Any]]
) -> str:
    payload = {
        "schemaVersion": 1,
        "adjustment": "all",
        "timeframe": timeframe,
        "instrumentId": instrument_id,
        "bars": [
            {
                "t": bar["bar_start_at"].astimezone(UTC).isoformat().replace("+00:00", "Z"),
                "o": bar["open"],
                "h": bar["high"],
                "l": bar["low"],
                "c": bar["close"],
                "v": bar["volume"],
            }
            for bar in bars
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _project_history(
    catalog: Any,
    object_store: S3ObjectStore,
    redis_client: Any,
    config: HistorySyncConfig,
    now: datetime,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    manifest_ids: list[str] = []
    projected_keys = 0
    hash_tag = "{" + config.redis_key_prefix + ":market}"
    with tempfile.TemporaryDirectory(prefix="i2s-history-") as temporary:
        temporary_root = Path(temporary)
        for timeframe, (layer, lookback_days) in TIMEFRAMES.items():
            manifests = _adjusted_manifests(catalog, timeframe, layer)
            if not manifests:
                raise RuntimeError(f"no AVAILABLE adjusted history for {timeframe}")
            historical_through = max(_timestamp(row["period_end"]) for row in manifests)
            cutoff = historical_through - timedelta(days=lookback_days)
            selected = [
                manifest
                for manifest in manifests
                if _timestamp(manifest["period_end"]) > cutoff
            ]
            manifest_ids.extend(str(row["id"]) for row in selected)
            bars_by_instrument: dict[str, deque[dict[str, Any]]] = defaultdict(
                lambda: deque(maxlen=config.limit)
            )
            relations = [
                relation
                for manifest in selected
                for relation in catalog.objects_for_manifest(str(manifest["id"]))
                if _timestamp(relation["period_end"]) > cutoff
            ]
            relations.sort(
                key=lambda row: (
                    _timestamp(row["period_start"]),
                    str(row["shard_key"]),
                    int(row["part_number"]),
                )
            )
            for relation in relations:
                path = _verified_parquet(object_store, relation, temporary_root)
                table = pq.read_table(
                    path,
                    columns=[
                        "instrument_id", "bar_start_at", "open", "high",
                        "low", "close", "volume",
                    ],
                    filters=[("bar_start_at", ">=", cutoff)],
                ).sort_by([("instrument_id", "ascending"), ("bar_start_at", "ascending")])
                for bar in table.to_pylist():
                    bars_by_instrument[str(bar["instrument_id"])].append(bar)
                path.unlink(missing_ok=True)
            counts[timeframe] = sum(len(bars) for bars in bars_by_instrument.values())
            for instrument_id, bars in bars_by_instrument.items():
                key = f"{hash_tag}:history:bars:{instrument_id}:{timeframe}"
                redis_client.set(
                    key,
                    compact_history_payload(instrument_id, timeframe, list(bars)),
                )
                projected_keys += 1
    metadata = {
        "schemaVersion": 1,
        "adjustment": "all",
        "generatedAt": now.isoformat().replace("+00:00", "Z"),
        "limit": config.limit,
        "manifestIds": sorted(set(manifest_ids)),
        "barCounts": counts,
    }
    redis_client.set(
        f"{hash_tag}:history:metadata",
        json.dumps(metadata, sort_keys=True, separators=(",", ":")),
    )
    return {"projected_key_count": projected_keys, "bar_counts": counts}


def execute(
    environment: Mapping[str, str] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = HistorySyncConfig.from_environment(environment)
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    import boto3
    import redis

    catalog = PostgresCatalog.connect(
        config.database_url,
        artifact_root=config.state_root / "market-history" / "catalog-artifacts",
        storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
    )
    object_store = S3ObjectStore(config.bucket, client=boto3.client("s3"))
    redis_client = redis.Redis.from_url(config.redis_uri, decode_responses=True)
    try:
        catalog.verify_schema()
        publication = _publish_missing_history(catalog, object_store, config, observed_at)
        projection = _project_history(
            catalog, object_store, redis_client, config, observed_at
        )
    finally:
        redis_client.close()
        catalog.close()

    from apps.pipeline_worker.publish_manifest_watermarks import execute as publish_watermarks

    watermarks = publish_watermarks([], environment)
    return {
        "status": "SUCCEEDED",
        "publication": publication,
        "projection": projection,
        "watermarks": watermarks,
    }
