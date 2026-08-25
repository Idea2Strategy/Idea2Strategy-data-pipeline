from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from apps.pipeline_worker import sync_market_history
from apps.pipeline_worker.main import EXIT_OK, main
from apps.pipeline_worker.sync_market_history import (
    HistorySyncConfig,
    _project_history,
    _publish_missing_history,
    _write_instrument_map,
    compact_history_payload,
    completed_sessions_after,
    execute,
)
from market_pipeline_lib.contracts import ADJUSTED_FEED, deterministic_uuid
from market_pipeline_lib.storage import VerificationResult


def test_history_sync_config_requires_canonical_sources_and_projection_target() -> None:
    with pytest.raises(ValueError, match="ALPACA_API_KEY"):
        HistorySyncConfig.from_environment({})


def test_history_sync_config_parses_and_bounds_the_projection() -> None:
    environment = {
        "PIPELINE_WORKER_DATABASE_URL": "postgresql://catalog",
        "MARKET_DATA_BUCKET": "bucket",
        "PIPELINE_WORKER_MARKET_HISTORY_REDIS_URI": "rediss://cache:6379",
        "PIPELINE_WORKER_MARKET_HISTORY_REDIS_KEY_PREFIX": "market",
        "PIPELINE_WORKER_OBJECT_STORE_ROOT": "state",
        "ALPACA_API_KEY": "key",
        "ALPACA_SECRET_KEY": "secret",
    }

    config = HistorySyncConfig.from_environment(environment)

    assert config.limit == 1000
    assert config.redis_key_prefix == "market"
    assert config.state_root == Path("state")
    with pytest.raises(ValueError, match="plain"):
        HistorySyncConfig.from_environment({
            **environment,
            "PIPELINE_WORKER_MARKET_HISTORY_REDIS_KEY_PREFIX": "{bad}",
        })
    with pytest.raises(ValueError, match="between"):
        HistorySyncConfig.from_environment({
            **environment,
            "PIPELINE_WORKER_MARKET_HISTORY_LIMIT": "0",
        })


def test_completed_sessions_selects_only_closes_after_manifest_and_before_now() -> None:
    sessions = completed_sessions_after(
        datetime(2026, 8, 7, 20, 0, tzinfo=UTC),
        datetime(2026, 8, 10, 21, 0, tzinfo=UTC),
    )

    assert sessions == [datetime(2026, 8, 10, tzinfo=UTC).date()]


def test_compact_projection_declares_adjusted_semantics_and_timeframe() -> None:
    encoded = compact_history_payload(
        "70000000-0000-4000-8000-000000000001",
        "4h",
        [
            {
                "bar_start_at": datetime(2026, 8, 10, 13, 30, tzinfo=UTC),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1200,
            }
        ],
    )

    payload = json.loads(encoded)
    assert payload == {
        "schemaVersion": 1,
        "adjustment": "all",
        "timeframe": "4h",
        "instrumentId": "70000000-0000-4000-8000-000000000001",
        "bars": [
            {
                "t": "2026-08-10T13:30:00Z",
                "o": 100.0,
                "h": 102.0,
                "l": 99.0,
                "c": 101.0,
                "v": 1200,
            }
        ],
    }


def test_pipeline_worker_exposes_the_scheduled_history_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[object] = []
    monkeypatch.setattr(
        sync_market_history,
        "execute",
        lambda environment: called.append(environment) or {"status": "SUCCEEDED"},
    )

    assert main(["--sync-market-history"], {"TEST": "value"}) == EXIT_OK
    assert called == [{"TEST": "value"}]


def test_projection_reads_exact_available_s3_versions_and_writes_separate_history_keys() -> None:
    instrument_id = "70000000-0000-4000-8000-000000000001"
    observed_at = datetime(2026, 8, 10, 21, 0, tzinfo=UTC)
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "instrument_id": [instrument_id],
                "bar_start_at": [datetime(2026, 8, 10, 13, 30, tzinfo=UTC)],
                "open": [100.0],
                "high": [102.0],
                "low": [99.0],
                "close": [101.0],
                "volume": [1200],
            }
        ),
        buffer,
    )
    parquet = buffer.getvalue()
    digest = hashlib.sha256(parquet).hexdigest()
    feed_id = deterministic_uuid("feed", "ALPACA", ADJUSTED_FEED)
    manifests = [
        {
            "id": timeframe,
            "feed_id": feed_id,
            "status": "AVAILABLE",
            "data_layer": "ADJUSTED" if timeframe == "30m" else "DERIVED",
            "resolution": timeframe,
            "period_start": "2026-08-10T13:30:00Z",
            "period_end": "2026-08-10T20:00:00Z",
            "revision_number": 1,
        }
        for timeframe in ("30m", "1h", "4h", "1d")
    ]

    class Catalog:
        def records(self, table: str) -> list[dict[str, object]]:
            assert table == "market_data.dataset_manifests"
            return manifests

        def objects_for_manifest(self, manifest_id: str) -> list[dict[str, object]]:
            return [{
                "period_start": "2026-08-10T13:30:00Z",
                "period_end": "2026-08-10T20:00:00Z",
                "shard_key": "s00-of-16",
                "part_number": 1,
                "storage": {
                    "id": f"object-{manifest_id}",
                    "object_key": f"history/{manifest_id}.parquet",
                    "provider_version_id": f"version-{manifest_id}",
                    "content_hash": digest,
                    "byte_size": len(parquet),
                },
            }]

    class ObjectStore:
        def __init__(self) -> None:
            self.versions: list[tuple[str, str]] = []

        def verify_version(self, key: str, version: str, sha: str, size: int) -> VerificationResult:
            self.versions.append((key, version))
            assert sha == digest
            assert size == len(parquet)
            return VerificationResult(True, sha, size)

        def open_version(self, key: str, version: str) -> BytesIO:
            assert (key, version) in self.versions
            return BytesIO(parquet)

    class Redis:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set(self, key: str, value: str) -> None:
            self.values[key] = value

    store = ObjectStore()
    redis = Redis()
    config = HistorySyncConfig(
        database_url="postgresql://example",
        bucket="bucket",
        redis_uri="rediss://cache:6379",
        redis_key_prefix="i2s",
        limit=400,
        api_key="key",
        api_secret="secret",
        state_root=Path("."),
    )

    report = _project_history(Catalog(), store, redis, config, observed_at)  # type: ignore[arg-type]

    assert report["projected_key_count"] == 4
    assert len(store.versions) == 4
    history_keys = {key for key in redis.values if ":history:bars:" in key}
    assert history_keys == {
        f"{{i2s:market}}:history:bars:{instrument_id}:{timeframe}"
        for timeframe in ("30m", "1h", "4h", "1d")
    }


def test_instrument_map_uses_only_the_active_primary_symbol(tmp_path: Path) -> None:
    class Catalog:
        def records(self, table: str) -> list[dict[str, object]]:
            if table == "market_data.instruments":
                return [{
                    "id": "70000000-0000-4000-8000-000000000001",
                    "primary_exchange_mic": "XNAS",
                    "listed_at": "2020-01-01",
                    "delisted_at": None,
                }]
            return [
                {
                    "instrument_id": "70000000-0000-4000-8000-000000000001",
                    "exchange_mic": "XNAS",
                    "symbol": "AAPL",
                    "effective_from": "2020-01-01T00:00:00Z",
                    "effective_to": None,
                },
                {
                    "instrument_id": "70000000-0000-4000-8000-000000000001",
                    "exchange_mic": "XNYS",
                    "symbol": "WRONG",
                    "effective_from": "2020-01-01T00:00:00Z",
                    "effective_to": None,
                },
            ]

    output = tmp_path / "active.csv"

    assert _write_instrument_map(Catalog(), output, datetime(2026, 8, 10, tzinfo=UTC)) == 1
    assert output.read_text(encoding="utf-8").splitlines() == [
        "provider_symbol,instrument_id",
        "AAPL,70000000-0000-4000-8000-000000000001",
    ]


def test_publication_is_a_no_op_when_the_available_manifest_is_current(tmp_path: Path) -> None:
    feed_id = deterministic_uuid("feed", "ALPACA", ADJUSTED_FEED)

    class Catalog:
        def records(self, table: str) -> list[dict[str, object]]:
            assert table == "market_data.dataset_manifests"
            return [{
                "id": "manifest",
                "feed_id": feed_id,
                "status": "AVAILABLE",
                "data_layer": "ADJUSTED",
                "resolution": "30m",
                "period_start": "2026-08-10T13:30:00Z",
                "period_end": "2026-08-10T20:00:00Z",
                "revision_number": 1,
            }]

    report = _publish_missing_history(
        Catalog(), object(), sync_config(tmp_path), datetime(2026, 8, 10, 21, 0, tzinfo=UTC)  # type: ignore[arg-type]
    )

    assert report["status"] == "CURRENT"


def test_publication_commits_recoverable_session_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    feed_id = deterministic_uuid("feed", "ALPACA", ADJUSTED_FEED)
    sessions = [datetime(2026, 7, day, tzinfo=UTC).date() for day in range(1, 22)]

    class Catalog:
        def records(self, table: str) -> list[dict[str, object]]:
            assert table == "market_data.dataset_manifests"
            return [{
                "id": "manifest",
                "feed_id": feed_id,
                "status": "AVAILABLE",
                "data_layer": "ADJUSTED",
                "resolution": "30m",
                "period_start": "2026-06-01T13:30:00Z",
                "period_end": "2026-06-30T20:00:00Z",
                "revision_number": 1,
            }]

    batches: list[list[object]] = []

    class Engine:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def incremental(self, **kwargs: object) -> dict[str, object]:
            batch = list(kwargs["sessions"])  # type: ignore[arg-type]
            batches.append(batch)
            return {"status": "SUCCEEDED", "pipeline_run_id": f"run-{len(batches)}"}

    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history.completed_sessions_after",
        lambda *_args: sessions,
    )
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history._write_instrument_map",
        lambda *_args: 725,
    )
    monkeypatch.setattr("apps.pipeline_worker.sync_market_history.MarketPipelineEngine", Engine)
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history.AlpacaBarSource", lambda *_args: object()
    )

    report = _publish_missing_history(
        Catalog(), object(), sync_config(tmp_path), datetime(2026, 8, 10, 21, 0, tzinfo=UTC)  # type: ignore[arg-type]
    )

    assert [len(batch) for batch in batches] == [20, 1]
    assert report["pipeline_run_ids"] == ["run-1", "run-2"]


def test_execute_closes_adapters_and_advances_watermarks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = sync_config(tmp_path)

    class Catalog:
        engine = object()

        def verify_schema(self) -> None:
            calls.append("verify")

        def close(self) -> None:
            calls.append("catalog-close")

    class Redis:
        def close(self) -> None:
            calls.append("redis-close")

    calls: list[str] = []
    monkeypatch.setattr(
        HistorySyncConfig, "from_environment", classmethod(lambda cls, env: config)
    )
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history.PostgresCatalog.connect",
        lambda *_args, **_kwargs: Catalog(),
    )
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history.S3ObjectStore",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("redis.Redis.from_url", lambda *_args, **_kwargs: Redis())
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history._publish_missing_history",
        lambda *_args: {"status": "CURRENT"},
    )
    monkeypatch.setattr(
        "apps.pipeline_worker.sync_market_history._project_history",
        lambda *_args: {"projected_key_count": 4},
    )
    monkeypatch.setattr(
        "apps.pipeline_worker.publish_manifest_watermarks.execute",
        lambda *_args: {"advanced": 4},
    )

    report = execute({}, now=datetime(2026, 8, 10, 21, 0, tzinfo=UTC))

    assert report["status"] == "SUCCEEDED"
    assert calls == ["verify", "redis-close", "catalog-close"]
    assert report["watermarks"] == {"advanced": 4}


def sync_config(root: Path) -> HistorySyncConfig:
    return HistorySyncConfig(
        database_url="postgresql://catalog",
        bucket="bucket",
        redis_uri="rediss://cache:6379",
        redis_key_prefix="i2s",
        limit=400,
        api_key="key",
        api_secret="secret",
        state_root=root,
    )
