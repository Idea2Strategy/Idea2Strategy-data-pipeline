from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import market_pipeline_lib.equity_history as equity_history
from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.contracts import InstrumentMapping, deterministic_uuid
from market_pipeline_lib.equity_history import derive_required_resolutions, publish_instrument_year
from market_pipeline_lib.processing import normalize_provider_frame
from market_pipeline_lib.storage import LocalObjectStore


class CountingObjectStore(LocalObjectStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.put_count = 0

    def put(self, source: Path, object_key: str):
        self.put_count += 1
        return super().put(source, object_key)


class RejectingObjectCatalog(LocalCatalog):
    def stage_object(self, storage_object_record, dataset_object_record) -> None:
        raise RuntimeError("catalog transaction rejected the object")


def _adjusted_bars() -> object:
    timestamps = pd.date_range("2025-01-02 14:30:00Z", periods=14, freq="30min")
    frame = pd.DataFrame({
        "timestamp": timestamps,
        "open": [100 + index for index in range(14)],
        "high": [101 + index for index in range(14)],
        "low": [99 + index for index in range(14)],
        "close": [100.5 + index for index in range(14)],
        "volume": [1_000 + index for index in range(14)],
    })
    return normalize_provider_frame(frame, InstrumentMapping("AAPL", deterministic_uuid("instrument", "AAPL")))


def test_target_equity_resolutions_are_derived_from_the_same_adjusted_30m_source() -> None:
    tables = derive_required_resolutions(_adjusted_bars())

    assert set(tables) == {"30m", "1h", "4h", "1d"}
    assert tables["30m"].num_rows == 13
    assert tables["30m"].column("bar_start_at")[-1].as_py() == datetime(
        2025, 1, 2, 20, 30, tzinfo=UTC
    )
    assert tables["1h"].num_rows > 0
    assert tables["4h"].num_rows > 0
    assert tables["1d"].num_rows == 1
    assert tables["1d"].column("open")[0].as_py() == 100
    # The 16:00 ET row is outside regular trading hours and is deliberately excluded.
    assert tables["1d"].column("close")[0].as_py() == 112.5
    assert tables["1d"].column("volume")[0].as_py() == sum(range(1_000, 1_013))


def test_latest_sip_query_end_is_the_newest_completed_bar_outside_the_delay() -> None:
    observed_at = datetime(2026, 8, 29, 16, 41, 27, tzinfo=UTC)

    assert equity_history.latest_permitted_sip_30m_end(observed_at) == datetime(
        2026, 8, 29, 16, 0, tzinfo=UTC
    )


def test_instrument_year_publication_records_measured_range_and_zstd_bytes(tmp_path: Path) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    store = LocalObjectStore(tmp_path / "objects")
    feed_id = deterministic_uuid("feed", "ALPACA_SIP_ALL_1D")
    instrument_id = deterministic_uuid("instrument", "AAPL")

    result = publish_instrument_year(
        catalog,
        store,
        provider_code="ALPACA",
        feed_id=feed_id,
        feed_code="ALPACA_SIP_ALL_1D",
        symbol="AAPL",
        instrument_id=instrument_id,
        resolution="1d",
        data_layer="ADJUSTED",
        manifest_schema_version="market-bars/1",
        year=2025,
        table=derive_required_resolutions(_adjusted_bars())["1d"],
        observed_at=datetime(2026, 8, 29, tzinfo=UTC),
    )

    assert result["status"] == "PUBLISHED"
    manifest = catalog.records("market_data.dataset_manifests")[0]
    relation = catalog.records("market_data.dataset_objects")[0]
    storage = catalog.records("storage.objects")[0]
    assert manifest["instrument_id"] == instrument_id
    assert manifest["actual_start_at"] == "2025-01-02T14:30:00Z"
    assert manifest["actual_end_at"] == "2025-01-02T14:30:00Z"
    assert relation["actual_start_at"] == manifest["actual_start_at"]
    assert relation["actual_end_at"] == manifest["actual_end_at"]
    assert storage["compression_codec"] == "ZSTD"
    assert storage["schema_version"] == manifest["schema_version"] == "market-bars/1"
    assert storage["object_key"] == (
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        "resolution=1d/revision=00000001/year=2025/shard=00-of-01/"
        f"manifest_id={manifest['id']}/part-00001.parquet"
    )
    legacy_payload = {
        "provider": "ALPACA",
        "feed": "ALPACA_SIP_ALL_1D",
        "adjustment": "all",
        "session": "XNYS_REGULAR",
        "resolution": "1d",
        "period_start": "2025-01-01",
        "period_end": "2026-01-01",
        "revision": 1,
        "schema_version": "market-bars/1",
        "processing_version": "market-loader/1.0.0",
        "objects": [{
            "content_sha256": storage["content_hash"],
            "row_count": 1,
            "period_start": "2025-01-01",
            "period_end": "2026-01-01",
            "shard": 0,
            "part": 1,
        }],
    }
    encoded = json.dumps(legacy_payload, sort_keys=True, separators=(",", ":"))
    assert manifest["dataset_hash"] == hashlib.sha256(encoded.encode()).hexdigest()
    with store.open(storage["object_key"]) as source:
        schema = pq.read_schema(source)
        metadata = schema.metadata
    assert schema.names[-2:] == ["source_bar_count", "source_minutes"]
    assert not schema.field("source_bar_count").nullable
    assert metadata == {
        b"schema_version": b"market-bars/1",
        b"provider": b"alpaca",
        b"feed": b"sip",
        b"adjustment": b"all",
        b"session_scope": b"regular",
        b"resolution": b"1d",
        b"manifest_id": str(manifest["id"]).encode("ascii"),
    }
    assert store.verify(storage["object_key"], storage["content_hash"]).ok


def test_identical_instrument_year_is_unchanged_without_uploading_an_orphan_version(
    tmp_path: Path,
) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    store = CountingObjectStore(tmp_path / "objects")
    kwargs = {
        "provider_code": "ALPACA",
        "feed_id": deterministic_uuid("feed", "ALPACA_SIP_ALL_1D"),
        "feed_code": "ALPACA_SIP_ALL_1D",
        "symbol": "AAPL",
        "instrument_id": deterministic_uuid("instrument", "AAPL"),
        "resolution": "1d",
        "data_layer": "ADJUSTED",
        "manifest_schema_version": "market-bars/1",
        "year": 2025,
        "table": derive_required_resolutions(_adjusted_bars())["1d"],
        "observed_at": datetime(2026, 8, 29, tzinfo=UTC),
    }

    first = publish_instrument_year(catalog, store, **kwargs)
    second = publish_instrument_year(catalog, store, **kwargs)

    assert first["status"] == "PUBLISHED"
    assert second["status"] == "UNCHANGED"
    assert store.put_count == 1


def test_failed_catalog_publication_removes_only_the_new_object_version(tmp_path: Path) -> None:
    catalog = RejectingObjectCatalog(tmp_path / "catalog")
    object_root = tmp_path / "objects"
    store = LocalObjectStore(object_root)

    with pytest.raises(RuntimeError, match="catalog transaction rejected"):
        publish_instrument_year(
            catalog,
            store,
            provider_code="ALPACA",
            feed_id=deterministic_uuid("feed", "ALPACA_SIP_ALL_1D"),
            feed_code="ALPACA_SIP_ALL_1D",
            symbol="AAPL",
            instrument_id=deterministic_uuid("instrument", "AAPL"),
            resolution="1d",
            data_layer="ADJUSTED",
            manifest_schema_version="market-bars/1",
            year=2025,
            table=derive_required_resolutions(_adjusted_bars())["1d"],
            observed_at=datetime(2026, 8, 29, tzinfo=UTC),
        )

    assert not list(object_root.rglob("*.parquet"))
