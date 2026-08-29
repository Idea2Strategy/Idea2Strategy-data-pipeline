from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.index_history import (
    BENCHMARKS,
    ensure_benchmark_metadata,
    parse_yahoo_chart,
    publish_benchmark_year,
)
from market_pipeline_lib.storage import LocalObjectStore


class CountingObjectStore(LocalObjectStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.put_count = 0

    def put(self, source: Path, object_key: str):
        self.put_count += 1
        return super().put(source, object_key)


def _payload() -> dict[str, object]:
    return {
        "chart": {
            "error": None,
            "result": [{
                "meta": {"symbol": "^NDX"},
                "timestamp": [1735828200, 1735914600],
                "indicators": {"quote": [{
                    "open": [21000.0, 21100.0],
                    "high": [21150.0, 21200.0],
                    "low": [20900.0, 21050.0],
                    "close": [21100.0, 21180.0],
                    "volume": [8737550000, 8214050000],
                }]},
            }],
        },
    }


def test_yahoo_index_parser_preserves_provider_timestamps_and_ohlcv() -> None:
    bars = parse_yahoo_chart(_payload(), expected_symbol="^NDX")

    assert bars == [
        {
            "bar_start_at": datetime.fromtimestamp(1735828200, UTC),
            "open": 21000.0,
            "high": 21150.0,
            "low": 20900.0,
            "close": 21100.0,
            "volume": 8737550000,
        },
        {
            "bar_start_at": datetime.fromtimestamp(1735914600, UTC),
            "open": 21100.0,
            "high": 21200.0,
            "low": 21050.0,
            "close": 21180.0,
            "volume": 8214050000,
        },
    ]


def test_yahoo_index_parser_rejects_missing_or_impossible_ohlcv() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["high"][0] = 20000.0

    with pytest.raises(ValueError, match="invalid OHLC"):
        parse_yahoo_chart(payload, expected_symbol="^NDX")


def test_yahoo_index_parser_omits_incomplete_provider_rows_without_inventing_values() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["close"][0] = None

    bars = parse_yahoo_chart(payload, expected_symbol="^NDX")

    assert bars == [{
        "bar_start_at": datetime.fromtimestamp(1735914600, UTC),
        "open": 21100.0,
        "high": 21200.0,
        "low": 21050.0,
        "close": 21180.0,
        "volume": 8214050000,
    }]


def test_yahoo_index_parser_rejects_a_response_with_no_complete_provider_rows() -> None:
    payload = _payload()
    quotes = payload["chart"]["result"][0]["indicators"]["quote"][0]
    quotes["close"] = [None, None]

    with pytest.raises(ValueError, match="no complete bars"):
        parse_yahoo_chart(payload, expected_symbol="^NDX")


def test_yahoo_index_parser_rejects_symbol_or_column_length_mismatch() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["volume"].pop()

    with pytest.raises(ValueError, match="column lengths"):
        parse_yahoo_chart(payload, expected_symbol="^NDX")

    with pytest.raises(ValueError, match="unexpected Yahoo symbol"):
        parse_yahoo_chart(_payload(), expected_symbol="^GSPC")


def test_index_year_is_published_with_measured_physical_range_and_provenance(tmp_path: Path) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    store = LocalObjectStore(tmp_path / "objects")
    observed_at = datetime(2026, 8, 29, tzinfo=UTC)
    ensure_benchmark_metadata(catalog, observed_at)

    result = publish_benchmark_year(
        catalog,
        store,
        "NDX",
        2025,
        parse_yahoo_chart(_payload(), expected_symbol="^NDX"),
        source_response_hash="c" * 64,
        observed_at=observed_at,
    )

    assert result["status"] == "PUBLISHED"
    manifest = catalog.records("market_data.dataset_manifests")[0]
    relation = catalog.records("market_data.dataset_objects")[0]
    storage = catalog.records("storage.objects")[0]
    assert manifest["instrument_id"] == BENCHMARKS["NDX"]["instrument_id"]
    assert manifest["actual_start_at"] == "2025-01-02T14:30:00Z"
    assert manifest["actual_end_at"] == "2025-01-03T14:30:00Z"
    assert manifest["object_count"] == 1
    assert relation["actual_start_at"] == manifest["actual_start_at"]
    assert relation["actual_end_at"] == manifest["actual_end_at"]
    assert relation["row_count"] == 2
    assert storage["content_hash"] and store.verify(
        storage["object_key"], storage["content_hash"]
    ).ok


def test_identical_index_year_is_unchanged_without_uploading_an_orphan_version(
    tmp_path: Path,
) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    store = CountingObjectStore(tmp_path / "objects")
    observed_at = datetime(2026, 8, 29, tzinfo=UTC)
    ensure_benchmark_metadata(catalog, observed_at)
    args = (
        catalog,
        store,
        "NDX",
        2025,
        parse_yahoo_chart(_payload(), expected_symbol="^NDX"),
    )
    kwargs = {"source_response_hash": "c" * 64, "observed_at": observed_at}

    first = publish_benchmark_year(*args, **kwargs)
    second = publish_benchmark_year(*args, **kwargs)

    assert first["status"] == "PUBLISHED"
    assert second["status"] == "UNCHANGED"
    assert store.put_count == 1
