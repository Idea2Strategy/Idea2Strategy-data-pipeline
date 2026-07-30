from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq

from market_loader.model.bar import Bar
from market_loader.pipeline.parquet_writer import bar_schema, validate_parquet, write_parquet


def _bar(minute: int) -> Bar:
    return Bar(
        instrument_id="11111111-1111-1111-1111-111111111111",
        provider_symbol="AAPL",
        bar_start_at=datetime(2024, 1, 2, 14, minute, tzinfo=UTC),
        session_date_et=date(2024, 1, 2),
        open=10.0,
        high=12.0,
        low=9.0,
        close=11.0,
        volume=100,
        trade_count=4,
        vwap=10.5,
    )


def _write(path: Path, bars: list[Bar]) -> object:
    return write_parquet(
        bars=bars,
        output_path=path,
        derived=False,
        schema_version="market-bars/1",
        processing_version="market-loader/1.0.0",
        adjustment="raw",
        resolution="30m",
        period_start=date(2024, 1, 1),
        period_end=date(2025, 1, 1),
        revision=1,
        manifest_id=UUID("11111111-1111-1111-1111-111111111111"),
    )


def test_parquet_schema_metadata_and_sorted_logical_rows(tmp_path: Path) -> None:
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    written = _write(first_path, [_bar(30), _bar(0)])
    _write(second_path, [_bar(0), _bar(30)])
    assert written.row_count == 2
    assert validate_parquet(first_path, derived=False, expected_sha256=written.content_sha256) == 2
    first = pq.read_table(first_path).replace_schema_metadata()
    second = pq.read_table(second_path).replace_schema_metadata()
    assert first.equals(second)
    assert first.schema.equals(bar_schema(derived=False))
    metadata = pq.ParquetFile(first_path).schema_arrow.metadata
    assert metadata[b"schema_version"] == b"market-bars/1"
    assert b"ALPACA_API_SECRET" not in b"".join(metadata.values())
