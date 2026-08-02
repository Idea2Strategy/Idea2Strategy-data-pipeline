from __future__ import annotations

from market_loader.pipeline.parquet_writer import bar_schema


def test_parquet_schema_snapshot() -> None:
    assert str(bar_schema(derived=False)) == (
        "instrument_id: string not null\n"
        "provider_symbol: string not null\n"
        "bar_start_at: timestamp[us, tz=UTC] not null\n"
        "session_date_et: date32[day] not null\n"
        "open: double not null\n"
        "high: double not null\n"
        "low: double not null\n"
        "close: double not null\n"
        "volume: int64 not null\n"
        "trade_count: int64\n"
        "vwap: double"
    )
