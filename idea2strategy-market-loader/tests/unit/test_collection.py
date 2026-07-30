from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from market_loader.alpaca.mapper import map_bar, parse_utc_timestamp
from market_loader.calendar.xnys import XnysCalendar
from market_loader.errors import PermanentAlpacaError
from market_loader.model.bar import Bar
from market_loader.model.catalog import UniverseInstrument
from market_loader.pipeline.collector import collect_chunk
from market_loader.pipeline.normalizer import normalize_bars

INSTRUMENT_ID = "11111111-1111-1111-1111-111111111111"


class FakeClient:
    def iter_bar_pages(self, *_: object) -> object:
        yield {
            "bars": {
                "AAPL": [
                    {
                        "t": "2024-01-02T14:30:00Z",
                        "o": 10,
                        "h": 12,
                        "l": 9,
                        "c": 11,
                        "v": 100,
                        "n": 4,
                        "vw": 10.5,
                    },
                    {
                        "t": "2024-01-02T09:00:00Z",
                        "o": 10,
                        "h": 12,
                        "l": 9,
                        "c": 11,
                        "v": 100,
                    },
                ]
            },
            "next_page_token": None,
        }


def _instrument(identifier: str | None = INSTRUMENT_ID) -> UniverseInstrument:
    return UniverseInstrument(
        provider_symbol="AAPL",
        asset_type="STOCK",
        primary_exchange_mic="XNAS",
        effective_from=date(2020, 1, 1),
        effective_to=None,
        support_status="ACTIVE",
        instrument_id=identifier,
    )


def test_mapper_and_collection_filter_to_regular_session() -> None:
    calendar = XnysCalendar()
    result = collect_chunk(
        FakeClient(), calendar, [_instrument()], date(2024, 1, 2), date(2024, 1, 3), "raw"
    )
    assert len(result) == 1
    assert result[0].bar_start_at == datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
    assert result[0].session_date_et == date(2024, 1, 2)


def test_mapper_rejects_bad_contract_and_missing_persistent_id() -> None:
    assert parse_utc_timestamp("2024-01-02T14:30:00Z").tzinfo is UTC
    with pytest.raises(PermanentAlpacaError):
        parse_utc_timestamp("not-a-timestamp")
    with pytest.raises(PermanentAlpacaError, match="missing"):
        map_bar(
            symbol="AAPL",
            instrument_id=INSTRUMENT_ID,
            session_date_et=date(2024, 1, 2),
            payload={"t": "2024-01-02T14:30:00Z"},
        )
    with pytest.raises(PermanentAlpacaError, match="persistent"):
        collect_chunk(
            FakeClient(),
            XnysCalendar(),
            [_instrument(None)],
            date(2024, 1, 2),
            date(2024, 1, 3),
            "raw",
        )


def test_normalizer_enforces_range_and_deduplicates_last_observation() -> None:
    first = Bar(
        instrument_id=INSTRUMENT_ID,
        provider_symbol="AAPL",
        bar_start_at=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        session_date_et=date(2024, 1, 2),
        open=10,
        high=12,
        low=9,
        close=11,
        volume=1,
        trade_count=1,
        vwap=10,
    )
    replacement = Bar(
        instrument_id=INSTRUMENT_ID,
        provider_symbol="AAPL",
        bar_start_at=first.bar_start_at,
        session_date_et=first.session_date_et,
        open=20,
        high=22,
        low=19,
        close=21,
        volume=2,
        trade_count=2,
        vwap=20,
    )
    result = normalize_bars(
        [first, replacement],
        datetime(2024, 1, 2, tzinfo=UTC),
        datetime(2024, 1, 3, tzinfo=UTC),
    )
    assert result == [replacement]
