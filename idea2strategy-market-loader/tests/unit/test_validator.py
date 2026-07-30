from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from market_loader.calendar.xnys import XnysCalendar
from market_loader.errors import QualityError
from market_loader.model.bar import Bar
from market_loader.pipeline.validator import validate_bars


def _bar(timestamp: datetime, *, low: float = 9.0, high: float = 12.0) -> Bar:
    return Bar(
        instrument_id="11111111-1111-1111-1111-111111111111",
        provider_symbol="AAPL",
        bar_start_at=timestamp,
        session_date_et=date(2024, 1, 2),
        open=10,
        high=high,
        low=low,
        close=11,
        volume=1,
        trade_count=None,
        vwap=None,
    )


def test_out_of_session_is_fatal_and_null_activity_is_warning() -> None:
    calendar = XnysCalendar()
    windows = calendar.sessions(date(2024, 1, 2), date(2024, 1, 3))
    result = validate_bars([_bar(datetime(2024, 1, 2, 14, 30, tzinfo=UTC))], windows, derived=False)
    assert set(result.warnings) == {"NULL_TRADE_COUNT", "NULL_VWAP"}
    with pytest.raises(QualityError, match="outside"):
        validate_bars([_bar(datetime(2024, 1, 2, 2, 0, tzinfo=UTC))], windows, derived=False)


def test_invalid_ohlc_is_fatal() -> None:
    with pytest.raises(QualityError):
        _bar(datetime(2024, 1, 2, 14, 30, tzinfo=UTC), low=13, high=12).validate_values()
