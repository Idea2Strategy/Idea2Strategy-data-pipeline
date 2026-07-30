from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_loader.calendar.xnys import XnysCalendar
from market_loader.model.bar import Bar
from market_loader.pipeline.resampler import resample_bars

INSTRUMENT_ID = "11111111-1111-1111-1111-111111111111"


def _bars_for_session(calendar: XnysCalendar, session_date: object) -> tuple[list[Bar], dict]:
    windows = calendar.sessions(session_date, session_date + timedelta(days=1))
    window = windows[session_date]
    bars = []
    timestamp = window.opens_at
    index = 1
    while timestamp < window.closes_at:
        bars.append(
            Bar(
                instrument_id=INSTRUMENT_ID,
                provider_symbol="AAPL",
                bar_start_at=timestamp,
                session_date_et=session_date,
                open=float(index),
                high=float(index + 1),
                low=float(index),
                close=float(index + 0.5),
                volume=index * 10,
                trade_count=index,
                vwap=float(index + 0.25),
            )
        )
        timestamp += timedelta(minutes=30)
        index += 1
    return bars, windows


def test_dst_and_holiday_and_early_close() -> None:
    calendar = XnysCalendar()
    before = calendar.sessions(
        datetime(2024, 3, 8, tzinfo=UTC).date(),
        datetime(2024, 3, 9, tzinfo=UTC).date(),
    )
    after = calendar.sessions(
        datetime(2024, 3, 11, tzinfo=UTC).date(),
        datetime(2024, 3, 12, tzinfo=UTC).date(),
    )
    assert next(iter(before.values())).opens_at.hour == 14
    assert next(iter(after.values())).opens_at.hour == 13
    july = calendar.sessions(
        datetime(2024, 7, 3, tzinfo=UTC).date(),
        datetime(2024, 7, 5, tzinfo=UTC).date(),
    )
    assert datetime(2024, 7, 4, tzinfo=UTC).date() not in july
    assert july[datetime(2024, 7, 3, tzinfo=UTC).date()].is_early_close


def test_regular_day_partial_buckets_and_daily_grouping() -> None:
    calendar = XnysCalendar()
    session_date = datetime(2024, 7, 1, tzinfo=UTC).date()
    bars, windows = _bars_for_session(calendar, session_date)
    hourly = resample_bars(bars, windows, "1h")
    four_hour = resample_bars(bars, windows, "4h")
    daily = resample_bars(bars, windows, "1d")
    assert len(bars) == 13
    assert [bar.source_minutes for bar in hourly] == [60, 60, 60, 60, 60, 60, 30]
    assert [bar.source_minutes for bar in four_hour] == [240, 150]
    assert len(daily) == 1
    assert daily[0].source_minutes == 390
    assert daily[0].open == bars[0].open
    assert daily[0].close == bars[-1].close


def test_vwap_is_positive_volume_weighted() -> None:
    calendar = XnysCalendar()
    session_date = datetime(2024, 7, 1, tzinfo=UTC).date()
    bars, windows = _bars_for_session(calendar, session_date)
    first = bars[0]
    second = bars[1]
    hourly = resample_bars([first, second], windows, "1h")
    expected = (first.vwap * first.volume + second.vwap * second.volume) / (
        first.volume + second.volume
    )
    assert hourly[0].vwap == expected
