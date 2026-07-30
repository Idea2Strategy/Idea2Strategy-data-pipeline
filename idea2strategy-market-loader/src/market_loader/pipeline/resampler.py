from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date, datetime, timedelta

from market_loader.calendar.xnys import SessionWindow
from market_loader.errors import InputError
from market_loader.model.bar import Bar


def _bucket_start(bar: Bar, window: SessionWindow, resolution: str) -> datetime:
    if resolution == "1d":
        return window.opens_at
    minutes = {"1h": 60, "4h": 240}.get(resolution)
    if minutes is None:
        raise InputError(f"unsupported derived resolution: {resolution}")
    elapsed = int((bar.bar_start_at - window.opens_at).total_seconds() // 60)
    return window.opens_at + timedelta(minutes=(elapsed // minutes) * minutes)


def _aggregate(group: list[Bar], bucket: datetime) -> Bar:
    ordered = sorted(group, key=lambda item: item.bar_start_at)
    weighted = [
        (bar.vwap, bar.volume) for bar in ordered if bar.vwap is not None and bar.volume > 0
    ]
    total_weight = sum(volume for _, volume in weighted)
    trade_values = [bar.trade_count for bar in ordered if bar.trade_count is not None]
    return Bar(
        instrument_id=ordered[0].instrument_id,
        provider_symbol=ordered[0].provider_symbol,
        bar_start_at=bucket,
        session_date_et=ordered[0].session_date_et,
        open=ordered[0].open,
        high=max(bar.high for bar in ordered),
        low=min(bar.low for bar in ordered),
        close=ordered[-1].close,
        volume=sum(bar.volume for bar in ordered),
        trade_count=sum(trade_values) if trade_values else None,
        vwap=(
            sum(float(vwap) * volume for vwap, volume in weighted) / total_weight
            if total_weight
            else None
        ),
        source_bar_count=len(ordered),
        source_minutes=len(ordered) * 30,
    )


def resample_bars(
    bars: list[Bar],
    windows: Mapping[date, SessionWindow],
    resolution: str,
) -> list[Bar]:
    groups: dict[tuple[str, object, datetime], list[Bar]] = defaultdict(list)
    for bar in bars:
        window = windows.get(bar.session_date_et)
        if window is None or not window.contains(bar.bar_start_at):
            continue
        bucket = _bucket_start(bar, window, resolution)
        groups[(bar.instrument_id, bar.session_date_et, bucket)].append(bar)
    result = [_aggregate(group, key[2]) for key, group in groups.items()]
    return sorted(result, key=lambda item: (item.instrument_id, item.bar_start_at))
