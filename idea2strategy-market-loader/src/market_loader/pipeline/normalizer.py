from __future__ import annotations

from datetime import datetime

from market_loader.model.bar import Bar


def normalize_bars(bars: list[Bar], start: datetime, end: datetime) -> list[Bar]:
    deduplicated: dict[tuple[str, datetime], Bar] = {}
    for bar in bars:
        if start <= bar.bar_start_at < end:
            deduplicated[(bar.instrument_id, bar.bar_start_at)] = bar
    return sorted(deduplicated.values(), key=lambda item: (item.instrument_id, item.bar_start_at))
