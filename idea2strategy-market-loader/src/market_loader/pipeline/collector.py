from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time

from market_loader.alpaca.client import AlpacaBarsClient
from market_loader.alpaca.mapper import map_bar, parse_utc_timestamp
from market_loader.calendar.xnys import XnysCalendar
from market_loader.errors import PermanentAlpacaError
from market_loader.model.bar import Bar
from market_loader.model.catalog import UniverseInstrument
from market_loader.pipeline.normalizer import normalize_bars


def collect_chunk(
    client: AlpacaBarsClient,
    calendar: XnysCalendar,
    instruments: Sequence[UniverseInstrument],
    start: date,
    end: date,
    adjustment: str,
) -> list[Bar]:
    symbol_map = {
        item.provider_symbol: item.instrument_id
        for item in instruments
        if item.instrument_id is not None
    }
    if len(symbol_map) != len(instruments):
        raise PermanentAlpacaError("all instruments must have persistent IDs before collection")
    range_start = datetime.combine(start, time.min, tzinfo=UTC)
    range_end = datetime.combine(end, time.min, tzinfo=UTC)
    windows = calendar.sessions(start, end)
    bars: list[Bar] = []
    for page in client.iter_bar_pages(list(symbol_map), range_start, range_end, adjustment):
        for symbol, symbol_bars in page["bars"].items():
            if symbol not in symbol_map or not isinstance(symbol_bars, list):
                raise PermanentAlpacaError("response contains an unexpected symbol or bars shape")
            for payload in symbol_bars:
                if not isinstance(payload, dict):
                    raise PermanentAlpacaError("bar payload must be an object")
                timestamp = parse_utc_timestamp(str(payload.get("t", "")))
                session_date = calendar.session_date(timestamp)
                bars.append(
                    map_bar(
                        symbol=symbol,
                        instrument_id=str(symbol_map[symbol]),
                        session_date_et=session_date,
                        payload=payload,
                    )
                )
    return calendar.filter_regular(normalize_bars(bars, range_start, range_end), windows)
