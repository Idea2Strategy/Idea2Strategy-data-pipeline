"""Map raw Alpaca bar payloads onto the pipeline's provider-frame row shape.

The column names and order here are exactly what
`market_pipeline_lib.processing.normalize_provider_frame` consumes, so a page
can go straight from the wire into the canonical `bar_schema` table without an
intermediate model.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from .errors import AlpacaResponseError, PermanentAlpacaError

__all__ = [
    "BAR_ROW_COLUMNS",
    "map_bar",
    "map_page",
    "parse_utc_timestamp",
]

# `symbol` and `timestamp` are the names `_source_dataframe` expects; the rest
# are the provider-frame measures.
BAR_ROW_COLUMNS = (
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
)

_REQUIRED_KEYS = ("t", "o", "h", "l", "c", "v")


def parse_utc_timestamp(raw: str) -> datetime:
    """Parse an Alpaca RFC-3339 timestamp into an aware UTC datetime."""
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise PermanentAlpacaError(f"Alpaca 타임스탬프를 해석할 수 없습니다: {raw!r}") from exc
    if parsed.tzinfo is None:
        raise PermanentAlpacaError(f"Alpaca 타임스탬프에 시간대가 없습니다: {raw!r}")
    return parsed.astimezone(UTC)


def map_bar(symbol: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Map one Alpaca bar. Absent optional fields stay `None`, never 0."""
    missing = [key for key in _REQUIRED_KEYS if key not in payload]
    if missing:
        raise PermanentAlpacaError(f"Alpaca 봉에 필수 필드가 없습니다: {missing}")
    try:
        return {
            "symbol": symbol,
            "timestamp": parse_utc_timestamp(str(payload["t"])),
            "open": float(payload["o"]),
            "high": float(payload["h"]),
            "low": float(payload["l"]),
            "close": float(payload["c"]),
            "volume": int(payload["v"]),
            "trade_count": None if payload.get("n") is None else int(payload["n"]),
            "vwap": None if payload.get("vw") is None else float(payload["vw"]),
        }
    except (TypeError, ValueError) as exc:
        raise PermanentAlpacaError(f"Alpaca 봉 값이 잘못되었습니다: {symbol}") from exc


def map_page(page: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Map every bar in one `/v2/stocks/bars` page, sorted by symbol then time."""
    bars = page.get("bars")
    if bars is None:
        return []
    if not isinstance(bars, Mapping):
        raise AlpacaResponseError("Alpaca 응답의 bars가 매핑이 아닙니다.")
    rows: list[dict[str, Any]] = []
    for symbol in sorted(bars):
        payloads = bars[symbol]
        if not isinstance(payloads, Sequence) or isinstance(payloads, (str, bytes)):
            raise AlpacaResponseError(f"Alpaca 응답의 {symbol} 봉 목록이 배열이 아닙니다.")
        for payload in payloads:
            if not isinstance(payload, Mapping):
                raise AlpacaResponseError(f"Alpaca 응답의 {symbol} 봉이 객체가 아닙니다.")
            rows.append(map_bar(symbol, payload))
    rows.sort(key=lambda row: (row["symbol"], row["timestamp"]))
    return rows
