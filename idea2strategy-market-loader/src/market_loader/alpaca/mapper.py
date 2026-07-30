from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from market_loader.errors import PermanentAlpacaError
from market_loader.model.bar import Bar


def parse_utc_timestamp(raw: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PermanentAlpacaError(f"invalid Alpaca timestamp: {raw}") from exc
    if parsed.tzinfo is None:
        raise PermanentAlpacaError("Alpaca timestamp is not timezone-aware")
    return parsed.astimezone(UTC)


def map_bar(
    *,
    symbol: str,
    instrument_id: str,
    session_date_et: date,
    payload: dict[str, Any],
) -> Bar:
    required = {"t", "o", "h", "l", "c", "v"}
    missing = required - payload.keys()
    if missing:
        raise PermanentAlpacaError(f"Alpaca bar is missing fields: {sorted(missing)}")
    try:
        return Bar(
            instrument_id=instrument_id,
            provider_symbol=symbol,
            bar_start_at=parse_utc_timestamp(str(payload["t"])),
            session_date_et=session_date_et,
            open=float(payload["o"]),
            high=float(payload["h"]),
            low=float(payload["l"]),
            close=float(payload["c"]),
            volume=int(payload["v"]),
            trade_count=int(payload["n"]) if payload.get("n") is not None else None,
            vwap=float(payload["vw"]) if payload.get("vw") is not None else None,
        )
    except (TypeError, ValueError) as exc:
        raise PermanentAlpacaError("Alpaca bar contains invalid values") from exc
