from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite

from market_loader.errors import QualityError


@dataclass(frozen=True, slots=True)
class Bar:
    instrument_id: str
    provider_symbol: str
    bar_start_at: datetime
    session_date_et: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    trade_count: int | None
    vwap: float | None
    source_bar_count: int | None = None
    source_minutes: int | None = None

    def validate_values(self) -> None:
        if self.bar_start_at.tzinfo is None or self.bar_start_at.utcoffset() is None:
            raise QualityError("bar_start_at must be timezone-aware UTC")
        if str(self.bar_start_at.tzinfo) not in {"UTC", "UTC+00:00"}:
            raise QualityError("bar_start_at must use UTC")
        prices = (self.open, self.high, self.low, self.close)
        if any(not isfinite(value) or value <= 0 for value in prices):
            raise QualityError("OHLC values must be finite and positive")
        if self.low > self.high or not self.low <= self.open <= self.high:
            raise QualityError("open is outside [low, high]")
        if not self.low <= self.close <= self.high:
            raise QualityError("close is outside [low, high]")
        if self.volume < 0 or (self.trade_count is not None and self.trade_count < 0):
            raise QualityError("volume and trade_count cannot be negative")
        if self.source_bar_count is not None:
            if self.source_minutes != self.source_bar_count * 30:
                raise QualityError("source_minutes must equal source_bar_count * 30")
