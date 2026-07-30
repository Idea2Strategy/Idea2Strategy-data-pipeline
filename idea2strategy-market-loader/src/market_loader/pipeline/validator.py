from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from market_loader.calendar.xnys import SessionWindow
from market_loader.errors import QualityError
from market_loader.model.bar import Bar
from market_loader.model.status import QualityStatus


@dataclass(frozen=True, slots=True)
class ValidationResult:
    status: QualityStatus
    warnings: tuple[str, ...]


def validate_bars(
    bars: list[Bar],
    windows: dict[date, SessionWindow],
    *,
    derived: bool,
) -> ValidationResult:
    seen: set[tuple[str, object]] = set()
    warnings: list[str] = []
    for bar in bars:
        bar.validate_values()
        key = (bar.instrument_id, bar.bar_start_at)
        if key in seen:
            raise QualityError(f"duplicate bar key: {key}")
        seen.add(key)
        window = windows.get(bar.session_date_et)
        if window is None or not window.contains(bar.bar_start_at):
            raise QualityError("bar is outside the XNYS regular session")
        if derived and (bar.source_bar_count is None or bar.source_minutes is None):
            raise QualityError("derived bar is missing source count metadata")
        if bar.trade_count is None:
            warnings.append("NULL_TRADE_COUNT")
        if bar.vwap is None:
            warnings.append("NULL_VWAP")
    return ValidationResult(
        QualityStatus.PASSED_WITH_WARNINGS if warnings else QualityStatus.PASSED,
        tuple(sorted(set(warnings))),
    )
