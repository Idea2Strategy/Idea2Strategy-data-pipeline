from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

from market_loader.model.bar import Bar

NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class SessionWindow:
    session_date_et: date
    opens_at: datetime
    closes_at: datetime

    @property
    def is_early_close(self) -> bool:
        local_close = self.closes_at.astimezone(NEW_YORK).time()
        return local_close < time(16, 0)

    def contains(self, timestamp: datetime) -> bool:
        value = timestamp.astimezone(UTC)
        return self.opens_at <= value < self.closes_at


class XnysCalendar:
    def __init__(self) -> None:
        self._calendar = xcals.get_calendar("XNYS")

    def sessions(self, start: date, end: date) -> dict[date, SessionWindow]:
        if start >= end:
            return {}
        schedule = self._calendar.schedule.loc[start.isoformat() : end.isoformat()]  # type: ignore[misc]
        result: dict[date, SessionWindow] = {}
        for label, row in schedule.iterrows():
            session_date = label.date()
            if session_date >= end:
                continue
            opens_at = row["open"].to_pydatetime().astimezone(UTC)
            closes_at = row["close"].to_pydatetime().astimezone(UTC)
            result[session_date] = SessionWindow(session_date, opens_at, closes_at)
        return result

    @staticmethod
    def session_date(timestamp: datetime) -> date:
        return timestamp.astimezone(NEW_YORK).date()

    def filter_regular(self, bars: list[Bar], windows: dict[date, SessionWindow]) -> list[Bar]:
        return [
            bar
            for bar in bars
            if (window := windows.get(bar.session_date_et)) is not None
            and window.contains(bar.bar_start_at)
        ]
