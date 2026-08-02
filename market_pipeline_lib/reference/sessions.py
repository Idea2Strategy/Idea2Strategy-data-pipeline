"""`market_data.trading_sessions`, produced from the real XNYS calendar.

The table had zero references in the repository before D04.  Its canonical purpose
is stated in the DDL comment: the calendar version pins the session boundaries a
backtest, a realtime evaluation, an ET close snapshot and a weekly object check all
work from.  That makes two things load-bearing.

**The source has to be real.**  Sessions come from `pandas_market_calendars`, the
same library `engine.py`, `processing.py` and `quality.py` already use for XNYS,
including its own `early_closes` classification.  Nothing here re-derives a
holiday rule.

**The version has to be honest.**  `calendar_version` is the label downstream
replay pins to, so it names the library release that produced the boundaries and
`xnys_sessions` refuses to emit rows under that label from any other release.  A
recalculated calendar is therefore a *new version* -- the unique index is
``(exchange_mic, session_date, calendar_version)``, so old rows survive and a
completed backtest's boundaries cannot be silently rewritten.

A row is emitted for every date in the requested range, not only for trading days.
"Is 2024-11-28 a session?" is then one lookup with no weekend/holiday fallback
logic on the caller's side, which is precisely the logic that goes wrong.

Duplication note: `backtest-engine` pins the same XNYS facts for its replay clock.
The two repositories may not import each other, so the facts are pinned
independently in each, with literal expected values in both test suites.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal

from ..contracts import ET, deterministic_uuid, iso_utc
from .errors import CalendarSourceDrift, InvalidTradingSession
from .tables import TRADING_SESSIONS, ReferenceCatalog

__all__ = [
    "SESSION_TYPES",
    "SESSION_TYPE_CLOSED",
    "SESSION_TYPE_EARLY_CLOSE",
    "SESSION_TYPE_REGULAR",
    "XNYS_CALENDAR_LIBRARY_VERSION",
    "XNYS_CALENDAR_VERSION",
    "XNYS_MIC",
    "TradingSession",
    "TradingSessionRegistry",
    "xnys_sessions",
]


XNYS_MIC = "XNYS"

#: The three values `trading_sessions.session_type` may take, per the DDL comment
#: ``값은 REGULAR, EARLY_CLOSE, CLOSED``.  It is a note, not an enum type, so the
#: constraint lives here.
SESSION_TYPE_REGULAR = "REGULAR"
SESSION_TYPE_EARLY_CLOSE = "EARLY_CLOSE"
SESSION_TYPE_CLOSED = "CLOSED"
SESSION_TYPES: tuple[str, ...] = (SESSION_TYPE_REGULAR, SESSION_TYPE_EARLY_CLOSE, SESSION_TYPE_CLOSED)

#: The `pandas_market_calendars` release these boundaries were verified against.
XNYS_CALENDAR_LIBRARY_VERSION = "5.4.0"

#: `varchar(40)`.  Names the calendar and the library release that produced it.
XNYS_CALENDAR_VERSION = f"XNYS/mcal-{XNYS_CALENDAR_LIBRARY_VERSION}"

#: `trading_sessions.calendar_version varchar(40)`.
CALENDAR_VERSION_MAX_LENGTH = 40
#: `trading_sessions.session_type varchar(30)`.
SESSION_TYPE_MAX_LENGTH = 30
#: `trading_sessions.exchange_mic char(4)`, blank-padded like every other `char`.
EXCHANGE_MIC_LENGTH = 4

_UUID_PURPOSE = "trading-session"

# The DDL comment fixes the vocabulary and the column fixes the width; asserting the
# agreement here means `TradingSession` only has to check membership.
if max(len(label) for label in SESSION_TYPES) > SESSION_TYPE_MAX_LENGTH:  # pragma: no cover
    raise ImportError("a session_type label does not fit trading_sessions.session_type varchar(30)")


def _installed_calendar_library_version() -> str:
    """Read at call time, so a version change is caught on the next call."""

    return str(getattr(mcal, "__version__", "unknown"))


@dataclass(frozen=True)
class TradingSession:
    """One `market_data.trading_sessions` row.

    A `CLOSED` day carries no boundaries and a tradeable day carries both; the
    applied DDL makes `opens_at` / `closes_at` nullable and states nothing about
    which combination is meaningful, so the pairing is enforced here.
    """

    exchange_mic: str
    session_date: date
    opens_at: datetime | None
    closes_at: datetime | None
    session_type: str
    calendar_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.exchange_mic, str) or len(self.exchange_mic) != EXCHANGE_MIC_LENGTH:
            raise InvalidTradingSession(
                f"exchange_mic={self.exchange_mic!r} must be exactly {EXCHANGE_MIC_LENGTH} "
                "characters; the column is char(4) and PostgreSQL blank-pads a shorter value."
            )
        if not isinstance(self.session_date, date) or isinstance(self.session_date, datetime):
            raise InvalidTradingSession("session_date must be a date, not a datetime")
        if self.session_type not in SESSION_TYPES:
            raise InvalidTradingSession(
                f"session_type={self.session_type!r} is not one of {list(SESSION_TYPES)}"
            )
        if not self.calendar_version or len(self.calendar_version) > CALENDAR_VERSION_MAX_LENGTH:
            raise InvalidTradingSession(
                f"calendar_version={self.calendar_version!r} must be 1..{CALENDAR_VERSION_MAX_LENGTH} "
                "characters"
            )
        if self.session_type == SESSION_TYPE_CLOSED:
            if self.opens_at is not None or self.closes_at is not None:
                raise InvalidTradingSession(
                    f"{self.session_date.isoformat()} is CLOSED and cannot carry session boundaries"
                )
            return
        if self.opens_at is None or self.closes_at is None:
            raise InvalidTradingSession(
                f"{self.session_date.isoformat()} is {self.session_type} and needs both boundaries"
            )
        opens_at = self._utc(self.opens_at, "opens_at")
        closes_at = self._utc(self.closes_at, "closes_at")
        if closes_at <= opens_at:
            raise InvalidTradingSession(
                f"closes_at {closes_at.isoformat()} does not follow opens_at {opens_at.isoformat()}"
            )
        object.__setattr__(self, "opens_at", opens_at)
        object.__setattr__(self, "closes_at", closes_at)
        # `session_date` is an ET calendar date everywhere in this pipeline -- it is
        # what `bar_schema`'s `partition_timezone` metadata and every partition
        # boundary are expressed in -- so both instants have to land on it in ET.
        on_session_date = {opens_at.astimezone(ET).date(), closes_at.astimezone(ET).date()}
        if on_session_date != {self.session_date}:
            raise InvalidTradingSession(
                f"session boundaries {opens_at.isoformat()}~{closes_at.isoformat()} are not on "
                f"the ET session_date {self.session_date.isoformat()}"
            )

    @staticmethod
    def _utc(value: Any, label: str) -> datetime:
        if not isinstance(value, datetime):
            raise InvalidTradingSession(f"{label} must be a datetime, got {type(value).__name__}")
        if value.tzinfo is None:
            raise InvalidTradingSession(f"{label} must be timezone-aware")
        return value.astimezone(UTC)

    @property
    def row_id(self) -> str:
        return str(
            deterministic_uuid(
                _UUID_PURPOSE,
                self.exchange_mic,
                self.session_date.isoformat(),
                self.calendar_version,
            )
        )

    def to_record(self) -> dict[str, Any]:
        """The canonical `market_data.trading_sessions` row, every column present."""

        return {
            "id": self.row_id,
            "exchange_mic": self.exchange_mic,
            "session_date": self.session_date.isoformat(),
            "opens_at": iso_utc(self.opens_at) if self.opens_at else None,
            "closes_at": iso_utc(self.closes_at) if self.closes_at else None,
            "session_type": self.session_type,
            "calendar_version": self.calendar_version,
        }


def xnys_sessions(start: date, end: date) -> tuple[TradingSession, ...]:
    """Every date in ``[start, end]``, classified from the pinned XNYS calendar.

    Raises `CalendarSourceDrift` when the installed `pandas_market_calendars` is
    not the release `XNYS_CALENDAR_VERSION` names: the label is what downstream
    replay pins its boundaries to, so it may not be attached to rows a different
    release produced.
    """

    installed = _installed_calendar_library_version()
    if installed != XNYS_CALENDAR_LIBRARY_VERSION:
        raise CalendarSourceDrift(
            f"pandas-market-calendars {installed} is installed but {XNYS_CALENDAR_VERSION} "
            f"labels boundaries produced by {XNYS_CALENDAR_LIBRARY_VERSION}. Re-verify the "
            "XNYS holiday and early-close facts against the new release, then bump "
            "XNYS_CALENDAR_LIBRARY_VERSION and the pinned expectations in "
            "tests/test_reference_catalog.py."
        )
    if end < start:
        raise ValueError(f"end {end.isoformat()} precedes start {start.isoformat()}")

    calendar = mcal.get_calendar(XNYS_MIC)
    schedule = calendar.schedule(start_date=start, end_date=end)
    early = {pd.Timestamp(index).date() for index in calendar.early_closes(schedule).index}
    boundaries = {
        pd.Timestamp(index).date(): (
            pd.Timestamp(row["market_open"]).tz_convert("UTC").to_pydatetime(),
            pd.Timestamp(row["market_close"]).tz_convert("UTC").to_pydatetime(),
        )
        for index, row in schedule.iterrows()
    }

    produced: list[TradingSession] = []
    current = start
    while current <= end:
        window = boundaries.get(current)
        if window is None:
            produced.append(
                TradingSession(
                    exchange_mic=XNYS_MIC,
                    session_date=current,
                    opens_at=None,
                    closes_at=None,
                    session_type=SESSION_TYPE_CLOSED,
                    calendar_version=XNYS_CALENDAR_VERSION,
                )
            )
        else:
            produced.append(
                TradingSession(
                    exchange_mic=XNYS_MIC,
                    session_date=current,
                    opens_at=window[0],
                    closes_at=window[1],
                    session_type=(
                        SESSION_TYPE_EARLY_CLOSE if current in early else SESSION_TYPE_REGULAR
                    ),
                    calendar_version=XNYS_CALENDAR_VERSION,
                )
            )
        current += timedelta(days=1)
    return tuple(produced)


class TradingSessionRegistry:
    """Reads and writes `market_data.trading_sessions` through one catalog.

    Like `InstrumentRegistry`, no method opens a transaction; a caller that wants a
    whole calendar year to land together wraps `publish` in
    ``catalog.transaction()``.
    """

    def __init__(self, catalog: ReferenceCatalog) -> None:
        self._catalog = catalog

    def publish(self, sessions: Iterable[TradingSession]) -> int:
        """Write each session, returning how many rows were written.

        The row id is derived from the table's real identity
        ``(exchange_mic, session_date, calendar_version)``, so republishing a range
        converges on the same rows on both catalogs rather than duplicating them.
        """

        written = 0
        for session in sessions:
            self._catalog.upsert(TRADING_SESSIONS, session.to_record())
            written += 1
        return written

    def session_on(
        self,
        exchange_mic: str,
        session_date: date,
        *,
        calendar_version: str = XNYS_CALENDAR_VERSION,
    ) -> dict[str, Any] | None:
        """The stored row for one venue, date and calendar version, or `None`."""

        rows: Sequence[dict[str, Any]] = self._catalog.records(
            TRADING_SESSIONS,
            where={
                "exchange_mic": exchange_mic,
                "session_date": session_date.isoformat(),
                "calendar_version": calendar_version,
            },
        )
        return rows[0] if rows else None

    def is_trading_day(
        self,
        exchange_mic: str,
        session_date: date,
        *,
        calendar_version: str = XNYS_CALENDAR_VERSION,
    ) -> bool:
        """Whether the stored calendar says this date trades.

        Raises `LookupError` when the calendar has not been published for that
        date: "no row" is not "closed", and answering `False` would let a caller
        skip a real session because the calendar load never ran.
        """

        row = self.session_on(exchange_mic, session_date, calendar_version=calendar_version)
        if row is None:
            raise LookupError(
                f"{exchange_mic} has no {calendar_version} session row for "
                f"{session_date.isoformat()}; publish the calendar for that range first."
            )
        return str(row["session_type"]) != SESSION_TYPE_CLOSED
