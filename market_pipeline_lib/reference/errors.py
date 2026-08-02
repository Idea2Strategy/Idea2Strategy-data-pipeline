"""Typed failures for the D04 reference-data catalog.

Each one names a distinct refusal, so a caller separates "this row is malformed"
from "this instrument already exists with a different identity" from "this ticker
belongs to someone else right now" without matching on message text.
"""

from __future__ import annotations

__all__ = [
    "CalendarSourceDrift",
    "InstrumentIdentityConflict",
    "InvalidInstrumentIdentity",
    "InvalidTradingSession",
    "ReferenceDataError",
    "SymbolAlreadyAssigned",
    "SymbolNotAssigned",
    "UnknownInstrument",
]


class ReferenceDataError(Exception):
    """Base class for every failure raised by `market_pipeline_lib.reference`."""


class InvalidInstrumentIdentity(ReferenceDataError, ValueError):
    """A registration or symbol assignment cannot be represented by the applied DDL.

    Covers the enum, the `char(4)` / `char(3)` widths and the `varchar` limits.
    Refusing here rather than at the driver is what keeps `LocalCatalog` and
    `PostgresCatalog` interchangeable: PostgreSQL blank-pads a short `char` value
    and truncates nothing, so a three-character MIC would read back differently
    from the two implementations.
    """


class InstrumentIdentityConflict(ReferenceDataError, PermissionError):
    """A registered instrument was asked to change what it *is*.

    `instrument_id` is the shard key, the quality-incident scope and the target of
    fifteen foreign keys across `market_data`, `bot` and `trading`.  Moving an
    existing id onto a different asset type, exchange or currency silently
    reinterprets every row that already cites it.
    """


class UnknownInstrument(ReferenceDataError, LookupError):
    """A symbol assignment names an instrument that is not registered.

    `instrument_symbols.instrument_id` is a real foreign key in the applied DDL;
    `LocalCatalog` has no referential integrity of its own, so this is where both
    implementations refuse identically.
    """


class SymbolAlreadyAssigned(ReferenceDataError, ValueError):
    """A ticker is claimed over a period that overlaps an existing assignment.

    The DBML requires the migration to prevent overlapping symbol validity
    periods.  The applied DDL has only a unique index on
    ``(exchange_mic, symbol, effective_from)``, which two overlapping periods with
    different start instants slip straight past; see
    `db.tables.SCHEMA_CONTRADICTIONS`.
    """


class SymbolNotAssigned(ReferenceDataError, LookupError):
    """A rename was requested for an instrument with no symbol currently in force."""


class InvalidTradingSession(ReferenceDataError, ValueError):
    """A session row contradicts itself or the applied `trading_sessions` DDL."""


class CalendarSourceDrift(ReferenceDataError, RuntimeError):
    """The installed calendar library is not the one `calendar_version` names.

    `trading_sessions.calendar_version` is what a backtest, a realtime evaluation
    and a weekly object check pin their session boundaries to.  Emitting rows
    labelled with a version that did not produce them would make the label a lie.
    """
