"""D04: the supported-instrument, symbol-history and trading-session catalog.

Three canonical tables had no write path before this package existed:

===============================  ================================================
`market_data.instruments`        read once (`operations.py` `existing_ids`), never
                                 written -- so `quality_incidents.instrument_id`,
                                 a foreign key to it, could only ever be NULL
`market_data.instrument_symbols` read-only
`market_data.trading_sessions`   zero references
===============================  ================================================

Everything here writes through the shared `MarketDataCatalog` boundary, so the
same code drives `LocalCatalog` and `PostgresCatalog`.  No DDL is authored or
executed: the tables already exist in the applied central schema.
"""

from __future__ import annotations

from .errors import (
    CalendarSourceDrift,
    InstrumentIdentityConflict,
    InvalidInstrumentIdentity,
    InvalidTradingSession,
    ReferenceDataError,
    SymbolAlreadyAssigned,
    SymbolNotAssigned,
    UnknownInstrument,
)
from .instruments import (
    CURRENCY_CODE_LENGTH,
    EXCHANGE_MIC_LENGTH,
    INSTRUMENT_IDENTITY_COLUMNS,
    PROVIDER_REFERENCE_MAX_LENGTH,
    SYMBOL_MAX_LENGTH,
    InstrumentRegistration,
    InstrumentRegistry,
    SymbolAssignment,
)
from .loading import (
    REQUIRED_MAP_COLUMNS,
    instrument_registration,
    load_reference_data,
    symbol_assignment,
)
from .sessions import (
    SESSION_TYPE_CLOSED,
    SESSION_TYPE_EARLY_CLOSE,
    SESSION_TYPE_REGULAR,
    SESSION_TYPES,
    XNYS_CALENDAR_LIBRARY_VERSION,
    XNYS_CALENDAR_VERSION,
    XNYS_MIC,
    TradingSession,
    TradingSessionRegistry,
    xnys_sessions,
)
from .tables import INSTRUMENT_SYMBOLS, INSTRUMENTS, TRADING_SESSIONS, ReferenceCatalog

__all__ = [
    "CURRENCY_CODE_LENGTH",
    "EXCHANGE_MIC_LENGTH",
    "INSTRUMENTS",
    "INSTRUMENT_IDENTITY_COLUMNS",
    "INSTRUMENT_SYMBOLS",
    "PROVIDER_REFERENCE_MAX_LENGTH",
    "REQUIRED_MAP_COLUMNS",
    "SESSION_TYPES",
    "SESSION_TYPE_CLOSED",
    "SESSION_TYPE_EARLY_CLOSE",
    "SESSION_TYPE_REGULAR",
    "SYMBOL_MAX_LENGTH",
    "TRADING_SESSIONS",
    "XNYS_CALENDAR_LIBRARY_VERSION",
    "XNYS_CALENDAR_VERSION",
    "XNYS_MIC",
    "CalendarSourceDrift",
    "InstrumentIdentityConflict",
    "InstrumentRegistration",
    "InstrumentRegistry",
    "InvalidInstrumentIdentity",
    "InvalidTradingSession",
    "ReferenceCatalog",
    "ReferenceDataError",
    "SymbolAlreadyAssigned",
    "SymbolAssignment",
    "SymbolNotAssigned",
    "TradingSession",
    "TradingSessionRegistry",
    "UnknownInstrument",
    "instrument_registration",
    "load_reference_data",
    "symbol_assignment",
    "xnys_sessions",
]
