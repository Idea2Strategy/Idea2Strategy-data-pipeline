"""Turn the operator's instrument map into registered reference data.

The instrument map is already the single place this repository states which
instruments it supports and what UUID each one has -- `engine.py` shards on that
UUID, and `contracts.load_instrument_map` already reads `asset_type` and
`primary_exchange_mic` columns that nothing consumed.  This module consumes them,
so registration has exactly one source and cannot drift from the collection path.

Nothing is defaulted.  A mapping without an asset type, a MIC or a symbol start
instant is reported by symbol and the whole load is refused, because a guessed
`asset_type` or a guessed listing venue is a wrong fact recorded as a right one.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from typing import Any

from ..contracts import InstrumentMapping
from .errors import InvalidInstrumentIdentity
from .instruments import InstrumentRegistration, InstrumentRegistry, SymbolAssignment
from .sessions import TradingSession, TradingSessionRegistry, xnys_sessions
from .tables import ReferenceCatalog

__all__ = [
    "REQUIRED_MAP_COLUMNS",
    "instrument_registration",
    "load_reference_data",
    "symbol_assignment",
]


#: Instrument-map columns the reference path needs on top of what collection needs.
REQUIRED_MAP_COLUMNS: tuple[str, ...] = (
    "asset_type",
    "primary_exchange_mic",
    "symbol_effective_from",
)


def _required(mapping: InstrumentMapping, column: str) -> str:
    value = getattr(mapping, column)
    if value is None or not str(value).strip():
        raise InvalidInstrumentIdentity(
            f"instrument map row {mapping.provider_symbol} has no {column}. The reference "
            f"catalog needs {list(REQUIRED_MAP_COLUMNS)}; none of them is defaulted, because a "
            "guessed asset type or listing venue is a wrong fact recorded as a right one."
        )
    return str(value).strip()


def _optional_date(mapping: InstrumentMapping, column: str) -> date | None:
    value = getattr(mapping, column)
    if value is None or not str(value).strip():
        return None
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise InvalidInstrumentIdentity(
            f"instrument map row {mapping.provider_symbol} has {column}={value!r}, "
            "which is not an ISO-8601 date"
        ) from exc


def instrument_registration(mapping: InstrumentMapping) -> InstrumentRegistration:
    """The `market_data.instruments` registration this map row describes."""

    return InstrumentRegistration(
        instrument_id=mapping.instrument_id,
        asset_type=_required(mapping, "asset_type"),
        primary_exchange_mic=_required(mapping, "primary_exchange_mic"),
        currency_code=(mapping.currency_code or "USD"),
        provider_reference=mapping.provider_reference,
        listed_at=_optional_date(mapping, "listed_at"),
        delisted_at=_optional_date(mapping, "delisted_at"),
    )


def symbol_assignment(mapping: InstrumentMapping) -> SymbolAssignment:
    """The `market_data.instrument_symbols` period this map row opens.

    The venue is the instrument's primary MIC: the map states one symbol per
    instrument, so that is the venue the symbol is being asserted on.  A second
    listing is a second `assign_symbol` call, not a second map row.
    """

    raw = _required(mapping, "symbol_effective_from")
    try:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidInstrumentIdentity(
            f"instrument map row {mapping.provider_symbol} has "
            f"symbol_effective_from={raw!r}, which is not an ISO-8601 timestamp"
        ) from exc
    if moment.tzinfo is None:
        raise InvalidInstrumentIdentity(
            f"instrument map row {mapping.provider_symbol} has a symbol_effective_from "
            "without a timezone; this pipeline works in ET and UTC at once and never "
            "assumes UTC."
        )
    return SymbolAssignment(
        instrument_id=mapping.instrument_id,
        exchange_mic=_required(mapping, "primary_exchange_mic"),
        symbol=mapping.provider_symbol,
        effective_from=moment,
    )


def load_reference_data(
    catalog: ReferenceCatalog,
    mappings: Iterable[InstrumentMapping] | Mapping[str, InstrumentMapping],
    *,
    calendar_start: date | None = None,
    calendar_end: date | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Register every mapping and, when a range is given, the XNYS calendar.

    Everything commits as one unit of work: a load that fails halfway must not
    leave instruments registered with no symbols, or a half-written calendar.
    """

    if (calendar_start is None) != (calendar_end is None):
        raise ValueError("calendar_start and calendar_end must be given together")
    rows = list(mappings.values()) if isinstance(mappings, Mapping) else list(mappings)
    # Built before the transaction opens, so a malformed row costs no writes at all.
    registrations = [instrument_registration(mapping) for mapping in rows]
    assignments = [symbol_assignment(mapping) for mapping in rows]
    sessions: tuple[TradingSession, ...] = (
        xnys_sessions(calendar_start, calendar_end)
        if calendar_start is not None and calendar_end is not None
        else ()
    )

    moment = created_at or datetime.now(UTC)
    instruments = InstrumentRegistry(catalog)
    calendar = TradingSessionRegistry(catalog)
    with catalog.transaction():
        for registration in registrations:
            instruments.register(registration, created_at=moment)
        for assignment in assignments:
            instruments.assign_symbol(assignment)
        session_count = calendar.publish(sessions)
    return {
        "status": "REGISTERED",
        "instrument_count": len(registrations),
        "symbol_count": len(assignments),
        "trading_session_count": session_count,
    }
