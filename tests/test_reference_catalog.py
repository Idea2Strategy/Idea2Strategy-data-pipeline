"""D04: the instrument, symbol-history and trading-session catalog.

Three canonical tables had no write path at all before this card:
`market_data.instruments` (one read of `existing_ids` at `operations.py:659`),
`market_data.instrument_symbols` (read-only) and `market_data.trading_sessions`
(zero references).  Everything here is run against **both** catalog
implementations through the shared `catalog` fixture, because the registry is
production code that has to behave the same whether it is pointed at JSONL or at
PostgreSQL -- the type gates were removed and must stay gone.

Two groups of assertions are deliberately literal rather than computed:

* every deterministic id and hash is a pinned UUID string, so a change to the
  identity salt fails here instead of silently re-keying published rows;
* every XNYS session boundary is a pinned instant.  `backtest-engine` pins the
  same facts for its replay clock; the two repositories may not import each
  other, so the duplication is intentional and is reported.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

import pytest

from market_pipeline_lib.catalog import LocalCatalog, PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.cli import build_parser, execute
from market_pipeline_lib.contracts import load_instrument_map, stable_shard_key
from market_pipeline_lib.quality import (
    ImpactScope,
    QualityIncident,
    ScopeBreadth,
    record_quality_incidents,
)
from market_pipeline_lib.reference import (
    INSTRUMENT_SYMBOLS,
    INSTRUMENTS,
    TRADING_SESSIONS,
    XNYS_CALENDAR_VERSION,
    XNYS_MIC,
    CalendarSourceDrift,
    InstrumentIdentityConflict,
    InstrumentRegistration,
    InstrumentRegistry,
    InvalidInstrumentIdentity,
    InvalidTradingSession,
    SymbolAlreadyAssigned,
    SymbolAssignment,
    SymbolNotAssigned,
    TradingSession,
    TradingSessionRegistry,
    UnknownInstrument,
    load_reference_data,
    xnys_sessions,
)

pytestmark = pytest.mark.usefixtures("_catalog_isolation")


# --------------------------------------------------------------------------------------
# Fixed identities.  The three instrument ids are the ones
# `tests/test_market_pipeline_lib.py` already shards, so the rename invariant below is
# checked against the same pinned shard keys that suite pins.
# --------------------------------------------------------------------------------------

AAPL_ID = "11111111-1111-4111-8111-111111111111"
MSFT_ID = "22222222-2222-4222-8222-222222222222"
BRKB_ID = "33333333-3333-4333-8333-333333333333"

CREATED_AT = datetime(2026, 1, 2, tzinfo=UTC)
LATER = datetime(2026, 6, 1, tzinfo=UTC)

#: `deterministic_uuid("instrument-symbol", instrument_id, mic, symbol, effective_from)`.
AAPL_SYMBOL_ROW_ID = "57cadaf8-00b7-5a04-822d-b0ba9b21c3ba"
BRKB_OLD_SYMBOL_ROW_ID = "06417a45-7fd3-5ac7-8691-b55c5c397149"
BRKB_NEW_SYMBOL_ROW_ID = "b929c4bb-b429-50c3-9006-659689b618fa"

#: `deterministic_uuid("trading-session", mic, session_date, calendar_version)`.
SESSION_ID_2024_11_27 = "ade7427e-7ab2-5787-9ba0-07e03b723414"
SESSION_ID_2024_11_28 = "953192b3-9410-5e0e-b081-8057509eba44"
SESSION_ID_2024_11_29 = "9216b56d-6cb0-5374-87b0-077a6beafcd8"


def aapl(**overrides: Any) -> InstrumentRegistration:
    values: dict[str, Any] = {
        "instrument_id": AAPL_ID,
        "asset_type": "STOCK",
        "primary_exchange_mic": "XNAS",
        "currency_code": "USD",
        "provider_reference": "ALPACA:AAPL",
        "listed_at": date(1980, 12, 12),
    }
    values.update(overrides)
    return InstrumentRegistration(**values)


def aapl_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": AAPL_ID,
        "asset_type": "STOCK",
        "primary_exchange_mic": "XNAS",
        "currency_code": "USD",
        "provider_reference": "ALPACA:AAPL",
        "listed_at": "1980-12-12",
        "delisted_at": None,
        "created_at": "2026-01-02T00:00:00Z",
    }
    row.update(overrides)
    return row


@pytest.fixture
def instruments(catalog: Any) -> InstrumentRegistry:
    return InstrumentRegistry(catalog)


@pytest.fixture
def sessions(catalog: Any) -> TradingSessionRegistry:
    return TradingSessionRegistry(catalog)


# --------------------------------------------------------------------------------------
# market_data.instruments
# --------------------------------------------------------------------------------------


def test_registering_an_instrument_writes_one_canonical_row(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)

    assert catalog.records(INSTRUMENTS) == [aapl_row()]


def test_registration_is_idempotent_and_never_restamps_created_at(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """A rerun of the reference loader must not rewrite when the instrument was created.

    `PostgresCatalog.upsert` overwrites every non-key column on conflict, so an
    unconditional second write would move `created_at` forward on every run and
    destroy the only record of when the instrument entered the catalog.
    """

    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.register(aapl(), created_at=LATER)
    instruments.register(aapl(), created_at=LATER)

    assert catalog.records(INSTRUMENTS) == [aapl_row()]


def test_re_registering_with_a_different_identity_is_refused(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """`instrument_id` is the shard key and the quality-incident scope.

    Fifteen foreign keys across `market_data`, `bot` and `trading` point at this row.
    Silently moving an existing id onto a different exchange or currency would
    re-interpret every one of them, so it is refused rather than merged.
    """

    instruments.register(aapl(), created_at=CREATED_AT)

    with pytest.raises(InstrumentIdentityConflict) as failure:
        instruments.register(aapl(primary_exchange_mic="XNYS"), created_at=LATER)

    assert "primary_exchange_mic" in str(failure.value)
    assert catalog.records(INSTRUMENTS) == [aapl_row()]


def test_a_delisting_updates_the_mutable_attributes_in_place(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """Listing dates and the provider reference are facts that change; identity is not."""

    instruments.register(aapl(), created_at=CREATED_AT)

    instruments.register(
        aapl(delisted_at=date(2026, 5, 4), provider_reference="ALPACA:AAPL:V2"),
        created_at=LATER,
    )

    assert catalog.records(INSTRUMENTS) == [
        aapl_row(delisted_at="2026-05-04", provider_reference="ALPACA:AAPL:V2")
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("primary_exchange_mic", "XNY"),
        ("primary_exchange_mic", "XNASX"),
        ("currency_code", "US"),
        ("currency_code", "USDX"),
    ],
)
def test_a_mic_or_currency_of_the_wrong_width_is_refused(field: str, value: str) -> None:
    """`char(4)` and `char(3)` blank-pad, which breaks catalog interchangeability.

    Verified against the applied DDL in a container: PostgreSQL stores
    ``primary_exchange_mic='XNY'`` and reads it back as ``'XNY '``, and
    ``currency_code='US'`` as ``'US '``, while `LocalCatalog` returns exactly what
    was written.  Over-long values are refused by PostgreSQL with
    ``StringDataRightTruncation`` and accepted by `LocalCatalog`.  Neither
    difference is expressible in the SQLAlchemy metadata, so the width is enforced
    here, before either catalog is reached.
    """

    with pytest.raises(InvalidInstrumentIdentity):
        aapl(**{field: value})


def test_an_asset_type_outside_the_canonical_enum_is_refused() -> None:
    with pytest.raises(InvalidInstrumentIdentity) as failure:
        aapl(asset_type="BOND")

    assert "STOCK" in str(failure.value)


def test_an_instrument_id_that_is_not_a_uuid_is_refused() -> None:
    with pytest.raises(InvalidInstrumentIdentity):
        aapl(instrument_id="AAPL")


def test_a_delisting_before_the_listing_is_refused() -> None:
    with pytest.raises(InvalidInstrumentIdentity):
        aapl(listed_at=date(2020, 1, 2), delisted_at=date(2019, 12, 31))


def test_a_provider_reference_longer_than_the_column_is_refused() -> None:
    with pytest.raises(InvalidInstrumentIdentity):
        aapl(provider_reference="A" * 161)


# --------------------------------------------------------------------------------------
# market_data.instrument_symbols
# --------------------------------------------------------------------------------------


def test_assigning_a_symbol_opens_a_period_that_is_still_in_force(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)

    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    assert catalog.records(INSTRUMENT_SYMBOLS) == [
        {
            "id": AAPL_SYMBOL_ROW_ID,
            "instrument_id": AAPL_ID,
            "exchange_mic": "XNAS",
            "symbol": "AAPL",
            "effective_from": "2026-01-02T00:00:00Z",
            "effective_to": None,
        }
    ]


def test_assigning_the_same_period_twice_is_one_row(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)
    assignment = SymbolAssignment(
        instrument_id=AAPL_ID,
        exchange_mic="XNAS",
        symbol="AAPL",
        effective_from=CREATED_AT,
    )

    instruments.assign_symbol(assignment)
    instruments.assign_symbol(assignment)

    assert len(catalog.records(INSTRUMENT_SYMBOLS)) == 1


def test_a_symbol_for_an_unregistered_instrument_is_refused(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """`instrument_symbols.instrument_id` is a real foreign key in the applied DDL.

    `LocalCatalog` has no referential integrity of its own, so the check lives in
    the registry and both implementations refuse identically.
    """

    with pytest.raises(UnknownInstrument):
        instruments.assign_symbol(
            SymbolAssignment(
                instrument_id=AAPL_ID,
                exchange_mic="XNAS",
                symbol="AAPL",
                effective_from=CREATED_AT,
            )
        )

    assert catalog.records(INSTRUMENT_SYMBOLS) == []


def test_a_rename_closes_the_old_period_and_does_not_move_the_instrument(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """The invariant `test_symbol_change_does_not_change_shard` asserts, now persisted.

    That test pins the shard for a renamed ticker but never wrote a row anywhere.
    This one records the rename in `instrument_symbols` and checks that the stored
    history still resolves to a single `instrument_id`, and therefore to the same
    shard -- the pinned literal is the one the existing suite pins for ``BRK.B``.
    """

    instruments.register(
        InstrumentRegistration(
            instrument_id=BRKB_ID,
            asset_type="STOCK",
            primary_exchange_mic="XNYS",
        ),
        created_at=CREATED_AT,
    )
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=BRKB_ID,
            exchange_mic="XNYS",
            symbol="BRK.B",
            effective_from=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        )
    )

    instruments.rename(
        instrument_id=BRKB_ID,
        exchange_mic="XNYS",
        symbol="BRKB",
        effective_from=datetime(2026, 3, 2, 14, 30, tzinfo=UTC),
    )

    assert len(catalog.records(INSTRUMENT_SYMBOLS)) == 2
    rows = list(instruments.symbol_history(BRKB_ID))
    assert rows == [
        {
            "id": BRKB_OLD_SYMBOL_ROW_ID,
            "instrument_id": BRKB_ID,
            "exchange_mic": "XNYS",
            "symbol": "BRK.B",
            "effective_from": "2024-01-02T14:30:00Z",
            "effective_to": "2026-03-02T14:30:00Z",
        },
        {
            "id": BRKB_NEW_SYMBOL_ROW_ID,
            "instrument_id": BRKB_ID,
            "exchange_mic": "XNYS",
            "symbol": "BRKB",
            "effective_from": "2026-03-02T14:30:00Z",
            "effective_to": None,
        },
    ]
    assert {row["instrument_id"] for row in rows} == {BRKB_ID}
    assert stable_shard_key(rows[0]["instrument_id"], 16) == "s15-of-16"
    assert stable_shard_key(rows[1]["instrument_id"], 16) == "s15-of-16"


def test_symbol_at_answers_which_ticker_was_in_force(instruments: InstrumentRegistry) -> None:
    instruments.register(
        InstrumentRegistration(
            instrument_id=BRKB_ID,
            asset_type="STOCK",
            primary_exchange_mic="XNYS",
        ),
        created_at=CREATED_AT,
    )
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=BRKB_ID,
            exchange_mic="XNYS",
            symbol="BRK.B",
            effective_from=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        )
    )
    instruments.rename(
        instrument_id=BRKB_ID,
        exchange_mic="XNYS",
        symbol="BRKB",
        effective_from=datetime(2026, 3, 2, 14, 30, tzinfo=UTC),
    )

    assert instruments.symbol_at(BRKB_ID, datetime(2023, 12, 31, tzinfo=UTC)) is None
    assert instruments.symbol_at(BRKB_ID, datetime(2024, 1, 2, 14, 30, tzinfo=UTC)) == "BRK.B"
    assert instruments.symbol_at(BRKB_ID, datetime(2026, 3, 2, 14, 29, tzinfo=UTC)) == "BRK.B"
    # Half-open: the new period owns the instant the old one is closed at.
    assert instruments.symbol_at(BRKB_ID, datetime(2026, 3, 2, 14, 30, tzinfo=UTC)) == "BRKB"
    assert instruments.symbol_at(BRKB_ID, datetime(2030, 1, 1, tzinfo=UTC)) == "BRKB"


def test_the_retired_ticker_still_resolves_to_the_same_instrument(
    instruments: InstrumentRegistry,
) -> None:
    """A backfill of 2024 data keyed by ``BRK.B`` must land on today's instrument."""

    instruments.register(
        InstrumentRegistration(
            instrument_id=BRKB_ID,
            asset_type="STOCK",
            primary_exchange_mic="XNYS",
        ),
        created_at=CREATED_AT,
    )
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=BRKB_ID,
            exchange_mic="XNYS",
            symbol="BRK.B",
            effective_from=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        )
    )
    instruments.rename(
        instrument_id=BRKB_ID,
        exchange_mic="XNYS",
        symbol="BRKB",
        effective_from=datetime(2026, 3, 2, 14, 30, tzinfo=UTC),
    )

    at_the_time = datetime(2024, 6, 3, 14, 30, tzinfo=UTC)
    assert instruments.instrument_for_symbol("XNYS", "BRK.B", at_the_time) == BRKB_ID
    assert instruments.instrument_for_symbol("XNYS", "BRKB", at_the_time) is None
    now = datetime(2026, 6, 3, 14, 30, tzinfo=UTC)
    assert instruments.instrument_for_symbol("XNYS", "BRKB", now) == BRKB_ID
    assert instruments.instrument_for_symbol("XNYS", "BRK.B", now) is None


def test_a_ticker_held_by_another_instrument_over_the_same_period_is_refused(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """The DBML requires the migration to prevent overlapping validity periods.

    The applied DDL has no exclusion constraint -- only a unique index on
    ``(exchange_mic, symbol, effective_from)`` -- so two instruments claiming
    ``AAPL`` over overlapping but differently-starting periods would both be
    stored.  `db/tables.py::SCHEMA_CONTRADICTIONS` records that gap; this is where
    it is closed in application code, identically on both catalogs.
    """

    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.register(
        InstrumentRegistration(
            instrument_id=MSFT_ID,
            asset_type="STOCK",
            primary_exchange_mic="XNAS",
        ),
        created_at=CREATED_AT,
    )
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    with pytest.raises(SymbolAlreadyAssigned) as failure:
        instruments.assign_symbol(
            SymbolAssignment(
                instrument_id=MSFT_ID,
                exchange_mic="XNAS",
                symbol="AAPL",
                effective_from=datetime(2027, 1, 2, tzinfo=UTC),
            )
        )

    assert AAPL_ID in str(failure.value)
    assert [row["instrument_id"] for row in catalog.records(INSTRUMENT_SYMBOLS)] == [AAPL_ID]


def test_a_ticker_can_be_reused_once_the_previous_holder_released_it(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.register(
        InstrumentRegistration(
            instrument_id=MSFT_ID,
            asset_type="STOCK",
            primary_exchange_mic="XNAS",
        ),
        created_at=CREATED_AT,
    )
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
            effective_to=datetime(2027, 1, 2, tzinfo=UTC),
        )
    )

    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=MSFT_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=datetime(2027, 1, 2, tzinfo=UTC),
        )
    )

    assert sorted(row["instrument_id"] for row in catalog.records(INSTRUMENT_SYMBOLS)) == sorted(
        [AAPL_ID, MSFT_ID]
    )
    assert instruments.instrument_for_symbol("XNAS", "AAPL", datetime(2028, 1, 1, tzinfo=UTC)) == MSFT_ID


def test_renaming_an_instrument_with_no_open_symbol_is_refused(
    instruments: InstrumentRegistry,
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)

    with pytest.raises(SymbolNotAssigned):
        instruments.rename(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPLX",
            effective_from=LATER,
        )


def test_a_rename_effective_before_the_open_period_started_is_refused(
    instruments: InstrumentRegistry,
) -> None:
    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    with pytest.raises(SymbolAlreadyAssigned):
        instruments.rename(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPLX",
            effective_from=datetime(2025, 12, 31, tzinfo=UTC),
        )


def test_one_instrument_cannot_carry_two_tickers_on_one_venue_at_once(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """`symbol_at` must have exactly one answer for every instant.

    Nothing in the applied DDL prevents this: the unique index is on
    ``(exchange_mic, symbol, effective_from)``, and these two rows differ in symbol.
    A rename closes the old period, which is why `rename` is the supported way to
    change a ticker.
    """

    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    with pytest.raises(SymbolAlreadyAssigned):
        instruments.assign_symbol(
            SymbolAssignment(
                instrument_id=AAPL_ID,
                exchange_mic="XNAS",
                symbol="AAPLX",
                effective_from=LATER,
            )
        )

    assert [row["symbol"] for row in catalog.records(INSTRUMENT_SYMBOLS)] == ["AAPL"]


def test_renaming_to_the_ticker_already_in_force_is_refused(
    catalog: Any, instruments: InstrumentRegistry
) -> None:
    """Two adjacent periods carrying the same ticker would record a change that never
    happened, and would make `symbol_history` unreadable."""

    instruments.register(aapl(), created_at=CREATED_AT)
    instruments.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    with pytest.raises(SymbolAlreadyAssigned):
        instruments.rename(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=LATER,
        )

    assert len(catalog.records(INSTRUMENT_SYMBOLS)) == 1
    assert catalog.records(INSTRUMENT_SYMBOLS)[0]["effective_to"] is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbol": "S" * 33},
        {"symbol": "   "},
        {"exchange_mic": "XNA"},
        {"effective_from": datetime(2026, 1, 2)},
        {"effective_to": datetime(2026, 1, 1, tzinfo=UTC)},
        {"effective_to": CREATED_AT},
    ],
)
def test_a_malformed_symbol_assignment_is_refused(overrides: dict[str, Any]) -> None:
    """`varchar(32)`, `char(4)`, and a half-open period that is not half-open."""

    values: dict[str, Any] = {
        "instrument_id": AAPL_ID,
        "exchange_mic": "XNAS",
        "symbol": "AAPL",
        "effective_from": CREATED_AT,
    }
    values.update(overrides)

    with pytest.raises((InvalidInstrumentIdentity, ValueError)):
        SymbolAssignment(**values)


# --------------------------------------------------------------------------------------
# The XNYS session source.  Every boundary below is a pinned instant.
# --------------------------------------------------------------------------------------


def test_the_thanksgiving_week_of_2024_is_pinned() -> None:
    """One regular session, one holiday, one early close, one weekend day."""

    produced = {
        session.session_date: session
        for session in xnys_sessions(date(2024, 11, 27), date(2024, 11, 30))
    }

    assert produced[date(2024, 11, 27)] == TradingSession(
        exchange_mic="XNYS",
        session_date=date(2024, 11, 27),
        opens_at=datetime(2024, 11, 27, 14, 30, tzinfo=UTC),
        closes_at=datetime(2024, 11, 27, 21, 0, tzinfo=UTC),
        session_type="REGULAR",
        calendar_version=XNYS_CALENDAR_VERSION,
    )
    assert produced[date(2024, 11, 28)] == TradingSession(
        exchange_mic="XNYS",
        session_date=date(2024, 11, 28),
        opens_at=None,
        closes_at=None,
        session_type="CLOSED",
        calendar_version=XNYS_CALENDAR_VERSION,
    )
    assert produced[date(2024, 11, 29)] == TradingSession(
        exchange_mic="XNYS",
        session_date=date(2024, 11, 29),
        opens_at=datetime(2024, 11, 29, 14, 30, tzinfo=UTC),
        closes_at=datetime(2024, 11, 29, 18, 0, tzinfo=UTC),
        session_type="EARLY_CLOSE",
        calendar_version=XNYS_CALENDAR_VERSION,
    )
    assert produced[date(2024, 11, 30)].session_type == "CLOSED"


def test_the_spring_dst_change_moves_the_utc_boundaries() -> None:
    """09:30-16:00 ET is 14:30-21:00Z in winter and 13:30-20:00Z in summer."""

    winter, summer = (
        xnys_sessions(date(2024, 3, 8), date(2024, 3, 8))[0],
        xnys_sessions(date(2024, 3, 11), date(2024, 3, 11))[0],
    )

    assert (winter.opens_at, winter.closes_at) == (
        datetime(2024, 3, 8, 14, 30, tzinfo=UTC),
        datetime(2024, 3, 8, 21, 0, tzinfo=UTC),
    )
    assert (summer.opens_at, summer.closes_at) == (
        datetime(2024, 3, 11, 13, 30, tzinfo=UTC),
        datetime(2024, 3, 11, 20, 0, tzinfo=UTC),
    )


def test_the_2024_calendar_has_the_pinned_shape() -> None:
    """366 dates, 252 trading days, exactly three early closes, all named."""

    produced = xnys_sessions(date(2024, 1, 1), date(2024, 12, 31))
    counts: dict[str, int] = {}
    for session in produced:
        counts[session.session_type] = counts.get(session.session_type, 0) + 1

    assert len(produced) == 366
    assert counts == {"REGULAR": 249, "EARLY_CLOSE": 3, "CLOSED": 114}
    assert [
        session.session_date.isoformat() for session in produced if session.session_type == "EARLY_CLOSE"
    ] == ["2024-07-03", "2024-11-29", "2024-12-24"]
    closed_weekdays = [
        session.session_date.isoformat()
        for session in produced
        if session.session_type == "CLOSED" and session.session_date.weekday() < 5
    ]
    assert closed_weekdays == [
        "2024-01-01",
        "2024-01-15",
        "2024-02-19",
        "2024-03-29",
        "2024-05-27",
        "2024-06-19",
        "2024-07-04",
        "2024-09-02",
        "2024-11-28",
        "2024-12-25",
    ]


def test_the_calendar_version_names_the_library_that_produced_it() -> None:
    assert XNYS_CALENDAR_VERSION == "XNYS/mcal-5.4.0"
    assert len(XNYS_CALENDAR_VERSION) <= 40


def test_a_different_calendar_library_version_refuses_to_produce_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`calendar_version` is what downstream replay pins its boundaries to.

    Producing rows labelled ``mcal-5.4.0`` from a different release would make the
    label a lie, so the source refuses and names what has to be re-verified.
    """

    import pandas_market_calendars

    monkeypatch.setattr(pandas_market_calendars, "__version__", "5.5.0", raising=False)

    with pytest.raises(CalendarSourceDrift) as failure:
        xnys_sessions(date(2024, 11, 27), date(2024, 11, 27))

    assert "5.5.0" in str(failure.value)
    assert "5.4.0" in str(failure.value)


def test_a_backwards_range_is_refused() -> None:
    with pytest.raises(ValueError):
        xnys_sessions(date(2024, 11, 29), date(2024, 11, 27))


@pytest.mark.parametrize(
    "overrides",
    [
        {"session_type": "HALF_DAY"},
        {"session_type": "CLOSED"},
        {"opens_at": None},
        {"closes_at": datetime(2024, 11, 27, 14, 0, tzinfo=UTC)},
        {"closes_at": datetime(2024, 11, 28, 21, 0, tzinfo=UTC)},
        {"exchange_mic": "XNY"},
        {"calendar_version": "v" * 41},
    ],
)
def test_a_malformed_trading_session_is_refused(overrides: dict[str, Any]) -> None:
    values: dict[str, Any] = {
        "exchange_mic": "XNYS",
        "session_date": date(2024, 11, 27),
        "opens_at": datetime(2024, 11, 27, 14, 30, tzinfo=UTC),
        "closes_at": datetime(2024, 11, 27, 21, 0, tzinfo=UTC),
        "session_type": "REGULAR",
        "calendar_version": XNYS_CALENDAR_VERSION,
    }
    values.update(overrides)

    with pytest.raises(InvalidTradingSession):
        TradingSession(**values)


def test_a_closed_session_with_boundaries_is_refused() -> None:
    with pytest.raises(InvalidTradingSession):
        TradingSession(
            exchange_mic="XNYS",
            session_date=date(2024, 11, 28),
            opens_at=datetime(2024, 11, 28, 14, 30, tzinfo=UTC),
            closes_at=datetime(2024, 11, 28, 21, 0, tzinfo=UTC),
            session_type="CLOSED",
            calendar_version=XNYS_CALENDAR_VERSION,
        )


# --------------------------------------------------------------------------------------
# market_data.trading_sessions
# --------------------------------------------------------------------------------------


def test_publishing_sessions_writes_canonical_rows(
    catalog: Any, sessions: TradingSessionRegistry
) -> None:
    written = sessions.publish(xnys_sessions(date(2024, 11, 27), date(2024, 11, 29)))

    assert written == 3
    rows = sorted(catalog.records(TRADING_SESSIONS), key=lambda row: row["session_date"])
    assert rows == [
        {
            "id": SESSION_ID_2024_11_27,
            "exchange_mic": "XNYS",
            "session_date": "2024-11-27",
            "opens_at": "2024-11-27T14:30:00Z",
            "closes_at": "2024-11-27T21:00:00Z",
            "session_type": "REGULAR",
            "calendar_version": "XNYS/mcal-5.4.0",
        },
        {
            "id": SESSION_ID_2024_11_28,
            "exchange_mic": "XNYS",
            "session_date": "2024-11-28",
            "opens_at": None,
            "closes_at": None,
            "session_type": "CLOSED",
            "calendar_version": "XNYS/mcal-5.4.0",
        },
        {
            "id": SESSION_ID_2024_11_29,
            "exchange_mic": "XNYS",
            "session_date": "2024-11-29",
            "opens_at": "2024-11-29T14:30:00Z",
            "closes_at": "2024-11-29T18:00:00Z",
            "session_type": "EARLY_CLOSE",
            "calendar_version": "XNYS/mcal-5.4.0",
        },
    ]


def test_republishing_the_same_range_does_not_duplicate_rows(
    catalog: Any, sessions: TradingSessionRegistry
) -> None:
    """The row id is the unique index `(exchange_mic, session_date, calendar_version)`."""

    produced = xnys_sessions(date(2024, 11, 27), date(2024, 11, 29))

    sessions.publish(produced)
    sessions.publish(produced)

    assert len(catalog.records(TRADING_SESSIONS)) == 3


def test_two_calendar_versions_coexist_for_the_same_date(
    catalog: Any, sessions: TradingSessionRegistry
) -> None:
    """A recalculated calendar is a new version, never an edit of the old boundaries.

    Downstream replay pins a `calendar_version`; overwriting the row it pinned
    would silently change a completed backtest's session boundaries.
    """

    sessions.publish(xnys_sessions(date(2024, 11, 29), date(2024, 11, 29)))
    sessions.publish(
        [
            TradingSession(
                exchange_mic="XNYS",
                session_date=date(2024, 11, 29),
                opens_at=datetime(2024, 11, 29, 14, 30, tzinfo=UTC),
                closes_at=datetime(2024, 11, 29, 21, 0, tzinfo=UTC),
                session_type="REGULAR",
                calendar_version="XNYS/pinned-test-1",
            )
        ]
    )

    rows = sorted(catalog.records(TRADING_SESSIONS), key=lambda row: row["calendar_version"])
    assert [row["calendar_version"] for row in rows] == ["XNYS/mcal-5.4.0", "XNYS/pinned-test-1"]
    assert [row["session_type"] for row in rows] == ["EARLY_CLOSE", "REGULAR"]
    assert rows[1]["id"] == "7c8e375c-75a5-5baa-8caf-f4b25a06165a"


def test_session_lookup_answers_whether_a_date_is_a_trading_day(
    sessions: TradingSessionRegistry
) -> None:
    sessions.publish(xnys_sessions(date(2024, 11, 27), date(2024, 11, 30)))

    assert sessions.session_on(XNYS_MIC, date(2024, 11, 27)) is not None
    assert sessions.session_on(XNYS_MIC, date(2024, 11, 28)) is not None
    assert sessions.is_trading_day(XNYS_MIC, date(2024, 11, 27)) is True
    assert sessions.is_trading_day(XNYS_MIC, date(2024, 11, 28)) is False
    assert sessions.is_trading_day(XNYS_MIC, date(2024, 11, 29)) is True
    assert sessions.session_on(XNYS_MIC, date(2024, 12, 2)) is None


# --------------------------------------------------------------------------------------
# Loading from the instrument map, which is where instrument identity already lives.
# --------------------------------------------------------------------------------------


FULL_MAP = (
    "provider_symbol,instrument_id,provider_reference,asset_type,primary_exchange_mic,"
    "currency_code,listed_at,delisted_at,symbol_effective_from\n"
    f"AAPL,{AAPL_ID},ALPACA:AAPL,STOCK,XNAS,USD,1980-12-12,,2026-01-02T00:00:00Z\n"
    f"MSFT,{MSFT_ID},ALPACA:MSFT,STOCK,XNAS,USD,1986-03-13,,2026-01-02T00:00:00Z\n"
)


def write_map(tmp_path: Any, body: str = FULL_MAP) -> Any:
    path = tmp_path / "instrument_map.csv"
    path.write_text(body, encoding="utf-8")
    return path


def test_a_full_instrument_map_registers_instruments_symbols_and_the_calendar(
    catalog: Any, tmp_path: Any
) -> None:
    mappings = load_instrument_map(write_map(tmp_path))

    report = load_reference_data(
        catalog,
        mappings,
        calendar_start=date(2024, 11, 27),
        calendar_end=date(2024, 11, 29),
        created_at=CREATED_AT,
    )

    assert report == {
        "status": "REGISTERED",
        "instrument_count": 2,
        "symbol_count": 2,
        "trading_session_count": 3,
    }
    assert sorted(row["id"] for row in catalog.records(INSTRUMENTS)) == sorted([AAPL_ID, MSFT_ID])
    assert sorted(row["symbol"] for row in catalog.records(INSTRUMENT_SYMBOLS)) == ["AAPL", "MSFT"]
    assert len(catalog.records(TRADING_SESSIONS)) == 3
    aapl_stored = next(row for row in catalog.records(INSTRUMENTS) if row["id"] == AAPL_ID)
    assert aapl_stored == aapl_row()


def test_reloading_the_same_map_changes_nothing(catalog: Any, tmp_path: Any) -> None:
    mappings = load_instrument_map(write_map(tmp_path))
    load_reference_data(catalog, mappings, created_at=CREATED_AT)

    load_reference_data(catalog, mappings, created_at=LATER)

    assert len(catalog.records(INSTRUMENTS)) == 2
    assert len(catalog.records(INSTRUMENT_SYMBOLS)) == 2
    assert {row["created_at"] for row in catalog.records(INSTRUMENTS)} == {"2026-01-02T00:00:00Z"}


def test_a_map_row_without_the_reference_columns_is_refused_by_name(
    catalog: Any, tmp_path: Any
) -> None:
    """The collection path needs two columns; the reference catalog needs five.

    A missing `asset_type` is reported rather than defaulted to ``STOCK``: the
    column is `NOT NULL` with no default in the applied DDL, and a guessed asset
    type is a wrong fact recorded as a right one.
    """

    mappings = load_instrument_map(
        write_map(tmp_path, f"provider_symbol,instrument_id\nAAPL,{AAPL_ID}\n")
    )

    with pytest.raises(InvalidInstrumentIdentity) as failure:
        load_reference_data(catalog, mappings, created_at=CREATED_AT)

    assert "AAPL" in str(failure.value)
    assert "asset_type" in str(failure.value)
    assert catalog.records(INSTRUMENTS) == []


def test_a_malformed_row_costs_no_writes_at_all(catalog: Any, tmp_path: Any) -> None:
    """The second row is broken; the first must not be half-registered."""

    body = FULL_MAP + f"BRKB,{BRKB_ID},ALPACA:BRKB,STOCK,XNYS,USD,1996-05-09,,not-a-timestamp\n"
    mappings = load_instrument_map(write_map(tmp_path, body))

    with pytest.raises(InvalidInstrumentIdentity):
        load_reference_data(catalog, mappings, created_at=CREATED_AT)

    assert catalog.records(INSTRUMENTS) == []
    assert catalog.records(INSTRUMENT_SYMBOLS) == []


def test_a_calendar_range_needs_both_ends(catalog: Any, tmp_path: Any) -> None:
    mappings = load_instrument_map(write_map(tmp_path))

    with pytest.raises(ValueError):
        load_reference_data(catalog, mappings, calendar_start=date(2024, 11, 27))


def test_the_cli_dry_run_validates_without_writing(tmp_path: Any) -> None:
    instrument_map = write_map(tmp_path)

    report = execute(
        build_parser().parse_args(
            [
                "register-reference-data",
                "--instrument-map",
                str(instrument_map),
                "--local-root",
                str(tmp_path / "objects"),
                "--calendar-start",
                "2024-11-27",
                "--calendar-end",
                "2024-11-29",
            ]
        )
    )

    assert report == {
        "status": "DRY_RUN",
        "target": "local",
        "instrument_count": 2,
        "symbol_count": 2,
        "trading_session_count": 3,
    }
    assert not (tmp_path / "objects" / "catalog-export").exists()


def test_the_cli_writes_the_local_catalog_on_execute(tmp_path: Any) -> None:
    instrument_map = write_map(tmp_path)
    arguments = [
        "register-reference-data",
        "--instrument-map",
        str(instrument_map),
        "--local-root",
        str(tmp_path / "objects"),
        "--calendar-start",
        "2024-11-27",
        "--calendar-end",
        "2024-11-29",
        "--execute",
    ]

    report = execute(build_parser().parse_args(arguments))

    assert report["status"] == "REGISTERED"
    assert report["target"] == "local"
    written = LocalCatalog(tmp_path / "objects" / "catalog-export")
    assert len(written.records(INSTRUMENTS)) == 2
    assert len(written.records(INSTRUMENT_SYMBOLS)) == 2
    assert len(written.records(TRADING_SESSIONS)) == 3


@pytest.mark.integration
def test_the_cli_writes_postgres_when_it_is_told_to(
    postgres_catalog: Any, postgres_url: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The operator path that finally populates `market_data.instruments` for real."""

    monkeypatch.setenv("DATABASE_URL", postgres_url)
    instrument_map = write_map(tmp_path)

    report = execute(
        build_parser().parse_args(
            [
                "register-reference-data",
                "--instrument-map",
                str(instrument_map),
                "--local-root",
                str(tmp_path / "objects"),
                "--target",
                "postgres",
                "--calendar-start",
                "2024-11-27",
                "--calendar-end",
                "2024-11-29",
                "--execute",
            ]
        )
    )

    assert report["status"] == "REGISTERED"
    assert report["target"] == "postgres"
    assert sorted(row["id"] for row in postgres_catalog.records(INSTRUMENTS)) == sorted(
        [AAPL_ID, MSFT_ID]
    )
    assert len(postgres_catalog.records(INSTRUMENT_SYMBOLS)) == 2
    assert len(postgres_catalog.records(TRADING_SESSIONS)) == 3


@pytest.mark.integration
def test_the_cli_postgres_target_refuses_to_write_storage_objects(
    postgres_url: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reference data never touches `storage`, so the command cannot write it.

    `StorageObjectsPolicy.READ_ONLY` narrows the connection's writable schema set,
    not just the Python API; see `db/engine.install_runtime_guards`.
    """

    from market_pipeline_lib.db.errors import StorageOwnershipUnresolved

    monkeypatch.setenv("DATABASE_URL", postgres_url)
    catalog = PostgresCatalog.connect(
        postgres_url,
        artifact_root=tmp_path / "artifacts",
        storage_objects=StorageObjectsPolicy.READ_ONLY,
    )
    try:
        with pytest.raises(StorageOwnershipUnresolved):
            catalog.upsert("storage.objects", {"id": AAPL_ID})
    finally:
        catalog.close()


# --------------------------------------------------------------------------------------
# The dependency this card unblocks: a per-instrument quality incident.
# --------------------------------------------------------------------------------------


def per_instrument_incident(instrument_id: str) -> QualityIncident:
    """A ``BAR_RANGE`` incident: the scope that *requires* an instrument."""

    return QualityIncident(
        incident_code="MISSING_BARS",
        severity="ERROR",
        scope=ImpactScope(
            breadth=ScopeBreadth.BAR_RANGE,
            period_start=datetime(2024, 11, 27, 15, 0, tzinfo=UTC),
            period_end=datetime(2024, 11, 27, 16, 0, tzinfo=UTC),
            instrument_id=instrument_id,
            shard_key="s04-of-16",
            partition_start=date(2024, 11, 27),
            partition_end=date(2024, 11, 28),
            affected_bar_count=2,
        ),
        detected_at=datetime(2024, 11, 28, tzinfo=UTC),
    )


@pytest.mark.integration
def test_a_per_instrument_incident_is_refused_while_the_instrument_is_unregistered(
    postgres_catalog: Any,
) -> None:
    """The foreign key is real, and nothing populated the table it points at.

    This is the concrete consequence D04 exists to remove: `quality_incidents`
    could only ever be written with ``instrument_id = NULL``.
    """

    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        record_quality_incidents(
            postgres_catalog,
            [per_instrument_incident(AAPL_ID)],
            dataset_manifest_id=None,
        )


@pytest.mark.integration
def test_a_per_instrument_incident_is_recorded_once_the_instrument_exists(
    postgres_catalog: Any,
) -> None:
    """End to end against PostgreSQL: register, then scope an incident to the instrument."""

    registry = InstrumentRegistry(postgres_catalog)
    registry.register(aapl(), created_at=CREATED_AT)
    registry.assign_symbol(
        SymbolAssignment(
            instrument_id=AAPL_ID,
            exchange_mic="XNAS",
            symbol="AAPL",
            effective_from=CREATED_AT,
        )
    )

    written = record_quality_incidents(
        postgres_catalog,
        [per_instrument_incident(AAPL_ID)],
        dataset_manifest_id=None,
    )

    assert written == 1
    incidents = postgres_catalog.records("market_data.quality_incidents")
    assert [row["instrument_id"] for row in incidents] == [AAPL_ID]
    assert [row["incident_code"] for row in incidents] == ["MISSING_BARS"]
    assert incidents[0]["period_start"] == "2024-11-27T15:00:00Z"
    assert incidents[0]["period_end"] == "2024-11-27T16:00:00Z"
    # The registered instrument is what the shard the incident names is derived from.
    assert stable_shard_key(str(uuid.UUID(incidents[0]["instrument_id"])), 16) == "s04-of-16"


@pytest.mark.integration
def test_the_whole_reference_load_commits_as_one_unit_of_work(postgres_catalog: Any) -> None:
    """Registration failing halfway must not leave a half-loaded reference catalog."""

    registry = InstrumentRegistry(postgres_catalog)

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with postgres_catalog.transaction():
            registry.register(aapl(), created_at=CREATED_AT)
            registry.assign_symbol(
                SymbolAssignment(
                    instrument_id=AAPL_ID,
                    exchange_mic="XNAS",
                    symbol="AAPL",
                    effective_from=CREATED_AT,
                )
            )
            raise Boom("the reference source went away mid-load")

    assert postgres_catalog.records(INSTRUMENTS) == []
    assert postgres_catalog.records(INSTRUMENT_SYMBOLS) == []
