"""`market_data.instruments` and `market_data.instrument_symbols`.

Before D04 neither table had a write path.  `instruments` was read once --
`operations.py:659` asks which ids already exist and refuses the apply when any is
missing -- and nothing ever inserted one, so the check could only ever fail or be
satisfied by hand.  `instrument_symbols` was read-only.  The consequence that
mattered most is that `quality_incidents.instrument_id` is a foreign key to
`instruments`, so a per-instrument incident could not be written at all.

Two rules shape this module.

**Identity is not editable.**  `instrument_id` is the shard key
(`contracts.stable_shard_key`), the scope of a quality incident, and the target of
fifteen foreign keys across `market_data`, `bot` and `trading`.  `register` merges
a repeat registration and refuses one that changes the asset type, the exchange or
the currency; `created_at` is never restamped, because it is the only record of
when the instrument entered the catalog and `PostgresCatalog.upsert` would
otherwise overwrite it on every run.

**A ticker is a period, not a property.**  A rename closes the current
`instrument_symbols` period and opens a new one against the *same*
`instrument_id`, which is why a rename cannot move an instrument between shards.
Both directions of the historical lookup are provided, because a backfill of 2024
data keyed by a since-retired ticker has to land on today's instrument.

Column widths are enforced before either catalog is reached.  Verified against the
applied DDL in a container: `primary_exchange_mic char(4)` and
`currency_code char(3)` are blank-padded, so PostgreSQL reads back ``'XNY '`` for a
value `LocalCatalog` returns as ``'XNY'``, and an over-long value raises
`StringDataRightTruncation` on one implementation and is silently stored by the
other.  Neither difference is expressible in the SQLAlchemy metadata.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from ..contracts import deterministic_uuid, iso_utc
from ..db.tables import ASSET_TYPE_LABELS
from .errors import (
    InstrumentIdentityConflict,
    InvalidInstrumentIdentity,
    SymbolAlreadyAssigned,
    SymbolNotAssigned,
    UnknownInstrument,
)
from .tables import INSTRUMENT_SYMBOLS, INSTRUMENTS, ReferenceCatalog

__all__ = [
    "CURRENCY_CODE_LENGTH",
    "EXCHANGE_MIC_LENGTH",
    "INSTRUMENT_IDENTITY_COLUMNS",
    "PROVIDER_REFERENCE_MAX_LENGTH",
    "SYMBOL_MAX_LENGTH",
    "InstrumentRegistration",
    "InstrumentRegistry",
    "SymbolAssignment",
]


#: `char(4)`, blank-padded, so the value must be exactly this wide.
EXCHANGE_MIC_LENGTH = 4
#: `char(3)`, blank-padded, same reason.
CURRENCY_CODE_LENGTH = 3
#: `instrument_symbols.symbol varchar(32)`.
SYMBOL_MAX_LENGTH = 32
#: `instruments.provider_reference varchar(160)`.
PROVIDER_REFERENCE_MAX_LENGTH = 160

#: What an instrument *is*.  A repeat registration may not change any of these.
INSTRUMENT_IDENTITY_COLUMNS: tuple[str, ...] = (
    "asset_type",
    "primary_exchange_mic",
    "currency_code",
)

_UUID_PURPOSE_SYMBOL = "instrument-symbol"

_MIC_PATTERN = re.compile(r"^[A-Z0-9]{4}$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


def _canonical_uuid(value: Any, label: str) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    if not isinstance(value, str):
        raise InvalidInstrumentIdentity(f"{label} must be a UUID, got {type(value).__name__}")
    try:
        return str(uuid.UUID(value.strip()))
    except ValueError as exc:
        raise InvalidInstrumentIdentity(f"{label}={value!r} is not a UUID") from exc


def _canonical_mic(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise InvalidInstrumentIdentity(f"{label} must be a string, got {type(value).__name__}")
    text = value.strip().upper()
    if not _MIC_PATTERN.match(text):
        raise InvalidInstrumentIdentity(
            f"{label}={value!r} is not a {EXCHANGE_MIC_LENGTH}-character ISO 10383 MIC. "
            "The column is char(4) and PostgreSQL blank-pads a shorter value, so a "
            "narrower MIC would read back differently from the local catalog."
        )
    return text


def _canonical_currency(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise InvalidInstrumentIdentity(f"{label} must be a string, got {type(value).__name__}")
    text = value.strip().upper()
    if not _CURRENCY_PATTERN.match(text):
        raise InvalidInstrumentIdentity(
            f"{label}={value!r} is not a {CURRENCY_CODE_LENGTH}-character ISO 4217 code. "
            "The column is char(3) and blank-pads a shorter value."
        )
    return text


def _canonical_symbol(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise InvalidInstrumentIdentity(f"{label} must be a string, got {type(value).__name__}")
    text = value.strip().upper()
    if not text:
        raise InvalidInstrumentIdentity(f"{label} must not be blank")
    if len(text) > SYMBOL_MAX_LENGTH:
        raise InvalidInstrumentIdentity(
            f"{label}={value!r} exceeds the {SYMBOL_MAX_LENGTH}-character column"
        )
    return text


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise InvalidInstrumentIdentity(f"{label} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        raise InvalidInstrumentIdentity(
            f"{label} has no timezone. This pipeline works in ET and UTC at once; a "
            "naive instant is ambiguous and is never assumed to be UTC."
        )
    return value.astimezone(UTC)


def _parse(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _optional_date(value: Any, label: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise InvalidInstrumentIdentity(f"{label} is a date column; pass a date, not a datetime")
    if isinstance(value, date):
        return value
    raise InvalidInstrumentIdentity(f"{label} must be a date, got {type(value).__name__}")


def _periods_overlap(
    left_from: datetime,
    left_to: datetime | None,
    right_from: datetime,
    right_to: datetime | None,
) -> bool:
    """Half-open ``[from, to)`` overlap, with `None` meaning "still in force"."""

    if left_to is not None and left_to <= right_from:
        return False
    if right_to is not None and right_to <= left_from:
        return False
    return True


@dataclass(frozen=True)
class InstrumentRegistration:
    """One `market_data.instruments` row, validated against the applied DDL.

    `currency_code` restates the column's own ``DEFAULT 'USD'``; `asset_type` and
    `primary_exchange_mic` are `NOT NULL` with no default and are therefore
    required here rather than guessed.
    """

    instrument_id: str
    asset_type: str
    primary_exchange_mic: str
    currency_code: str = "USD"
    provider_reference: str | None = None
    listed_at: date | None = None
    delisted_at: date | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_id", _canonical_uuid(self.instrument_id, "instrument_id"))
        if self.asset_type not in ASSET_TYPE_LABELS:
            raise InvalidInstrumentIdentity(
                f"asset_type={self.asset_type!r} is not a market_data.asset_type label; "
                f"the enum is {list(ASSET_TYPE_LABELS)}"
            )
        object.__setattr__(
            self, "primary_exchange_mic", _canonical_mic(self.primary_exchange_mic, "primary_exchange_mic")
        )
        object.__setattr__(self, "currency_code", _canonical_currency(self.currency_code, "currency_code"))
        if self.provider_reference is not None:
            if not isinstance(self.provider_reference, str) or not self.provider_reference.strip():
                raise InvalidInstrumentIdentity("provider_reference must be a non-empty string or None")
            if len(self.provider_reference) > PROVIDER_REFERENCE_MAX_LENGTH:
                raise InvalidInstrumentIdentity(
                    f"provider_reference exceeds the {PROVIDER_REFERENCE_MAX_LENGTH}-character column"
                )
        listed_at = _optional_date(self.listed_at, "listed_at")
        delisted_at = _optional_date(self.delisted_at, "delisted_at")
        if listed_at is not None and delisted_at is not None and delisted_at < listed_at:
            raise InvalidInstrumentIdentity(
                f"delisted_at={delisted_at.isoformat()} precedes listed_at={listed_at.isoformat()}"
            )

    def to_record(self, *, created_at: datetime) -> dict[str, Any]:
        """The canonical `market_data.instruments` row, every column present."""

        return {
            "id": self.instrument_id,
            "asset_type": self.asset_type,
            "primary_exchange_mic": self.primary_exchange_mic,
            "currency_code": self.currency_code,
            "provider_reference": self.provider_reference,
            "listed_at": self.listed_at.isoformat() if self.listed_at else None,
            "delisted_at": self.delisted_at.isoformat() if self.delisted_at else None,
            "created_at": iso_utc(_aware(created_at, "created_at")),
        }


@dataclass(frozen=True)
class SymbolAssignment:
    """One `market_data.instrument_symbols` row: a ticker over a half-open period.

    ``effective_to = None`` means "still in force".  The row id is derived from the
    period's own identity, so re-recording the same assignment converges on one row
    instead of appending a duplicate.
    """

    instrument_id: str
    exchange_mic: str
    symbol: str
    effective_from: datetime
    effective_to: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_id", _canonical_uuid(self.instrument_id, "instrument_id"))
        object.__setattr__(self, "exchange_mic", _canonical_mic(self.exchange_mic, "exchange_mic"))
        object.__setattr__(self, "symbol", _canonical_symbol(self.symbol, "symbol"))
        object.__setattr__(self, "effective_from", _aware(self.effective_from, "effective_from"))
        if self.effective_to is not None:
            object.__setattr__(self, "effective_to", _aware(self.effective_to, "effective_to"))
            if self.effective_to <= self.effective_from:
                raise InvalidInstrumentIdentity(
                    "effective_to must follow effective_from: "
                    f"{self.effective_from.isoformat()} -> {self.effective_to.isoformat()}"
                )

    @property
    def row_id(self) -> str:
        return str(
            deterministic_uuid(
                _UUID_PURPOSE_SYMBOL,
                self.instrument_id,
                self.exchange_mic,
                self.symbol,
                iso_utc(self.effective_from),
            )
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "id": self.row_id,
            "instrument_id": self.instrument_id,
            "exchange_mic": self.exchange_mic,
            "symbol": self.symbol,
            "effective_from": iso_utc(self.effective_from),
            "effective_to": iso_utc(self.effective_to) if self.effective_to else None,
        }


class InstrumentRegistry:
    """Reads and writes `instruments` and `instrument_symbols` through one catalog.

    No method opens a transaction.  A caller that needs several writes to land
    together wraps them in ``catalog.transaction()``; nesting is rejected by both
    catalogs, so opening one here would make the registry unusable inside a wider
    unit of work such as a reference-data load.
    """

    def __init__(self, catalog: ReferenceCatalog) -> None:
        self._catalog = catalog

    # -- instruments -------------------------------------------------------------------

    def register(
        self,
        registration: InstrumentRegistration,
        *,
        created_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Register `registration`, or merge it into the row already stored.

        Returns the stored row.  Raises `InstrumentIdentityConflict` when the
        instrument exists and any of `INSTRUMENT_IDENTITY_COLUMNS` differs;
        `provider_reference`, `listed_at` and `delisted_at` are facts that change
        over an instrument's life and are updated in place.
        """

        record = registration.to_record(created_at=created_at or datetime.now(UTC))
        existing = self.instrument(registration.instrument_id)
        if existing is not None:
            differences = [
                column for column in INSTRUMENT_IDENTITY_COLUMNS if existing[column] != record[column]
            ]
            if differences:
                detail = ", ".join(
                    f"{column}: {existing[column]!r} -> {record[column]!r}" for column in differences
                )
                raise InstrumentIdentityConflict(
                    f"instrument {registration.instrument_id} is already registered and this "
                    f"registration would change {detail}. The id is the shard key and the "
                    "quality-incident scope; a different identity is a different instrument."
                )
            # Never restamped: `created_at` records when the instrument entered the
            # catalog, and `PostgresCatalog.upsert` overwrites every non-key column.
            record["created_at"] = existing["created_at"]
            if record == existing:
                return existing
        self._catalog.upsert(INSTRUMENTS, record)
        return record

    def instrument(self, instrument_id: str) -> dict[str, Any] | None:
        """The stored `instruments` row, or `None`."""

        rows = self._catalog.records(
            INSTRUMENTS, where={"id": _canonical_uuid(instrument_id, "instrument_id")}
        )
        return rows[0] if rows else None

    def is_registered(self, instrument_id: str) -> bool:
        return self.instrument(instrument_id) is not None

    # -- symbol history ----------------------------------------------------------------

    def assign_symbol(self, assignment: SymbolAssignment) -> dict[str, Any]:
        """Record one ticker period, refusing an overlap with a different assignment.

        Two overlaps are refused, and neither is caught by the database.  The
        applied DDL's only relevant index is
        ``(exchange_mic, symbol, effective_from)``, which two overlapping periods
        with different start instants slip straight past; the DBML asks the
        migration for a real exclusion constraint and the migration does not have
        one.  So the checks live here, and both catalogs refuse the same writes:

        * one ticker may not be held by two instruments at the same instant --
          otherwise `instrument_for_symbol` has two answers;
        * one instrument may not carry two tickers on one venue at the same
          instant -- otherwise `symbol_at` has two answers.
        """

        if not self.is_registered(assignment.instrument_id):
            raise UnknownInstrument(
                f"instrument {assignment.instrument_id} is not registered, so "
                f"{assignment.symbol} cannot be assigned to it. "
                "instrument_symbols.instrument_id is a foreign key to market_data.instruments."
            )
        record = assignment.to_record()
        candidates = {
            row["id"]: row
            for row in (
                *self._symbol_rows(exchange_mic=assignment.exchange_mic, symbol=assignment.symbol),
                *self._catalog.records(
                    INSTRUMENT_SYMBOLS,
                    where={
                        "instrument_id": assignment.instrument_id,
                        "exchange_mic": assignment.exchange_mic,
                    },
                ),
            )
        }
        for row_id, row in candidates.items():
            if row_id == record["id"]:
                continue
            if _periods_overlap(
                _parse(row["effective_from"]),
                _parse(row["effective_to"]) if row["effective_to"] else None,
                assignment.effective_from,
                assignment.effective_to,
            ):
                raise SymbolAlreadyAssigned(
                    f"{assignment.exchange_mic}:{row['symbol']} is already assigned to instrument "
                    f"{row['instrument_id']} from {row['effective_from']} to "
                    f"{row['effective_to'] or 'now'}, which overlaps the requested "
                    f"{assignment.exchange_mic}:{assignment.symbol} period from "
                    f"{record['effective_from']} to {record['effective_to'] or 'now'}."
                )
        self._catalog.upsert(INSTRUMENT_SYMBOLS, record)
        return record

    def rename(
        self,
        *,
        instrument_id: str,
        exchange_mic: str,
        symbol: str,
        effective_from: datetime,
    ) -> dict[str, Any]:
        """Close the ticker currently in force and open the new one at the same instant.

        The instrument does not move: both rows carry the same `instrument_id`, so
        `contracts.stable_shard_key` returns the same shard before and after.
        """

        mic = _canonical_mic(exchange_mic, "exchange_mic")
        new_symbol = _canonical_symbol(symbol, "symbol")
        moment = _aware(effective_from, "effective_from")
        current = self.open_symbol_row(instrument_id, mic)
        if current is None:
            raise SymbolNotAssigned(
                f"instrument {instrument_id} has no symbol in force on {mic}, so there is "
                "nothing to rename. Use assign_symbol to open the first period."
            )
        if _parse(current["effective_from"]) >= moment:
            raise SymbolAlreadyAssigned(
                f"{mic}:{current['symbol']} took effect at {current['effective_from']}, which is "
                f"not before the requested rename instant {iso_utc(moment)}."
            )
        if current["symbol"] == new_symbol:
            raise SymbolAlreadyAssigned(
                f"instrument {current['instrument_id']} already carries {mic}:{new_symbol}; "
                "a rename has to change the symbol."
            )
        self._catalog.upsert(INSTRUMENT_SYMBOLS, {**current, "effective_to": iso_utc(moment)})
        return self.assign_symbol(
            SymbolAssignment(
                instrument_id=current["instrument_id"],
                exchange_mic=mic,
                symbol=new_symbol,
                effective_from=moment,
            )
        )

    def open_symbol_row(self, instrument_id: str, exchange_mic: str) -> dict[str, Any] | None:
        """The still-in-force `instrument_symbols` row for one instrument and venue."""

        rows = [
            row
            for row in self._catalog.records(
                INSTRUMENT_SYMBOLS,
                where={
                    "instrument_id": _canonical_uuid(instrument_id, "instrument_id"),
                    "exchange_mic": _canonical_mic(exchange_mic, "exchange_mic"),
                },
            )
            if row["effective_to"] is None
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: _parse(row["effective_from"]))

    def symbol_at(self, instrument_id: str, moment: datetime) -> str | None:
        """Which ticker this instrument traded under at `moment`, or `None`."""

        row = self._row_in_force(
            self._catalog.records(
                INSTRUMENT_SYMBOLS,
                where={"instrument_id": _canonical_uuid(instrument_id, "instrument_id")},
            ),
            _aware(moment, "moment"),
        )
        return None if row is None else str(row["symbol"])

    def instrument_for_symbol(
        self, exchange_mic: str, symbol: str, moment: datetime
    ) -> str | None:
        """Which instrument held `symbol` on `exchange_mic` at `moment`, or `None`."""

        row = self._row_in_force(
            self._symbol_rows(
                exchange_mic=_canonical_mic(exchange_mic, "exchange_mic"),
                symbol=_canonical_symbol(symbol, "symbol"),
            ),
            _aware(moment, "moment"),
        )
        return None if row is None else str(row["instrument_id"])

    def symbol_history(self, instrument_id: str) -> tuple[dict[str, Any], ...]:
        """Every recorded ticker period for one instrument, oldest first."""

        rows = self._catalog.records(
            INSTRUMENT_SYMBOLS,
            where={"instrument_id": _canonical_uuid(instrument_id, "instrument_id")},
        )
        return tuple(sorted(rows, key=lambda row: _parse(row["effective_from"])))

    # -- internals ---------------------------------------------------------------------

    def _symbol_rows(self, *, exchange_mic: str, symbol: str) -> list[dict[str, Any]]:
        return self._catalog.records(
            INSTRUMENT_SYMBOLS, where={"exchange_mic": exchange_mic, "symbol": symbol}
        )

    @staticmethod
    def _row_in_force(
        rows: Iterable[Mapping[str, Any]], moment: datetime
    ) -> Mapping[str, Any] | None:
        for row in rows:
            starts = _parse(row["effective_from"])
            ends = _parse(row["effective_to"]) if row["effective_to"] else None
            if starts <= moment and (ends is None or moment < ends):
                return row
        return None
