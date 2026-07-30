from __future__ import annotations

import csv
import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from itertools import pairwise
from pathlib import Path

from market_loader.errors import InputError

REQUIRED_UNIVERSE_COLUMNS = {
    "provider_symbol",
    "asset_type",
    "primary_exchange_mic",
    "effective_from",
    "effective_to",
    "support_status",
}


@dataclass(frozen=True, slots=True)
class UniverseInstrument:
    provider_symbol: str
    asset_type: str
    primary_exchange_mic: str
    effective_from: date
    effective_to: date | None
    support_status: str
    instrument_id: str | None

    def active_during(self, start: date, end: date) -> bool:
        return self.effective_from < end and (
            self.effective_to is None or self.effective_to >= start
        )


def _parse_uuid(raw: str) -> str | None:
    if not raw:
        return None
    try:
        return str(uuid.UUID(raw))
    except ValueError as exc:
        raise InputError(f"invalid instrument_id: {raw}") from exc


def read_universe(path: Path) -> list[UniverseInstrument]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        missing = REQUIRED_UNIVERSE_COLUMNS - set(reader.fieldnames or ())
        if missing:
            raise InputError(f"universe is missing columns: {sorted(missing)}")
        result: list[UniverseInstrument] = []
        for line_number, row in enumerate(reader, start=2):
            try:
                asset_type = row["asset_type"].strip().upper()
                if asset_type not in {"STOCK", "ETF"}:
                    raise ValueError("asset_type must be STOCK or ETF")
                effective_from = date.fromisoformat(row["effective_from"].strip())
                effective_to = (
                    date.fromisoformat(row["effective_to"].strip())
                    if row["effective_to"].strip()
                    else None
                )
                if effective_to is not None and effective_to < effective_from:
                    raise ValueError("effective_to is before effective_from")
                result.append(
                    UniverseInstrument(
                        provider_symbol=row["provider_symbol"].strip().upper(),
                        asset_type=asset_type,
                        primary_exchange_mic=row["primary_exchange_mic"].strip().upper(),
                        effective_from=effective_from,
                        effective_to=effective_to,
                        support_status=row["support_status"].strip().upper(),
                        instrument_id=_parse_uuid(row.get("instrument_id", "").strip()),
                    )
                )
            except (KeyError, ValueError) as exc:
                raise InputError(f"invalid universe row {line_number}: {exc}") from exc
    _validate_symbol_periods(result)
    return result


def _validate_symbol_periods(instruments: list[UniverseInstrument]) -> None:
    groups: dict[tuple[str, str], list[UniverseInstrument]] = {}
    for item in instruments:
        if not item.provider_symbol:
            raise InputError("provider_symbol cannot be empty")
        groups.setdefault((item.primary_exchange_mic, item.provider_symbol), []).append(item)
    for key, group in groups.items():
        ordered = sorted(group, key=lambda item: item.effective_from)
        for previous, current in pairwise(ordered):
            if previous.effective_to is None or current.effective_from <= previous.effective_to:
                raise InputError(f"overlapping symbol effective periods: {key}")


def universe_hash(instruments: list[UniverseInstrument]) -> str:
    rows = [
        {
            **asdict(item),
            "effective_from": item.effective_from.isoformat(),
            "effective_to": item.effective_to.isoformat() if item.effective_to else None,
        }
        for item in instruments
    ]
    encoded = json.dumps(
        sorted(rows, key=lambda row: (row["provider_symbol"], row["effective_from"])),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
