"""The three canonical tables D04 writes, and the narrow catalog port it needs.

The `Table` objects live in `market_pipeline_lib.db.tables`; this module only
names them and states the *minimum* catalog surface reference-data registration
depends on.  Depending on a three-method protocol rather than on
`MarketDataCatalog` is what keeps `LocalCatalog`, `PostgresCatalog` and a test
double interchangeable here -- the same reason `features/tables.py` does it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from ..db.tables import TABLES_BY_NAME

__all__ = [
    "INSTRUMENTS",
    "INSTRUMENT_SYMBOLS",
    "TRADING_SESSIONS",
    "ReferenceCatalog",
]


INSTRUMENTS = "market_data.instruments"
INSTRUMENT_SYMBOLS = "market_data.instrument_symbols"
TRADING_SESSIONS = "market_data.trading_sessions"

# Fail at import time rather than at the first write if a table this package
# addresses is not in the canonical metadata.
for _name in (INSTRUMENTS, INSTRUMENT_SYMBOLS, TRADING_SESSIONS):
    if _name not in TABLES_BY_NAME:  # pragma: no cover - a schema regression, not a branch
        raise ImportError(f"{_name} is not declared in market_pipeline_lib.db.tables")
del _name


@runtime_checkable
class ReferenceCatalog(Protocol):
    """The catalog surface D04 uses.  `LocalCatalog` and `PostgresCatalog` satisfy it.

    `records` takes the equality-only `where` predicate both catalogs implement, so
    a lookup is pushed into SQL on PostgreSQL instead of scanning a production-sized
    `instrument_symbols` into Python.
    """

    def records(self, table: str, *, where: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        """Rows of `table` matching `where`, in the canonical record shape."""

    def upsert(self, table: str, record: Mapping[str, Any]) -> None:
        """Insert or replace one row of an `id`-keyed table."""

    def transaction(self) -> Any:
        """A context manager committing every write in the block together."""
