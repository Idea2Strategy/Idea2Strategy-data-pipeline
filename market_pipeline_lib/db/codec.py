"""One canonical record shape, shared by the local and PostgreSQL catalogs.

`LocalCatalog` stores JSON, `PostgresCatalog` stores typed PostgreSQL columns.  For the
two to be interchangeable -- which is the whole point of deleting the
`isinstance(self.catalog, LocalCatalog)` gates in `engine.py` -- a value written through
one has to read back identical through the other.  This module is where that is decided.

The canonical shape is JSON-compatible:

===================  ================================================================
column type          canonical Python value
===================  ================================================================
``uuid``             lower-case canonical string, e.g. ``"0f9d...-...-..."``
``timestamptz``      ISO-8601 in UTC with a ``Z`` suffix, e.g. ``"2026-01-05T14:30:00Z"``
``date``             ``"YYYY-MM-DD"``
``bigint`` / ``int``  ``int`` -- never a float, so 2**53+1 survives
``boolean``          ``bool``
``jsonb``            ``dict`` / ``list``
everything else      ``str``
===================  ================================================================

Timestamps are *normalised*, not preserved verbatim.  ``engine.publish_dataset`` writes
a mixture of ``iso_utc()`` (``Z``) and ``datetime.now(UTC).isoformat()`` (``+00:00``)
into the same row, and it recomputes ``dataset_hash`` from carried-forward
``dataset_objects`` rows.  Two catalogs that rendered the same instant differently would
therefore produce two different manifest hashes for identical data.  One rendering, both
sides.

A naive datetime is rejected rather than assumed to be UTC: the pipeline works in ET and
UTC at once, so guessing here would corrupt partition boundaries silently.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, Table
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.sql.sqltypes import Integer as SAInteger

from .errors import UnknownCatalogColumn, UnknownCatalogTable
from .tables import TABLES_BY_NAME

__all__ = [
    "canonical_columns",
    "from_db_row",
    "normalise_record",
    "table_for",
    "to_canonical",
    "to_db_params",
]


def table_for(name: str) -> Table:
    """Resolve ``"<schema>.<table>"``, or raise `UnknownCatalogTable`."""

    try:
        return TABLES_BY_NAME[name]
    except KeyError as exc:
        known = ", ".join(sorted(TABLES_BY_NAME))
        raise UnknownCatalogTable(f"{name!r} is not a canonical catalog table. Known tables: {known}") from exc


def canonical_columns(name: str) -> tuple[str, ...]:
    """Canonical column order for a table, as the applied DDL declares it."""

    return tuple(column.name for column in table_for(name).columns)


def _check_columns(name: str, record: Mapping[str, Any]) -> Table:
    table = table_for(name)
    unknown = sorted(set(record) - {column.name for column in table.columns})
    if unknown:
        raise UnknownCatalogColumn(
            f"{name} has no column(s) {unknown}; the canonical columns are "
            f"{list(canonical_columns(name))}"
        )
    return table


def _canonical_uuid(value: Any, column: str) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    if not isinstance(value, str):
        raise TypeError(f"{column} must be a UUID or its string form, got {type(value).__name__}")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ValueError(f"{column}={value!r} is not a UUID") from exc


def _canonical_timestamp(value: Any, column: str) -> str:
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        try:
            moment = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{column}={value!r} is not an ISO-8601 timestamp") from exc
    else:
        raise TypeError(f"{column} must be a datetime or ISO-8601 string, got {type(value).__name__}")
    if moment.tzinfo is None:
        raise ValueError(
            f"{column}={value!r} has no timezone. This pipeline works in ET and UTC at "
            "once; a naive timestamp is ambiguous and is never assumed to be UTC."
        )
    return moment.astimezone(tz=UTC).isoformat().replace("+00:00", "Z")


def _canonical_date(value: Any, column: str) -> str:
    if isinstance(value, datetime):
        raise TypeError(f"{column} is a date column; pass a date, not a datetime ({value!r})")
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        raise TypeError(f"{column} must be a date or ISO-8601 string, got {type(value).__name__}")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"{column}={value!r} is not an ISO-8601 date") from exc


def _canonical_int(value: Any, column: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{column} is an integer column, got a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"{column}={value!r} is not an integer") from exc
    raise TypeError(f"{column} must be an int, got {type(value).__name__}")


def _canonical_bool(value: Any, column: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{column} must be a bool, got {type(value).__name__}")


def _canonical_json(value: Any, column: str) -> Any:
    if isinstance(value, (dict, list)):
        return value
    raise TypeError(f"{column} is a jsonb column and must be a dict or list, got {type(value).__name__}")


def to_canonical(table_name: str, column_name: str, value: Any) -> Any:
    """Render one value in the canonical, JSON-compatible shape."""

    if value is None:
        return None
    column = table_for(table_name).columns[column_name]
    qualified = f"{table_name}.{column_name}"
    type_ = column.type
    if isinstance(type_, UUID):
        return _canonical_uuid(value, qualified)
    if isinstance(type_, TIMESTAMP):
        return _canonical_timestamp(value, qualified)
    if isinstance(type_, Date):
        return _canonical_date(value, qualified)
    if isinstance(type_, Boolean):
        return _canonical_bool(value, qualified)
    if isinstance(type_, JSONB):
        return _canonical_json(value, qualified)
    if isinstance(type_, SAInteger):
        return _canonical_int(value, qualified)
    return str(value)


def normalise_record(table_name: str, record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the columns and render every value canonically.

    The result is ordered by the canonical column order, so the JSONL a `LocalCatalog`
    writes and the row a `PostgresCatalog` reads back compare equal as dicts *and* look
    the same to a human diffing two exports.
    """

    _check_columns(table_name, record)
    return {
        name: to_canonical(table_name, name, record[name]) for name in canonical_columns(table_name) if name in record
    }


def to_db_params(table_name: str, record: Mapping[str, Any]) -> dict[str, Any]:
    """Canonical record -> SQLAlchemy Core bind parameters.

    Runs `normalise_record` first, so an out-of-contract column or an ambiguous
    timestamp fails before any SQL is built.
    """

    canonical = normalise_record(table_name, record)
    table = table_for(table_name)
    params: dict[str, Any] = {}
    for name, value in canonical.items():
        if value is None:
            params[name] = None
            continue
        type_ = table.columns[name].type
        if isinstance(type_, UUID):
            params[name] = uuid.UUID(value)
        elif isinstance(type_, TIMESTAMP):
            params[name] = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif isinstance(type_, Date):
            params[name] = date.fromisoformat(value)
        else:
            params[name] = value
    return params


def from_db_row(table_name: str, row: Mapping[str, Any]) -> dict[str, Any]:
    """A `Row._mapping` -> the canonical record shape."""

    return {name: to_canonical(table_name, name, row[name]) for name in canonical_columns(table_name)}
