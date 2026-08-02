"""Startup verification that the live schema matches this code.

The runtime never applies DDL, so the only safe alternative to creating what is missing
is to check and refuse.  `describe_schema_drift` reports *every* problem it finds rather
than dying on the first, because a partially-applied migration bundle usually breaks
several tables at once and a one-line error sends the reader on a false trail.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from sqlalchemy import Connection, MetaData, Table, UniqueConstraint, inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.engine import Inspector
from sqlalchemy.types import TypeEngine

from .errors import SchemaDriftError
from .tables import METADATA

__all__ = ["describe_schema_drift", "verify_schema"]


_TYPE_ALIASES = {
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "character varying": "varchar",
    "character": "char",
    "integer": "int",
    "bigserial": "bigint",
}


def _normalise_type(rendered: str) -> str:
    value = rendered.strip().lower()
    value = re.sub(r"\s*\(\s*", "(", value)
    value = re.sub(r"\s*\)\s*", ")", value)
    value = re.sub(r"\s*,\s*", ",", value)
    base, _, suffix = value.partition("(")
    base = _TYPE_ALIASES.get(base.strip(), base.strip())
    return f"{base}({suffix}" if suffix else base


def _render(type_: TypeEngine[object]) -> str:
    # `str(type)` drops `WITH TIME ZONE`; compile through the dialect instead.
    rendered: str = type_.compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
    return _normalise_type(rendered)


def describe_schema_drift(connection: Connection, metadata: MetaData = METADATA) -> list[str]:
    """Return human-readable drift descriptions; an empty list means healthy."""

    problems: list[str] = []
    inspector = inspect(connection)
    known_enums = _reflect_enum_labels(connection)

    for table in metadata.sorted_tables:
        qualified = f"{table.schema}.{table.name}"
        if not inspector.has_table(table.name, schema=table.schema):
            problems.append(f"{qualified}: table is missing")
            continue

        actual = {column["name"]: column for column in inspector.get_columns(table.name, schema=table.schema)}
        for column in table.columns:
            found = actual.get(column.name)
            if found is None:
                problems.append(f"{qualified}.{column.name}: column is missing")
                continue
            if isinstance(column.type, ENUM):
                problems.extend(_enum_problems(qualified, column.name, column.type, known_enums))
            else:
                expected_type = _render(column.type)
                actual_type = _render(found["type"])
                if expected_type != actual_type:
                    problems.append(
                        f"{qualified}.{column.name}: type is {actual_type}, expected {expected_type}"
                    )
            if bool(found["nullable"]) != bool(column.nullable):
                problems.append(
                    f"{qualified}.{column.name}: nullable is {found['nullable']}, expected {column.nullable}"
                )

        problems.extend(_unique_problems(inspector, table, qualified))

    return problems


def verify_schema(connection: Connection, metadata: MetaData = METADATA) -> None:
    """Raise `SchemaDriftError` unless the live schema matches `metadata`."""

    problems = describe_schema_drift(connection, metadata)
    if problems:
        raise SchemaDriftError(
            "the live database does not match the canonical market_data schema:\n  - " + "\n  - ".join(problems)
        )


def _reflect_enum_labels(connection: Connection) -> dict[tuple[str, str], tuple[str, ...]]:
    rows = connection.execute(
        text(
            """
            SELECT n.nspname AS schema_name, t.typname AS type_name, e.enumlabel AS label
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_enum e ON e.enumtypid = t.oid
            ORDER BY n.nspname, t.typname, e.enumsortorder
            """
        )
    ).all()
    labels: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        labels.setdefault((row.schema_name, row.type_name), []).append(row.label)
    return {key: tuple(value) for key, value in labels.items()}


def _enum_problems(
    qualified: str,
    column_name: str,
    enum_type: ENUM,
    known_enums: dict[tuple[str, str], tuple[str, ...]],
) -> Iterable[str]:
    key = (enum_type.schema or "public", enum_type.name or "")
    actual = known_enums.get(key)
    if actual is None:
        yield f"{qualified}.{column_name}: enum type {key[0]}.{key[1]} does not exist"
        return
    expected = tuple(enum_type.enums)
    if actual != expected:
        yield f"{qualified}.{column_name}: enum {key[0]}.{key[1]} has labels {list(actual)}, expected {list(expected)}"


def _unique_problems(inspector: Inspector, table: Table, qualified: str) -> Iterable[str]:
    actual: set[frozenset[str]] = set()
    for index in inspector.get_indexes(table.name, schema=table.schema):
        if index.get("unique"):
            actual.add(frozenset(name for name in index["column_names"] if name))
    for unique in inspector.get_unique_constraints(table.name, schema=table.schema):
        actual.add(frozenset(unique["column_names"]))
    primary_key = inspector.get_pk_constraint(table.name, schema=table.schema).get("constrained_columns")
    if primary_key:
        actual.add(frozenset(primary_key))

    expected: set[frozenset[str]] = set()
    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint):
            expected.add(frozenset(column.name for column in constraint.columns))
    for table_index in table.indexes:
        if table_index.unique:
            expected.add(frozenset(column.name for column in table_index.columns))
    expected.add(frozenset(column.name for column in table.primary_key.columns))

    for missing in sorted(expected - actual, key=sorted):
        yield f"{qualified}: unique constraint on {sorted(missing)} is missing"
