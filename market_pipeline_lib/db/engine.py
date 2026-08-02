"""Engine construction and the two runtime guards.

Both guards are `before_cursor_execute` listeners, so they see the final SQL text
whatever produced it -- Core constructs, `text()`, or a raw driver call made through the
same connection.

* **No DDL.**  Migration execution belongs to the central Flyway bundle in
  `backend/db-migration`.  An application connection must not be able to create, alter,
  drop or truncate anything, no matter which code path asks.  `MetaData.create_all()`
  against an engine built here raises rather than quietly inventing a schema that then
  diverges from the canonical one.
* **Declared schemas only.**  `db/migration-contributions/contribution.properties`
  declares ``schemas=market_data,storage``, and `PostgresCatalog` narrows that further
  to whichever `StorageObjectsPolicy` the caller chose.  The applied baseline contains
  no role ``GRANT``s, so the database itself does not enforce ownership; this is the
  part of that boundary this repository can enforce for itself.

The writable set is passed in explicitly.  A wrong default here is a database-ownership
violation, so there is no default.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import URL

from .errors import RuntimeDdlForbidden, SchemaWriteForbidden
from .tables import MARKET_DATA_SCHEMA, STORAGE_SCHEMA

__all__ = [
    "READABLE_SCHEMAS",
    "check_statement",
    "create_market_data_engine",
    "install_runtime_guards",
]


_COMMENT = re.compile(r"(?s)/\*.*?\*/|--[^\n]*")

_DDL_VERBS = re.compile(
    r"^(?:CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COMMENT|REINDEX|CLUSTER|VACUUM|REFRESH"
    r"|IMPORT|SECURITY\s+LABEL)\b",
    re.I,
)

_WRITE_TARGET = re.compile(
    r"^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+(?:ONLY\s+)?"
    r'"?(?P<schema>[a-z_][a-z0-9_]*)"?\s*\.\s*"?(?P<table>[a-z_][a-z0-9_]*)"?',
    re.I,
)

#: Schemas the pipeline reads.  `identity`, `bot`, `trading` and the rest are never
#: touched by this layer, so they are not listed.
READABLE_SCHEMAS: frozenset[str] = frozenset({MARKET_DATA_SCHEMA, STORAGE_SCHEMA})


def _strip(statement: str) -> str:
    return _COMMENT.sub(" ", statement).strip()


def check_statement(statement: str, writable_schemas: frozenset[str]) -> None:
    """Raise if `statement` is DDL, or writes a schema outside `writable_schemas`.

    Pure and side-effect free, so the policy is unit-testable without a database.
    """

    cleaned = _strip(statement)
    if not cleaned:
        return
    if _DDL_VERBS.match(cleaned):
        raise RuntimeDdlForbidden(
            "the market-data runtime must not execute DDL; migrations belong to the "
            f"central Flyway bundle. Rejected: {cleaned[:120]!r}"
        )
    target = _WRITE_TARGET.match(cleaned)
    if target is not None and target.group("schema").lower() not in writable_schemas:
        raise SchemaWriteForbidden(
            f"this repository may only write {sorted(writable_schemas)}; rejected write "
            f"to {target.group('schema')}.{target.group('table')}"
        )


def install_runtime_guards(engine: Engine, writable_schemas: Sequence[str]) -> None:
    """Refuse DDL and out-of-contract writes on every cursor this engine executes."""

    writable = frozenset(schema.lower() for schema in writable_schemas)
    if not writable:
        raise ValueError("writable_schemas must not be empty")

    @event.listens_for(engine, "before_cursor_execute")
    def _guard(  # type: ignore[no-untyped-def]
        conn, cursor, statement, parameters, context, executemany
    ) -> None:
        check_statement(statement, writable)


def create_market_data_engine(
    url: str | URL,
    *,
    writable_schemas: Sequence[str],
    application_name: str = "idea2strategy-data-pipeline",
    pool_pre_ping: bool = True,
    echo: bool = False,
    **engine_kwargs: Any,
) -> Engine:
    """Build a guarded SQLAlchemy Core engine for the market-data catalog."""

    connect_args: dict[str, Any] = dict(engine_kwargs.pop("connect_args", {}))
    connect_args.setdefault("application_name", application_name)
    engine = create_engine(
        url,
        future=True,
        pool_pre_ping=pool_pre_ping,
        echo=echo,
        connect_args=connect_args,
        **engine_kwargs,
    )
    install_runtime_guards(engine, writable_schemas)
    return engine
