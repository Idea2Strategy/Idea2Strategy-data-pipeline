"""SQLAlchemy Core access to the canonical `market_data` schema.

Spec section 0.3 and `docs/backend-implementation-master-checklist.md` line 75 allow
Python exactly one database technique: SQLAlchemy Core, no ORM session, no Alembic.
This package is that boundary.  psycopg appears only as SQLAlchemy's driver.

Layout mirrors the sibling `backtest-engine` persistence package so the two D-bundle
repositories read the same way:

``tables``        canonical `Table` metadata, never used to emit DDL
``codec``         the one canonical record shape shared by both catalog implementations
``schema_guard``  startup drift detection; fails loudly, repairs nothing
``engine``        guarded engine construction (no DDL, declared schemas only)
``errors``        the typed failure hierarchy
"""

from __future__ import annotations

from .codec import canonical_columns, from_db_row, normalise_record, table_for, to_db_params
from .engine import check_statement, create_market_data_engine, install_runtime_guards
from .errors import (
    CanonicalTableMissing,
    CatalogError,
    DuplicateAvailableManifest,
    ManifestNotFound,
    PipelineRunNotFound,
    RuntimeDdlForbidden,
    SchemaDriftError,
    SchemaWriteForbidden,
    StorageObjectConflict,
    StorageOwnershipUnresolved,
    UnknownCatalogColumn,
    UnknownCatalogTable,
    UnsupportedCatalogCapability,
)
from .schema_guard import describe_schema_drift, verify_schema
from .tables import (
    MARKET_DATA_SCHEMA,
    METADATA,
    READ_ONLY_TABLES,
    SCHEMA_CONTRADICTIONS,
    STORAGE_SCHEMA,
    TABLES_BY_NAME,
    WRITABLE_TABLES,
)

__all__ = [
    "MARKET_DATA_SCHEMA",
    "METADATA",
    "READ_ONLY_TABLES",
    "SCHEMA_CONTRADICTIONS",
    "STORAGE_SCHEMA",
    "TABLES_BY_NAME",
    "WRITABLE_TABLES",
    "CanonicalTableMissing",
    "CatalogError",
    "DuplicateAvailableManifest",
    "ManifestNotFound",
    "PipelineRunNotFound",
    "RuntimeDdlForbidden",
    "SchemaDriftError",
    "SchemaWriteForbidden",
    "StorageOwnershipUnresolved",
    "StorageObjectConflict",
    "UnknownCatalogColumn",
    "UnknownCatalogTable",
    "UnsupportedCatalogCapability",
    "canonical_columns",
    "check_statement",
    "create_market_data_engine",
    "describe_schema_drift",
    "from_db_row",
    "install_runtime_guards",
    "normalise_record",
    "table_for",
    "to_db_params",
    "verify_schema",
]
