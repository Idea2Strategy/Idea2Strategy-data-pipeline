"""Exception hierarchy for the SQLAlchemy Core catalog boundary.

Every failure this package can produce has a named type.  Callers that need to
distinguish "the schema is not what this code was built against" from "this catalog
cannot record that fact" from "the runtime tried to execute DDL" get to do so without
matching on message text.
"""

from __future__ import annotations

__all__ = [
    "CanonicalTableMissing",
    "CatalogError",
    "DuplicateAvailableManifest",
    "ManifestNotFound",
    "PipelineRunNotFound",
    "RuntimeDdlForbidden",
    "SchemaDriftError",
    "SchemaWriteForbidden",
    "StorageOwnershipUnresolved",
    "UnknownCatalogColumn",
    "UnknownCatalogTable",
    "UnsupportedCatalogCapability",
]


class CatalogError(Exception):
    """Base class for every failure raised by `market_pipeline_lib.db`."""


class UnknownCatalogTable(CatalogError, ValueError):
    """A table name outside the canonical `market_data` / `storage` contract."""


class UnknownCatalogColumn(CatalogError, ValueError):
    """A record carries a column the canonical schema does not define.

    Raised on the local catalog too.  A column that only exists in JSONL is a column the
    database will reject at the moment the pipeline is finally pointed at PostgreSQL,
    which is the worst possible time to discover it.
    """


class UnsupportedCatalogCapability(CatalogError, NotImplementedError):
    """A catalog was asked to do something it declares, via `supports()`, it cannot."""


class CanonicalTableMissing(UnsupportedCatalogCapability):
    """The canonical model has no table for the fact the caller wants to record.

    Distinct from "not implemented yet": no amount of work in this repository can fix
    it, because the table does not exist in `db/schema.dbml`.  Carries the table name
    so the message names the missing thing rather than describing it.
    """

    def __init__(self, table: str, detail: str) -> None:
        super().__init__(f"{table} does not exist in the canonical schema: {detail}")
        self.table = table


class StorageOwnershipUnresolved(CatalogError, PermissionError):
    """A write to `storage.objects` was attempted without an explicit ownership choice.

    `DatabaseAccessPolicy.java` registers `storage` as SHARED; the implementation
    checklist calls it D-owned.  Until that is settled centrally, the caller has to say
    which side it is acting on rather than a default deciding for it.
    """


class DuplicateAvailableManifest(CatalogError, RuntimeError):
    """A unit of work would commit two AVAILABLE manifests for the same period."""


class ManifestNotFound(CatalogError, KeyError):
    """A referenced `dataset_manifests` row does not exist."""


class PipelineRunNotFound(CatalogError, KeyError):
    """A referenced `pipeline_runs` row does not exist."""


class SchemaDriftError(CatalogError, RuntimeError):
    """The live database does not match the schema this code was written against."""


class RuntimeDdlForbidden(CatalogError, RuntimeError):
    """A runtime connection attempted DDL. Migrations belong to the central bundle."""


class SchemaWriteForbidden(CatalogError, PermissionError):
    """A runtime connection attempted to write a schema this repository does not own."""
