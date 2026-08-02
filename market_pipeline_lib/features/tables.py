"""The canonical tables D13 writes, and the narrow catalog port it needs.

The table objects themselves live in `market_pipeline_lib.db.tables` (owned by the
catalog work); this module only names them and states the *minimum* catalog surface a
feature writer depends on.  Depending on a four-method protocol rather than on
`MarketDataCatalog` keeps `LocalCatalog`, `PostgresCatalog` and a test double
interchangeable here, and keeps this package out of the way of the catalog rewrite.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from ..db.tables import TABLES_BY_NAME

__all__ = [
    "DATASET_LINEAGE",
    "FEATURE_DEFINITIONS",
    "FEATURE_MATERIALIZATIONS",
    "FEATURE_SNAPSHOT_BATCHES",
    "FeatureCatalog",
]


FEATURE_DEFINITIONS = "market_data.feature_definitions"
FEATURE_MATERIALIZATIONS = "market_data.feature_materializations"
FEATURE_SNAPSHOT_BATCHES = "market_data.feature_snapshot_batches"
DATASET_LINEAGE = "market_data.dataset_lineage"

# Fail at import time rather than at the first write if a table this package addresses
# is not in the canonical metadata.
for _name in (FEATURE_DEFINITIONS, FEATURE_MATERIALIZATIONS, FEATURE_SNAPSHOT_BATCHES, DATASET_LINEAGE):
    if _name not in TABLES_BY_NAME:  # pragma: no cover - a schema regression, not a branch
        raise ImportError(f"{_name} is not declared in market_pipeline_lib.db.tables")
del _name


@runtime_checkable
class FeatureCatalog(Protocol):
    """The catalog surface D13 uses.  `LocalCatalog` and `PostgresCatalog` satisfy it.

    Deliberately four methods.  A wider dependency would couple feature materialization
    to parts of the catalog that are still being rewritten, and a narrower one could not
    express "read what is already there before deciding to write", which is how
    definition immutability and batch completeness are enforced.
    """

    def records(self, table: str) -> list[dict[str, Any]]:
        """Every row of `table`, in the canonical JSON-compatible record shape."""

    def upsert(self, table: str, record: Mapping[str, Any]) -> None:
        """Insert or replace one row of an `id`-keyed table."""

    def record_dataset_lineage(self, record: Mapping[str, Any]) -> None:
        """Record one `dataset_lineage` edge, at most once per triple."""

    def transaction(self) -> Any:
        """A context manager committing every write in the block together."""
