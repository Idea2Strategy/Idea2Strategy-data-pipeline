"""D13 -- feature definition, materialization and point-in-time snapshot batches.

Three canonical tables, three ideas:

`market_data.feature_definitions`
    *What* a feature is.  Content-addressed and immutable: a changed parameter is a new
    `definition_hash`, therefore a new row, therefore a new version.  See `definitions`.

`market_data.feature_materializations`
    *What it produced*, over which inputs, for which instrument and period, with
    lineage back to the source objects.  Reproducible: the same definition over the
    same inputs yields the same `result_hash`.  See `materialization`.

`market_data.feature_snapshot_batches`
    *Which set of materializations a consumer may treat as one moment in time.*  A
    batch missing any planned member is not consumable, and there is no version string
    for it.  See `snapshots`.

The string a consumer pins is `feature_materialization_version`, defined in `hashing`.
It is one of the two fields the COM06 backtest-request contract requires and the
producer's own fixture did not carry, which is why the consumer's validator rejected
the producer's output; this package is where that field acquires a meaning.

Persistence is SQLAlchemy Core through the catalog boundary, via the four-method
`FeatureCatalog` protocol in `tables`.  Nothing here authors DDL: all three tables
already exist in the central schema.
"""

from __future__ import annotations

from .calculators import (
    MACD_OUTPUT_LINES,
    PRECISION_RULES_VERSION,
    PRICE_FIELDS,
    QUANTUM,
    BarPoint,
    FeatureCalculator,
    FeatureValue,
    get_calculator,
    known_calculators,
    quantize,
)
from .catalog import (
    INPUT_ADJUSTMENTS,
    OFFICIAL_FEATURE_CATALOG_HASH,
    OFFICIAL_FEATURE_CATALOG_VERSION,
    OfficialFeature,
    OfficialFeatureCatalog,
)
from .definitions import (
    FEATURE_DEFINITION_SCHEMA_VERSION,
    FeatureDefinition,
    FeatureDefinitionRegistry,
)
from .errors import (
    DefinitionIntegrityError,
    FeatureCatalogIntegrityError,
    FeatureDefinitionImmutable,
    FeatureDefinitionNotPublished,
    FeatureError,
    InsufficientHistory,
    InvalidBarSeries,
    InvalidFeatureParameters,
    MaterializationConflict,
    PartialSnapshotBatch,
    SnapshotBatchNotConsumable,
    UnknownCalculator,
    UnknownOfficialFeature,
)
from .hashing import (
    FEATURE_VERSION_PREFIX,
    ParsedFeatureVersion,
    batch_version,
    materialization_version,
    parse_feature_materialization_version,
)
from .materialization import (
    FeatureMaterializer,
    MaterializationRequest,
    MaterializationResult,
    SourceObject,
    input_bundle_fingerprint,
)
from .snapshots import (
    FeatureSnapshotBatchBuilder,
    MarketInput,
    SealedSnapshotBatch,
    SnapshotBatchPlan,
)
from .tables import (
    FEATURE_DEFINITIONS,
    FEATURE_MATERIALIZATIONS,
    FEATURE_SNAPSHOT_BATCHES,
    FeatureCatalog,
)
from .validation import FeatureSnapshotValidator

__all__ = [
    "FEATURE_DEFINITIONS",
    "FEATURE_DEFINITION_SCHEMA_VERSION",
    "FEATURE_MATERIALIZATIONS",
    "FEATURE_SNAPSHOT_BATCHES",
    "FEATURE_VERSION_PREFIX",
    "INPUT_ADJUSTMENTS",
    "MACD_OUTPUT_LINES",
    "OFFICIAL_FEATURE_CATALOG_HASH",
    "OFFICIAL_FEATURE_CATALOG_VERSION",
    "PRECISION_RULES_VERSION",
    "PRICE_FIELDS",
    "QUANTUM",
    "BarPoint",
    "DefinitionIntegrityError",
    "FeatureCalculator",
    "FeatureCatalog",
    "FeatureCatalogIntegrityError",
    "FeatureDefinition",
    "FeatureDefinitionImmutable",
    "FeatureDefinitionNotPublished",
    "FeatureDefinitionRegistry",
    "FeatureError",
    "FeatureMaterializer",
    "FeatureSnapshotBatchBuilder",
    "FeatureSnapshotValidator",
    "FeatureValue",
    "InsufficientHistory",
    "InvalidBarSeries",
    "InvalidFeatureParameters",
    "MarketInput",
    "MaterializationConflict",
    "MaterializationRequest",
    "MaterializationResult",
    "OfficialFeature",
    "OfficialFeatureCatalog",
    "ParsedFeatureVersion",
    "PartialSnapshotBatch",
    "SealedSnapshotBatch",
    "SnapshotBatchNotConsumable",
    "SnapshotBatchPlan",
    "SourceObject",
    "UnknownCalculator",
    "UnknownOfficialFeature",
    "batch_version",
    "get_calculator",
    "input_bundle_fingerprint",
    "known_calculators",
    "materialization_version",
    "parse_feature_materialization_version",
    "quantize",
]
