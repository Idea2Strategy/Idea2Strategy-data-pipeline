"""Typed failures for D13 feature definition and materialization.

Every one is named, so a caller distinguishes "this definition was never published"
from "this definition was published and someone tried to edit it" from "this batch is
incomplete" without matching on message text.
"""

from __future__ import annotations

__all__ = [
    "DefinitionIntegrityError",
    "FeatureDefinitionImmutable",
    "FeatureDefinitionNotPublished",
    "FeatureError",
    "InsufficientHistory",
    "InvalidBarSeries",
    "InvalidFeatureParameters",
    "MaterializationConflict",
    "PartialSnapshotBatch",
    "SnapshotBatchNotConsumable",
    "UnknownCalculator",
]


class FeatureError(Exception):
    """Base class for every failure raised by `market_pipeline_lib.features`."""


class UnknownCalculator(FeatureError, LookupError):
    """No calculator is registered for a `(feature_code, calculator_version)` pair.

    Calculators are versioned and never removed, so this is always either a typo or a
    definition row written by a newer deployment than the one reading it.
    """


class InvalidFeatureParameters(FeatureError, ValueError):
    """Parameters do not satisfy the calculator's declared parameter contract."""


class FeatureDefinitionImmutable(FeatureError, PermissionError):
    """A published `feature_definitions` row was asked to change.

    A definition is identified by its content hash.  Changing any hashed field is a new
    version, which is a new row; there is no in-place edit path and this is what refuses
    one.
    """


class FeatureDefinitionNotPublished(FeatureError, LookupError):
    """A definition was used before it was written to `feature_definitions`."""


class DefinitionIntegrityError(FeatureError, ValueError):
    """A stored row's `definition_hash` does not match the row's own content."""


class InvalidBarSeries(FeatureError, ValueError):
    """The input bars are not a usable series for the requested period."""


class InsufficientHistory(FeatureError, ValueError):
    """Fewer bars than the definition's `required_history_points`.

    A real outcome, not a programming error: it is recorded as a ``FAILED``
    materialization so the gap is visible in the catalog rather than silently producing
    an empty feature.
    """


class MaterializationConflict(FeatureError, ValueError):
    """A write would violate a canonical uniqueness rule on `feature_materializations`."""


class PartialSnapshotBatch(FeatureError, RuntimeError):
    """A snapshot batch was sealed while planned members are missing or failed."""


class SnapshotBatchNotConsumable(FeatureError, RuntimeError):
    """A consumer asked for a batch that has not reached ``SUCCEEDED``."""
