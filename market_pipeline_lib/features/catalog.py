"""The official v1 feature catalog (data-pipeline issue #15).

`feature_definitions` says what *a* feature is.  This module says which features are
**the** features: a closed, named, versioned list that a strategy author, the backtest
engine and this pipeline can all point at and mean the same thing.

Why a catalog and not just definitions
--------------------------------------
A `feature_definitions` row is content-addressed, so it cannot be edited -- but nothing
in it says whether it is *official*.  Two teams can publish two perfectly valid RSI
definitions with different periods, different calendars or different input adjustments,
and a consumer holding one of them has no way to tell it apart from the other.  The
catalog is the missing half: one entry per official feature, and one hash over
everything that decides what the feature means.

What an entry binds
-------------------
Issue #15 asks for the element catalog version, calculator version, normalized
parameters, output type, required history, calendar and precision to be bound together.
`OfficialFeature.canonical_payload` is that binding, plus two more things the same
condition list asks for separately:

* ``input_adjustment`` -- whether the closes an official RSI is computed over are raw or
  split/dividend adjusted.  On a split day the two differ by an order of magnitude, so
  this is not metadata, it is the feature's meaning.
* ``formula_rules`` -- the calculator's declared seed / smoothing / null / precision /
  input rules (see `calculators`).  A change to any of them is a new calculator version,
  which changes every entry hash that cites it.

The element catalog version is the one thing *not* inside the entry hash: it is supplied
per deployment, and `definition()` folds it in to produce the `definition_hash`.  So one
entry maps to exactly one definition per element catalog version, and
`verify_published` is what checks that the definition actually in the database is that
one -- not a plausible neighbour.

Immutability
------------
`OFFICIAL_FEATURE_CATALOG_HASH` is a declared constant and the catalog recomputes it at
construction.  Editing an entry without minting a new `catalog_version` therefore fails
the moment anything constructs the catalog, rather than silently redefining a feature
that materializations already cite.

.. warning::

   The contents of `_OFFICIAL_V1_ENTRIES`, and the `formula_rules` they bind, are a
   product-meaning contract.  They are pinned here and covered by
   `tests/test_feature_catalog.py` so that they are reviewable, not because they are
   settled: every one of them is listed in this card's report as a decision awaiting
   product-authority approval.  Approving a change means a new ``catalog_version`` and a
   new set of entry hashes, never an edit in place.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .calculators import PRECISION_RULES_VERSION, get_calculator
from .definitions import FeatureDefinition, FeatureDefinitionRegistry
from .errors import (
    FeatureCatalogIntegrityError,
    FeatureDefinitionNotPublished,
    InvalidFeatureParameters,
    UnknownOfficialFeature,
)
from .hashing import canonical_json, canonical_sha256

__all__ = [
    "CATALOG_ENTRY_SCHEMA_VERSION",
    "CATALOG_SCHEMA_VERSION",
    "INPUT_ADJUSTMENTS",
    "OFFICIAL_FEATURE_CATALOG_HASH",
    "OFFICIAL_FEATURE_CATALOG_VERSION",
    "OfficialFeature",
    "OfficialFeatureCatalog",
]


#: Bump for any change to the official set or to what an entry means.  It is inside the
#: hashed payload, so a new version renames every entry hash on purpose.
OFFICIAL_FEATURE_CATALOG_VERSION = "feature-catalog:1.0.0"

#: Bump when a field enters or leaves the hashed entry / catalog payloads.
CATALOG_ENTRY_SCHEMA_VERSION = 1
CATALOG_SCHEMA_VERSION = 1

#: How the input bars were adjusted.  ``RAW`` is the provider's prints; the adjusted
#: layer is the one D14/D15 rebuild after a corporate action.
INPUT_ADJUSTMENTS: tuple[str, ...] = ("RAW", "SPLIT_DIVIDEND_ADJUSTED")

#: The sha256 of the catalog's canonical payload.  Verified at construction; see the
#: module docstring.
OFFICIAL_FEATURE_CATALOG_HASH = "17b0359f625a785bc16a00f383f9673f8a7d16a89c8a55211a4ba6da62c66663"


@dataclass(frozen=True)
class OfficialFeature:
    """One entry of the official catalog.

    `name` is what a consumer asks for (``RSI_14``); the calculator, its version and the
    normalized parameters are how it is computed.  Everything derivable from the
    calculator -- output type, required history, formula rules -- is derived rather than
    restated, because an entry that claimed a different warm-up length than its
    calculator needs would be an entry the pipeline could not honour.
    """

    name: str
    feature_code: str
    calculator_version: str
    resolution: str
    parameters: Mapping[str, Any]
    calendar_id: str
    input_adjustment: str

    def __post_init__(self) -> None:
        for label, value in (
            ("name", self.name),
            ("feature_code", self.feature_code),
            ("calculator_version", self.calculator_version),
            ("resolution", self.resolution),
            ("calendar_id", self.calendar_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise InvalidFeatureParameters(
                    f"official feature {label} must be a non-empty string, got {value!r}"
                )
        if self.input_adjustment not in INPUT_ADJUSTMENTS:
            raise InvalidFeatureParameters(
                f"official feature {self.name} input_adjustment must be one of "
                f"{list(INPUT_ADJUSTMENTS)}, got {self.input_adjustment!r}. Whether the "
                "closes were adjusted for splits and dividends changes the values, so it "
                "is pinned rather than assumed."
            )
        # Fails now, not at the first materialization, if the parameters do not satisfy
        # the calculator's contract.
        self.calculator.normalize_parameters(self.parameters)

    # -- derived from the calculator -----------------------------------------------------

    @property
    def calculator(self) -> Any:
        return get_calculator(self.feature_code, self.calculator_version)

    @property
    def calculator_id(self) -> str:
        """``rsi:1.0.0`` -- the short label the calculator is referred to by."""

        return f"{self.feature_code.lower()}:{self.calculator_version}"

    @property
    def formula_rules(self) -> dict[str, str]:
        return dict(self.calculator.formula_rules)

    @property
    def normalized_parameters(self) -> dict[str, Any]:
        result: dict[str, Any] = self.calculator.normalize_parameters(self.parameters)
        return result

    @property
    def output_value_type(self) -> str:
        return str(self.calculator.output_value_type)

    @property
    def required_history_points(self) -> int:
        return int(self.calculator.required_history_points(self.normalized_parameters))

    @property
    def precision_rules_version(self) -> str:
        return PRECISION_RULES_VERSION

    # -- identity ------------------------------------------------------------------------

    def payload(self) -> dict[str, Any]:
        """Everything that decides what this official feature means."""

        return {
            "calculator_version": self.calculator_version,
            "calendar_id": self.calendar_id,
            "catalog_version": OFFICIAL_FEATURE_CATALOG_VERSION,
            "entry_schema_version": CATALOG_ENTRY_SCHEMA_VERSION,
            "feature_code": self.feature_code,
            "formula_rules": self.formula_rules,
            "input_adjustment": self.input_adjustment,
            "name": self.name,
            "normalized_parameters": self.normalized_parameters,
            "output_value_type": self.output_value_type,
            "precision_rules_version": self.precision_rules_version,
            "required_history_points": self.required_history_points,
            "resolution": self.resolution,
        }

    def canonical_payload(self) -> str:
        """The exact bytes `entry_hash` is taken over."""

        return canonical_json(self.payload())

    @property
    def entry_hash(self) -> str:
        return canonical_sha256(self.payload())

    # -- definitions ---------------------------------------------------------------------

    def definition(self, *, element_catalog_version_id: str) -> FeatureDefinition:
        """The one `feature_definitions` row this entry means, for this catalog version."""

        return FeatureDefinition.create(
            element_catalog_version_id=element_catalog_version_id,
            feature_code=self.feature_code,
            calculator_version=self.calculator_version,
            resolution=self.resolution,
            parameters=self.parameters,
        )

    def verify_definition(
        self, definition: FeatureDefinition, *, element_catalog_version_id: str
    ) -> None:
        """Refuse a definition that is not the one this entry resolves to."""

        expected = self.definition(element_catalog_version_id=element_catalog_version_id)
        if definition.definition_hash != expected.definition_hash:
            raise FeatureCatalogIntegrityError(
                f"official feature {self.name} ({self.entry_hash}) resolves to definition "
                f"{expected.definition_hash}, but {definition.definition_hash} was offered "
                f"({definition.feature_code} {definition.normalized_parameters}). A feature "
                "that computes something else is a different feature, not this one."
            )


#: The official v1 set.
#:
#: XNYS, over split- and dividend-adjusted closes.  The two MACD legs are catalogued in
#: their own right because a strategy may want them separately, and because doing so
#: makes the identity of a MACD leg checkable against a standalone EMA of the same
#: window.
#:
#: **Resolutions are deliberately not uniform.**  ``RSI_14`` is ``1m`` because the
#: backend's live compiled-plan contract already pins it there (see the note on that
#: entry); the other six are ``1d`` because no counterpart has decided them and daily is
#: the conventional meaning of SMA_20, EMA_12, EMA_26 and MACD 12/26/9.  Every entry
#: states its own resolution and every resolution is inside the entry hash, so the mix
#: is a recorded position rather than a default nobody chose.  Serving one of the other
#: six intraday is a *new entry under a new catalog version* -- names are unique, so it
#: would need its own name -- never an edit to one of these.
_OFFICIAL_V1_ENTRIES: tuple[OfficialFeature, ...] = (
    OfficialFeature(
        name="SMA_20",
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={"price_field": "close", "window": 20},
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    OfficialFeature(
        name="EMA_12",
        feature_code="EMA",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={"price_field": "close", "window": 12},
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    OfficialFeature(
        name="EMA_26",
        feature_code="EMA",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={"price_field": "close", "window": 26},
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    # One minute, not one day, and not this repository's choice to make: the backend's
    # live `strategy-bot.v1` compiled plan already decided it.  Its warm-up requirement
    # is `{"requirementId": "rsi-14-pt1m", "resolution": "PT1M",
    # "requiredObservations": 14}` and its plan step is
    # `LOAD_FEATURE {"feature": "RSI_14", "resolution": "1m"}`.  A bot asking for RSI_14
    # at one minute could never be served by a catalog that only materialized it daily,
    # so this entry conforms to the counterpart rather than restating a convention.
    # The short `1m` token (rather than the ISO-8601 `PT1M`) is the backend's own
    # spelling for the *feature's* resolution, and it is the vocabulary
    # `market_data.feature_definitions.resolution` already uses.
    OfficialFeature(
        name="RSI_14",
        feature_code="RSI",
        calculator_version="1.0.0",
        resolution="1m",
        parameters={"period": 14, "price_field": "close"},
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    OfficialFeature(
        name="MACD_12_26_9",
        feature_code="MACD",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={
            "fast_period": 12,
            "output_line": "MACD",
            "price_field": "close",
            "signal_period": 9,
            "slow_period": 26,
        },
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    OfficialFeature(
        name="MACD_12_26_9_SIGNAL",
        feature_code="MACD",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={
            "fast_period": 12,
            "output_line": "SIGNAL",
            "price_field": "close",
            "signal_period": 9,
            "slow_period": 26,
        },
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
    OfficialFeature(
        name="MACD_12_26_9_HISTOGRAM",
        feature_code="MACD",
        calculator_version="1.0.0",
        resolution="1d",
        parameters={
            "fast_period": 12,
            "output_line": "HISTOGRAM",
            "price_field": "close",
            "signal_period": 9,
            "slow_period": 26,
        },
        calendar_id="XNYS",
        input_adjustment="SPLIT_DIVIDEND_ADJUSTED",
    ),
)


class OfficialFeatureCatalog:
    """The official feature set, verified against its own declared hash."""

    def __init__(
        self,
        *,
        entries: tuple[OfficialFeature, ...] = _OFFICIAL_V1_ENTRIES,
        expected_hash: str = OFFICIAL_FEATURE_CATALOG_HASH,
    ) -> None:
        ordered = tuple(sorted(entries, key=lambda item: item.name))
        names = [item.name for item in ordered]
        if len(set(names)) != len(names):
            raise FeatureCatalogIntegrityError(
                f"the official feature catalog lists a name twice: {sorted(names)}"
            )
        self._entries = ordered
        self._by_name = {item.name: item for item in ordered}
        digest = canonical_sha256(self._payload())
        if digest != expected_hash:
            raise FeatureCatalogIntegrityError(
                f"the official feature catalog {OFFICIAL_FEATURE_CATALOG_VERSION} hashes to "
                f"{digest}, but {expected_hash} was expected (the module declares "
                f"{OFFICIAL_FEATURE_CATALOG_HASH}). An entry has changed without a new "
                "catalog_version: the official meaning of a feature is not edited in place, "
                "it is republished under a new version."
            )
        self.catalog_hash = digest

    # -- reads -----------------------------------------------------------------------------

    @property
    def version(self) -> str:
        return OFFICIAL_FEATURE_CATALOG_VERSION

    def names(self) -> tuple[str, ...]:
        return tuple(item.name for item in self._entries)

    def entries(self) -> tuple[OfficialFeature, ...]:
        return self._entries

    def entry(self, name: str) -> OfficialFeature:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise UnknownOfficialFeature(
                f"{name!r} is not in the official feature catalog "
                f"{OFFICIAL_FEATURE_CATALOG_VERSION}; it lists {list(self.names())}"
            ) from exc

    def _payload(self) -> dict[str, Any]:
        return {
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "catalog_version": OFFICIAL_FEATURE_CATALOG_VERSION,
            "entries": [
                {"entry_hash": item.entry_hash, "name": item.name} for item in self._entries
            ],
        }

    def canonical_payload(self) -> str:
        """The exact bytes `catalog_hash` is taken over."""

        return canonical_json(self._payload())

    def definition(self, name: str, *, element_catalog_version_id: str) -> FeatureDefinition:
        return self.entry(name).definition(element_catalog_version_id=element_catalog_version_id)

    # -- writes ----------------------------------------------------------------------------

    def publish(
        self, registry: FeatureDefinitionRegistry, *, element_catalog_version_id: str
    ) -> dict[str, FeatureDefinition]:
        """Publish every official definition for one element catalog version.

        Idempotent, because `FeatureDefinitionRegistry.publish` is: an entry that is
        already published returns the stored row, and one that has drifted raises rather
        than being overwritten.
        """

        return {
            item.name: registry.publish(
                item.definition(element_catalog_version_id=element_catalog_version_id)
            )
            for item in self._entries
        }

    def verify_published(
        self, registry: FeatureDefinitionRegistry, *, element_catalog_version_id: str
    ) -> dict[str, FeatureDefinition]:
        """Check that every official definition is published, and is the official one.

        This is the "immutable `definition_hash` verification" of issue #15.  Lookup is
        *by hash*, so a published RSI with a different period -- a perfectly valid
        definition -- does not satisfy the entry that names ``RSI_14``.  Reading a row
        back through the registry also re-checks that its stored content still hashes to
        its stored `definition_hash`.
        """

        found: dict[str, FeatureDefinition] = {}
        missing: list[str] = []
        for item in self._entries:
            expected = item.definition(element_catalog_version_id=element_catalog_version_id)
            try:
                stored = registry.get(expected.definition_hash)
            except FeatureDefinitionNotPublished:
                missing.append(f"{item.name} (expects definition_hash {expected.definition_hash})")
                continue
            item.verify_definition(stored, element_catalog_version_id=element_catalog_version_id)
            found[item.name] = stored
        if missing:
            raise FeatureCatalogIntegrityError(
                f"official feature catalog {OFFICIAL_FEATURE_CATALOG_VERSION} "
                f"({self.catalog_hash}) is not fully published against element catalog "
                f"version {element_catalog_version_id}: {missing}. A consumer pinning one of "
                "these names would have nothing to read."
            )
        return found

    # -- export ----------------------------------------------------------------------------

    def to_document(self, *, element_catalog_version_id: str) -> dict[str, Any]:
        """The catalog as a storable, canonical document.

        Everything a consumer needs to decide whether the artefact in front of it is the
        official one, without a database: the catalog hash, each entry hash, and the
        `definition_hash` each entry resolves to for this element catalog version.
        """

        return {
            "catalog_version": OFFICIAL_FEATURE_CATALOG_VERSION,
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "catalog_hash": self.catalog_hash,
            "element_catalog_version_id": element_catalog_version_id,
            "precision_rules_version": PRECISION_RULES_VERSION,
            "entries": [
                self._entry_document(item, element_catalog_version_id=element_catalog_version_id)
                for item in self._entries
            ],
        }

    @staticmethod
    def _entry_document(
        item: OfficialFeature, *, element_catalog_version_id: str
    ) -> dict[str, Any]:
        definition = item.definition(element_catalog_version_id=element_catalog_version_id)
        return {
            "calculator_id": item.calculator_id,
            "calculator_version": item.calculator_version,
            "calendar_id": item.calendar_id,
            "definition_hash": definition.definition_hash,
            "definition_id": definition.id,
            "entry_hash": item.entry_hash,
            "feature_code": item.feature_code,
            "feature_definition_version": definition.feature_definition_version,
            "formula_rules": item.formula_rules,
            "input_adjustment": item.input_adjustment,
            "name": item.name,
            "normalized_parameters": item.normalized_parameters,
            "output_value_type": item.output_value_type,
            "precision_rules_version": item.precision_rules_version,
            "required_history_points": item.required_history_points,
            "resolution": item.resolution,
        }
