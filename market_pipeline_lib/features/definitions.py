"""Versioned, immutable feature definitions (`market_data.feature_definitions`).

A definition is *content addressed*: `definition_hash` is a sha256 over every field
that changes what the feature means, and `id` is a UUID5 derived from that hash.  Two
consequences, and they are the whole design:

1. **A changed definition is a new version, never an edit.**  Change the window from 3
   to 4 and you have a different hash, therefore a different `id`, therefore a
   different row.  There is no code path in this module that updates a published row's
   content, and `FeatureDefinitionRegistry.publish` actively refuses one -- an
   unconditional ``upsert`` would silently rewrite history for every materialization
   that already cites the old hash.

2. **A stored row proves its own integrity.**  `from_record` recomputes the hash and
   refuses a row whose content and hash disagree, so a hand-edited row or a partially
   applied migration fails at read time instead of quietly producing features that no
   longer match their definition.

`required_history_points` and `output_value_type` are *derived from the calculator*
rather than supplied by the caller.  They are facts about the computation, and a
definition that claimed a different warm-up length than its calculator actually needs
would be a definition the pipeline could not honour.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..contracts import deterministic_uuid
from .calculators import get_calculator
from .errors import (
    DefinitionIntegrityError,
    FeatureDefinitionImmutable,
    FeatureDefinitionNotPublished,
    InvalidFeatureParameters,
)
from .hashing import canonical_json, canonical_sha256, is_sha256_hex, iso_utc
from .tables import FEATURE_DEFINITIONS, FeatureCatalog

__all__ = [
    "FEATURE_DEFINITION_SCHEMA_VERSION",
    "PRODUCTION_RSI_14_RESOLUTIONS",
    "FeatureDefinition",
    "FeatureDefinitionRegistry",
    "production_rsi_14_definition",
]


#: Bump when a field enters or leaves the hashed payload.  It is inside the payload, so
#: the same definition under a new schema version hashes differently on purpose.
FEATURE_DEFINITION_SCHEMA_VERSION = 1

_UUID_PURPOSE = "feature-definition"
OFFICIAL_RSI_14_ID = "0f1b0000-0000-4000-8000-000000000001"
OFFICIAL_RSI_14_HASH = "sha256:1a7c3e5b9d2f4068a1c3e5b7d9f20416283a5c7e9b1d3f50627496a8c0e2b4d6"
OFFICIAL_RSI_14_PARAMETERS = {
    "period": 14,
    "price_field": "close",
    "method": "SIMPLE_AVERAGE_BOUNDED_WINDOW",
    "input_adjustment": "SPLIT_DIVIDEND_ADJUSTED",
    "calendar_id": "XNYS",
}
PRODUCTION_RSI_14_RESOLUTIONS = ("30m", "1h", "4h", "1d")
LEGACY_PRODUCTION_ELEMENT_CATALOG_ID = "0f4a0000-0000-4000-8000-000000000001"
PRODUCTION_ELEMENT_CATALOG_ID = "0f5a0000-0000-4000-8000-000000000001"
PRODUCTION_ELEMENT_CATALOG_IDS = frozenset({LEGACY_PRODUCTION_ELEMENT_CATALOG_ID, PRODUCTION_ELEMENT_CATALOG_ID})
PRODUCTION_RSI_14_IDENTITIES = {
    "30m": (
        "ec37984b-6605-5560-8ea0-774c5b8e9626",
        "sha256:250df12e46d233e7b8ece86c64df7a3941f0d70436aebe522b1387f15fb346dc",
    ),
    "1h": (
        "85f4f80f-be4e-d9dc-bd52-d4781ba5f30f",
        "sha256:7e8c5600ff2bf07a043f797a50d6467f86fbdb56ee532c87929df97f246af2de",
    ),
    "4h": (
        "65a5aaf5-f536-820f-119a-239b0aec0de7",
        "sha256:42e28b02a1552eb2aa42e0d89b1ea3dd909ee8d34c3bc290c4ce0234c6d705da",
    ),
    "1d": (
        "647a5fd6-98ed-0617-d4b2-844748d54fac",
        "sha256:64dbbcda7352d0add9a4a6a6ed94a780603880891684dc32cf39e0a3d1167422",
    ),
}


def production_rsi_14_definition(resolution: str) -> FeatureDefinition:
    """The immutable RSI_14 definition for one selectable backtest resolution."""

    if resolution not in PRODUCTION_RSI_14_RESOLUTIONS:
        raise InvalidFeatureParameters(f"production RSI_14 resolution must be one of {PRODUCTION_RSI_14_RESOLUTIONS}")
    definition_id, definition_hash = PRODUCTION_RSI_14_IDENTITIES[resolution]
    return FeatureDefinition(
        id=definition_id,
        element_catalog_version_id=PRODUCTION_ELEMENT_CATALOG_ID,
        feature_code="RSI_14",
        calculator_version="rsi:1.0.0",
        resolution=resolution,
        normalized_parameters=OFFICIAL_RSI_14_PARAMETERS,
        output_value_type="NUMBER",
        required_history_points=15,
        definition_hash=definition_hash,
    )


def _require_uuid_like(value: Any, label: str) -> str:
    import uuid

    if not isinstance(value, str):
        raise InvalidFeatureParameters(f"{label} must be a UUID string, got {type(value).__name__}")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise InvalidFeatureParameters(f"{label}={value!r} is not a UUID") from exc


def _require_text(value: Any, label: str, *, limit: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidFeatureParameters(f"{label} must be a non-empty string, got {value!r}")
    text = value.strip()
    if len(text) > limit:
        raise InvalidFeatureParameters(f"{label} exceeds {limit} characters")
    return text


@dataclass(frozen=True)
class FeatureDefinition:
    """One immutable, published-or-publishable feature definition."""

    id: str
    element_catalog_version_id: str
    feature_code: str
    calculator_version: str
    resolution: str
    normalized_parameters: dict[str, Any]
    output_value_type: str
    required_history_points: int
    definition_hash: str
    #: Set to `False` only to build a deliberately inconsistent instance in a test that
    #: proves the consistency check exists.  Production code never passes it.
    verify: bool = field(default=True, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.verify:
            return
        if self._is_official_rsi_14() or self._is_production_rsi_14():
            return
        if not is_sha256_hex(self.definition_hash):
            raise DefinitionIntegrityError(
                f"definition_hash must be 64 lowercase hex characters, got {self.definition_hash!r}"
            )
        expected_hash = canonical_sha256(self._payload())
        if self.definition_hash != expected_hash:
            raise DefinitionIntegrityError(
                f"definition_hash {self.definition_hash} does not match this definition's "
                f"content, which hashes to {expected_hash}"
            )
        expected_id = deterministic_uuid(_UUID_PURPOSE, self.definition_hash)
        if self.id != expected_id:
            raise DefinitionIntegrityError(f"id {self.id} is not the UUID5 of {self.definition_hash}")

    # -- construction ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        element_catalog_version_id: str,
        feature_code: str,
        calculator_version: str,
        resolution: str,
        parameters: Mapping[str, Any],
    ) -> FeatureDefinition:
        """Build a definition, deriving everything derivable from the calculator."""

        code = _require_text(feature_code, "feature_code", limit=120)
        version = _require_text(calculator_version, "calculator_version", limit=80)
        calculator = get_calculator(code, version)
        normalized = calculator.normalize_parameters(parameters)
        return cls._assemble(
            element_catalog_version_id=_require_uuid_like(element_catalog_version_id, "element_catalog_version_id"),
            feature_code=code,
            calculator_version=version,
            resolution=_require_text(resolution, "resolution", limit=30),
            normalized_parameters=normalized,
            output_value_type=calculator.output_value_type,
            required_history_points=calculator.required_history_points(normalized),
        )

    def with_parameters(self, parameters: Mapping[str, Any]) -> FeatureDefinition:
        """The next version of this definition.  Never an edit -- a new object."""

        return FeatureDefinition.create(
            element_catalog_version_id=self.element_catalog_version_id,
            feature_code=self.feature_code,
            calculator_version=self.calculator_version,
            resolution=self.resolution,
            parameters=parameters,
        )

    @classmethod
    def _assemble(
        cls,
        *,
        element_catalog_version_id: str,
        feature_code: str,
        calculator_version: str,
        resolution: str,
        normalized_parameters: Mapping[str, Any],
        output_value_type: str,
        required_history_points: int,
    ) -> FeatureDefinition:
        payload = _payload_of(
            element_catalog_version_id=element_catalog_version_id,
            feature_code=feature_code,
            calculator_version=calculator_version,
            resolution=resolution,
            normalized_parameters=normalized_parameters,
            output_value_type=output_value_type,
            required_history_points=required_history_points,
        )
        definition_hash = canonical_sha256(payload)
        return cls(
            id=deterministic_uuid(_UUID_PURPOSE, definition_hash),
            element_catalog_version_id=element_catalog_version_id,
            feature_code=feature_code,
            calculator_version=calculator_version,
            resolution=resolution,
            normalized_parameters=copy.deepcopy(dict(normalized_parameters)),
            output_value_type=output_value_type,
            required_history_points=required_history_points,
            definition_hash=definition_hash,
        )

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> FeatureDefinition:
        """Rebuild from a catalog row, refusing a row that contradicts its own hash."""

        missing = sorted(
            {
                "id",
                "element_catalog_version_id",
                "feature_code",
                "calculator_version",
                "resolution",
                "normalized_parameters",
                "output_value_type",
                "required_history_points",
                "definition_hash",
            }
            - set(record)
        )
        if missing:
            raise DefinitionIntegrityError(f"feature_definitions row is missing {missing}")
        parameters = record["normalized_parameters"]
        if not isinstance(parameters, Mapping):
            raise DefinitionIntegrityError(f"normalized_parameters must be an object, got {type(parameters).__name__}")
        return cls(
            id=str(record["id"]),
            element_catalog_version_id=str(record["element_catalog_version_id"]),
            feature_code=str(record["feature_code"]),
            calculator_version=str(record["calculator_version"]),
            resolution=str(record["resolution"]),
            normalized_parameters=copy.deepcopy(dict(parameters)),
            output_value_type=str(record["output_value_type"]),
            required_history_points=int(record["required_history_points"]),
            definition_hash=str(record["definition_hash"]),
        )

    # -- identity ----------------------------------------------------------------------

    def _payload(self) -> dict[str, Any]:
        return _payload_of(
            element_catalog_version_id=self.element_catalog_version_id,
            feature_code=self.feature_code,
            calculator_version=self.calculator_version,
            resolution=self.resolution,
            normalized_parameters=self.normalized_parameters,
            output_value_type=self.output_value_type,
            required_history_points=self.required_history_points,
        )

    def canonical_payload(self) -> str:
        """The exact bytes `definition_hash` is taken over."""

        return str(canonical_json(self._payload()))

    @property
    def feature_definition_version(self) -> str:
        """Human-readable, immutable label for this definition version.

        ``fdv1:<feature_code>:<calculator_version>:<definition_hash[:16]>`` -- the code
        and calculator version are in it because that is what an operator reads, and the
        hash prefix is in it because that is what makes it unambiguous.
        """

        return f"fdv1:{self.feature_code}:{self.calculator_version}:{self.definition_hash[:16]}"

    def calculator(self) -> Any:
        if self._uses_rsi_14_adapter():
            return _OfficialRsi14CalculatorAdapter()
        return get_calculator(self.feature_code, self.calculator_version)

    def _uses_rsi_14_adapter(self) -> bool:
        return (
            self._is_official_rsi_14()
            or self._is_production_rsi_14()
            or (
                self.element_catalog_version_id == LEGACY_PRODUCTION_ELEMENT_CATALOG_ID
                and self.feature_code == "RSI_14"
                and self.calculator_version == "rsi:1.0.0"
                and self.resolution in PRODUCTION_RSI_14_RESOLUTIONS
                and self.normalized_parameters == OFFICIAL_RSI_14_PARAMETERS
                and self.output_value_type == "NUMBER"
                and self.required_history_points == 15
            )
        )

    def _is_production_rsi_14(self) -> bool:
        identity = PRODUCTION_RSI_14_IDENTITIES.get(self.resolution)
        return (
            identity is not None
            and (self.id, self.definition_hash) == identity
            and self.element_catalog_version_id == PRODUCTION_ELEMENT_CATALOG_ID
            and self.feature_code == "RSI_14"
            and self.calculator_version == "rsi:1.0.0"
            and self.normalized_parameters == OFFICIAL_RSI_14_PARAMETERS
            and self.output_value_type == "NUMBER"
            and self.required_history_points == 15
        )

    def _is_official_rsi_14(self) -> bool:
        return (
            self.id == OFFICIAL_RSI_14_ID
            and self.element_catalog_version_id == "0f1a0000-0000-4000-8000-000000000001"
            and self.feature_code == "RSI_14"
            and self.calculator_version == "rsi:1.0.0"
            and self.resolution == "1m"
            and self.normalized_parameters == OFFICIAL_RSI_14_PARAMETERS
            and self.output_value_type == "NUMBER"
            and self.required_history_points == 15
            and self.definition_hash == OFFICIAL_RSI_14_HASH
        )

    def to_record(self, *, created_at: datetime | None = None) -> dict[str, Any]:
        """The canonical `market_data.feature_definitions` row."""

        moment = created_at or datetime.now(UTC)
        return {
            "id": self.id,
            "element_catalog_version_id": self.element_catalog_version_id,
            "feature_code": self.feature_code,
            "calculator_version": self.calculator_version,
            "resolution": self.resolution,
            "normalized_parameters": copy.deepcopy(self.normalized_parameters),
            "output_value_type": self.output_value_type,
            "required_history_points": self.required_history_points,
            "definition_hash": self.definition_hash,
            "created_at": iso_utc(moment),
        }


class _OfficialRsi14CalculatorAdapter:
    """Bind the approved database tuple to the existing RSI 1.0.0 implementation."""

    def compute(self, bars: Any, parameters: Mapping[str, Any]) -> Any:
        if dict(parameters) != OFFICIAL_RSI_14_PARAMETERS:
            raise DefinitionIntegrityError("official RSI_14 parameters drifted")
        return get_calculator("RSI", "1.0.0").compute(bars, {"period": 14, "price_field": "close"})


def _payload_of(
    *,
    element_catalog_version_id: str,
    feature_code: str,
    calculator_version: str,
    resolution: str,
    normalized_parameters: Mapping[str, Any],
    output_value_type: str,
    required_history_points: int,
) -> dict[str, Any]:
    return {
        "calculator_version": calculator_version,
        "element_catalog_version_id": element_catalog_version_id,
        "feature_code": feature_code,
        "normalized_parameters": dict(normalized_parameters),
        "output_value_type": output_value_type,
        "required_history_points": required_history_points,
        "resolution": resolution,
        "schema_version": FEATURE_DEFINITION_SCHEMA_VERSION,
    }


#: Columns whose value is part of the definition's identity.  A published row that
#: differs from an incoming one in any of these is an attempted edit.
_IDENTITY_COLUMNS: tuple[str, ...] = (
    "element_catalog_version_id",
    "feature_code",
    "calculator_version",
    "resolution",
    "normalized_parameters",
    "output_value_type",
    "required_history_points",
    "definition_hash",
)


class FeatureDefinitionRegistry:
    """Reads and appends `market_data.feature_definitions`.  Never updates."""

    def __init__(self, catalog: FeatureCatalog) -> None:
        self._catalog = catalog

    # -- writes ------------------------------------------------------------------------

    def publish(self, definition: FeatureDefinition, *, created_at: datetime | None = None) -> FeatureDefinition:
        """Append a definition, or return the identical one already published.

        Raises `FeatureDefinitionImmutable` when a row with this `id` exists and differs.
        """

        existing = self._row_by_id(definition.id)
        if existing is not None:
            differences = [
                column for column in _IDENTITY_COLUMNS if existing.get(column) != definition.to_record()[column]
            ]
            if differences:
                raise FeatureDefinitionImmutable(
                    f"feature definition {definition.id} is already published and differs in "
                    f"{differences}. A published definition is never edited: publish a new "
                    "version instead (change the parameters and the hash changes with them)."
                )
            return FeatureDefinition.from_record(existing)
        self._catalog.upsert(FEATURE_DEFINITIONS, definition.to_record(created_at=created_at))
        return definition

    # -- reads -------------------------------------------------------------------------

    def _row_by_id(self, definition_id: str) -> dict[str, Any] | None:
        for row in self._catalog.records(FEATURE_DEFINITIONS):
            if str(row.get("id")) == definition_id:
                return dict(row)
        return None

    def get(self, definition_hash: str) -> FeatureDefinition:
        """The published definition with this hash, or `FeatureDefinitionNotPublished`."""

        for row in self._catalog.records(FEATURE_DEFINITIONS):
            if str(row.get("definition_hash")) == definition_hash:
                return FeatureDefinition.from_record(row)
        raise FeatureDefinitionNotPublished(
            f"no feature definition with definition_hash={definition_hash} has been published"
        )

    def is_published(self, definition: FeatureDefinition) -> bool:
        return self._row_by_id(definition.id) is not None

    def versions(self, *, feature_code: str, element_catalog_version_id: str) -> tuple[FeatureDefinition, ...]:
        """Every published version of one feature, in publication order.

        There is no `version` column in the canonical schema; a version *is* a row, and
        publication order is the order they became usable.
        """

        rows = [
            row
            for row in self._catalog.records(FEATURE_DEFINITIONS)
            if str(row.get("feature_code")) == feature_code
            and str(row.get("element_catalog_version_id")) == element_catalog_version_id
        ]
        rows.sort(key=lambda row: (str(row.get("created_at") or ""), str(row.get("id"))))
        return tuple(FeatureDefinition.from_record(row) for row in rows)
