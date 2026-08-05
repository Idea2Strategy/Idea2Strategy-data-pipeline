"""Production command adapter for immutable historical feature outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from apps.common.errors import MalformedEventError
from market_pipeline_lib.features import (
    BarPoint,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    MaterializationRequest,
    SourceObject,
)
from market_pipeline_lib.features.output import FeatureOutputPublisher
from market_pipeline_lib.features.tables import FeatureCatalog
from market_pipeline_lib.storage import ObjectStore

FEATURE_OUTPUT_FIELDS = frozenset(
    {
        "definition_hash",
        "instrument_id",
        "pipeline_run_id",
        "sources",
        "bars",
        "period_start",
        "period_end",
        "source_watermark",
        "output_dataset_manifest_id",
        "output_feed_id",
        "output_revision_number",
    }
)
SOURCE_FIELDS = frozenset(
    {
        "dataset_object_id",
        "dataset_manifest_id",
        "content_hash",
        "partition_start",
        "partition_end",
        "row_count",
    }
)
BAR_FIELDS = frozenset({"bar_start_at", "open", "high", "low", "close", "volume"})


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MalformedEventError(f"{label} must be an object")
    return value


def _exact_fields(document: Mapping[str, Any], expected: frozenset[str], label: str) -> None:
    missing = sorted(expected - set(document))
    unknown = sorted(set(document) - expected)
    if missing or unknown:
        raise MalformedEventError(f"{label} fields mismatch: missing={missing}, unknown={unknown}")


def _text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value.strip():
        raise MalformedEventError(f"{label}.{field} must be a non-empty string")
    return value.strip()


def _timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MalformedEventError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise MalformedEventError(f"{label} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise MalformedEventError(f"{label} must include a timezone")
    return parsed


def _positive_int(value: Any, label: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise MalformedEventError(f"{label} must be a {qualifier} integer")
    return value


def _decimal(value: Any, label: str) -> Decimal:
    if not isinstance(value, str) or not value.strip():
        raise MalformedEventError(f"{label} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise MalformedEventError(f"{label} must be a decimal string") from error
    if not parsed.is_finite():
        raise MalformedEventError(f"{label} must be finite")
    return parsed


class ProductionFeatureMaterializationPort:
    """Decode one explicit request and run the existing atomic publisher."""

    def __init__(
        self,
        catalog: FeatureCatalog,
        object_store: ObjectStore,
        *,
        staging_root: Path,
    ) -> None:
        self._registry = FeatureDefinitionRegistry(catalog)
        self._materializer = FeatureMaterializer(
            catalog,
            self._registry,
            output_publisher=FeatureOutputPublisher(
                catalog,
                object_store,
                staging_root=staging_root,
            ),
        )
        self._staging_root = staging_root

    def prepare(self) -> None:
        self._staging_root.mkdir(parents=True, exist_ok=True)

    def materialize(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        request = self._request(payload)
        result = self._materializer.materialize(request)
        return {
            "status": result.status,
            "materialization_id": result.materialization_id,
            "feature_materialization_version": result.feature_materialization_version,
            "result_hash": result.result_hash,
            "row_count": result.row_count,
            "output_dataset_manifest_id": request.output_dataset_manifest_id,
            "output_feed_id": request.output_feed_id,
            "output_revision_number": request.output_revision_number,
            "output_content_hash": result.output_content_hash,
            "output_provider_version_id": result.output_provider_version_id,
        }

    def _request(self, payload: Mapping[str, Any]) -> MaterializationRequest:
        document = _mapping(payload, "MATERIALIZE_FEATURE_OUTPUT payload")
        _exact_fields(document, FEATURE_OUTPUT_FIELDS, "MATERIALIZE_FEATURE_OUTPUT payload")

        sources_value = document["sources"]
        bars_value = document["bars"]
        if not isinstance(sources_value, Sequence) or isinstance(sources_value, (str, bytes)):
            raise MalformedEventError("MATERIALIZE_FEATURE_OUTPUT payload.sources must be an array")
        if not isinstance(bars_value, Sequence) or isinstance(bars_value, (str, bytes)):
            raise MalformedEventError("MATERIALIZE_FEATURE_OUTPUT payload.bars must be an array")
        if not sources_value or not bars_value:
            raise MalformedEventError("feature materialization requires non-empty sources and bars")

        sources: list[SourceObject] = []
        bars: list[BarPoint] = []
        try:
            for index, value in enumerate(sources_value):
                source = _mapping(value, f"sources[{index}]")
                _exact_fields(source, SOURCE_FIELDS, f"sources[{index}]")
                sources.append(
                    SourceObject(
                        dataset_object_id=_text(source, "dataset_object_id", f"sources[{index}]"),
                        dataset_manifest_id=_text(source, "dataset_manifest_id", f"sources[{index}]"),
                        content_hash=_text(source, "content_hash", f"sources[{index}]"),
                        partition_start=_text(source, "partition_start", f"sources[{index}]"),
                        partition_end=_text(source, "partition_end", f"sources[{index}]"),
                        row_count=_positive_int(
                            source["row_count"], f"sources[{index}].row_count", allow_zero=True
                        ),
                    )
                )
            for index, value in enumerate(bars_value):
                bar = _mapping(value, f"bars[{index}]")
                _exact_fields(bar, BAR_FIELDS, f"bars[{index}]")
                bars.append(
                    BarPoint(
                        bar_start_at=_timestamp(bar["bar_start_at"], f"bars[{index}].bar_start_at"),
                        open=_decimal(bar["open"], f"bars[{index}].open"),
                        high=_decimal(bar["high"], f"bars[{index}].high"),
                        low=_decimal(bar["low"], f"bars[{index}].low"),
                        close=_decimal(bar["close"], f"bars[{index}].close"),
                        volume=_positive_int(
                            bar["volume"], f"bars[{index}].volume", allow_zero=True
                        ),
                    )
                )
            request = MaterializationRequest(
                definition=self._registry.get(_text(document, "definition_hash", "payload")),
                instrument_id=_text(document, "instrument_id", "payload"),
                pipeline_run_id=_text(document, "pipeline_run_id", "payload"),
                sources=tuple(sources),
                bars=tuple(bars),
                period_start=_timestamp(document["period_start"], "payload.period_start"),
                period_end=_timestamp(document["period_end"], "payload.period_end"),
                source_watermark=_text(document, "source_watermark", "payload"),
                output_dataset_manifest_id=_text(
                    document, "output_dataset_manifest_id", "payload"
                ),
                output_feed_id=_text(document, "output_feed_id", "payload"),
                output_revision_number=_positive_int(
                    document["output_revision_number"], "payload.output_revision_number"
                ),
            )
            request.validate()
            request.validate_output_publication()
            return request
        except MalformedEventError:
            raise
        except ValueError as error:
            raise MalformedEventError(str(error)) from error
