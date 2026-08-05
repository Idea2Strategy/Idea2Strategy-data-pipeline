"""Fail-closed command adapter for immutable historical feature outputs.

This is an implementation draft for the authority-owned mapping proposed in root PR
#306.  It intentionally refuses to invent or seed that mapping: until the canonical
contract is approved and its provider/feed rows are installed, production commands
fail before creating a pipeline run or an output object.
"""

from __future__ import annotations

import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow.parquet as pq

from apps.common.errors import MalformedEventError, PortNotConfiguredError
from market_pipeline_lib.contracts import SCHEMA_VERSION, bar_schema, deterministic_uuid
from market_pipeline_lib.features import (
    FEATURE_SERIES_SCHEMA_VERSION,
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    MaterializationRequest,
    SourceObject,
)
from market_pipeline_lib.features.hashing import canonical_sha256, iso_utc
from market_pipeline_lib.features.output import FeatureOutputPublisher
from market_pipeline_lib.features.tables import FeatureCatalog
from market_pipeline_lib.storage import ObjectStore

FEATURE_OUTPUT_FIELDS = frozenset(
    {"definition_hash", "instrument_id", "source_dataset_object_ids", "period_start", "period_end"}
)
INTERNAL_PROVIDER_CODE = "IDEA2STRATEGY_INTERNAL"


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
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as error:
            raise MalformedEventError(f"{label} must be an ISO-8601 timestamp") from error
    else:
        raise MalformedEventError(f"{label} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None:
        raise MalformedEventError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def _uuid(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise MalformedEventError(f"{label} must be a UUID string")
    try:
        return str(UUID(value.strip()))
    except (AttributeError, ValueError) as error:
        raise MalformedEventError(f"{label} must be a UUID string") from error


def _token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").upper()


def feature_output_provider_id() -> str:
    return str(deterministic_uuid("provider", INTERNAL_PROVIDER_CODE))


def feature_output_feed_id(definition: FeatureDefinition) -> str:
    return str(
        deterministic_uuid(
            "feature-output-feed",
            definition.definition_hash,
            definition.calculator_version,
            definition.resolution,
            FEATURE_SERIES_SCHEMA_VERSION,
        )
    )


def feature_output_feed_code(definition: FeatureDefinition) -> str:
    parameters = "_".join(
        str(value) for _, value in sorted(definition.normalized_parameters.items()) if isinstance(value, int)
    )
    feature = _token("_".join(part for part in (definition.feature_code, parameters) if part))
    return "_".join(
        (
            "FEATURE",
            feature,
            _token(definition.resolution),
            _token(definition.feature_code),
            _token(definition.calculator_version),
        )
    )


def feature_output_feed_version(definition: FeatureDefinition) -> str:
    return f"{definition.feature_code.lower()}-{definition.calculator_version}+{FEATURE_SERIES_SCHEMA_VERSION}"


def feature_output_run_id(command_id: str) -> str:
    return str(deterministic_uuid("feature-output-pipeline-run", command_id))


@dataclass(frozen=True)
class CanonicalFeatureInput:
    sources: tuple[SourceObject, ...]
    bars: tuple[BarPoint, ...]
    source_watermark: str


@dataclass(frozen=True)
class FeatureOutputTarget:
    feed_id: str
    manifest_id: str
    revision_number: int


class CanonicalFeatureSourceReader:
    """Reconstruct bars and provenance only from canonical rows and exact object versions."""

    def __init__(self, catalog: FeatureCatalog, object_store: ObjectStore) -> None:
        self._catalog = catalog
        self._object_store = object_store

    def read(
        self,
        object_ids: Sequence[Any],
        *,
        definition: FeatureDefinition,
        instrument_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> CanonicalFeatureInput:
        if isinstance(object_ids, (str, bytes)) or not object_ids:
            raise MalformedEventError("payload.source_dataset_object_ids must be a non-empty array")
        wanted = tuple(
            _uuid(value, f"payload.source_dataset_object_ids[{index}]") for index, value in enumerate(object_ids)
        )
        if len(set(wanted)) != len(wanted):
            raise MalformedEventError("source dataset object IDs must be unique")

        relations = {str(row["id"]): row for row in self._catalog.records("market_data.dataset_objects")}
        manifests = {str(row["id"]): row for row in self._catalog.records("market_data.dataset_manifests")}
        storage = {str(row["id"]): row for row in self._catalog.records("storage.objects")}
        sources: list[SourceObject] = []
        bars: list[BarPoint] = []
        receipts: list[dict[str, Any]] = []
        expected_schema = bar_schema(False)

        for object_id in sorted(wanted):
            relation = relations.get(object_id)
            if relation is None:
                raise MalformedEventError(f"canonical dataset object is missing: {object_id}")
            manifest = manifests.get(str(relation["dataset_manifest_id"]))
            receipt = storage.get(str(relation["object_id"]))
            if manifest is None or receipt is None:
                raise MalformedEventError(f"canonical source receipt is incomplete: {object_id}")
            if relation["object_kind"] != "MARKET_BARS":
                raise MalformedEventError(f"source {object_id} is not MARKET_BARS")
            if manifest["status"] != "AVAILABLE" or receipt["status"] != "AVAILABLE":
                raise MalformedEventError(f"source {object_id} is not AVAILABLE")
            if str(manifest.get("instrument_id")) != instrument_id:
                raise MalformedEventError(f"source {object_id} belongs to another instrument")
            if manifest["resolution"] != definition.resolution:
                raise MalformedEventError(f"source {object_id} resolution does not match the definition")
            if manifest["schema_version"] != SCHEMA_VERSION or receipt["schema_version"] != SCHEMA_VERSION:
                raise MalformedEventError(f"source {object_id} schema version is not canonical")
            if receipt["file_format"] != "PARQUET":
                raise MalformedEventError(f"source {object_id} is not Parquet")
            if int(relation["row_count"]) != int(receipt["row_count"]):
                raise MalformedEventError(f"source {object_id} row count receipt mismatch")
            manifest_start = _timestamp(manifest["period_start"], "manifest.period_start")
            manifest_end = _timestamp(manifest["period_end"], "manifest.period_end")
            relation_start = _timestamp(relation["period_start"], "dataset_object.period_start")
            relation_end = _timestamp(relation["period_end"], "dataset_object.period_end")
            storage_start = _timestamp(receipt["period_start"], "storage_object.period_start")
            storage_end = _timestamp(receipt["period_end"], "storage_object.period_end")
            if not (manifest_start <= period_start < period_end <= manifest_end):
                raise MalformedEventError(f"source {object_id} does not cover the requested period")
            if (relation_start, relation_end) != (storage_start, storage_end):
                raise MalformedEventError(f"source {object_id} period receipt mismatch")

            verified = self._object_store.verify_version(
                str(receipt["object_key"]),
                str(receipt["provider_version_id"]),
                str(receipt["content_hash"]),
                int(receipt["byte_size"]),
            )
            if not verified.ok:
                raise MalformedEventError(f"source {object_id} exact object verification failed: {verified.message}")
            with self._object_store.open_version(
                str(receipt["object_key"]), str(receipt["provider_version_id"])
            ) as stream:
                with tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024) as seekable:
                    while chunk := stream.read(1024 * 1024):
                        seekable.write(chunk)
                    seekable.seek(0)
                    table = pq.read_table(seekable)
            if table.schema != expected_schema:
                raise MalformedEventError(f"source {object_id} Parquet schema does not match canonical bars")
            if table.num_rows != int(receipt["row_count"]):
                raise MalformedEventError(f"source {object_id} decoded row count does not match receipt")
            rows = table.to_pylist()
            previous: datetime | None = None
            for row_index, row in enumerate(rows):
                if str(row["instrument_id"]) != instrument_id:
                    raise MalformedEventError(f"source {object_id} row {row_index} belongs to another instrument")
                moment = _timestamp(row["bar_start_at"], f"source {object_id} row {row_index}.bar_start_at")
                if previous is not None and moment <= previous:
                    raise MalformedEventError(f"source {object_id} bars are not strictly ordered and unique")
                previous = moment
                if period_start <= moment < period_end:
                    bars.append(
                        BarPoint(
                            bar_start_at=moment,
                            open=Decimal(str(row["open"])),
                            high=Decimal(str(row["high"])),
                            low=Decimal(str(row["low"])),
                            close=Decimal(str(row["close"])),
                            volume=int(row["volume"]),
                        )
                    )
            sources.append(
                SourceObject(
                    dataset_object_id=object_id,
                    dataset_manifest_id=str(manifest["id"]),
                    content_hash=str(receipt["content_hash"]),
                    partition_start=str(relation["partition_start"]),
                    partition_end=str(relation["partition_end"]),
                    row_count=int(relation["row_count"]),
                )
            )
            receipts.append(
                {
                    "dataset_object_id": object_id,
                    "provider_version_id": receipt["provider_version_id"],
                    "content_hash": receipt["content_hash"],
                    "byte_size": receipt["byte_size"],
                    "row_count": receipt["row_count"],
                }
            )
        bars.sort(key=lambda item: item.bar_start_at)
        if not bars:
            raise MalformedEventError("canonical sources contain no bars in the requested period")
        if any(
            current.bar_start_at <= previous.bar_start_at for previous, current in zip(bars, bars[1:], strict=False)
        ):
            raise MalformedEventError("canonical source bars overlap or are out of order")
        return CanonicalFeatureInput(
            sources=tuple(sources),
            bars=tuple(bars),
            source_watermark=f"feature-source-set:{canonical_sha256(receipts)}",
        )


class DeterministicFeatureOutputResolver:
    """Resolve, but never create, the authority-owned provider/feed mapping."""

    def __init__(self, catalog: FeatureCatalog) -> None:
        self._catalog = catalog

    def resolve_feed(self, definition: FeatureDefinition) -> str:
        provider_id = feature_output_provider_id()
        provider = next(
            (row for row in self._catalog.records("market_data.providers") if str(row["id"]) == provider_id), None
        )
        expected_provider = {
            "code": INTERNAL_PROVIDER_CODE,
            "display_name": "Idea2Strategy Derived Data",
            "rights_version": "internal-derived-v1",
            "status": "ACTIVE",
        }
        feed_id = feature_output_feed_id(definition)
        feed = next((row for row in self._catalog.records("market_data.feeds") if str(row["id"]) == feed_id), None)
        expected_feed = {
            "provider_id": provider_id,
            "code": feature_output_feed_code(definition),
            "data_kind": "FEATURE_SERIES",
            "resolution": definition.resolution,
            "timezone_name": "UTC",
            "feed_version": feature_output_feed_version(definition),
            "retired_at": None,
        }
        if provider is None or feed is None:
            raise PortNotConfiguredError(
                "authority-owned feature feed seed is missing; this draft refuses to invent canonical mappings"
            )
        if any(provider.get(key) != value for key, value in expected_provider.items()) or any(
            feed.get(key) != value for key, value in expected_feed.items()
        ):
            raise PortNotConfiguredError(
                "authority-owned feature feed seed does not match the approved deterministic mapping"
            )
        return feed_id

    def resolve_target(
        self,
        *,
        feed_id: str,
        materialization_id: str,
        instrument_id: str,
        resolution: str,
        period_start: datetime,
    ) -> FeatureOutputTarget:
        manifest_id = str(
            deterministic_uuid("feature-output-manifest", materialization_id, FEATURE_SERIES_SCHEMA_VERSION)
        )
        manifests = self._catalog.records("market_data.dataset_manifests")
        existing = next((row for row in manifests if str(row["id"]) == manifest_id), None)
        if existing is not None:
            if (
                str(existing["feed_id"]) != feed_id
                or str(existing.get("instrument_id")) != instrument_id
                or existing["resolution"] != resolution
            ):
                raise MalformedEventError("deterministic output manifest identity conflicts with canonical state")
            return FeatureOutputTarget(feed_id, manifest_id, int(existing["revision_number"]))
        siblings = [
            row
            for row in manifests
            if str(row["feed_id"]) == feed_id
            and str(row.get("instrument_id")) == instrument_id
            and row["data_layer"] == "DERIVED"
            and row["resolution"] == resolution
            and _timestamp(row["period_start"], "manifest.period_start") == period_start
        ]
        return FeatureOutputTarget(
            feed_id, manifest_id, max((int(row["revision_number"]) for row in siblings), default=0) + 1
        )


class ProductionFeatureMaterializationPort:
    """Materialize only provider-verified canonical input under worker-owned identity."""

    def __init__(
        self,
        catalog: FeatureCatalog,
        object_store: ObjectStore,
        *,
        staging_root: Path,
        source_reader: CanonicalFeatureSourceReader | None = None,
        target_resolver: DeterministicFeatureOutputResolver | None = None,
    ) -> None:
        self._catalog = catalog
        self._registry = FeatureDefinitionRegistry(catalog)
        self._source_reader = source_reader or CanonicalFeatureSourceReader(catalog, object_store)
        self._target_resolver = target_resolver or DeterministicFeatureOutputResolver(catalog)
        self._materializer = FeatureMaterializer(
            catalog,
            self._registry,
            output_publisher=FeatureOutputPublisher(catalog, object_store, staging_root=staging_root),
        )
        self._staging_root = staging_root

    def prepare(self) -> None:
        self._staging_root.mkdir(parents=True, exist_ok=True)

    def materialize(self, payload: Mapping[str, Any], *, command_id: str) -> Mapping[str, Any]:
        document = _mapping(payload, "MATERIALIZE_FEATURE_OUTPUT payload")
        _exact_fields(document, FEATURE_OUTPUT_FIELDS, "MATERIALIZE_FEATURE_OUTPUT payload")
        command_id = command_id.strip() if isinstance(command_id, str) else ""
        if not command_id or len(command_id) > 160:
            raise MalformedEventError("MATERIALIZE_FEATURE_OUTPUT command_id must contain 1..160 characters")
        definition = self._registry.get(_text(document, "definition_hash", "payload"))
        instrument_id = _uuid(document["instrument_id"], "payload.instrument_id")
        period_start = _timestamp(document["period_start"], "payload.period_start")
        period_end = _timestamp(document["period_end"], "payload.period_end")
        if period_end <= period_start:
            raise MalformedEventError("payload.period_end must be after payload.period_start")
        object_ids = document["source_dataset_object_ids"]
        if not isinstance(object_ids, Sequence) or isinstance(object_ids, (str, bytes)):
            raise MalformedEventError("payload.source_dataset_object_ids must be an array")
        source = self._source_reader.read(
            object_ids,
            definition=definition,
            instrument_id=instrument_id,
            period_start=period_start,
            period_end=period_end,
        )
        feed_id = self._target_resolver.resolve_feed(definition)
        input_hash = canonical_sha256(
            {
                "command": "MATERIALIZE_FEATURE_OUTPUT",
                "definition_hash": definition.definition_hash,
                "instrument_id": instrument_id,
                "period_start": iso_utc(period_start),
                "period_end": iso_utc(period_end),
                "sources": [item.fingerprint_entry() for item in source.sources],
            }
        )
        run_id = feature_output_run_id(command_id)
        existing_run = self._catalog.pipeline_run(run_id)
        if existing_run is not None and existing_run["input_hash"] != input_hash:
            raise MalformedEventError(f"command_id {command_id!r} was already used with different input")

        provisional = MaterializationRequest(
            definition=definition,
            instrument_id=instrument_id,
            pipeline_run_id=run_id,
            sources=source.sources,
            bars=source.bars,
            period_start=period_start,
            period_end=period_end,
            source_watermark=source.source_watermark,
            output_dataset_manifest_id=str(deterministic_uuid("feature-output-pending", command_id)),
            output_feed_id=feed_id,
            output_revision_number=1,
        )
        target = self._target_resolver.resolve_target(
            feed_id=feed_id,
            materialization_id=provisional.materialization_id,
            instrument_id=instrument_id,
            resolution=definition.resolution,
            period_start=period_start,
        )
        request = MaterializationRequest(
            definition=definition,
            instrument_id=instrument_id,
            pipeline_run_id=run_id,
            sources=source.sources,
            bars=source.bars,
            period_start=period_start,
            period_end=period_end,
            source_watermark=source.source_watermark,
            output_dataset_manifest_id=target.manifest_id,
            output_feed_id=target.feed_id,
            output_revision_number=target.revision_number,
        )
        self._catalog.begin_pipeline_run(
            {
                "id": run_id,
                "pipeline_code": "MATERIALIZE_FEATURE_OUTPUT",
                "pipeline_version": FEATURE_SERIES_SCHEMA_VERSION,
                "idempotency_key": command_id,
                "status": "RUNNING",
                "input_hash": input_hash,
                "output_hash": None,
                "started_at": iso_utc(datetime.now(UTC)),
                "completed_at": None,
                "failure_code": None,
            }
        )
        try:
            result = self._materializer.materialize(request)
            self._catalog.finish_pipeline_run(run_id, status="SUCCEEDED", output_hash=result.result_hash)
        except Exception as error:
            self._catalog.finish_pipeline_run(
                run_id, status="FAILED", output_hash=None, failure_code=type(error).__name__[:80]
            )
            raise
        return {
            "status": result.status,
            "materialization_id": result.materialization_id,
            "feature_materialization_version": result.feature_materialization_version,
            "result_hash": result.result_hash,
            "row_count": result.row_count,
            "output_dataset_manifest_id": target.manifest_id,
            "output_feed_id": target.feed_id,
            "output_revision_number": target.revision_number,
            "output_content_hash": result.output_content_hash,
            "output_provider_version_id": result.output_provider_version_id,
        }
