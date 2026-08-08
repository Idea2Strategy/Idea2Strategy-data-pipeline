"""Fail-closed command adapter for immutable historical feature outputs.

The authority-owned mapping is approved in the root canonical contract (PR #320).
Production commands still fail closed before creating a pipeline run or output object
when the exact approved provider/feed rows have not been installed.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from apps.common.errors import MalformedEventError, PortNotConfiguredError
from market_pipeline_lib.contracts import (
    SCHEMA_VERSION,
    bar_schema,
    deterministic_uuid,
    stable_shard_key,
)
from market_pipeline_lib.features import (
    FEATURE_SERIES_SCHEMA_VERSION,
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    MaterializationRequest,
    SourceObject,
)
from market_pipeline_lib.features.definitions import (
    OFFICIAL_RSI_14_ID,
    PRODUCTION_ELEMENT_CATALOG_ID,
    PRODUCTION_RSI_14_RESOLUTIONS,
)
from market_pipeline_lib.features.hashing import canonical_sha256, iso_utc
from market_pipeline_lib.features.output import FeatureOutputPublisher
from market_pipeline_lib.features.tables import FeatureCatalog
from market_pipeline_lib.storage import ObjectStore

FEATURE_OUTPUT_FIELDS = frozenset(
    {"definition_hash", "instrument_id", "source_dataset_object_ids", "period_start", "period_end"}
)
INTERNAL_PROVIDER_CODE = "IDEA2STRATEGY_INTERNAL"
MAX_FEATURE_SOURCE_OBJECTS = 512
MAX_FEATURE_SOURCE_ROWS = 2_000_000
MAX_FEATURE_SOURCE_BYTES = 512 * 1024 * 1024
FEATURE_READ_BATCH_ROWS = 65_536
LEGACY_LOADER_SCHEMA_VERSION = "market-bars/1"
SUPPORTED_MARKET_BAR_SCHEMA_VERSIONS = frozenset(
    {SCHEMA_VERSION, LEGACY_LOADER_SCHEMA_VERSION}
)


def _legacy_loader_bar_schemas() -> tuple[pa.Schema, pa.Schema]:
    base = bar_schema(False).remove_metadata()
    derived = pa.schema(
        [
            *base,
            pa.field("source_bar_count", pa.int16(), nullable=False),
            pa.field("source_minutes", pa.int16(), nullable=False),
        ]
    )
    return base, derived


def _matches_source_parquet_schema(
    actual: pa.Schema,
    *,
    manifest: Mapping[str, Any],
) -> bool:
    schema_version = str(manifest["schema_version"])
    if schema_version == SCHEMA_VERSION:
        return bool(actual.equals(bar_schema(False), check_metadata=False))
    if schema_version != LEGACY_LOADER_SCHEMA_VERSION:
        return False
    if not any(
        actual.equals(candidate, check_metadata=False)
        for candidate in _legacy_loader_bar_schemas()
    ):
        return False
    metadata = actual.metadata or {}
    expected_metadata = {
        b"schema_version": LEGACY_LOADER_SCHEMA_VERSION.encode("ascii"),
        b"provider": b"alpaca",
        b"feed": b"sip",
        b"adjustment": b"all",
        b"session_scope": b"regular",
        b"resolution": str(manifest["resolution"]).encode("ascii"),
        b"manifest_id": str(manifest["id"]).encode("ascii"),
    }
    return all(metadata.get(key) == value for key, value in expected_metadata.items())


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


def _source_targets_instrument(
    manifest: Mapping[str, Any], relation: Mapping[str, Any], instrument_id: str
) -> bool:
    manifest_instrument = manifest.get("instrument_id")
    if manifest_instrument is not None:
        return str(manifest_instrument) == instrument_id
    shard_key = str(relation.get("shard_key", ""))
    match = re.fullmatch(r"s\d+-of-(\d+)", shard_key)
    if match is None or int(match.group(1)) < 1:
        raise MalformedEventError(
            "multi-instrument source must declare a canonical stable shard key"
        )
    expected_shard: str = stable_shard_key(instrument_id, int(match.group(1)))
    return expected_shard == shard_key


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
    if definition.id == OFFICIAL_RSI_14_ID:
        return "FEATURE_RSI_14_1M_RSI_1_0_0"
    if (
        definition.element_catalog_version_id == PRODUCTION_ELEMENT_CATALOG_ID
        and definition.feature_code == "RSI_14"
        and definition.calculator_version == "rsi:1.0.0"
        and definition.resolution in PRODUCTION_RSI_14_RESOLUTIONS
    ):
        return f"FEATURE_RSI_14_{_token(definition.resolution)}_RSI_1_0_0"
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
    if definition.id == OFFICIAL_RSI_14_ID or (
        definition.element_catalog_version_id == PRODUCTION_ELEMENT_CATALOG_ID
        and definition.feature_code == "RSI_14"
        and definition.calculator_version == "rsi:1.0.0"
        and definition.resolution in PRODUCTION_RSI_14_RESOLUTIONS
    ):
        return "rsi-1.0.0+feature-series.parquet.v1"
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
        if len(object_ids) > MAX_FEATURE_SOURCE_OBJECTS:
            raise MalformedEventError(
                f"payload.source_dataset_object_ids accepts at most {MAX_FEATURE_SOURCE_OBJECTS} objects"
            )
        wanted = tuple(
            _uuid(value, f"payload.source_dataset_object_ids[{index}]") for index, value in enumerate(object_ids)
        )
        if len(set(wanted)) != len(wanted):
            raise MalformedEventError("source dataset object IDs must be unique")

        relations: dict[str, dict[str, Any]] = {}
        manifests: dict[str, dict[str, Any]] = {}
        for object_id in wanted:
            matches = self._catalog.records(
                "market_data.dataset_objects", where={"id": object_id}
            )
            if not matches:
                raise MalformedEventError(f"canonical dataset object is missing: {object_id}")
            relations[object_id] = matches[0]
            manifest_id = str(matches[0]["dataset_manifest_id"])
            if manifest_id not in manifests:
                manifest_rows = self._catalog.records(
                    "market_data.dataset_manifests", where={"id": manifest_id}
                )
                if not manifest_rows:
                    raise MalformedEventError(f"canonical source receipt is incomplete: {object_id}")
                manifests[manifest_id] = manifest_rows[0]

        selected_manifests = tuple(manifests.values())
        for object_id, relation in relations.items():
            manifest = manifests[str(relation["dataset_manifest_id"])]
            if relation["object_kind"] != "MARKET_BARS":
                raise MalformedEventError(f"source {object_id} is not MARKET_BARS")
            if manifest["status"] != "AVAILABLE":
                raise MalformedEventError(f"source {object_id} is not AVAILABLE")
            if not _source_targets_instrument(manifest, relation, instrument_id):
                raise MalformedEventError(f"source {object_id} belongs to another instrument shard")
            if manifest["resolution"] != definition.resolution:
                raise MalformedEventError(
                    f"source {object_id} resolution does not match the definition"
                )
            if manifest["schema_version"] not in SUPPORTED_MARKET_BAR_SCHEMA_VERSIONS:
                raise MalformedEventError(f"source {object_id} schema version is not canonical")
        feed_ids = {str(row["feed_id"]) for row in selected_manifests}
        if len(feed_ids) != 1:
            raise MalformedEventError("canonical sources must belong to one feed")
        feed_id = next(iter(feed_ids))
        feed_manifests = self._catalog.records(
            "market_data.dataset_manifests", where={"feed_id": feed_id}
        )
        for selected in selected_manifests:
            identity = (
                str(selected.get("instrument_id")),
                selected.get("data_layer"),
                selected.get("resolution"),
                _timestamp(selected["period_start"], "manifest.period_start"),
                _timestamp(selected["period_end"], "manifest.period_end"),
            )
            available_revisions = [
                row
                for row in feed_manifests
                if row["status"] == "AVAILABLE"
                and (
                    str(row.get("instrument_id")),
                    row.get("data_layer"),
                    row.get("resolution"),
                    _timestamp(row["period_start"], "manifest.period_start"),
                    _timestamp(row["period_end"], "manifest.period_end"),
                )
                == identity
            ]
            current_revision = max(int(row["revision_number"]) for row in available_revisions)
            if int(selected["revision_number"]) != current_revision:
                raise MalformedEventError(
                    "source manifest is not the current AVAILABLE revision for its exact identity"
                )

        coverage = sorted(
            (
                max(period_start, _timestamp(row["period_start"], "manifest.period_start")),
                min(period_end, _timestamp(row["period_end"], "manifest.period_end")),
            )
            for row in selected_manifests
            if _timestamp(row["period_start"], "manifest.period_start") < period_end
            and _timestamp(row["period_end"], "manifest.period_end") > period_start
        )
        cursor = period_start
        for start, end in coverage:
            if start > cursor:
                break
            cursor = max(cursor, end)
        if cursor < period_end:
            raise MalformedEventError(
                "canonical source manifest set does not cover the complete requested period"
            )

        authoritative_ids: set[str] = set()
        object_coverage: list[tuple[datetime, datetime]] = []
        for manifest in selected_manifests:
            for relation in self._catalog.records(
                "market_data.dataset_objects", where={"dataset_manifest_id": str(manifest["id"])}
            ):
                if relation["object_kind"] != "MARKET_BARS":
                    continue
                if not _source_targets_instrument(manifest, relation, instrument_id):
                    continue
                relation_start = _timestamp(relation["period_start"], "dataset_object.period_start")
                relation_end = _timestamp(relation["period_end"], "dataset_object.period_end")
                if relation_start < period_end and relation_end > period_start:
                    authoritative_ids.add(str(relation["id"]))
                    object_coverage.append(
                        (max(period_start, relation_start), min(period_end, relation_end))
                    )
        if set(wanted) != authoritative_ids:
            raise MalformedEventError(
                "source IDs must equal the complete authoritative object set for the requested period"
            )
        cursor = period_start
        for start, end in sorted(object_coverage):
            if start > cursor:
                break
            cursor = max(cursor, end)
        if cursor < period_end:
            raise MalformedEventError(
                "canonical source objects contain a period gap in the requested interval"
            )
        sources: list[SourceObject] = []
        bars: list[BarPoint] = []
        receipts: list[dict[str, Any]] = []
        total_rows = 0
        total_bytes = 0

        for object_id in sorted(wanted):
            relation = relations[object_id]
            manifest = manifests[str(relation["dataset_manifest_id"])]
            storage_rows = self._catalog.records(
                "storage.objects", where={"id": str(relation["object_id"])}
            )
            receipt = storage_rows[0] if storage_rows else None
            if receipt is None:
                raise MalformedEventError(f"canonical source receipt is incomplete: {object_id}")
            if relation["object_kind"] != "MARKET_BARS":
                raise MalformedEventError(f"source {object_id} is not MARKET_BARS")
            if manifest["status"] != "AVAILABLE" or receipt["status"] != "AVAILABLE":
                raise MalformedEventError(f"source {object_id} is not AVAILABLE")
            if not _source_targets_instrument(manifest, relation, instrument_id):
                raise MalformedEventError(f"source {object_id} belongs to another instrument shard")
            if manifest["resolution"] != definition.resolution:
                raise MalformedEventError(f"source {object_id} resolution does not match the definition")
            if (
                manifest["schema_version"] not in SUPPORTED_MARKET_BAR_SCHEMA_VERSIONS
                or receipt["schema_version"] != manifest["schema_version"]
            ):
                raise MalformedEventError(f"source {object_id} schema version is not canonical")
            if receipt["file_format"] != "PARQUET":
                raise MalformedEventError(f"source {object_id} is not Parquet")
            if int(relation["row_count"]) != int(receipt["row_count"]):
                raise MalformedEventError(f"source {object_id} row count receipt mismatch")
            total_rows += int(receipt["row_count"])
            total_bytes += int(receipt["byte_size"])
            if total_rows > MAX_FEATURE_SOURCE_ROWS or total_bytes > MAX_FEATURE_SOURCE_BYTES:
                raise MalformedEventError(
                    "canonical source set exceeds the bounded feature materialization budget"
                )
            relation_start = _timestamp(relation["period_start"], "dataset_object.period_start")
            relation_end = _timestamp(relation["period_end"], "dataset_object.period_end")
            storage_start = _timestamp(receipt["period_start"], "storage_object.period_start")
            storage_end = _timestamp(receipt["period_end"], "storage_object.period_end")
            if (relation_start, relation_end) != (storage_start, storage_end):
                raise MalformedEventError(f"source {object_id} period receipt mismatch")
            if not self._object_store.owns_receipt(
                str(receipt["storage_provider"]), str(receipt["bucket_name"])
            ):
                raise MalformedEventError(f"source {object_id} storage identity does not match the reader")

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
                    downloaded = hashlib.sha256()
                    downloaded_size = 0
                    while chunk := stream.read(1024 * 1024):
                        downloaded.update(chunk)
                        downloaded_size += len(chunk)
                        seekable.write(chunk)
                    if (
                        downloaded.hexdigest() != str(receipt["content_hash"])
                        or downloaded_size != int(receipt["byte_size"])
                    ):
                        raise MalformedEventError(
                            f"source {object_id} downloaded object hash or size mismatch"
                        )
                    seekable.seek(0)
                    parquet = pq.ParquetFile(seekable)
                    if not _matches_source_parquet_schema(
                        parquet.schema_arrow,
                        manifest=manifest,
                    ):
                        raise MalformedEventError(
                            f"source {object_id} Parquet schema does not match canonical bars"
                        )
                    decoded_rows = 0
                    previous: datetime | None = None
                    for batch in parquet.iter_batches(batch_size=FEATURE_READ_BATCH_ROWS):
                        decoded_rows += batch.num_rows
                        for row_index, row in enumerate(batch.to_pylist(), start=decoded_rows - batch.num_rows):
                            if str(row["instrument_id"]) != instrument_id:
                                continue
                            moment = _timestamp(
                                row["bar_start_at"], f"source {object_id} row {row_index}.bar_start_at"
                            )
                            if previous is not None and moment <= previous:
                                raise MalformedEventError(
                                    f"source {object_id} bars are not strictly ordered and unique"
                                )
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
                    if decoded_rows != int(receipt["row_count"]):
                        raise MalformedEventError(
                            f"source {object_id} decoded row count does not match receipt"
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
        provider_rows = self._catalog.records("market_data.providers", where={"id": provider_id})
        provider = provider_rows[0] if provider_rows else None
        expected_provider = {
            "code": INTERNAL_PROVIDER_CODE,
            "display_name": "Idea2Strategy Derived Data",
            "rights_version": "internal-derived-v1",
            "status": "ACTIVE",
        }
        feed_id = feature_output_feed_id(definition)
        feed_rows = self._catalog.records("market_data.feeds", where={"id": feed_id})
        feed = feed_rows[0] if feed_rows else None
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
        period_end: datetime,
    ) -> FeatureOutputTarget:
        manifest_id = str(
            deterministic_uuid("feature-output-manifest", materialization_id, FEATURE_SERIES_SCHEMA_VERSION)
        )
        existing_rows = self._catalog.records(
            "market_data.dataset_manifests", where={"id": manifest_id}
        )
        existing = existing_rows[0] if existing_rows else None
        if existing is not None:
            expected_existing = {
                "feed_id": feed_id,
                "instrument_id": instrument_id,
                "data_layer": "DERIVED",
                "resolution": resolution,
                "period_start": iso_utc(period_start),
                "period_end": iso_utc(period_end),
                "schema_version": FEATURE_SERIES_SCHEMA_VERSION,
            }
            if any(
                (
                    iso_utc(_timestamp(existing[key], f"manifest.{key}"))
                    if key.startswith("period_")
                    else str(existing.get(key))
                )
                != value
                for key, value in expected_existing.items()
            ):
                raise MalformedEventError(
                    "deterministic output manifest identity conflicts with immutable canonical state"
                )
            return FeatureOutputTarget(feed_id, manifest_id, int(existing["revision_number"]))
        siblings = [
            row
            for row in self._catalog.records(
                "market_data.dataset_manifests", where={"feed_id": feed_id}
            )
            if str(row.get("instrument_id")) == instrument_id
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
        run_id = feature_output_run_id(command_id)
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
        input_hash = provisional.input_dataset_set_hash
        existing_run = self._catalog.pipeline_run(run_id)
        existing_materializations = self._catalog.records(
            "market_data.feature_materializations", where={"pipeline_run_id": run_id}
        )
        if (
            existing_run is not None
            and existing_run["input_hash"] != input_hash
            or any(str(row["id"]) != provisional.materialization_id for row in existing_materializations)
        ):
            raise MalformedEventError(f"command_id {command_id!r} was already used with different input")
        target = self._target_resolver.resolve_target(
            feed_id=feed_id,
            materialization_id=provisional.materialization_id,
            instrument_id=instrument_id,
            resolution=definition.resolution,
            period_start=period_start,
            period_end=period_end,
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
                "idempotency_key": run_id,
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
