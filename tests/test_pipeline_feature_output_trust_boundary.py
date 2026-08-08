from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from apps.common.errors import MalformedEventError, PortNotConfiguredError
from apps.pipeline_worker.feature_output import (
    CanonicalFeatureSourceReader,
    DeterministicFeatureOutputResolver,
    ProductionFeatureMaterializationPort,
    feature_output_feed_code,
    feature_output_feed_id,
    feature_output_feed_version,
    feature_output_provider_id,
    feature_output_run_id,
)
from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.contracts import (
    SCHEMA_VERSION,
    bar_schema,
    deterministic_uuid,
    stable_shard_key,
)
from market_pipeline_lib.features import (
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    production_rsi_14_definition,
)
from market_pipeline_lib.features.definitions import OFFICIAL_RSI_14_HASH, OFFICIAL_RSI_14_PARAMETERS
from market_pipeline_lib.storage import LocalObjectStore, VerificationResult

CATALOG_VERSION = "10000000-0000-4000-8000-000000000001"
INSTRUMENT = "20000000-0000-4000-8000-000000000001"
OTHER_INSTRUMENT_SAME_SHARD = "20000000-0000-4000-8000-000000000003"
SOURCE_PROVIDER = "30000000-0000-4000-8000-000000000001"
SOURCE_FEED = "40000000-0000-4000-8000-000000000001"
SOURCE_MANIFEST = "50000000-0000-4000-8000-000000000001"
SOURCE_OBJECT = "60000000-0000-4000-8000-000000000001"
SOURCE_STORAGE = "70000000-0000-4000-8000-000000000001"
CURRENT_SOURCE_MANIFEST = "50000000-0000-4000-8000-000000000002"
CURRENT_SOURCE_OBJECT = "60000000-0000-4000-8000-000000000002"
PERIOD_START = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
PERIOD_END = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
LEGACY_LOADER_SCHEMA_VERSION = "market-bars/1"


def definition() -> FeatureDefinition:
    return FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="1m",
        parameters={"window": 2, "price_field": "close"},
    )


def canonical_bar_table() -> pa.Table:
    moments = [PERIOD_START + timedelta(minutes=offset) for offset in range(3)]
    values = {
        "instrument_id": [INSTRUMENT] * 3,
        "provider_symbol": ["AAPL"] * 3,
        "bar_start_at": moments,
        "session_date_et": [date(2026, 1, 5)] * 3,
        "open": [100.0, 101.0, 102.0],
        "high": [101.0, 102.0, 103.0],
        "low": [99.0, 100.0, 101.0],
        "close": [100.0, 101.0, 102.0],
        "volume": [1000, 1001, 1002],
        "trade_count": [10, 11, 12],
        "vwap": [100.0, 101.0, 102.0],
    }
    schema = bar_schema(False)
    return pa.Table.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in schema],
        schema=schema,
    )


def legacy_loader_bar_table(*, derived: bool) -> pa.Table:
    canonical = canonical_bar_table()
    fields = list(canonical.schema.remove_metadata())
    arrays = list(canonical.columns)
    if derived:
        fields.extend(
            [
                pa.field("source_bar_count", pa.int16(), nullable=False),
                pa.field("source_minutes", pa.int16(), nullable=False),
            ]
        )
        arrays.extend(
            [
                pa.array([1, 1, 1], type=pa.int16()),
                pa.array([60, 60, 60], type=pa.int16()),
            ]
        )
    schema = pa.schema(
        fields,
        metadata={
            b"schema_version": LEGACY_LOADER_SCHEMA_VERSION.encode("ascii"),
            b"processing_version": b"market-loader/1.0.0",
            b"provider": b"alpaca",
            b"feed": b"sip",
            b"adjustment": b"all",
            b"session_scope": b"regular",
            b"resolution": b"1m",
            b"period_start": b"2026-01-05",
            b"period_end": b"2026-01-06",
            b"revision": b"1",
            b"manifest_id": SOURCE_MANIFEST.encode("ascii"),
            b"created_at": b"2026-01-05T15:00:00+00:00",
        },
    )
    return pa.Table.from_arrays(arrays, schema=schema)


def sharded_bar_table() -> pa.Table:
    target = canonical_bar_table()
    moments = [PERIOD_START + timedelta(minutes=offset) for offset in range(3)]
    values = {
        "instrument_id": [OTHER_INSTRUMENT_SAME_SHARD] * 3,
        "provider_symbol": ["MSFT"] * 3,
        "bar_start_at": moments,
        "session_date_et": [date(2026, 1, 5)] * 3,
        "open": [200.0, 201.0, 202.0],
        "high": [201.0, 202.0, 203.0],
        "low": [199.0, 200.0, 201.0],
        "close": [200.0, 201.0, 202.0],
        "volume": [2000, 2001, 2002],
        "trade_count": [20, 21, 22],
        "vwap": [200.0, 201.0, 202.0],
    }
    schema = bar_schema(False)
    other = pa.Table.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in schema],
        schema=schema,
    )
    return pa.concat_tables([target, other])


def seed_catalog(
    tmp_path: Path,
    *,
    include_output_feed: bool = True,
    relation_row_count: int | None = None,
    source_table: pa.Table | None = None,
    manifest_instrument_id: str | None = INSTRUMENT,
    relation_shard_key: str = INSTRUMENT,
    source_schema_version: str = SCHEMA_VERSION,
    catalog: Any | None = None,
) -> tuple[Any, LocalObjectStore, LocalObjectStore, FeatureDefinition]:
    catalog = catalog or LocalCatalog(tmp_path / "catalog")
    published = FeatureDefinitionRegistry(catalog).publish(definition())
    source_store = LocalObjectStore(tmp_path / "source-objects", bucket_name="market-data")
    output_store = LocalObjectStore(tmp_path / "output-objects", bucket_name="market-data")

    catalog.upsert(
        "market_data.providers",
        {
            "id": SOURCE_PROVIDER,
            "code": "ALPACA",
            "display_name": "Alpaca",
            "rights_version": "test-v1",
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
        },
    )
    catalog.upsert(
        "market_data.feeds",
        {
            "id": SOURCE_FEED,
            "provider_id": SOURCE_PROVIDER,
            "code": "ALPACA_SIP_RAW_1M",
            "data_kind": "BARS",
            "resolution": "1m",
            "timezone_name": "America/New_York",
            "feed_version": "alpaca-sip-raw-1m-v1",
            "created_at": "2026-01-01T00:00:00Z",
            "retired_at": None,
        },
    )
    if include_output_feed:
        provider_id = feature_output_provider_id()
        catalog.upsert(
            "market_data.providers",
            {
                "id": provider_id,
                "code": "IDEA2STRATEGY_INTERNAL",
                "display_name": "Idea2Strategy Derived Data",
                "rights_version": "internal-derived-v1",
                "status": "ACTIVE",
                "created_at": "2026-01-01T00:00:00Z",
            },
        )
        catalog.upsert(
            "market_data.feeds",
            {
                "id": feature_output_feed_id(published),
                "provider_id": provider_id,
                "code": feature_output_feed_code(published),
                "data_kind": "FEATURE_SERIES",
                "resolution": "1m",
                "timezone_name": "UTC",
                "feed_version": feature_output_feed_version(published),
                "created_at": "2026-01-01T00:00:00Z",
                "retired_at": None,
            },
        )
    catalog.upsert(
        "market_data.instruments",
        {
            "id": INSTRUMENT,
            "asset_type": "STOCK",
            "primary_exchange_mic": "XNYS",
            "currency_code": "USD",
            "provider_reference": "AAPL",
            "listed_at": None,
            "delisted_at": None,
            "created_at": "2026-01-01T00:00:00Z",
        },
    )

    table = source_table or canonical_bar_table()
    path = tmp_path / "bars.parquet"
    pq.write_table(table, path, compression="zstd", version="2.6")
    receipt = source_store.put(path, "market-data/source.parquet")
    catalog.publish_manifest(
        {
            "id": SOURCE_MANIFEST,
            "feed_id": SOURCE_FEED,
            "instrument_id": manifest_instrument_id,
            "data_layer": "RAW",
            "resolution": "1m",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": PERIOD_START.isoformat(),
            "period_end": PERIOD_END.isoformat(),
            "schema_version": source_schema_version,
            "dataset_hash": "a" * 64,
            "supersedes_manifest_id": None,
            "created_at": PERIOD_END.isoformat(),
            "available_at": PERIOD_END.isoformat(),
        }
    )
    catalog.stage_object(
        {
            "id": SOURCE_STORAGE,
            "status": "AVAILABLE",
            "storage_provider": receipt.storage_provider,
            "bucket_name": receipt.bucket_name,
            "object_key": receipt.object_key,
            "provider_version_id": receipt.provider_version_id,
            "content_hash": receipt.content_hash,
            "byte_size": receipt.byte_size,
            "file_format": "PARQUET",
            "compression_codec": "ZSTD",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": source_schema_version,
            "row_count": table.num_rows,
            "period_start": PERIOD_START.isoformat(),
            "period_end": PERIOD_END.isoformat(),
            "encryption_key_ref": None,
            "retention_policy_version": "UNSPECIFIED",
            "retention_until": None,
            "legal_hold": False,
            "created_at": PERIOD_END.isoformat(),
            "verified_at": PERIOD_END.isoformat(),
            "quarantined_at": None,
            "superseded_at": None,
            "deleted_at": None,
        },
        {
            "id": SOURCE_OBJECT,
            "dataset_manifest_id": SOURCE_MANIFEST,
            "object_id": SOURCE_STORAGE,
            "object_kind": "MARKET_BARS",
            "partition_granularity": "DAY",
            "partition_start": "2026-01-05",
            "partition_end": "2026-01-06",
            "period_start": PERIOD_START.isoformat(),
            "period_end": PERIOD_END.isoformat(),
            "shard_key": relation_shard_key,
            "part_number": 1,
            "row_count": table.num_rows if relation_row_count is None else relation_row_count,
            "min_instrument_id": INSTRUMENT,
            "max_instrument_id": (
                OTHER_INSTRUMENT_SAME_SHARD
                if manifest_instrument_id is None
                else INSTRUMENT
            ),
        },
    )
    return catalog, source_store, output_store, published


def payload(published: FeatureDefinition) -> dict[str, object]:
    return {
        "definition_hash": published.definition_hash,
        "instrument_id": INSTRUMENT,
        "source_dataset_object_ids": [SOURCE_OBJECT],
        "period_start": PERIOD_START.isoformat(),
        "period_end": PERIOD_END.isoformat(),
    }


def production_port(
    tmp_path: Path,
    catalog: Any,
    source_store: LocalObjectStore,
    output_store: LocalObjectStore,
) -> ProductionFeatureMaterializationPort:
    return ProductionFeatureMaterializationPort(
        catalog,
        output_store,
        source_reader=CanonicalFeatureSourceReader(catalog, source_store),
        target_resolver=DeterministicFeatureOutputResolver(catalog),
        staging_root=tmp_path / "staging",
    )


def add_current_source_revision(catalog: Any) -> None:
    previous = catalog.records(
        "market_data.dataset_manifests", where={"id": SOURCE_MANIFEST}
    )[0]
    relation = catalog.records("market_data.dataset_objects", where={"id": SOURCE_OBJECT})[0]
    catalog.publish_manifest(
        {
            **previous,
            "id": CURRENT_SOURCE_MANIFEST,
            "revision_number": 2,
            "dataset_hash": "b" * 64,
            "supersedes_manifest_id": SOURCE_MANIFEST,
        }
    )
    catalog.upsert(
        "market_data.dataset_objects",
        {
            **relation,
            "id": CURRENT_SOURCE_OBJECT,
            "dataset_manifest_id": CURRENT_SOURCE_MANIFEST,
        },
    )


class FailsFirstPutStore:
    def __init__(self, delegate: LocalObjectStore) -> None:
        self.delegate = delegate
        self.failed = False

    def put(self, source_path: Path, object_key: str):
        if not self.failed:
            self.failed = True
            raise OSError("simulated output upload failure")
        return self.delegate.put(source_path, object_key)

    def __getattr__(self, name: str):
        return getattr(self.delegate, name)


def test_materialization_derives_every_output_identity_and_owns_the_pipeline_run(
    tmp_path: Path,
) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)

    first = port.materialize(payload(published), command_id="feature-command-1")
    second = port.materialize(payload(published), command_id="feature-command-1")

    assert first == second
    assert first["output_feed_id"] == feature_output_feed_id(published)
    assert first["output_revision_number"] == 1
    assert first["output_dataset_manifest_id"] == str(
        deterministic_uuid(
            "feature-output-manifest",
            first["materialization_id"],
            "feature-series.parquet.v1",
        )
    )
    run = catalog.pipeline_run(feature_output_run_id("feature-command-1"))
    assert run is not None
    assert run["status"] == "SUCCEEDED"
    assert run["output_hash"] == first["result_hash"]
    materialization = catalog.records("market_data.feature_materializations")[0]
    assert run["input_hash"] == materialization["input_dataset_set_hash"]
    assert run["idempotency_key"] == feature_output_run_id("feature-command-1")
    assert len(catalog.records("market_data.pipeline_runs")) == 1
    assert len(catalog.records("market_data.feature_materializations")) == 1
    assert len(catalog.records("storage.objects")) == 2


def test_multi_instrument_shard_materializes_only_the_requested_instrument(
    tmp_path: Path,
) -> None:
    shard = stable_shard_key(INSTRUMENT, 2)
    assert shard == stable_shard_key(OTHER_INSTRUMENT_SAME_SHARD, 2)
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        source_table=sharded_bar_table(),
        manifest_instrument_id=None,
        relation_shard_key=shard,
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    result = port.materialize(payload(published), command_id="feature-command-sharded")

    assert result["row_count"] == 2


@pytest.mark.parametrize("derived", [False, True])
def test_exact_legacy_loader_bar_schemas_are_accepted(tmp_path: Path, derived: bool) -> None:
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        source_table=legacy_loader_bar_table(derived=derived),
        source_schema_version=LEGACY_LOADER_SCHEMA_VERSION,
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    result = port.materialize(payload(published), command_id=f"legacy-loader-{derived}")

    assert result["status"] == "SUCCEEDED"
    assert result["row_count"] == 2


def test_legacy_loader_schema_with_an_unknown_field_is_rejected(tmp_path: Path) -> None:
    table = legacy_loader_bar_table(derived=True).append_column(
        "unexpected", pa.array([1, 1, 1], type=pa.int8())
    )
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        source_table=table,
        source_schema_version=LEGACY_LOADER_SCHEMA_VERSION,
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="Parquet schema"):
        port.materialize(payload(published), command_id="legacy-loader-unknown-field")


def test_legacy_loader_schema_requires_matching_producer_metadata(tmp_path: Path) -> None:
    table = legacy_loader_bar_table(derived=False)
    metadata = {**(table.schema.metadata or {}), b"provider": b"unknown"}
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        source_table=table.replace_schema_metadata(metadata),
        source_schema_version=LEGACY_LOADER_SCHEMA_VERSION,
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="Parquet schema"):
        port.materialize(payload(published), command_id="legacy-loader-wrong-metadata")


def test_legacy_loader_schema_rejects_raw_bars_for_adjusted_features(tmp_path: Path) -> None:
    table = legacy_loader_bar_table(derived=False)
    metadata = {**(table.schema.metadata or {}), b"adjustment": b"raw"}
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        source_table=table.replace_schema_metadata(metadata),
        source_schema_version=LEGACY_LOADER_SCHEMA_VERSION,
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="Parquet schema"):
        port.materialize(payload(published), command_id="legacy-loader-raw-bars")


def test_multi_instrument_manifest_rejects_an_unrelated_shard(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path,
        manifest_instrument_id=None,
        relation_shard_key="s01-of-2",
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="shard"):
        port.materialize(payload(published), command_id="feature-command-wrong-shard")


@pytest.mark.parametrize(
    "field",
    ["output_feed_id", "output_dataset_manifest_id", "output_revision_number", "bars", "sources", "pipeline_run_id"],
)
def test_legacy_caller_attestations_are_rejected_before_any_write(tmp_path: Path, field: str) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    request = {**payload(published), field: "caller-controlled"}
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="fields mismatch"):
        port.materialize(request, command_id="feature-command-untrusted")

    assert catalog.records("market_data.pipeline_runs") == []
    assert catalog.records("market_data.feature_materializations") == []


def test_missing_authority_seed_fails_closed_without_a_pipeline_run_or_output(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path, include_output_feed=False)
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(PortNotConfiguredError, match="authority-owned feature feed seed"):
        port.materialize(payload(published), command_id="feature-command-no-feed")

    assert catalog.records("market_data.pipeline_runs") == []
    assert catalog.records("market_data.feature_materializations") == []
    assert list((tmp_path / "output-objects").rglob("*.parquet")) == []


def test_canonical_source_row_count_mismatch_fails_before_run_creation(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path, relation_row_count=4)
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="row count"):
        port.materialize(payload(published), command_id="feature-command-bad-source")

    assert catalog.records("market_data.pipeline_runs") == []
    assert catalog.records("market_data.feature_materializations") == []


def test_changed_input_under_the_same_command_id_fails_closed(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)
    port.materialize(payload(published), command_id="feature-command-conflict")

    changed = {**payload(published), "period_end": (PERIOD_END - timedelta(minutes=1)).isoformat()}
    with pytest.raises(MalformedEventError, match="different input"):
        port.materialize(changed, command_id="feature-command-conflict")


def test_failed_worker_owned_run_is_retryable_without_orphaning_an_output(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    failing_output = FailsFirstPutStore(output_store)
    port = ProductionFeatureMaterializationPort(
        catalog,
        failing_output,  # type: ignore[arg-type]
        source_reader=CanonicalFeatureSourceReader(catalog, source_store),
        target_resolver=DeterministicFeatureOutputResolver(catalog),
        staging_root=tmp_path / "staging",
    )

    with pytest.raises(OSError, match="simulated output upload failure"):
        port.materialize(payload(published), command_id="feature-command-retry")

    run_id = feature_output_run_id("feature-command-retry")
    assert catalog.pipeline_run(run_id)["status"] == "FAILED"  # type: ignore[index]
    assert list((tmp_path / "output-objects").rglob("*.parquet")) == []

    result = port.materialize(payload(published), command_id="feature-command-retry")

    assert result["status"] == "SUCCEEDED"
    assert catalog.pipeline_run(run_id)["status"] == "SUCCEEDED"  # type: ignore[index]
    assert len(catalog.records("market_data.pipeline_runs")) == 1
    assert len(catalog.records("market_data.feature_materializations")) == 1


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "must be an object"),
        ({}, "fields mismatch"),
    ],
)
def test_malformed_command_documents_fail_before_catalog_access(tmp_path: Path, document: object, message: str) -> None:
    catalog, source_store, output_store, _published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match=message):
        port.materialize(document, command_id="malformed-document")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("definition_hash", "", "non-empty string"),
        ("instrument_id", "not-a-uuid", "UUID string"),
        ("period_start", "not-a-time", "ISO-8601"),
        ("period_start", "2026-01-05T14:30:00", "timezone"),
        ("period_end", PERIOD_START.isoformat(), "must be after"),
        ("source_dataset_object_ids", "not-an-array", "must be an array"),
    ],
)
def test_invalid_worker_owned_inputs_fail_before_a_run(tmp_path: Path, field: str, value: object, message: str) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    request = {**payload(published), field: value}
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match=message):
        port.materialize(request, command_id="invalid-worker-input")

    assert catalog.records("market_data.pipeline_runs") == []


@pytest.mark.parametrize("command_id", ["", "x" * 161, None])
def test_invalid_worker_command_identity_is_rejected(tmp_path: Path, command_id: object) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="1..160"):
        port.materialize(payload(published), command_id=command_id)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("source_ids", "message"),
    [
        ([], "non-empty array"),
        ([SOURCE_OBJECT, SOURCE_OBJECT], "must be unique"),
        (["60000000-0000-4000-8000-000000000002"], "is missing"),
    ],
)
def test_invalid_canonical_source_identity_sets_are_rejected(
    tmp_path: Path, source_ids: list[str], message: str
) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)
    request = {**payload(published), "source_dataset_object_ids": source_ids}

    with pytest.raises(MalformedEventError, match=message):
        port.materialize(request, command_id="invalid-source-set")


def test_source_manifest_must_cover_the_requested_period(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    manifest = catalog.records("market_data.dataset_manifests")[0]
    catalog.upsert(
        "market_data.dataset_manifests",
        {**manifest, "period_end": (PERIOD_END - timedelta(minutes=1)).isoformat()},
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="does not cover"):
        port.materialize(payload(published), command_id="short-source-period")


def test_tampered_source_bytes_fail_exact_version_verification(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    receipt = catalog.records("storage.objects")[0]
    source_store.path_for(receipt["object_key"]).write_bytes(b"tampered")
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="exact object verification failed"):
        port.materialize(payload(published), command_id="tampered-source")


def test_requested_period_must_contain_canonical_bars(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    request = {
        **payload(published),
        "period_start": (PERIOD_START + timedelta(minutes=10)).isoformat(),
        "period_end": (PERIOD_START + timedelta(minutes=20)).isoformat(),
    }
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="contain no bars"):
        port.materialize(request, command_id="empty-request-window")


def test_mismatched_authority_seed_fails_closed(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    provider_id = feature_output_provider_id()
    provider = next(row for row in catalog.records("market_data.providers") if row["id"] == provider_id)
    catalog.upsert("market_data.providers", {**provider, "rights_version": "unapproved"})
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(PortNotConfiguredError, match="does not match"):
        port.materialize(payload(published), command_id="mismatched-authority-seed")


@pytest.mark.parametrize(
    ("table", "record_id", "updates", "message"),
    [
        ("market_data.dataset_objects", SOURCE_OBJECT, {"object_kind": "FEATURE_SERIES"}, "not MARKET_BARS"),
        ("market_data.dataset_manifests", SOURCE_MANIFEST, {"status": "QUARANTINED"}, "not AVAILABLE"),
        ("storage.objects", SOURCE_STORAGE, {"status": "QUARANTINED"}, "not AVAILABLE"),
        (
            "market_data.dataset_manifests",
            SOURCE_MANIFEST,
            {"instrument_id": "20000000-0000-4000-8000-000000000002"},
            "another instrument",
        ),
        ("market_data.dataset_manifests", SOURCE_MANIFEST, {"resolution": "1d"}, "resolution"),
        ("storage.objects", SOURCE_STORAGE, {"schema_version": "unknown"}, "schema version"),
        ("storage.objects", SOURCE_STORAGE, {"file_format": "CSV"}, "not Parquet"),
        (
            "storage.objects",
            SOURCE_STORAGE,
            {"period_end": (PERIOD_END - timedelta(minutes=1)).isoformat()},
            "period receipt",
        ),
    ],
)
def test_untrusted_canonical_source_state_fails_closed(
    tmp_path: Path,
    table: str,
    record_id: str,
    updates: dict[str, object],
    message: str,
) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    record = next(row for row in catalog.records(table) if row["id"] == record_id)
    catalog.upsert(table, {**record, **updates})
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match=message):
        port.materialize(payload(published), command_id="untrusted-canonical-source")

    assert catalog.records("market_data.pipeline_runs") == []


def test_only_the_highest_available_exact_manifest_revision_is_required(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    add_current_source_revision(catalog)
    port = production_port(tmp_path, catalog, source_store, output_store)

    result = port.materialize(
        {**payload(published), "source_dataset_object_ids": [CURRENT_SOURCE_OBJECT]},
        command_id="current-source-revision",
    )

    assert result["status"] == "SUCCEEDED"
    assert catalog.records(
        "market_data.dataset_manifests", where={"id": SOURCE_MANIFEST}
    )[0]["status"] == "AVAILABLE"
    with pytest.raises(MalformedEventError, match="current AVAILABLE revision"):
        port.materialize(payload(published), command_id="stale-source-revision")


def test_omitted_object_from_current_manifest_revision_is_rejected(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    add_current_source_revision(catalog)
    storage = catalog.records("storage.objects")[0]
    relation = catalog.records("market_data.dataset_objects")[0]
    catalog.upsert(
        "storage.objects",
        {**storage, "id": "70000000-0000-4000-8000-000000000002", "object_key": "market-data/second.parquet"},
    )
    catalog.upsert(
        "market_data.dataset_objects",
        {
            **relation,
            "id": "60000000-0000-4000-8000-000000000003",
            "dataset_manifest_id": CURRENT_SOURCE_MANIFEST,
            "object_id": "70000000-0000-4000-8000-000000000002",
            "part_number": 2,
        },
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="complete authoritative object set"):
        port.materialize(
            {**payload(published), "source_dataset_object_ids": [CURRENT_SOURCE_OBJECT]},
            command_id="omitted-source-object",
        )


def test_authoritative_object_period_gap_is_rejected(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    relation = catalog.records("market_data.dataset_objects", where={"id": SOURCE_OBJECT})[0]
    receipt = catalog.records("storage.objects", where={"id": SOURCE_STORAGE})[0]
    shortened = (PERIOD_END - timedelta(minutes=5)).isoformat()
    catalog.upsert("market_data.dataset_objects", {**relation, "period_end": shortened})
    catalog.upsert("storage.objects", {**receipt, "period_end": shortened})
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="period gap"):
        port.materialize(payload(published), command_id="source-period-gap")


def test_existing_output_manifest_immutable_drift_is_rejected(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)
    result = port.materialize(payload(published), command_id="manifest-drift")
    manifest = catalog.records(
        "market_data.dataset_manifests", where={"id": result["output_dataset_manifest_id"]}
    )[0]
    catalog.upsert("market_data.dataset_manifests", {**manifest, "schema_version": "drifted.v9"})

    with pytest.raises(MalformedEventError, match="immutable canonical state"):
        port.materialize(payload(published), command_id="manifest-drift")


def test_source_receipt_bucket_is_bound_to_the_reader(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    receipt = catalog.records("storage.objects", where={"id": SOURCE_STORAGE})[0]
    catalog.upsert("storage.objects", {**receipt, "bucket_name": "another-bucket"})
    port = production_port(tmp_path, catalog, source_store, output_store)

    with pytest.raises(MalformedEventError, match="storage identity"):
        port.materialize(payload(published), command_id="wrong-source-bucket")


def test_downloaded_source_bytes_are_hashed_instead_of_trusting_head_only(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)

    class TamperedDownloadStore:
        def owns_receipt(self, storage_provider: str, bucket_name: str) -> bool:
            return source_store.owns_receipt(storage_provider, bucket_name)

        def verify_version(self, *args, **kwargs):
            receipt = catalog.records("storage.objects", where={"id": SOURCE_STORAGE})[0]
            return VerificationResult(True, receipt["content_hash"], receipt["byte_size"])

        def open_version(self, *args, **kwargs):
            return BytesIO(b"tampered after HEAD")

    port = ProductionFeatureMaterializationPort(
        catalog,
        output_store,
        source_reader=CanonicalFeatureSourceReader(catalog, TamperedDownloadStore()),  # type: ignore[arg-type]
        target_resolver=DeterministicFeatureOutputResolver(catalog),
        staging_root=tmp_path / "staging",
    )

    with pytest.raises(MalformedEventError, match="downloaded object hash"):
        port.materialize(payload(published), command_id="tampered-download")


def test_source_object_count_is_bounded_before_catalog_reads(tmp_path: Path) -> None:
    catalog, source_store, output_store, published = seed_catalog(tmp_path)
    port = production_port(tmp_path, catalog, source_store, output_store)
    too_many = [str(deterministic_uuid("too-many-source", str(index))) for index in range(513)]

    with pytest.raises(MalformedEventError, match="at most 512"):
        port.materialize(
            {**payload(published), "source_dataset_object_ids": too_many},
            command_id="too-many-source-objects",
        )


def test_approved_official_rsi_row_resolves_exact_feed_and_existing_calculator() -> None:
    official = FeatureDefinition.from_record(
        {
            "id": "0f1b0000-0000-4000-8000-000000000001",
            "element_catalog_version_id": "0f1a0000-0000-4000-8000-000000000001",
            "feature_code": "RSI_14",
            "calculator_version": "rsi:1.0.0",
            "resolution": "1m",
            "normalized_parameters": OFFICIAL_RSI_14_PARAMETERS,
            "output_value_type": "NUMBER",
            "required_history_points": 15,
            "definition_hash": OFFICIAL_RSI_14_HASH,
        }
    )

    assert feature_output_feed_id(official) == "063f8f27-5c6a-5348-b2bb-abc3c634149c"
    assert feature_output_feed_code(official) == "FEATURE_RSI_14_1M_RSI_1_0_0"
    assert feature_output_feed_version(official) == "rsi-1.0.0+feature-series.parquet.v1"
    bars = tuple(
        BarPoint(
            bar_start_at=PERIOD_START + timedelta(minutes=index),
            open=Decimal(100 + index),
            high=Decimal(101 + index),
            low=Decimal(99 + index),
            close=Decimal(100 + index),
            volume=1000 + index,
        )
        for index in range(15)
    )
    assert len(official.calculator().compute(bars, OFFICIAL_RSI_14_PARAMETERS)) == 1


@pytest.mark.parametrize(
    ("resolution", "definition_id", "feed_id", "feed_code"),
    [
        (
            "30m",
            "4b1c6801-0259-5176-a857-0e5ea923d898",
            "57794d8c-2254-53e4-966e-44f97edd9e6a",
            "FEATURE_RSI_14_30M_RSI_1_0_0",
        ),
        (
            "1h",
            "2e18c093-5d4e-5d9a-bd22-b7e5679f1a3e",
            "28012549-4f45-56d3-8bb6-329e4c7a9d77",
            "FEATURE_RSI_14_1H_RSI_1_0_0",
        ),
        (
            "4h",
            "1b2785bd-20f0-50a2-ae96-6a1f7bad74b9",
            "e1d7d508-aaf1-5ae9-8098-c4af870f6fa4",
            "FEATURE_RSI_14_4H_RSI_1_0_0",
        ),
        (
            "1d",
            "eddfb2d4-8586-5260-8fc9-9c8125990270",
            "6d2647f8-5caf-55ee-8821-869dc693f68a",
            "FEATURE_RSI_14_1D_RSI_1_0_0",
        ),
    ],
)
def test_production_rsi_definition_and_feed_follow_the_selected_resolution(
    resolution: str, definition_id: str, feed_id: str, feed_code: str,
) -> None:
    definition = production_rsi_14_definition(resolution)

    assert definition.id == definition_id
    assert definition.resolution == resolution
    assert feature_output_feed_id(definition) == feed_id
    assert feature_output_feed_code(definition) == feed_code
    assert feature_output_feed_version(definition) == "rsi-1.0.0+feature-series.parquet.v1"


@pytest.mark.integration
def test_production_command_commits_and_reconciles_through_postgres_16(
    tmp_path: Path, postgres_catalog: Any, admin_engine: Any
) -> None:
    from sqlalchemy import text

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO strategy.element_catalog_versions "
                "(id, language_version, schema_version, catalog_version, "
                " data_requirement_version, definition_hash, published_at) "
                "VALUES (:id, '248.0.0', '248', '248.0.0', '248.0.0', :digest, "
                "TIMESTAMPTZ '2026-01-01 00:00:00+00') "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": CATALOG_VERSION,
                "digest": hashlib.sha256(CATALOG_VERSION.encode("ascii")).hexdigest(),
            },
        )
    catalog, source_store, output_store, published = seed_catalog(
        tmp_path, catalog=postgres_catalog
    )
    port = production_port(tmp_path, catalog, source_store, output_store)

    first = port.materialize(payload(published), command_id="postgres-production-command")
    second = port.materialize(payload(published), command_id="postgres-production-command")

    assert first == second
    assert postgres_catalog.pipeline_run(feature_output_run_id("postgres-production-command"))[
        "status"
    ] == "SUCCEEDED"
    assert len(postgres_catalog.records("market_data.feature_materializations")) == 1
    assert len(
        postgres_catalog.records(
            "market_data.dataset_manifests", where={"id": first["output_dataset_manifest_id"]}
        )
    ) == 1
