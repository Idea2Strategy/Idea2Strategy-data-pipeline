from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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
from market_pipeline_lib.contracts import SCHEMA_VERSION, bar_schema, deterministic_uuid
from market_pipeline_lib.features import FeatureDefinition, FeatureDefinitionRegistry
from market_pipeline_lib.storage import LocalObjectStore

CATALOG_VERSION = "10000000-0000-4000-8000-000000000001"
INSTRUMENT = "20000000-0000-4000-8000-000000000001"
SOURCE_PROVIDER = "30000000-0000-4000-8000-000000000001"
SOURCE_FEED = "40000000-0000-4000-8000-000000000001"
SOURCE_MANIFEST = "50000000-0000-4000-8000-000000000001"
SOURCE_OBJECT = "60000000-0000-4000-8000-000000000001"
SOURCE_STORAGE = "70000000-0000-4000-8000-000000000001"
PERIOD_START = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
PERIOD_END = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)


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


def seed_catalog(
    tmp_path: Path,
    *,
    include_output_feed: bool = True,
    relation_row_count: int = 3,
) -> tuple[LocalCatalog, LocalObjectStore, LocalObjectStore, FeatureDefinition]:
    catalog = LocalCatalog(tmp_path / "catalog")
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

    path = tmp_path / "bars.parquet"
    pq.write_table(canonical_bar_table(), path, compression="zstd", version="2.6")
    receipt = source_store.put(path, "market-data/source.parquet")
    catalog.publish_manifest(
        {
            "id": SOURCE_MANIFEST,
            "feed_id": SOURCE_FEED,
            "instrument_id": INSTRUMENT,
            "data_layer": "RAW",
            "resolution": "1m",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": PERIOD_START.isoformat(),
            "period_end": PERIOD_END.isoformat(),
            "schema_version": SCHEMA_VERSION,
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
            "schema_version": SCHEMA_VERSION,
            "row_count": 3,
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
            "shard_key": INSTRUMENT,
            "part_number": 1,
            "row_count": relation_row_count,
            "min_instrument_id": INSTRUMENT,
            "max_instrument_id": INSTRUMENT,
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
    catalog: LocalCatalog,
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
    assert len(catalog.records("market_data.pipeline_runs")) == 1
    assert len(catalog.records("market_data.feature_materializations")) == 1
    assert len(catalog.records("storage.objects")) == 2


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
