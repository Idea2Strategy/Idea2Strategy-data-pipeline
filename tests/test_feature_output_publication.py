from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.features import (
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    MaterializationRequest,
    SourceObject,
)
from market_pipeline_lib.features.output import FEATURE_SERIES_SCHEMA_VERSION, FeatureOutputPublisher
from market_pipeline_lib.storage import LocalObjectStore

CATALOG_VERSION = "10000000-0000-4000-8000-000000000001"
INSTRUMENT = "20000000-0000-4000-8000-000000000001"
RUN = "30000000-0000-4000-8000-000000000001"
SOURCE_MANIFEST = "40000000-0000-4000-8000-000000000001"
SOURCE_OBJECT = "50000000-0000-4000-8000-000000000001"
OUTPUT_MANIFEST = "60000000-0000-4000-8000-000000000001"
FEED = "70000000-0000-4000-8000-000000000001"
PROVIDER = "80000000-0000-4000-8000-000000000001"
SOURCE_STORAGE = "90000000-0000-4000-8000-000000000001"


def definition() -> FeatureDefinition:
    return FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="1m",
        parameters={"window": 2, "price_field": "close"},
    )


def request(published: FeatureDefinition) -> MaterializationRequest:
    return MaterializationRequest(
        definition=published,
        instrument_id=INSTRUMENT,
        pipeline_run_id=RUN,
        sources=(
            SourceObject(
                dataset_object_id=SOURCE_OBJECT,
                dataset_manifest_id=SOURCE_MANIFEST,
                content_hash="a" * 64,
                partition_start="2026-01-05",
                partition_end="2026-01-06",
                row_count=3,
            ),
        ),
        bars=tuple(
            BarPoint(
                bar_start_at=datetime(2026, 1, 5, 14, 30 + offset, tzinfo=UTC),
                open=Decimal(100 + offset),
                high=Decimal(101 + offset),
                low=Decimal(99 + offset),
                close=Decimal(100 + offset),
                volume=1000,
            )
            for offset in range(3)
        ),
        period_start=datetime(2026, 1, 5, 14, 30, tzinfo=UTC),
        period_end=datetime(2026, 1, 5, 15, 0, tzinfo=UTC),
        source_watermark="ALPACA@2026-01-05T15:00:00Z",
        output_dataset_manifest_id=OUTPUT_MANIFEST,
        output_feed_id=FEED,
        output_revision_number=1,
    )


class TracingCatalog(LocalCatalog):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.events: list[tuple[str, str]] = []

    def publish_manifest(self, record: dict[str, object]) -> None:
        self.events.append(("manifest", str(record["status"])))
        super().publish_manifest(record)

    def upsert(self, table: str, record: dict[str, object]) -> None:
        if table == "market_data.feature_materializations":
            self.events.append(("materialization", str(record["status"])))
        super().upsert(table, record)


def make_materializer(tmp_path: Path, catalog: LocalCatalog | None = None):
    actual_catalog = catalog or TracingCatalog(tmp_path / "catalog")
    registry = FeatureDefinitionRegistry(actual_catalog)
    published = registry.publish(definition())
    store = LocalObjectStore(tmp_path / "objects")
    publisher = FeatureOutputPublisher(actual_catalog, store, staging_root=tmp_path / "staging")
    return actual_catalog, store, FeatureMaterializer(actual_catalog, registry, output_publisher=publisher), published


def test_feature_values_are_published_as_a_version_pinned_strict_parquet_dataset(tmp_path: Path) -> None:
    catalog, store, materializer, published = make_materializer(tmp_path)

    result = materializer.materialize(request(published))

    manifests = catalog.records("market_data.dataset_manifests")
    assert [(row["id"], row["status"], row["schema_version"]) for row in manifests] == [
        (OUTPUT_MANIFEST, "AVAILABLE", FEATURE_SERIES_SCHEMA_VERSION)
    ]
    storage = catalog.records("storage.objects")
    assert len(storage) == 1
    assert storage[0]["schema_version"] == FEATURE_SERIES_SCHEMA_VERSION
    assert storage[0]["provider_version_id"]
    assert storage[0]["content_hash"] == result.output_content_hash
    assert store.verify(storage[0]["object_key"], storage[0]["content_hash"]).ok

    with store.open_version(storage[0]["object_key"], storage[0]["provider_version_id"]) as stream:
        table = pq.read_table(stream)
    assert table.schema == pa.schema(
        [
            pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("value", pa.decimal128(38, 8), nullable=False),
        ]
    )
    assert table.column("bar_start_at").to_pylist() == sorted(table.column("bar_start_at").to_pylist())
    assert [str(value) for value in table.column("value").to_pylist()] == [
        "100.50000000",
        "101.50000000",
    ]
    assert result.verify_decoded_values(table)

    relation = catalog.records("market_data.dataset_objects")
    assert len(relation) == 1
    assert relation[0]["dataset_manifest_id"] == OUTPUT_MANIFEST
    assert relation[0]["object_kind"] == "FEATURE_SERIES"
    assert relation[0]["row_count"] == 2
    assert catalog.records("market_data.dataset_lineage") == [
        {
            "derived_manifest_id": OUTPUT_MANIFEST,
            "source_manifest_id": SOURCE_MANIFEST,
            "relation_type": "FEATURE_MATERIALIZED_FROM",
        }
    ]
    assert catalog.records("market_data.dataset_object_lineage")[0]["source_dataset_object_id"] == SOURCE_OBJECT

    assert isinstance(catalog, TracingCatalog)
    assert catalog.events.index(("manifest", "AVAILABLE")) < catalog.events.index(
        ("materialization", "SUCCEEDED")
    )


def test_duplicate_retry_reuses_identical_bytes_and_catalog_rows(tmp_path: Path) -> None:
    catalog, _store, materializer, published = make_materializer(tmp_path)
    first = materializer.materialize(request(published))
    first_storage = catalog.records("storage.objects")

    second = materializer.materialize(request(published))

    assert second.result_hash == first.result_hash
    assert second.output_content_hash == first.output_content_hash
    assert catalog.records("storage.objects") == first_storage
    assert len(catalog.records("market_data.dataset_objects")) == 1
    assert len(catalog.records("market_data.feature_materializations")) == 1


def test_out_of_order_feature_values_are_rejected_before_upload(tmp_path: Path) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    registry = FeatureDefinitionRegistry(catalog)
    published = registry.publish(definition())
    result = FeatureMaterializer(catalog, registry).materialize(request(published))
    publisher = FeatureOutputPublisher(
        catalog,
        LocalObjectStore(tmp_path / "objects"),
        staging_root=tmp_path / "staging",
    )

    with pytest.raises(ValueError, match="strictly increasing"):
        publisher.prepare(request(published), replace(result, values=tuple(reversed(result.values))))

    assert list((tmp_path / "objects").rglob("*.parquet")) == []


def test_catalog_failure_removes_a_newly_uploaded_version_and_staging_file(tmp_path: Path) -> None:
    class FailingCatalog(TracingCatalog):
        def stage_object(self, storage_record, dataset_object_record) -> None:
            raise RuntimeError("catalog unavailable")

    catalog = FailingCatalog(tmp_path / "catalog")
    catalog, store, materializer, published = make_materializer(tmp_path, catalog)

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        materializer.materialize(request(published))

    assert list((tmp_path / "objects").rglob("*.parquet")) == []
    assert list((tmp_path / "staging").rglob("*.parquet")) == []
    assert catalog.records("market_data.dataset_manifests") == []
    assert catalog.records("storage.objects") == []
    assert catalog.records("market_data.feature_materializations")[0]["status"] == "FAILED"


@pytest.mark.parametrize("feed,revision", [(None, 1), (FEED, None), (FEED, 0)])
def test_publication_requires_explicit_feed_and_positive_revision(
    tmp_path: Path, feed: str | None, revision: int | None
) -> None:
    catalog, _store, materializer, published = make_materializer(tmp_path)
    original = request(published)
    invalid = MaterializationRequest(
        **{
            **original.__dict__,
            "output_feed_id": feed,
            "output_revision_number": revision,
        }
    )
    with pytest.raises(ValueError, match="output_(feed_id|revision_number)"):
        materializer.materialize(invalid)
    assert catalog.records("market_data.feature_materializations") == []


@pytest.mark.integration
def test_feature_publication_commits_through_the_canonical_postgres_schema(
    tmp_path: Path, postgres_catalog, admin_engine
) -> None:
    from sqlalchemy import text

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO strategy.element_catalog_versions "
                "(id, language_version, schema_version, catalog_version, "
                " data_requirement_version, definition_hash, published_at) "
                "VALUES (:id, '1.0.0', '1', '1.0.0', '1.0.0', :digest, "
                " TIMESTAMPTZ '2026-01-01 00:00:00+00') "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": CATALOG_VERSION, "digest": "e" * 64},
        )
    postgres_catalog.upsert(
        "market_data.providers",
        {
            "id": PROVIDER,
            "code": "FEATURE_OUTPUT_TEST",
            "display_name": "Feature output test",
            "rights_version": "test-v1",
            "status": "ACTIVE",
            "created_at": "2026-01-01T00:00:00Z",
        },
    )
    postgres_catalog.upsert(
        "market_data.feeds",
        {
            "id": FEED,
            "provider_id": PROVIDER,
            "code": "FEATURE_OUTPUT_1M",
            "data_kind": "BARS",
            "resolution": "1m",
            "timezone_name": "UTC",
            "feed_version": "test-v1",
            "created_at": "2026-01-01T00:00:00Z",
            "retired_at": None,
        },
    )
    postgres_catalog.upsert(
        "market_data.instruments",
        {
            "id": INSTRUMENT,
            "asset_type": "STOCK",
            "primary_exchange_mic": "XNYS",
            "currency_code": "USD",
            "provider_reference": None,
            "listed_at": None,
            "delisted_at": None,
            "created_at": "2026-01-01T00:00:00Z",
        },
    )
    postgres_catalog.begin_pipeline_run(
        {
            "id": RUN,
            "pipeline_code": "FEATURE_MATERIALIZATION",
            "pipeline_version": "feature-series.parquet.v1",
            "idempotency_key": "feature-output-postgres-test",
            "status": "RUNNING",
            "input_hash": "a" * 64,
            "output_hash": None,
            "started_at": "2026-01-05T14:30:00Z",
            "completed_at": None,
            "failure_code": None,
        }
    )
    postgres_catalog.publish_manifest(
        {
            "id": SOURCE_MANIFEST,
            "feed_id": FEED,
            "instrument_id": INSTRUMENT,
            "data_layer": "RAW",
            "resolution": "1m",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": "2026-01-05T14:30:00Z",
            "period_end": "2026-01-05T15:00:00Z",
            "schema_version": "market-bars.v1",
            "dataset_hash": "b" * 64,
            "supersedes_manifest_id": None,
            "created_at": "2026-01-05T15:00:00Z",
            "available_at": "2026-01-05T15:00:00Z",
        }
    )
    postgres_catalog.stage_object(
        {
            "id": SOURCE_STORAGE,
            "status": "AVAILABLE",
            "storage_provider": "LOCAL",
            "bucket_name": "test",
            "object_key": "source.parquet",
            "provider_version_id": "a" * 64,
            "content_hash": "a" * 64,
            "byte_size": 1,
            "file_format": "PARQUET",
            "compression_codec": "UNCOMPRESSED",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": "market-bars.v1",
            "row_count": 3,
            "period_start": "2026-01-05T14:30:00Z",
            "period_end": "2026-01-05T15:00:00Z",
            "encryption_key_ref": None,
            "retention_policy_version": "UNSPECIFIED",
            "retention_until": None,
            "legal_hold": False,
            "created_at": "2026-01-05T15:00:00Z",
            "verified_at": "2026-01-05T15:00:00Z",
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
            "period_start": "2026-01-05T14:30:00Z",
            "period_end": "2026-01-05T15:00:00Z",
            "shard_key": INSTRUMENT,
            "part_number": 1,
            "row_count": 3,
            "min_instrument_id": INSTRUMENT,
            "max_instrument_id": INSTRUMENT,
        },
    )

    registry = FeatureDefinitionRegistry(postgres_catalog)
    published = registry.publish(definition())
    publisher = FeatureOutputPublisher(
        postgres_catalog,
        LocalObjectStore(tmp_path / "objects"),
        staging_root=tmp_path / "staging",
    )
    result = FeatureMaterializer(
        postgres_catalog, registry, output_publisher=publisher
    ).materialize(request(published))

    assert result.output_provider_version_id
    assert postgres_catalog.records("market_data.feature_materializations")[0]["status"] == "SUCCEEDED"
    assert postgres_catalog.records("market_data.dataset_manifests", where={"id": OUTPUT_MANIFEST})[0][
        "status"
    ] == "AVAILABLE"
    assert postgres_catalog.objects_for_manifest(OUTPUT_MANIFEST)[0]["storage"][
        "provider_version_id"
    ] == result.output_provider_version_id
