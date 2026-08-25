"""Fail-closed reads from the immutable retired market-loader V001 catalog."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, text

from market_pipeline_lib.db.errors import RuntimeDdlForbidden
from market_pipeline_lib.legacy_bootstrap import (
    BOOTSTRAP_TABLE_ORDER,
    BootstrapConflict,
    _translate_legacy_v001,
    connect_read_only_catalog,
)
from tests.conftest import POSTGRES_IMAGE, VENDORED_MIGRATIONS, _execute_scripts, docker_is_available

REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_V1 = VENDORED_MIGRATIONS / "V1__initial_schema.sql"
LEGACY_SHAPE = REPO_ROOT / "tests" / "fixtures" / "legacy-market-schema.sql"
MARKET_LOADER_V001 = REPO_ROOT / "tests" / "fixtures" / "legacy-market-loader-v001.sql"


def _uuid(value: int) -> UUID:
    return UUID(int=value)


def _populated_v001_snapshot() -> dict[str, list[dict[str, object]]]:
    now = datetime(2026, 7, 31, tzinfo=UTC)
    provider_id = _uuid(1)
    feed_id = _uuid(2)
    instrument_id = _uuid(3)
    run_id = _uuid(4)
    source: dict[str, list[dict[str, object]]] = {
        "market_data.providers": [
            {
                "id": provider_id,
                "code": "ALPACA",
                "name": "Alpaca Markets",
                "rights_version": "approved-v1",
                "status": "ACTIVE",
                "created_at": now,
            }
        ],
        "market_data.feeds": [
            {
                "id": feed_id,
                "provider_id": provider_id,
                "code": "ALPACA_SIP_RAW_1D",
                "data_kind": "BAR",
                "resolution": "1d",
                "session_scope": "REGULAR",
                "status": "ACTIVE",
                "created_at": now,
            }
        ],
        "market_data.instruments": [
            {
                "id": instrument_id,
                "asset_type": "STOCK",
                "primary_exchange_mic": "XNAS",
                "currency": "USD",
                "support_status": "ACTIVE",
                "listed_from": date(1900, 1, 1),
                "listed_to": None,
                "created_at": now,
            }
        ],
        "market_data.instrument_symbols": [
            {
                "id": _uuid(5),
                "instrument_id": instrument_id,
                "symbol": "AAPL",
                "exchange_mic": "XNAS",
                "effective_from": date(1900, 1, 1),
                "effective_to": None,
                "created_at": now,
            }
        ],
        "market_data.trading_sessions": [
            {
                "id": _uuid(6),
                "exchange_mic": "XNYS",
                "session_date": date(2024, 1, 2),
                "opens_at": datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
                "closes_at": datetime(2024, 1, 2, 21, 0, tzinfo=UTC),
                "session_type": "REGULAR",
                "calendar_version": "XNYS-v1",
                "created_at": now,
            }
        ],
        "market_data.pipeline_runs": [
            {
                "id": run_id,
                "pipeline_type": "HISTORICAL_BACKFILL",
                "processing_version": "market-loader-v1",
                "status": "SUCCEEDED",
                "idempotency_key": "a" * 64,
                "requested_at": now,
                "started_at": now,
                "completed_at": now,
                "input_config": {"source": "test"},
                "summary_result": {"manifests": 96, "objects": 768},
                "failure_code": None,
            }
        ],
        "storage.objects": [],
        "market_data.dataset_manifests": [],
        "market_data.dataset_objects": [],
        "market_data.dataset_lineage": [],
        "market_data.quality_incidents": [],
        "market_data.pipeline_partitions": [],
    }
    for manifest_number in range(96):
        year = 1920 + manifest_number
        manifest_id = _uuid(10_000 + manifest_number)
        source["market_data.dataset_manifests"].append(
            {
                "id": manifest_id,
                "feed_id": feed_id,
                "instrument_id": None,
                "data_layer": "DERIVED",
                "resolution": "1d",
                "period_start": date(year, 1, 1),
                "period_end": date(year + 1, 1, 1),
                "revision_number": 1,
                "as_of_at": now,
                "processing_version": "market-loader-v1",
                "quality_status": "PASSED",
                "status": "AVAILABLE",
                "row_count": 8,
                "manifest_hash": f"{manifest_number + 1:064x}",
                "created_at": now,
                "supersedes_manifest_id": None,
            }
        )
        for shard in range(8):
            object_number = manifest_number * 8 + shard
            object_id = _uuid(100_000 + object_number)
            relation_id = _uuid(200_000 + object_number)
            key = (
                "historical/provider=alpaca/feed=sip/adjustment=raw/session=regular/"
                f"resolution=1d/revision=00000001/year={year}/shard={shard:02d}-of-08/"
                f"manifest_id={manifest_id}/part-00001.parquet"
            )
            source["storage.objects"].append(
                {
                    "id": object_id,
                    "storage_class": "S3_STANDARD",
                    "bucket_code": "DEVELOPMENT_MARKET_DATA",
                    "object_key": key,
                    "provider_version_id": f"version-{object_number}",
                    "content_sha256": f"{object_number + 1:064x}",
                    "byte_size": object_number + 1,
                    "media_type": "application/vnd.apache.parquet",
                    "format_version": "market-bars/1",
                    "encryption_profile": "SSE-S3-AES256",
                    "created_at": now,
                    "verified_at": now,
                }
            )
            partition_key = f"adjustment=raw/resolution=1d/year={year}/shard={shard:02d}"
            source["market_data.dataset_objects"].append(
                {
                    "id": relation_id,
                    "dataset_manifest_id": manifest_id,
                    "object_id": object_id,
                    "object_kind": "BAR_PARQUET",
                    "partition_key": partition_key,
                    "row_count": 1,
                    "min_bar_start_at": datetime(year, 1, 2, tzinfo=UTC),
                    "max_bar_start_at": datetime(year, 1, 2, tzinfo=UTC),
                    "created_at": now,
                }
            )
            source["market_data.pipeline_partitions"].append(
                {
                    "id": _uuid(300_000 + object_number),
                    "pipeline_run_id": run_id,
                    "partition_key": partition_key,
                    "status": "SUCCEEDED",
                    "result_manifest_id": manifest_id,
                    "error_code": None,
                    "error_summary": None,
                    "created_at": now,
                    "updated_at": now,
                }
            )
    for lineage_number in range(72):
        source["market_data.dataset_lineage"].append(
            {
                "id": _uuid(400_000 + lineage_number),
                "dataset_manifest_id": _uuid(10_000 + lineage_number + 24),
                "source_manifest_id": _uuid(10_000 + lineage_number),
                "relationship_type": "DERIVED_FROM",
                "created_at": now,
            }
        )
    return source


def _run_raw(url: str, script: str) -> None:
    engine = create_engine(url, future=True)
    try:
        raw = engine.raw_connection()
        try:
            cursor = raw.cursor()
            cursor.execute(script)
            raw.commit()
        finally:
            raw.close()
    finally:
        engine.dispose()


@contextmanager
def _legacy_database(*, mutation: str = "") -> Iterator[str]:
    if not docker_is_available():
        pytest.skip("Docker is not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _execute_scripts(url, [CANONICAL_V1.read_text(encoding="utf-8")])
        _run_raw(url, LEGACY_SHAPE.read_text(encoding="utf-8"))
        if mutation:
            _run_raw(url, mutation)
        yield url


@contextmanager
def _market_loader_v001_database(*, mutation: str = "") -> Iterator[str]:
    if not docker_is_available():
        pytest.skip("Docker is not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _execute_scripts(url, [MARKET_LOADER_V001.read_text(encoding="utf-8")])
        if mutation:
            _run_raw(url, mutation)
        yield url


@pytest.mark.integration
def test_exact_empty_v001_shape_is_read_without_mutating_it(tmp_path: Path) -> None:
    with _legacy_database(
        mutation="""
        INSERT INTO market_data.providers
          (id, code, display_name, rights_version, status, created_at)
        VALUES
          ('10000000-0000-4000-8000-000000000001', 'ALPACA', 'Alpaca',
           'approved-v1', 'ACTIVE', '2026-07-31T00:00:00Z');
        """
    ) as url:
        catalog = connect_read_only_catalog(url, artifact_root=tmp_path)
        try:
            catalog.verify_schema()
            assert {row["code"] for row in catalog.records("market_data.providers")} == {
                "ALPACA",
                "IDEA2STRATEGY_INTERNAL",
            }
            assert catalog.records("market_data.dataset_objects") == []
            assert catalog.records("market_data.dataset_lineage") == []
            assert catalog.records("market_data.quality_incidents") == []
            assert catalog.records("market_data.dataset_object_lineage") == []
            assert catalog.records("market_data.corporate_actions") == []
            assert catalog.records("market_data.feature_materializations") == []

            with pytest.raises(RuntimeDdlForbidden, match="must not execute DDL"):
                with catalog.engine.begin() as connection:
                    connection.execute(text("CREATE TABLE market_data.must_not_exist (id int)"))
        finally:
            catalog.close()


@pytest.mark.integration
def test_v001_reader_refuses_each_populated_changed_table(tmp_path: Path) -> None:
    rows = {
        "dataset_objects": """
            SET session_replication_role = replica;
            INSERT INTO market_data.dataset_objects
              (id, dataset_manifest_id, object_id, object_kind, partition_key, row_count)
            VALUES
              ('10000000-0000-4000-8000-000000000001',
               '10000000-0000-4000-8000-000000000002',
               '10000000-0000-4000-8000-000000000003', 'BAR_PARQUET', 'legacy', 1);
            SET session_replication_role = origin;
        """,
        "dataset_lineage": """
            SET session_replication_role = replica;
            INSERT INTO market_data.dataset_lineage
              (id, dataset_manifest_id, source_manifest_id, relationship_type)
            VALUES
              ('20000000-0000-4000-8000-000000000001',
               '20000000-0000-4000-8000-000000000002',
               '20000000-0000-4000-8000-000000000003', 'DERIVED_FROM');
            SET session_replication_role = origin;
        """,
        "quality_incidents": """
            SET session_replication_role = replica;
            INSERT INTO market_data.quality_incidents
              (id, incident_type, severity, status, detail)
            VALUES
              ('30000000-0000-4000-8000-000000000001',
               'LEGACY', 'WARNING', 'OPEN', '{}'::jsonb);
            SET session_replication_role = origin;
        """,
    }

    for table, mutation in rows.items():
        with _legacy_database(mutation=mutation) as url:
            catalog = connect_read_only_catalog(url, artifact_root=tmp_path / table)
            try:
                with pytest.raises(BootstrapConflict, match=rf"{table} contains 1 legacy row"):
                    catalog.verify_schema()
            finally:
                catalog.close()


@pytest.mark.integration
def test_v001_reader_refuses_an_unknown_schema_variant(tmp_path: Path) -> None:
    with _legacy_database(
        mutation="ALTER TABLE market_data.providers ADD COLUMN unexpected text"
    ) as url:
        catalog = connect_read_only_catalog(url, artifact_root=tmp_path)
        try:
            with pytest.raises(BootstrapConflict, match="not the exact retired V001 schema"):
                catalog.verify_schema()
        finally:
            catalog.close()


@pytest.mark.integration
def test_exact_market_loader_v001_metadata_fingerprint_is_accepted(tmp_path: Path) -> None:
    with _market_loader_v001_database() as url:
        catalog = connect_read_only_catalog(
            url,
            artifact_root=tmp_path,
            legacy_bucket_name="development-market-data",
        )
        try:
            catalog.verify_schema()
            assert all(catalog.records(table) == [] for table in BOOTSTRAP_TABLE_ORDER)
        finally:
            catalog.close()


@pytest.mark.integration
def test_market_loader_v001_type_drift_fails_the_exact_fingerprint(tmp_path: Path) -> None:
    with _market_loader_v001_database(
        mutation="ALTER TABLE market_data.providers ALTER COLUMN name TYPE varchar(199)"
    ) as url:
        catalog = connect_read_only_catalog(url, artifact_root=tmp_path)
        try:
            with pytest.raises(BootstrapConflict, match="schema fingerprint was"):
                catalog.verify_schema()
        finally:
            catalog.close()


def test_populated_market_loader_v001_maps_768_objects_and_96_manifests_deterministically() -> None:
    source = _populated_v001_snapshot()
    unchanged = deepcopy(source)

    first = _translate_legacy_v001(source, bucket_name="development-market-data")
    second = _translate_legacy_v001(source, bucket_name="development-market-data")

    assert source == unchanged
    assert first == second
    assert tuple(first) == BOOTSTRAP_TABLE_ORDER
    assert len(first["storage.objects"]) == 768
    assert len(first["market_data.dataset_manifests"]) == 96
    assert len(first["market_data.dataset_objects"]) == 768
    assert len(first["market_data.dataset_lineage"]) == 72
    assert first["market_data.dataset_object_lineage"] == []
    assert first["market_data.stream_watermarks"] == []
    assert first["market_data.corporate_actions"] == []
    assert first["market_data.feature_definitions"] == []
    assert first["market_data.feature_materializations"] == []
    assert first["market_data.feature_snapshot_batches"] == []

    storage = first["storage.objects"][0]
    relation = first["market_data.dataset_objects"][0]
    manifest = first["market_data.dataset_manifests"][0]
    assert storage["bucket_name"] == "development-market-data"
    assert storage["content_hash"]
    assert storage["schema_version"] == "market-bars/1"
    assert relation["partition_granularity"] == "YEAR"
    assert relation["shard_key"].startswith("s")
    assert relation["part_number"] == 1
    assert manifest["schema_version"] == "market-bars/1"
    assert manifest["dataset_hash"]
    assert manifest["object_count"] == 8


def test_populated_market_loader_v001_requires_deployment_bucket_and_known_partition() -> None:
    source = _populated_v001_snapshot()
    with pytest.raises(BootstrapConflict, match="bucket name is required"):
        _translate_legacy_v001(source, bucket_name=None)

    source["market_data.dataset_objects"][0]["partition_key"] = "unknown"
    with pytest.raises(BootstrapConflict, match="partition key is unknown"):
        _translate_legacy_v001(source, bucket_name="development-market-data")
