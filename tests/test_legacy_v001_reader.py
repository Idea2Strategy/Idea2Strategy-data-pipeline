"""Fail-closed reads from the immutable retired market-loader V001 catalog."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from market_pipeline_lib.db.errors import RuntimeDdlForbidden
from market_pipeline_lib.legacy_bootstrap import BootstrapConflict, connect_read_only_catalog
from tests.conftest import POSTGRES_IMAGE, VENDORED_MIGRATIONS, _execute_scripts, docker_is_available

REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_V1 = VENDORED_MIGRATIONS / "V1__initial_schema.sql"
LEGACY_SHAPE = REPO_ROOT / "tests" / "fixtures" / "legacy-market-schema.sql"


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
            assert [row["code"] for row in catalog.records("market_data.providers")] == ["ALPACA"]
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
