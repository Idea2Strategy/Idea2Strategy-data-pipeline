"""Upgrade coverage for the pre-canonical market-data schema.

The development RDS was originally created by the retired market-loader V001.  A
fresh database starts at the canonical central V1, so ordinary migration tests cannot
prove that an additive migration repairs this exact historical shape.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from market_pipeline_lib.db.schema_guard import describe_schema_drift
from tests.conftest import POSTGRES_IMAGE, VENDORED_MIGRATIONS, _execute_scripts, docker_is_available

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION = REPO_ROOT / "db" / "migration-contributions" / "migrations" / (
    "V20260805010000__pipeline_upgrade_legacy_market_schema.sql"
)
LEGACY_SHAPE = REPO_ROOT / "tests" / "fixtures" / "legacy-market-schema.sql"
CANONICAL_V1 = VENDORED_MIGRATIONS / "V1__initial_schema.sql"


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


@pytest.fixture(scope="module")
def legacy_database_url() -> str:
    if not docker_is_available():
        pytest.skip("Docker is not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _execute_scripts(url, [CANONICAL_V1.read_text(encoding="utf-8")])
        _run_raw(url, LEGACY_SHAPE.read_text(encoding="utf-8"))
        yield url


@pytest.mark.integration
def test_empty_legacy_schema_upgrades_to_the_runtime_contract(legacy_database_url: str) -> None:
    _run_raw(legacy_database_url, MIGRATION.read_text(encoding="utf-8"))
    _run_raw(
        legacy_database_url,
        """
        SET session_replication_role = replica;
        INSERT INTO market_data.dataset_objects
          (id, dataset_manifest_id, object_id, object_kind, partition_granularity,
           partition_start, partition_end, period_start, period_end, shard_key,
           part_number, row_count)
        VALUES
          ('40000000-0000-4000-8000-000000000001',
           '40000000-0000-4000-8000-000000000002',
           '40000000-0000-4000-8000-000000000003', 'FEATURE_PARQUET', 'DAY',
           '2026-08-04', '2026-08-05', '2026-08-04T00:00:00Z',
           '2026-08-05T00:00:00Z', 'all', 1, 1);
        INSERT INTO market_data.dataset_lineage
          (derived_manifest_id, source_manifest_id, relation_type)
        VALUES
          ('50000000-0000-4000-8000-000000000001',
           '50000000-0000-4000-8000-000000000002', 'FEATURE_INPUT');
        INSERT INTO market_data.quality_incidents
          (id, severity, incident_code, period_start, status, detected_at)
        VALUES
          ('60000000-0000-4000-8000-000000000001', 'ERROR', 'CHECKSUM_FAILURE',
           '2026-08-04T00:00:00Z', 'ACTIVE', '2026-08-04T00:00:01Z');
        SET session_replication_role = origin;
        """,
    )

    engine = create_engine(legacy_database_url, future=True)
    try:
        with engine.connect() as connection:
            assert describe_schema_drift(connection) == []
            blockers = connection.execute(
                text(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'market_data'
                      AND ((table_name = 'dataset_objects' AND column_name = 'partition_key')
                        OR (table_name = 'quality_incidents' AND column_name = 'detail'))
                      AND is_nullable = 'NO'
                    ORDER BY table_name, column_name
                    """
                )
            ).all()
            assert blockers == []
            lineage_id_default = connection.execute(
                text(
                    """
                    SELECT column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'market_data'
                      AND table_name = 'dataset_lineage'
                      AND column_name = 'id'
                    """
                )
            ).scalar_one()
            assert "gen_random_uuid" in lineage_id_default
            lineage_unique = connection.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON kcu.constraint_schema = tc.constraint_schema
                         AND kcu.constraint_name = tc.constraint_name
                         AND kcu.table_schema = tc.table_schema
                         AND kcu.table_name = tc.table_name
                        WHERE tc.table_schema = 'market_data'
                          AND tc.table_name = 'dataset_lineage'
                          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                        GROUP BY tc.constraint_name
                        HAVING array_agg(kcu.column_name::text ORDER BY kcu.column_name) =
                               ARRAY['derived_manifest_id', 'relation_type', 'source_manifest_id']
                    )
                    """
                )
            ).scalar_one()
            assert lineage_unique is True
    finally:
        engine.dispose()


@pytest.mark.integration
def test_canonical_schema_takes_the_no_op_path() -> None:
    if not docker_is_available():
        pytest.skip("Docker is not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _execute_scripts(url, [CANONICAL_V1.read_text(encoding="utf-8")])
        _run_raw(url, MIGRATION.read_text(encoding="utf-8"))

        engine = create_engine(url, future=True)
        try:
            with engine.connect() as connection:
                assert describe_schema_drift(connection) == []
        finally:
            engine.dispose()


@pytest.mark.integration
def test_upgrade_refuses_every_populated_legacy_table() -> None:
    if not docker_is_available():
        pytest.skip("Docker is not available")

    from testcontainers.postgres import PostgresContainer

    rows = {
        "dataset_objects": """
            INSERT INTO market_data.dataset_objects
              (id, dataset_manifest_id, object_id, object_kind, partition_key, row_count)
            VALUES
              ('10000000-0000-4000-8000-000000000001',
               '10000000-0000-4000-8000-000000000002',
               '10000000-0000-4000-8000-000000000003', 'BAR_PARQUET', 'legacy', 1)
        """,
        "dataset_lineage": """
            INSERT INTO market_data.dataset_lineage
              (id, dataset_manifest_id, source_manifest_id, relationship_type)
            VALUES
              ('20000000-0000-4000-8000-000000000001',
               '20000000-0000-4000-8000-000000000002',
               '20000000-0000-4000-8000-000000000003', 'DERIVED_FROM')
        """,
        "quality_incidents": """
            INSERT INTO market_data.quality_incidents
              (id, incident_type, severity, status, detail)
            VALUES
              ('30000000-0000-4000-8000-000000000001',
               'LEGACY', 'WARNING', 'OPEN', '{}'::jsonb)
        """,
    }

    for table, insert_sql in rows.items():
        with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
            url = container.get_connection_url()
            _execute_scripts(url, [CANONICAL_V1.read_text(encoding="utf-8")])
            _run_raw(url, LEGACY_SHAPE.read_text(encoding="utf-8"))
            _run_raw(
                url,
                "SET session_replication_role = replica;\n"
                + insert_sql
                + ";\nSET session_replication_role = origin;",
            )

            with pytest.raises(Exception, match=rf"{table} contains 1 legacy row"):
                _run_raw(url, MIGRATION.read_text(encoding="utf-8"))
