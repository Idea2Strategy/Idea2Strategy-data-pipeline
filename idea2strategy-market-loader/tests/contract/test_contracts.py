from __future__ import annotations

from pathlib import Path

from market_loader.pipeline.parquet_writer import bar_schema


def test_parquet_schema_snapshot() -> None:
    assert str(bar_schema(derived=False)) == (
        "instrument_id: string not null\n"
        "provider_symbol: string not null\n"
        "bar_start_at: timestamp[us, tz=UTC] not null\n"
        "session_date_et: date32[day] not null\n"
        "open: double not null\n"
        "high: double not null\n"
        "low: double not null\n"
        "close: double not null\n"
        "volume: int64 not null\n"
        "trade_count: int64\n"
        "vwap: double"
    )


def test_migration_contains_required_tables_enums_and_available_query() -> None:
    root = Path(__file__).parents[2]
    migration = (root / "db/migration/V001__market_data_initial_schema.sql").read_text(
        encoding="utf-8"
    )
    for value in (
        "operations.work_status",
        "market_data.dataset_status",
        "market_data.pipeline_runs",
        "market_data.pipeline_partitions",
        "market_data.dataset_manifests",
        "market_data.dataset_objects",
        "market_data.dataset_lineage",
        "market_data.quality_incidents",
        "storage.objects",
    ):
        assert value in migration
    query = (root / "db/queries/available_manifest.sql").read_text(encoding="utf-8")
    assert "dm.status = 'AVAILABLE'" in query
    assert "so.provider_version_id" in query
    assert "so.content_sha256" in query
