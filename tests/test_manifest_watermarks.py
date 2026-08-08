from __future__ import annotations

from datetime import UTC, datetime

from apps.pipeline_worker import publish_manifest_watermarks as command
from apps.pipeline_worker.main import EXIT_RUNTIME_FAILURE, main
from market_pipeline_lib.manifest_watermarks import advance_available_manifest_watermarks
from market_pipeline_lib.watermarks import InMemoryWatermarkRepository


class FakeCatalog:
    def __init__(self, rows: dict[str, list[dict[str, object]]]) -> None:
        self._rows = rows

    def records(self, table: str):
        return list(self._rows.get(table, ()))


def test_available_manifests_advance_only_active_feed_watermarks() -> None:
    catalog = FakeCatalog(
        {
            "market_data.providers": [
                {"id": "active-provider", "status": "ACTIVE"},
                {"id": "blocked-provider", "status": "REVIEW_REQUIRED"},
            ],
            "market_data.feeds": [
                {"id": "active-30m", "provider_id": "active-provider", "retired_at": None},
                {"id": "retired-30m", "provider_id": "active-provider", "retired_at": "2026-01-01T00:00:00Z"},
                {"id": "blocked-30m", "provider_id": "blocked-provider", "retired_at": None},
            ],
            "market_data.dataset_manifests": [
                {
                    "feed_id": "active-30m",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-07T20:00:00Z",
                    "available_at": "2026-08-07T20:05:00Z",
                },
                {
                    "feed_id": "active-30m",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-08T20:00:00Z",
                    "available_at": "2026-08-08T20:04:00Z",
                },
                {
                    "feed_id": "active-30m",
                    "status": "QUARANTINED",
                    "period_end": "2026-08-09T20:00:00Z",
                    "available_at": None,
                },
                {
                    "feed_id": "retired-30m",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-08T20:00:00Z",
                    "available_at": "2026-08-08T20:04:00Z",
                },
                {
                    "feed_id": "blocked-30m",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-08T20:00:00Z",
                    "available_at": "2026-08-08T20:04:00Z",
                },
            ],
        }
    )
    repository = InMemoryWatermarkRepository()

    result = advance_available_manifest_watermarks(
        catalog,
        repository,
        observed_at=datetime(2026, 8, 9, 1, 0, tzinfo=UTC),
    )

    assert result == {
        "status": "SUCCEEDED",
        "active_feed_count": 1,
        "advanced_feed_count": 1,
        "watermarks": [
            {
                "feed_id": "active-30m",
                "last_source_event_at": "2026-08-08T20:00:00Z",
                "last_ingested_at": "2026-08-08T20:04:00Z",
            }
        ],
    }
    stored = repository.load("active-30m")
    assert stored is not None
    assert stored.position.source_event_at == datetime(2026, 8, 8, 20, tzinfo=UTC)
    assert repository.load("retired-30m") is None
    assert repository.load("blocked-30m") is None


def test_older_manifest_reconciliation_does_not_regress_watermark() -> None:
    catalog = FakeCatalog(
        {
            "market_data.providers": [{"id": "provider", "status": "ACTIVE"}],
            "market_data.feeds": [{"id": "feed", "provider_id": "provider", "retired_at": None}],
            "market_data.dataset_manifests": [
                {
                    "feed_id": "feed",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-07T20:00:00Z",
                    "available_at": "2026-08-07T20:05:00Z",
                }
            ],
        }
    )
    repository = InMemoryWatermarkRepository()
    newer_catalog = FakeCatalog(
        {
            **catalog._rows,
            "market_data.dataset_manifests": [
                {
                    "feed_id": "feed",
                    "status": "AVAILABLE",
                    "period_end": "2026-08-08T20:00:00Z",
                    "available_at": "2026-08-08T20:05:00Z",
                }
            ],
        }
    )
    advance_available_manifest_watermarks(newer_catalog, repository)

    advance_available_manifest_watermarks(catalog, repository)

    stored = repository.load("feed")
    assert stored is not None
    assert stored.position.source_event_at == datetime(2026, 8, 8, 20, tzinfo=UTC)


def test_worker_watermark_mode_fails_closed_without_database_url(capsys) -> None:
    assert main(["--publish-manifest-watermarks"], {}) == EXIT_RUNTIME_FAILURE
    assert "PIPELINE_WORKER_DATABASE_URL is required" in capsys.readouterr().err


def test_worker_watermark_command_uses_guarded_postgres_catalog(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {}

    class FakePostgresCatalog:
        engine = object()

        def verify_schema(self) -> None:
            calls["verified"] = True

        def close(self) -> None:
            calls["closed"] = True

    fake_catalog = FakePostgresCatalog()

    def connect(database_url, *, artifact_root, storage_objects):
        calls.update(
            database_url=database_url,
            artifact_root=artifact_root,
            storage_objects=storage_objects,
        )
        return fake_catalog

    monkeypatch.setattr(command.PostgresCatalog, "connect", connect)
    monkeypatch.setattr(command, "SqlWatermarkRepository", lambda engine: "repository")
    monkeypatch.setattr(
        command,
        "advance_available_manifest_watermarks",
        lambda catalog, repository: {
            "status": "SUCCEEDED",
            "catalog_matches": catalog is fake_catalog,
            "repository": repository,
        },
    )

    result = command.execute(
        ["--artifact-root", str(tmp_path)],
        {"PIPELINE_WORKER_DATABASE_URL": "postgresql://pipeline@example/db"},
    )

    assert result == {
        "status": "SUCCEEDED",
        "catalog_matches": True,
        "repository": "repository",
    }
    assert calls["verified"] is True
    assert calls["closed"] is True
    assert calls["artifact_root"] == tmp_path
