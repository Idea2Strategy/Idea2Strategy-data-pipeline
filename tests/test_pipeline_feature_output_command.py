from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from apps.common.errors import ConfigurationError, MalformedEventError, PortNotConfiguredError
from apps.pipeline_worker.commands import SUPPORTED_COMMANDS, Command, PipelineCommandExecutor
from apps.pipeline_worker.config import WorkerConfig
from apps.pipeline_worker.feature_output import ProductionFeatureMaterializationPort
from apps.pipeline_worker.messaging import InProcessMessageSource
from apps.pipeline_worker.worker import PipelineWorker
from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.features import FeatureDefinition, FeatureDefinitionRegistry
from market_pipeline_lib.storage import LocalObjectStore

CATALOG_VERSION = "10000000-0000-4000-8000-000000000001"
INSTRUMENT = "20000000-0000-4000-8000-000000000001"
RUN = "30000000-0000-4000-8000-000000000001"
SOURCE_MANIFEST = "40000000-0000-4000-8000-000000000001"
SOURCE_OBJECT = "50000000-0000-4000-8000-000000000001"
OUTPUT_MANIFEST = "60000000-0000-4000-8000-000000000001"
FEED = "70000000-0000-4000-8000-000000000001"


def _environment(root: Path, **updates: str) -> dict[str, str]:
    values = {
        "PIPELINE_WORKER_ENVIRONMENT": "test",
        "PIPELINE_WORKER_MESSAGE_SOURCE": "inprocess",
        "PIPELINE_WORKER_CATALOG_ROOT": str(root / "catalog"),
        "PIPELINE_WORKER_OBJECT_STORE_ROOT": str(root / "objects"),
    }
    values.update(updates)
    return values


def _definition() -> FeatureDefinition:
    return FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="1m",
        parameters={"window": 2, "price_field": "close"},
    )


def _payload(definition: FeatureDefinition) -> dict[str, object]:
    return {
        "definition_hash": definition.definition_hash,
        "instrument_id": INSTRUMENT,
        "pipeline_run_id": RUN,
        "sources": [
            {
                "dataset_object_id": SOURCE_OBJECT,
                "dataset_manifest_id": SOURCE_MANIFEST,
                "content_hash": "a" * 64,
                "partition_start": "2026-01-05",
                "partition_end": "2026-01-06",
                "row_count": 3,
            }
        ],
        "bars": [
            {
                "bar_start_at": datetime(2026, 1, 5, 14, 30 + offset, tzinfo=UTC).isoformat(),
                "open": str(Decimal(100 + offset)),
                "high": str(Decimal(101 + offset)),
                "low": str(Decimal(99 + offset)),
                "close": str(Decimal(100 + offset)),
                "volume": 1000,
            }
            for offset in range(3)
        ],
        "period_start": "2026-01-05T14:30:00Z",
        "period_end": "2026-01-05T15:00:00Z",
        "source_watermark": "ALPACA@2026-01-05T15:00:00Z",
        "output_dataset_manifest_id": OUTPUT_MANIFEST,
        "output_feed_id": FEED,
        "output_revision_number": 1,
    }


def test_production_port_rejects_the_legacy_caller_attested_contract(tmp_path: Path) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    definition = FeatureDefinitionRegistry(catalog).publish(_definition())
    port = ProductionFeatureMaterializationPort(
        catalog,
        LocalObjectStore(tmp_path / "objects"),
        staging_root=tmp_path / "staging",
    )

    with pytest.raises(MalformedEventError, match="fields mismatch"):
        port.materialize(_payload(definition), command_id="legacy-contract")

    assert catalog.records("storage.objects") == []
    assert catalog.records("market_data.feature_materializations") == []


@pytest.mark.parametrize(
    ("field", "value"),
    [("output_feed_id", None), ("output_revision_number", 0), ("bars", [])],
)
def test_production_port_rejects_incomplete_or_unsafe_payload_before_publication(
    tmp_path: Path, field: str, value: object
) -> None:
    catalog = LocalCatalog(tmp_path / "catalog")
    definition = FeatureDefinitionRegistry(catalog).publish(_definition())
    payload = {**_payload(definition), field: value}
    port = ProductionFeatureMaterializationPort(
        catalog,
        LocalObjectStore(tmp_path / "objects"),
        staging_root=tmp_path / "staging",
    )

    with pytest.raises(MalformedEventError):
        port.materialize(payload, command_id=f"unsafe-{field}")

    assert catalog.records("market_data.feature_materializations") == []
    assert list((tmp_path / "objects").rglob("*.parquet")) == []


def test_worker_command_routes_to_feature_materialization_port(tmp_path: Path) -> None:
    class RecordingPort:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []

        def materialize(self, payload, *, command_id):
            self.payloads.append({**dict(payload), "observed_command_id": command_id})
            return {"status": "SUCCEEDED", "output_revision_number": 2}

    assert "MATERIALIZE_FEATURE_OUTPUT" in SUPPORTED_COMMANDS
    config = WorkerConfig.from_environment(_environment(tmp_path))
    port = RecordingPort()
    executor = PipelineCommandExecutor(config, feature_materialization_port=port)
    command = Command.parse(
        {"command": "MATERIALIZE_FEATURE_OUTPUT", "payload": {"output_revision_number": 2}},
        fallback_command_id="feature-1",
    )

    assert executor.execute(command)["output_revision_number"] == 2
    assert port.payloads == [
        {"output_revision_number": 2, "observed_command_id": "feature-1"}
    ]


def test_unconfigured_feature_command_fails_loudly_and_remains_retryable(tmp_path: Path) -> None:
    config = WorkerConfig.from_environment(_environment(tmp_path))
    executor = PipelineCommandExecutor(config)
    command = Command.parse(
        {"command": "MATERIALIZE_FEATURE_OUTPUT", "payload": {}},
        fallback_command_id="feature-1",
    )

    with pytest.raises(PortNotConfiguredError, match="PIPELINE_WORKER_FEATURE_OUTPUT"):
        executor.execute(command)


def test_feature_output_runtime_configuration_requires_database(tmp_path: Path) -> None:
    settings = json.dumps(
        {"object_bucket": "feature-bucket", "object_prefix": "derived", "staging_root": "/tmp"}
    )
    with pytest.raises(ConfigurationError, match="PIPELINE_WORKER_DATABASE_URL"):
        WorkerConfig.from_environment(
            _environment(tmp_path, PIPELINE_WORKER_FEATURE_OUTPUT=settings)
        )


def test_transient_feature_publication_failure_is_retried_with_the_same_command(
    tmp_path: Path,
) -> None:
    class FailsOncePort:
        def __init__(self) -> None:
            self.calls = 0

        def materialize(self, payload, *, command_id):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("object store unavailable")
            return {"status": "SUCCEEDED", "output_revision_number": payload["revision"]}

    config = WorkerConfig.from_environment(
        _environment(
            tmp_path,
            PIPELINE_WORKER_RETRY_DELAY_SECONDS="0",
            PIPELINE_WORKER_POLL_INTERVAL_SECONDS="0.01",
        )
    )
    source = InProcessMessageSource()
    port = FailsOncePort()
    worker = PipelineWorker(
        config,
        message_source=source,
        executor=PipelineCommandExecutor(config, feature_materialization_port=port),
    )
    source.submit(
        {
            "command": "MATERIALIZE_FEATURE_OUTPUT",
            "command_id": "feature-retry-1",
            "payload": {"revision": 4},
        }
    )
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while worker.health.succeeded < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    worker.request_stop("test")
    thread.join(timeout=5)

    assert port.calls == 2
    assert worker.health.failed == 1
    assert worker.health.succeeded == 1
    assert source.pending() == 0
