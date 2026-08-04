"""Boot smoke coverage for the `pipeline-worker` execution app (D01, D11/D12/D90).

The worker must genuinely run: it boots, reports readiness on an HTTP endpoint,
performs real domain work on a real message, refuses to start when required
configuration is absent, drains on a stop signal, parks a message on the
dead-letter queue once it has been received too often, and -- with DP5 -- hosts
the realtime SQS consumer for real, against LocalStack.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from apps.common.errors import ConfigurationError, PortNotConfiguredError
from apps.common.logging import JsonFormatter, redact
from apps.pipeline_worker.commands import Command, PipelineCommandExecutor
from apps.pipeline_worker.config import REQUIRED_ENVIRONMENT_VARIABLES, WorkerConfig
from apps.pipeline_worker.health import HealthEndpoint, HealthState, ReadinessStatus
from apps.pipeline_worker.messaging import (
    InProcessMessageSource,
    SqsMessageSource,
    build_message_source,
)
from apps.pipeline_worker.worker import PipelineWorker

ET = ZoneInfo("America/New_York")
LOCALSTACK_ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT_URL")

REALTIME_INSTRUMENTS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
}


def _environment(root: Path, **overrides: str) -> dict[str, str]:
    environment = {
        "PIPELINE_WORKER_ENVIRONMENT": "test",
        "PIPELINE_WORKER_MESSAGE_SOURCE": "inprocess",
        "PIPELINE_WORKER_CATALOG_ROOT": str(root / "catalog"),
        "PIPELINE_WORKER_OBJECT_STORE_ROOT": str(root / "objects"),
        "PIPELINE_WORKER_LOG_LEVEL": "WARNING",
        "PIPELINE_WORKER_POLL_INTERVAL_SECONDS": "0.01",
    }
    environment.update(overrides)
    return environment


def _realtime_settings(root: Path) -> str:
    instrument_map = root / "instrument_map.csv"
    instrument_map.parent.mkdir(parents=True, exist_ok=True)
    instrument_map.write_text(
        "provider_symbol,instrument_id\n"
        + "".join(f"{symbol},{identifier}\n" for symbol, identifier in REALTIME_INSTRUMENTS.items()),
        encoding="utf-8",
    )
    return json.dumps(
        {
            "instrument_map_path": str(instrument_map),
            "price_type": "raw",
            "data_layer": "RAW",
            "resolution": "30m",
            "event_type": "BAR_30M",
            "source_provider": "ALPACA",
            "source_feed": "SIP",
            "source_resolution": "PT30M",
            "partition_granularity": "DAY",
            "shard_count": 2,
            "staging_root": str(root / "staging"),
            "value_fields": {
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "trade_count": "tradeCount",
                "vwap": "vwap",
            },
        }
    )


def _realtime_events(count: int = 4) -> list[dict[str, Any]]:
    open_at = datetime(2024, 1, 8, 9, 30, tzinfo=ET)
    events = []
    sequence = 1
    for index in range(count):
        bar_start = (open_at + timedelta(minutes=30 * index)).astimezone(UTC)
        for symbol, instrument_id in REALTIME_INSTRUMENTS.items():
            base = 100.0 + index / 10 + (0.5 if symbol == "MSFT" else 0.0)
            events.append(
                {
                    "schemaVersion": 1,
                    "eventId": f"evt-{sequence:05d}",
                    "instrumentId": instrument_id,
                    "provider": "ALPACA",
                    "feed": "SIP",
                    "eventType": "BAR_30M",
                    "providerEventId": f"{symbol}-{bar_start.isoformat()}",
                    "occurredAt": bar_start.isoformat().replace("+00:00", "Z"),
                    "receivedAt": bar_start.isoformat().replace("+00:00", "Z"),
                    "sequence": sequence,
                    "revision": 0,
                    "correctionOfEventId": None,
                    "values": {
                        "open": base,
                        "high": base + 1.0,
                        "low": base - 1.0,
                        "close": base + 0.5,
                        "volume": 1000 + index,
                        "tradeCount": 10 + index,
                        "vwap": base + 0.25,
                    },
                }
            )
            sequence += 1
    return events


def _get(url: str) -> tuple[int, dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310 - fixed localhost URL
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


class WorkerConfigurationTests(unittest.TestCase):
    def test_every_required_variable_is_documented(self) -> None:
        self.assertEqual(
            REQUIRED_ENVIRONMENT_VARIABLES,
            (
                "PIPELINE_WORKER_ENVIRONMENT",
                "PIPELINE_WORKER_MESSAGE_SOURCE",
                "PIPELINE_WORKER_CATALOG_ROOT",
                "PIPELINE_WORKER_OBJECT_STORE_ROOT",
            ),
        )

    def test_missing_required_configuration_is_rejected_with_every_name(self) -> None:
        with self.assertRaises(ConfigurationError) as raised:
            WorkerConfig.from_environment({})
        message = str(raised.exception)
        for name in REQUIRED_ENVIRONMENT_VARIABLES:
            self.assertIn(name, message)

    def test_a_single_missing_variable_is_still_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp))
            del environment["PIPELINE_WORKER_CATALOG_ROOT"]
            with self.assertRaises(ConfigurationError) as raised:
                WorkerConfig.from_environment(environment)
            self.assertIn("PIPELINE_WORKER_CATALOG_ROOT", str(raised.exception))

    def test_blank_value_counts_as_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_ENVIRONMENT="   ")
            with self.assertRaises(ConfigurationError):
                WorkerConfig.from_environment(environment)

    def test_unknown_message_source_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_MESSAGE_SOURCE="kafka")
            with self.assertRaises(ConfigurationError):
                WorkerConfig.from_environment(environment)

    def test_non_numeric_poll_interval_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_POLL_INTERVAL_SECONDS="soon")
            with self.assertRaises(ConfigurationError):
                WorkerConfig.from_environment(environment)

    def test_sqs_source_requires_a_queue_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_MESSAGE_SOURCE="sqs")
            with self.assertRaises(ConfigurationError) as raised:
                WorkerConfig.from_environment(environment)
            self.assertIn("PIPELINE_WORKER_QUEUE_URL", str(raised.exception))

    def test_sqs_source_requires_a_dead_letter_queue_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(
                Path(tmp),
                PIPELINE_WORKER_MESSAGE_SOURCE="sqs",
                PIPELINE_WORKER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/1/pipeline",
            )
            with self.assertRaises(ConfigurationError) as raised:
                WorkerConfig.from_environment(environment)
            self.assertIn("PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL", str(raised.exception))

    def test_max_receive_count_below_one_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_MAX_RECEIVE_COUNT="0")
            with self.assertRaises(ConfigurationError):
                WorkerConfig.from_environment(environment)

    def test_realtime_settings_are_parsed_from_one_json_variable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = WorkerConfig.from_environment(
                _environment(root, PIPELINE_WORKER_REALTIME_INGEST=_realtime_settings(root))
            )
            assert config.realtime is not None
            self.assertEqual(config.realtime.event_type, "BAR_30M")
            self.assertEqual(config.realtime.partition_granularity, "DAY")
            self.assertEqual(config.realtime.value_fields["trade_count"], "tradeCount")

    def test_malformed_realtime_settings_abort_boot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(Path(tmp), PIPELINE_WORKER_REALTIME_INGEST="{not json")
            with self.assertRaises(ConfigurationError):
                WorkerConfig.from_environment(environment)

    def test_realtime_settings_missing_a_field_abort_boot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = json.loads(_realtime_settings(root))
            del settings["partition_granularity"]
            environment = _environment(root, PIPELINE_WORKER_REALTIME_INGEST=json.dumps(settings))
            with self.assertRaises(ConfigurationError) as raised:
                WorkerConfig.from_environment(environment)
            self.assertIn("partition_granularity", str(raised.exception))

    def test_valid_environment_produces_a_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            self.assertEqual(config.environment, "test")
            self.assertEqual(config.message_source, "inprocess")
            self.assertEqual(config.poll_interval_seconds, 0.01)
            self.assertEqual(config.max_receive_count, 5)
            self.assertIsNone(config.realtime)

    def test_database_url_is_parsed_but_never_exposed_by_describe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            secret_url = "postgresql+psycopg://pipeline:secret@db/idea2strategy"
            config = WorkerConfig.from_environment(
                _environment(Path(tmp), PIPELINE_WORKER_DATABASE_URL=secret_url)
            )

            self.assertEqual(config.database_url, secret_url)
            self.assertTrue(config.describe()["database_configured"])
            self.assertNotIn(secret_url, json.dumps(config.describe()))


class MessageSourcePortTests(unittest.TestCase):
    def test_the_sqs_port_reports_a_missing_aws_sdk_rather_than_degrading(self) -> None:
        """A deployment without boto3 must fail loudly, not fall back to a local queue."""

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(
                    Path(tmp),
                    PIPELINE_WORKER_MESSAGE_SOURCE="sqs",
                    PIPELINE_WORKER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/1/pipeline",
                    PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/1/dlq",
                )
            )
            saved = sys.modules.get("boto3")
            sys.modules["boto3"] = None  # type: ignore[assignment]
            try:
                with self.assertRaises(PortNotConfiguredError) as raised:
                    build_message_source(config)
            finally:
                if saved is None:
                    del sys.modules["boto3"]
                else:
                    sys.modules["boto3"] = saved
            self.assertIn("boto3", str(raised.exception))

    def test_in_process_source_parks_a_message_on_its_dead_letter_queue(self) -> None:
        source = InProcessMessageSource()
        source.submit({"command": "PING"})
        message = source.poll(max_messages=1, wait_seconds=0.0)[0]
        source.dead_letter(message, reason="MAX_RECEIVES_EXCEEDED")
        self.assertEqual(source.pending(), 0)
        self.assertEqual(
            [(entry.body["command"], reason) for entry, reason in source.dead_letters],
            [("PING", "MAX_RECEIVES_EXCEEDED")],
        )

    def test_in_process_source_round_trips_a_message(self) -> None:
        source = InProcessMessageSource()
        source.submit({"command": "PING"})
        received = source.poll(max_messages=5, wait_seconds=0.0)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].body["command"], "PING")
        self.assertEqual(received[0].receive_count, 1)
        source.acknowledge(received[0])
        self.assertEqual(source.poll(max_messages=5, wait_seconds=0.0), [])

    def test_unacknowledged_message_is_redelivered(self) -> None:
        source = InProcessMessageSource()
        source.submit({"command": "PING"})
        first = source.poll(max_messages=1, wait_seconds=0.0)[0]
        source.retry_later(first, delay_seconds=0.0)
        second = source.poll(max_messages=1, wait_seconds=0.0)[0]
        self.assertEqual(second.message_id, first.message_id)
        self.assertEqual(second.receive_count, 2)


class WorkerBootTests(unittest.TestCase):
    def test_worker_boots_reports_ready_and_shuts_down_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = WorkerConfig.from_environment(_environment(root))
            worker = PipelineWorker(config)
            self.assertEqual(worker.health.status, ReadinessStatus.STARTING)
            self.assertFalse(worker.health.is_ready)

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.is_ready), "worker never became ready")

            worker.request_stop("test")
            thread.join(timeout=5.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(worker.health.status, ReadinessStatus.STOPPED)
            self.assertFalse(worker.health.is_ready)

    def test_event_run_mode_exits_cleanly_after_the_configured_idle_poll_limit(self) -> None:
        """A desired-zero RunTask must finish instead of polling forever."""

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(
                    Path(tmp),
                    PIPELINE_WORKER_EXIT_AFTER_IDLE_POLLS="1",
                )
            )
            worker = PipelineWorker(config, message_source=InProcessMessageSource())

            thread = threading.Thread(target=worker.run, name="event-run-worker", daemon=True)
            thread.start()
            thread.join(timeout=5.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(worker.health.status, ReadinessStatus.STOPPED)
            self.assertEqual(config.exit_after_idle_polls, 1)

    def test_health_snapshot_is_json_serialisable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            worker = PipelineWorker(config)
            snapshot = worker.health.snapshot()
            json.dumps(snapshot)
            self.assertEqual(snapshot["status"], ReadinessStatus.STARTING.value)
            self.assertIn("processed", snapshot)

    def test_health_file_is_written_and_removed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            health_file = root / "health" / "ready.json"
            config = WorkerConfig.from_environment(
                _environment(root, PIPELINE_WORKER_HEALTH_FILE=str(health_file))
            )
            worker = PipelineWorker(config)
            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(health_file.is_file), "health file never appeared")
            payload = json.loads(health_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], ReadinessStatus.READY.value)
            worker.request_stop("test")
            thread.join(timeout=5.0)
            self.assertFalse(health_file.exists())

    def test_worker_performs_real_domain_work_on_a_validate_catalog_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = WorkerConfig.from_environment(_environment(root))
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "cmd-1"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(
                self._await(lambda: worker.health.processed >= 1),
                "worker never processed the command",
            )
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(len(worker.results), 1)
            result = worker.results[0]
            self.assertEqual(result["command"], "VALIDATE_CATALOG")
            self.assertEqual(result["outcome"], "SUCCEEDED")
            # An empty catalog is a valid catalog: zero manifests, zero errors.
            self.assertEqual(result["detail"]["manifest_count"], 0)
            self.assertEqual(result["detail"]["status"], "PASSED")
            self.assertEqual(source.pending(), 0)

    def test_unknown_command_is_rejected_and_not_reported_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = WorkerConfig.from_environment(_environment(root))
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "MAKE_COFFEE", "command_id": "cmd-2"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.rejected >= 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(worker.health.succeeded, 0)
            self.assertEqual(len(worker.results), 1)
            self.assertEqual(worker.results[0]["outcome"], "REJECTED")

    def test_duplicate_command_id_is_processed_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = WorkerConfig.from_environment(_environment(root))
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "same"})
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "same"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.processed >= 2))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            outcomes = [result["outcome"] for result in worker.results]
            self.assertEqual(outcomes.count("SUCCEEDED"), 1)
            self.assertEqual(outcomes.count("DUPLICATE"), 1)

    def test_spot_stop_requeues_the_unstarted_remainder_of_a_received_batch(self) -> None:
        """A Fargate Spot SIGTERM must not start more work from a prefetched batch."""

        class _StopAfterFirstCommand:
            def __init__(self) -> None:
                self.worker: PipelineWorker | None = None
                self.executed: list[str] = []

            def prepare(self) -> None:
                return

            def execute(self, command: Command) -> dict[str, str]:
                self.executed.append(command.command_id)
                assert self.worker is not None
                self.worker.request_stop("SIGTERM")
                return {"status": "DONE"}

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            source = InProcessMessageSource()
            executor = _StopAfterFirstCommand()
            worker = PipelineWorker(config, message_source=source, executor=executor)  # type: ignore[arg-type]
            executor.worker = worker
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "cmd-first"})
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "cmd-requeued"})

            received = source.poll(max_messages=2, wait_seconds=0.0)
            worker._process_batch(source, received, deadline=None)

            self.assertEqual(executor.executed, ["cmd-first"])
            redelivered = source.poll(max_messages=1, wait_seconds=0.0)
            self.assertEqual(len(redelivered), 1)
            self.assertEqual(redelivered[0].body["command_id"], "cmd-requeued")
            self.assertEqual(redelivered[0].receive_count, 2)

    def test_long_running_command_renews_its_queue_visibility(self) -> None:
        """Work longer than one visibility window must not be delivered twice."""

        class _HeartbeatSource(InProcessMessageSource):
            def __init__(self) -> None:
                super().__init__()
                self.extensions: list[tuple[str, int]] = []

            def extend_visibility(self, message: Any, *, timeout_seconds: int) -> None:
                self.extensions.append((message.message_id, timeout_seconds))

        class _BlockingExecutor:
            def __init__(self) -> None:
                self.started = threading.Event()
                self.release = threading.Event()

            def prepare(self) -> None:
                return

            def execute(self, command: Command) -> dict[str, str]:
                self.started.set()
                self.release.wait(timeout=5.0)
                return {"status": "DONE"}

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(
                    Path(tmp),
                    PIPELINE_WORKER_VISIBILITY_TIMEOUT_SECONDS="1",
                )
            )
            source = _HeartbeatSource()
            executor = _BlockingExecutor()
            worker = PipelineWorker(config, message_source=source, executor=executor)  # type: ignore[arg-type]
            message_id = source.submit(
                {"command": "VALIDATE_CATALOG", "command_id": "cmd-long"}
            )
            queued_message_id = source.submit(
                {"command": "VALIDATE_CATALOG", "command_id": "cmd-prefetched"}
            )

            thread = threading.Thread(target=worker.run, name="worker-heartbeat", daemon=True)
            thread.start()
            self.assertTrue(executor.started.wait(timeout=2.0))
            self.assertTrue(
                self._await(
                    lambda: {message_id, queued_message_id}.issubset(
                        {entry[0] for entry in source.extensions}
                    ),
                    timeout=2.0,
                )
            )
            executor.release.set()
            self.assertTrue(self._await(lambda: worker.health.succeeded == 2))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(
                {entry[0] for entry in source.extensions},
                {message_id, queued_message_id},
            )
            self.assertTrue(all(entry[1] == 1 for entry in source.extensions))
            self.assertEqual(source.pending(), 0)

    def test_spot_stop_is_forwarded_to_a_cooperative_executor(self) -> None:
        class _CooperativeExecutor:
            def __init__(self) -> None:
                self.stop_reasons: list[str] = []

            def prepare(self) -> None:
                return

            def execute(self, command: Command) -> dict[str, str]:
                return {"status": "DONE"}

            def request_stop(self, reason: str) -> None:
                self.stop_reasons.append(reason)

        with tempfile.TemporaryDirectory() as tmp:
            executor = _CooperativeExecutor()
            worker = PipelineWorker(
                WorkerConfig.from_environment(_environment(Path(tmp))),
                executor=executor,  # type: ignore[arg-type]
            )

            worker.request_stop("SIGTERM")

            self.assertEqual(executor.stop_reasons, ["SIGTERM"])

    def test_visibility_renewal_failure_leaves_the_message_for_redelivery(self) -> None:
        class _FailingHeartbeatSource(InProcessMessageSource):
            def extend_visibility(self, message: Any, *, timeout_seconds: int) -> None:
                raise RuntimeError("SQS visibility renewal unavailable")

        class _SlowExecutor:
            def prepare(self) -> None:
                return

            def execute(self, command: Command) -> dict[str, str]:
                time.sleep(0.5)
                return {"status": "DONE"}

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(
                    Path(tmp),
                    PIPELINE_WORKER_VISIBILITY_TIMEOUT_SECONDS="1",
                    PIPELINE_WORKER_RETRY_DELAY_SECONDS="0",
                )
            )
            source = _FailingHeartbeatSource()
            worker = PipelineWorker(
                config,
                message_source=source,
                executor=_SlowExecutor(),  # type: ignore[arg-type]
            )
            source.submit({"command": "VALIDATE_CATALOG", "command_id": "cmd-lease-lost"})
            first = source.poll(max_messages=1, wait_seconds=0.0)[0]

            worker._process(source, first)

            self.assertEqual(worker.results[0]["outcome"], "FAILED")
            self.assertEqual(
                worker.results[0]["detail"]["code"], "VISIBILITY_HEARTBEAT_FAILED"
            )
            redelivered = source.poll(max_messages=1, wait_seconds=0.0)
            self.assertEqual(len(redelivered), 1)
            self.assertEqual(redelivered[0].message_id, first.message_id)

    def test_validate_dataset_manifest_command_runs_the_canonical_validator(self) -> None:
        fixtures = json.loads(
            (Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json").read_text(
                encoding="utf-8"
            )
        )
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit(
                {
                    "command": "VALIDATE_DATASET_MANIFEST",
                    "command_id": "cmd-manifest",
                    "payload": {"manifest": fixtures["dataset_manifest"]},
                }
            )
            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.succeeded >= 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            detail = worker.results[0]["detail"]
            self.assertEqual(detail["status"], "ACCEPTED")
            self.assertEqual(
                detail["manifest_id"], fixtures["dataset_manifest"]["manifest_id"]
            )

    def test_publish_dataset_port_is_unconfigured_and_the_message_is_retried(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(Path(tmp), PIPELINE_WORKER_RETRY_DELAY_SECONDS="600")
            )
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "PUBLISH_DATASET", "command_id": "cmd-publish"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.failed >= 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            result = worker.results[0]
            self.assertEqual(result["outcome"], "FAILED")
            self.assertEqual(result["detail"]["code"], "PORT_NOT_CONFIGURED")
            self.assertIn("DP4", result["detail"]["reason"])
            # The command was not acknowledged: an unwired port must not eat work.
            self.assertEqual(source.pending(), 1)

    def test_injected_publication_port_is_used(self) -> None:
        class _RecordingPort:
            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            def publish(self, payload: Any) -> dict[str, Any]:
                self.calls.append(dict(payload))
                return {"status": "PUBLISHED", "revision": payload["revision"]}

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            port = _RecordingPort()
            source = InProcessMessageSource()
            worker = PipelineWorker(
                config,
                message_source=source,
                executor=PipelineCommandExecutor(config, publication_port=port),
            )
            source.submit(
                {
                    "command": "PUBLISH_DATASET",
                    "command_id": "cmd-publish-ok",
                    "payload": {"revision": 7},
                }
            )
            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.succeeded >= 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(port.calls, [{"revision": 7}])
            self.assertEqual(worker.results[0]["detail"]["status"], "PUBLISHED")

    def test_approval_redelivery_and_tamper_reach_durable_domain_verification(self) -> None:
        class _ApprovalPort:
            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            def apply(self, payload: Any) -> dict[str, Any]:
                self.calls.append(dict(payload))
                return {"state": "APPROVED", "regenerated": len(self.calls) == 1}

        payload = {
            "candidateId": "10000000-0000-4000-8000-000000000001",
            "deliveryId": "40000000-0000-4000-8000-000000000001",
            "rationale": "reviewed",
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            port = _ApprovalPort()
            source = InProcessMessageSource()
            worker = PipelineWorker(
                config,
                message_source=source,
                executor=PipelineCommandExecutor(
                    config, corporate_action_approval_port=port
                ),
            )
            for body in (payload, payload, {**payload, "rationale": "tampered"}):
                source.submit(
                    {
                        "command": "APPLY_CORPORATE_ACTION_APPROVAL",
                        "command_id": payload["deliveryId"],
                        "payload": body,
                    }
                )
            thread = threading.Thread(target=worker.run, name="approval-worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: worker.health.succeeded >= 3))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(len(port.calls), 3)
            self.assertEqual(port.calls[-1]["rationale"], "tampered")

    def test_permanent_approval_refusal_is_parked_without_retry(self) -> None:
        from market_pipeline_lib.corporate_actions import ApprovalRefusedError

        class _RefusingApprovalPort:
            def __init__(self) -> None:
                self.calls = 0

            def apply(self, payload: Any) -> dict[str, Any]:
                self.calls += 1
                raise ApprovalRefusedError("STALE_CONTENT_HASH", "protected hash is stale")

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            port = _RefusingApprovalPort()
            source = InProcessMessageSource()
            worker = PipelineWorker(
                config,
                message_source=source,
                executor=PipelineCommandExecutor(config, corporate_action_approval_port=port),
            )
            source.submit(
                {
                    "command": "APPLY_CORPORATE_ACTION_APPROVAL",
                    "command_id": "40000000-0000-4000-8000-000000000001",
                    "payload": {"candidateId": "10000000-0000-4000-8000-000000000001"},
                }
            )
            thread = threading.Thread(target=worker.run, name="refusal-worker", daemon=True)
            thread.start()
            self.assertTrue(self._await(lambda: len(source.dead_letters) == 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(port.calls, 1)
            self.assertEqual(source.dead_letters[0][1], "STALE_CONTENT_HASH")
            self.assertEqual(worker.results[0]["outcome"], "REJECTED")

    def test_run_refuses_to_start_twice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            worker = PipelineWorker(config)
            worker.request_stop("test")
            worker.run()
            with self.assertRaises(RuntimeError):
                worker.run()

    @staticmethod
    def _await(predicate: Any, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return False


class CommandEnvelopeTests(unittest.TestCase):
    def test_command_id_defaults_to_the_message_id(self) -> None:
        command = Command.parse({"command": "VALIDATE_CATALOG"}, fallback_command_id="msg-9")
        self.assertEqual(command.command_id, "msg-9")

    def test_malformed_envelopes_are_rejected(self) -> None:
        from apps.common.errors import MalformedEventError

        cases: dict[str, Any] = {
            "not a mapping": ["VALIDATE_CATALOG"],
            "missing command": {"command_id": "x"},
            "unknown command": {"command": "SHRED_EVERYTHING"},
            "unknown field": {"command": "VALIDATE_CATALOG", "urgency": "high"},
            "payload not an object": {"command": "VALIDATE_CATALOG", "payload": 3},
            "blank command_id": {"command": "VALIDATE_CATALOG", "command_id": " "},
        }
        for label, body in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(MalformedEventError):
                    Command.parse(body, fallback_command_id="msg-1")

    def test_manifest_command_without_a_manifest_is_rejected(self) -> None:
        from apps.common.errors import MalformedEventError

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            executor = PipelineCommandExecutor(config)
            command = Command.parse(
                {"command": "VALIDATE_DATASET_MANIFEST", "payload": {}},
                fallback_command_id="msg-2",
            )
            with self.assertRaises(MalformedEventError):
                executor.execute(command)

    def test_contract_violation_is_reported_as_a_rejected_manifest(self) -> None:
        fixtures = json.loads(
            (Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json").read_text(
                encoding="utf-8"
            )
        )
        manifest = dict(fixtures["dataset_manifest"], dataset_hash="0" * 64)
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            executor = PipelineCommandExecutor(config)
            command = Command.parse(
                {"command": "VALIDATE_DATASET_MANIFEST", "payload": {"manifest": manifest}},
                fallback_command_id="msg-3",
            )
            detail = executor.execute(command)
            self.assertEqual(detail["status"], "REJECTED")
            self.assertIn("dataset_hash", detail["violation"])


class WorkerEntryPointTests(unittest.TestCase):
    def test_main_returns_non_zero_on_missing_configuration(self) -> None:
        from apps.pipeline_worker.main import main

        self.assertEqual(main(argv=[], environment={}), 2)

    def test_main_print_env_lists_every_variable(self) -> None:
        from apps.pipeline_worker.config import ENVIRONMENT_VARIABLES
        from apps.pipeline_worker.main import main

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.assertEqual(main(argv=["--print-env"], environment={}), 0)
        printed = buffer.getvalue()
        for name, _required, _default, _description in ENVIRONMENT_VARIABLES:
            self.assertIn(name, printed)

    def test_main_reports_an_unconfigured_port_with_its_own_exit_code(self) -> None:
        from apps.pipeline_worker.main import EXIT_PORT_NOT_CONFIGURED, main

        with tempfile.TemporaryDirectory() as tmp:
            environment = _environment(
                Path(tmp),
                PIPELINE_WORKER_MESSAGE_SOURCE="sqs",
                PIPELINE_WORKER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/1/pipeline",
                PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/1/dlq",
            )
            saved = sys.modules.get("boto3")
            sys.modules["boto3"] = None  # type: ignore[assignment]
            try:
                self.assertEqual(main(argv=[], environment=environment), EXIT_PORT_NOT_CONFIGURED)
            finally:
                if saved is None:
                    del sys.modules["boto3"]
                else:
                    sys.modules["boto3"] = saved

    def test_main_check_config_succeeds_on_a_valid_environment(self) -> None:
        from apps.pipeline_worker.main import main

        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                main(argv=["--check-config"], environment=_environment(Path(tmp))),
                0,
            )


class StructuredLoggingTests(unittest.TestCase):
    def test_secrets_are_redacted(self) -> None:
        payload = {
            "aws_secret_access_key": "AKIAIOSFODNN7EXAMPLE",
            "api_key": "live-key",
            "database_dsn": "postgresql://user:hunter2@host:5432/db",
            "nested": {"password": "hunter2"},
            "safe": "value",
        }
        redacted = redact(payload)
        serialised = json.dumps(redacted)
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", serialised)
        self.assertNotIn("hunter2", serialised)
        self.assertNotIn("live-key", serialised)
        self.assertIn("value", serialised)

    def test_dsn_inside_a_plain_string_is_redacted(self) -> None:
        self.assertEqual(
            redact("postgresql://svc:s3cr3t@db:5432/market"),
            "postgresql://svc:***@db:5432/market",
        )

    def test_formatter_emits_one_json_object_per_record(self) -> None:
        record = logging.LogRecord(
            name="apps.pipeline_worker",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="worker.started",
            args=(),
            exc_info=None,
        )
        record.queue_url = "https://sqs/queue"
        record.api_key = "should-not-appear"
        payload = json.loads(JsonFormatter().format(record))
        self.assertEqual(payload["event"], "worker.started")
        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["api_key"], "***")


class HealthStateTests(unittest.TestCase):
    def test_transitions_follow_the_documented_lifecycle(self) -> None:
        health = HealthState()
        self.assertEqual(health.status, ReadinessStatus.STARTING)
        health.mark_ready()
        self.assertTrue(health.is_ready)
        health.mark_draining("SIGTERM")
        self.assertFalse(health.is_ready)
        self.assertEqual(health.snapshot()["stop_reason"], "SIGTERM")
        health.mark_stopped()
        self.assertEqual(health.status, ReadinessStatus.STOPPED)

    def test_failure_marks_the_worker_unhealthy(self) -> None:
        health = HealthState()
        health.mark_ready()
        health.mark_failed("catalog unreachable")
        self.assertFalse(health.is_ready)
        self.assertEqual(health.status, ReadinessStatus.FAILED)
        self.assertIn("catalog unreachable", str(health.snapshot()["stop_reason"]))


class HealthEndpointTests(unittest.TestCase):
    """The probe endpoint has to answer over HTTP, not just write a file."""

    def test_liveness_stays_up_while_readiness_follows_the_lifecycle(self) -> None:
        state = HealthState()
        endpoint = HealthEndpoint(state, host="127.0.0.1", port=0)
        endpoint.start()
        self.addCleanup(endpoint.stop)
        base = f"http://127.0.0.1:{endpoint.port}"

        status, body = _get(f"{base}/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "STARTING")
        # Live but not ready: a probe must not route work here yet.
        self.assertEqual(_get(f"{base}/ready")[0], 503)

        state.mark_ready()
        status, body = _get(f"{base}/ready")
        self.assertEqual(status, 200)
        self.assertTrue(body["ready"])

        state.mark_draining("SIGTERM")
        self.assertEqual(_get(f"{base}/ready")[0], 503)
        self.assertEqual(_get(f"{base}/health")[0], 200)

    def test_an_unknown_path_is_a_404_not_a_silent_200(self) -> None:
        endpoint = HealthEndpoint(HealthState(), host="127.0.0.1", port=0)
        endpoint.start()
        self.addCleanup(endpoint.stop)
        status, body = _get(f"http://127.0.0.1:{endpoint.port}/admin")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "NOT_FOUND")

    def test_the_port_is_released_on_stop(self) -> None:
        endpoint = HealthEndpoint(HealthState(), host="127.0.0.1", port=0)
        endpoint.start()
        port = endpoint.port
        endpoint.stop()
        with self.assertRaises(OSError):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2)  # noqa: S310


class WorkerHealthEndpointTests(unittest.TestCase):
    def test_a_running_worker_serves_readiness_over_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(Path(tmp), PIPELINE_WORKER_HEALTH_PORT="0")
            )
            worker = PipelineWorker(config)
            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.addCleanup(thread.join, 5.0)
            self.assertTrue(WorkerBootTests._await(lambda: worker.health.is_ready))
            port = worker.health_endpoint_port
            assert port is not None

            status, body = _get(f"http://127.0.0.1:{port}/ready")
            self.assertEqual(status, 200)
            self.assertEqual(body["status"], "READY")

            worker.request_stop("test")
            thread.join(timeout=5.0)
            with self.assertRaises(OSError):
                urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=2)  # noqa: S310


class DeadLetterTests(unittest.TestCase):
    def test_a_dead_letter_send_failure_never_deletes_the_original_message(self) -> None:
        class _UnavailableDeadLetterSource(InProcessMessageSource):
            def dead_letter(self, message: Any, *, reason: str) -> None:
                raise RuntimeError("DLQ unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(Path(tmp), PIPELINE_WORKER_RETRY_DELAY_SECONDS="0")
            )
            source = _UnavailableDeadLetterSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "MAKE_COFFEE", "command_id": "cmd-preserved"})
            first = source.poll(max_messages=1, wait_seconds=0.0)[0]

            worker._process(source, first)

            self.assertEqual(source.pending(), 1)
            redelivered = source.poll(max_messages=1, wait_seconds=0.0)
            self.assertEqual(len(redelivered), 1)
            self.assertEqual(redelivered[0].message_id, first.message_id)
            self.assertEqual(worker.results[0]["outcome"], "FAILED")

    def test_a_repeatedly_failing_message_is_parked_after_max_receives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(
                _environment(
                    Path(tmp),
                    PIPELINE_WORKER_MAX_RECEIVE_COUNT="3",
                    PIPELINE_WORKER_RETRY_DELAY_SECONDS="0",
                )
            )
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "PUBLISH_DATASET", "command_id": "cmd-doomed"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(WorkerBootTests._await(lambda: len(source.dead_letters) == 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            outcomes = [result["outcome"] for result in worker.results]
            self.assertEqual(outcomes, ["FAILED", "FAILED", "DEAD_LETTERED"])
            self.assertEqual([attempt["attempt"] for attempt in worker.results], [1, 2, 3])
            self.assertEqual(source.dead_letters[0][1], "MAX_RECEIVES_EXCEEDED")
            self.assertEqual(source.pending(), 0)
            self.assertEqual(worker.health.snapshot()["dead_lettered"], 1)

    def test_a_poison_message_is_parked_rather_than_quietly_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = WorkerConfig.from_environment(_environment(Path(tmp)))
            source = InProcessMessageSource()
            worker = PipelineWorker(config, message_source=source)
            source.submit({"command": "MAKE_COFFEE", "command_id": "cmd-poison"})

            thread = threading.Thread(target=worker.run, name="worker", daemon=True)
            thread.start()
            self.assertTrue(WorkerBootTests._await(lambda: worker.health.rejected >= 1))
            worker.request_stop("test")
            thread.join(timeout=5.0)

            self.assertEqual(worker.results[0]["outcome"], "REJECTED")
            self.assertEqual([reason for _, reason in source.dead_letters], ["UNKNOWN_COMMAND"])


class RealtimeCommandTests(unittest.TestCase):
    """The worker hosts the DP5 consumer, not just a validator."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        self.addCleanup(self._remove)

    def _remove(self) -> None:
        from market_pipeline_lib.fs_paths import long_path

        shutil.rmtree(long_path(self.root), ignore_errors=True)

    def test_realtime_bars_are_ingested_and_published_under_the_canonical_key(self) -> None:
        root = Path(self.root)
        config = WorkerConfig.from_environment(
            _environment(root, PIPELINE_WORKER_REALTIME_INGEST=_realtime_settings(root))
        )
        source = InProcessMessageSource()
        worker = PipelineWorker(config, message_source=source)
        source.submit(
            {
                "command": "INGEST_REALTIME_BARS",
                "command_id": "cmd-realtime",
                "payload": {"events": _realtime_events(), "flush": True},
            }
        )
        thread = threading.Thread(target=worker.run, name="worker", daemon=True)
        thread.start()
        self.assertTrue(WorkerBootTests._await(lambda: worker.health.succeeded >= 1))
        worker.request_stop("test")
        thread.join(timeout=5.0)

        detail = worker.results[0]["detail"]
        self.assertEqual(detail["status"], "AVAILABLE")
        self.assertEqual(detail["accepted"], 8)
        self.assertEqual(detail["row_count"], 8)
        self.assertEqual(detail["watermark_position"], "2024-01-08T16:00:00Z")
        self.assertIn(
            "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_30M"
            "/dataset=2eab5266-777f-5cbb-8715-8e799b308cff/revision=1/layer=RAW"
            "/resolution=30m/granularity=DAY/partition_start=2024-01-08"
            "/partition_end=2024-01-09/shard=s00-of-2/part-00001.parquet",
            detail["object_keys"],
        )

    def test_a_realtime_command_without_configuration_fails_loudly(self) -> None:
        config = WorkerConfig.from_environment(_environment(Path(self.root)))
        executor = PipelineCommandExecutor(config)
        command = Command.parse(
            {"command": "INGEST_REALTIME_BARS", "payload": {"events": _realtime_events(1)}},
            fallback_command_id="msg-rt",
        )
        with self.assertRaises(PortNotConfiguredError) as raised:
            executor.execute(command)
        self.assertIn("PIPELINE_WORKER_REALTIME_INGEST", str(raised.exception))

    def test_a_redelivered_realtime_batch_does_not_duplicate_rows(self) -> None:
        root = Path(self.root)
        config = WorkerConfig.from_environment(
            _environment(root, PIPELINE_WORKER_REALTIME_INGEST=_realtime_settings(root))
        )
        executor = PipelineCommandExecutor(config)
        executor.prepare()
        events = _realtime_events()
        first = executor.execute(
            Command.parse(
                {"command": "INGEST_REALTIME_BARS", "payload": {"events": events, "flush": True}},
                fallback_command_id="msg-1",
            )
        )
        second = executor.execute(
            Command.parse(
                {"command": "INGEST_REALTIME_BARS", "payload": {"events": events, "flush": True}},
                fallback_command_id="msg-2",
            )
        )
        self.assertEqual(first["accepted"], 8)
        self.assertEqual((second["accepted"], second["skipped"]), (0, 8))
        self.assertEqual(second["status"], "NO_CHANGE")

    def test_database_url_selects_the_durable_sql_watermark_repository(self) -> None:
        from apps.pipeline_worker.realtime import EngineRealtimeIngestPort
        from market_pipeline_lib.watermarks import SqlWatermarkRepository

        root = Path(self.root)
        config = WorkerConfig.from_environment(
            _environment(
                root,
                PIPELINE_WORKER_REALTIME_INGEST=_realtime_settings(root),
                PIPELINE_WORKER_DATABASE_URL="postgresql+psycopg://pipeline:secret@db/idea2strategy",
            )
        )
        guarded_engine = Mock()
        with patch(
            "market_pipeline_lib.db.engine.create_market_data_engine",
            return_value=guarded_engine,
        ) as create_engine:
            repository = EngineRealtimeIngestPort(config)._watermark_repository()

        self.assertIsInstance(repository, SqlWatermarkRepository)
        create_engine.assert_called_once_with(
            "postgresql+psycopg://pipeline:secret@db/idea2strategy",
            writable_schemas=["market_data"],
            application_name="idea2strategy-pipeline-worker-watermarks",
        )

    def test_realtime_port_refuses_new_work_after_cooperative_stop(self) -> None:
        from apps.common.errors import ExecutionCancelledError
        from apps.pipeline_worker.realtime import EngineRealtimeIngestPort

        root = Path(self.root)
        config = WorkerConfig.from_environment(
            _environment(root, PIPELINE_WORKER_REALTIME_INGEST=_realtime_settings(root))
        )
        port = EngineRealtimeIngestPort(config)
        port.request_stop("SIGTERM")

        with self.assertRaises(ExecutionCancelledError) as raised:
            port.ingest(_realtime_events(1), flush=True)
        self.assertIn("SIGTERM", str(raised.exception))

    def test_executor_forwards_cooperative_stop_to_the_active_port(self) -> None:
        root = Path(self.root)
        config = WorkerConfig.from_environment(_environment(root))
        realtime = Mock()
        executor = PipelineCommandExecutor(config, realtime_port=realtime)

        executor.request_stop("spot-interruption")

        realtime.request_stop.assert_called_once_with("spot-interruption")


# --------------------------------------------------------------------------------------
# LocalStack
# --------------------------------------------------------------------------------------


@pytest.fixture
def worker_sqs_queues() -> Any:
    if not LOCALSTACK_ENDPOINT_URL:
        pytest.skip("set LOCALSTACK_ENDPOINT_URL to run the LocalStack worker integration tests")
    import boto3

    client = boto3.client(
        "sqs",
        endpoint_url=LOCALSTACK_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    suffix = uuid.uuid4().hex[:10]
    dead_letter_url = client.create_queue(QueueName=f"dp5-worker-dlq-{suffix}")["QueueUrl"]
    queue_url = client.create_queue(
        QueueName=f"dp5-worker-{suffix}", Attributes={"VisibilityTimeout": "0"}
    )["QueueUrl"]
    try:
        yield client, queue_url, dead_letter_url
    finally:
        for url in (queue_url, dead_letter_url):
            try:
                client.delete_queue(QueueUrl=url)
            except Exception:  # pragma: no cover - teardown must not mask a failure
                pass


@pytest.mark.integration
class TestWorkerOverLocalStackSqs:
    def _environment(self, root: Path, queue_url: str, dead_letter_url: str, **extra: str) -> dict[str, str]:
        return _environment(
            root,
            PIPELINE_WORKER_MESSAGE_SOURCE="sqs",
            PIPELINE_WORKER_QUEUE_URL=queue_url,
            PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL=dead_letter_url,
            PIPELINE_WORKER_AWS_ENDPOINT_URL=LOCALSTACK_ENDPOINT_URL or "",
            PIPELINE_WORKER_AWS_REGION="us-east-1",
            PIPELINE_WORKER_VISIBILITY_TIMEOUT_SECONDS="0",
            PIPELINE_WORKER_POLL_INTERVAL_SECONDS="1",
            **extra,
        )

    def test_the_worker_drains_a_real_queue_and_reports_ready_over_http(
        self, worker_sqs_queues: Any, tmp_path: Path
    ) -> None:
        client, queue_url, dead_letter_url = worker_sqs_queues
        client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"command": "VALIDATE_CATALOG", "command_id": "sqs-1"}),
        )
        config = WorkerConfig.from_environment(
            self._environment(tmp_path, queue_url, dead_letter_url, PIPELINE_WORKER_HEALTH_PORT="0")
        )
        worker = PipelineWorker(config)
        thread = threading.Thread(target=worker.run, name="worker-sqs", daemon=True)
        thread.start()
        try:
            assert WorkerBootTests._await(lambda: worker.health.succeeded >= 1, timeout=30.0)
            status, body = _get(f"http://127.0.0.1:{worker.health_endpoint_port}/ready")
            assert (status, body["status"]) == (200, "READY")
        finally:
            worker.request_stop("test")
            thread.join(timeout=15.0)

        assert worker.results[0]["detail"]["status"] == "PASSED"
        assert client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1).get("Messages", []) == []

    def test_a_failing_command_is_parked_on_the_real_dead_letter_queue(
        self, worker_sqs_queues: Any, tmp_path: Path
    ) -> None:
        client, queue_url, dead_letter_url = worker_sqs_queues
        client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"command": "PUBLISH_DATASET", "command_id": "sqs-doomed"}),
        )
        config = WorkerConfig.from_environment(
            self._environment(
                tmp_path,
                queue_url,
                dead_letter_url,
                PIPELINE_WORKER_MAX_RECEIVE_COUNT="2",
                PIPELINE_WORKER_RETRY_DELAY_SECONDS="0",
            )
        )
        worker = PipelineWorker(config)
        thread = threading.Thread(target=worker.run, name="worker-sqs-dlq", daemon=True)
        thread.start()
        try:
            assert WorkerBootTests._await(
                lambda: worker.health.snapshot()["dead_lettered"] >= 1, timeout=30.0
            )
        finally:
            worker.request_stop("test")
            thread.join(timeout=15.0)

        parked = client.receive_message(
            QueueUrl=dead_letter_url, MaxNumberOfMessages=1, WaitTimeSeconds=5, MessageAttributeNames=["All"]
        )["Messages"]
        assert len(parked) == 1
        assert json.loads(parked[0]["Body"])["command_id"] == "sqs-doomed"
        assert parked[0]["MessageAttributes"]["DeadLetterReason"]["StringValue"] == "MAX_RECEIVES_EXCEEDED"

    def test_approval_duplicate_and_tamper_cross_real_sqs_boundary(
        self, worker_sqs_queues: Any, tmp_path: Path
    ) -> None:
        class _ApprovalPort:
            def __init__(self) -> None:
                self.calls: list[dict[str, Any]] = []

            def apply(self, payload: Any) -> dict[str, Any]:
                self.calls.append(dict(payload))
                return {"state": "APPROVED", "regenerated": len(self.calls) == 1}

        client, queue_url, dead_letter_url = worker_sqs_queues
        delivery_id = "40000000-0000-4000-8000-000000000001"
        payload = {"candidateId": "10000000-0000-4000-8000-000000000001", "rationale": "reviewed"}
        for body in (payload, payload, {**payload, "rationale": "tampered"}):
            client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(
                    {
                        "command": "APPLY_CORPORATE_ACTION_APPROVAL",
                        "command_id": delivery_id,
                        "payload": body,
                    }
                ),
            )
        config = WorkerConfig.from_environment(
            self._environment(tmp_path, queue_url, dead_letter_url)
        )
        port = _ApprovalPort()
        worker = PipelineWorker(
            config,
            executor=PipelineCommandExecutor(config, corporate_action_approval_port=port),
        )
        thread = threading.Thread(target=worker.run, name="approval-sqs", daemon=True)
        thread.start()
        try:
            assert WorkerBootTests._await(lambda: worker.health.succeeded >= 3, timeout=30.0)
        finally:
            worker.request_stop("test")
            thread.join(timeout=15.0)

        assert len(port.calls) == 3
        assert port.calls[-1]["rationale"] == "tampered"

    def test_the_sqs_message_source_carries_the_receive_count_through(
        self, worker_sqs_queues: Any, tmp_path: Path
    ) -> None:
        client, queue_url, dead_letter_url = worker_sqs_queues
        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"command": "VALIDATE_CATALOG"}))
        config = WorkerConfig.from_environment(
            self._environment(tmp_path, queue_url, dead_letter_url)
        )
        source = build_message_source(config)
        assert isinstance(source, SqsMessageSource)
        try:
            first = source.poll(max_messages=1, wait_seconds=5.0)[0]
            assert first.receive_count == 1
            source.retry_later(first, delay_seconds=0.0)
            second = source.poll(max_messages=1, wait_seconds=5.0)[0]
            assert second.receive_count == 2
            source.acknowledge(second)
            assert source.poll(max_messages=1, wait_seconds=1.0) == []
        finally:
            source.close()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
