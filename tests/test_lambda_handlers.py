"""Contract tests for the three D-bundle Lambda handlers (D01).

Per handler:
  * a valid event produces a structured result;
  * a malformed or unknown event is rejected with a typed error, never an
    empty success;
  * a duplicate delivery is idempotent (AWS delivers at least once);
  * selecting a port whose adapter belongs to a later stage fails loudly.
"""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from apps.common.errors import MalformedEventError, PortNotConfiguredError
from apps.common.idempotency import InMemoryIdempotencyStore
from lambdas.corporate_action_research.handler import (
    CorporateActionResearchHandler,
    PartialResearchSlotError,
    ResearchFinding,
)
from lambdas.corporate_action_research.handler import handler as corporate_action_handler
from lambdas.lightweight_validation.handler import LightweightValidationHandler
from lambdas.lightweight_validation.handler import handler as lightweight_handler
from lambdas.pipeline_trigger.handler import PipelineTriggerHandler
from lambdas.pipeline_trigger.handler import handler as pipeline_trigger_handler
from market_pipeline_lib.corporate_action_research import Claim, Evidence, SplitTerms

FIXTURE_PATH = Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json"


class _FakeContext:
    aws_request_id = "req-0001"
    function_name = "test-function"


class _RecordingCommandSink:
    def __init__(self) -> None:
        self.sent: list[Mapping[str, Any]] = []

    def send(self, command: Mapping[str, Any]) -> str:
        self.sent.append(dict(command))
        return f"sqs-{len(self.sent)}"


class _StubResearchPort:
    def __init__(self, findings: Mapping[str, Sequence[ResearchFinding]]) -> None:
        self.findings = findings
        self.calls: list[str] = []

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        self.calls.append(ticker)
        return self.findings.get(ticker, ())


def _worker_config(root: Path) -> Any:
    """A `WorkerConfig` pointing at throwaway roots.

    Built directly rather than from the environment: this test is about command
    routing, and going through `from_environment` would couple it to the worker's
    configuration contract, which is not what it is checking.
    """

    from apps.pipeline_worker.config import WorkerConfig

    return WorkerConfig(
        environment="test",
        message_source="inline",
        catalog_root=root / "catalog",
        object_store_root=root / "objects",
        queue_url=None,
        dead_letter_queue_url=None,
        aws_endpoint_url=None,
        aws_region="us-east-1",
        log_level="INFO",
        poll_interval_seconds=0.1,
        max_messages_per_poll=1,
        retry_delay_seconds=0.1,
        shutdown_grace_seconds=0.1,
        idempotency_cache_size=16,
        max_receive_count=3,
        visibility_timeout_seconds=30,
        health_file=None,
        health_host="127.0.0.1",
        health_port=None,
        realtime=None,
    )


def _sealed_snapshot_document() -> dict[str, Any]:
    """A genuine D13 `feature-snapshot` document, produced by the D13 code path.

    Not hand-written: the point of validating it here is that the producer's own output
    passes the consumer's gate, which is exactly what the COM06 fixtures failed to do.
    """

    from decimal import Decimal

    from market_pipeline_lib.catalog import LocalCatalog
    from market_pipeline_lib.features import (
        BarPoint,
        FeatureDefinition,
        FeatureDefinitionRegistry,
        FeatureMaterializer,
        FeatureSnapshotBatchBuilder,
        MarketInput,
        MaterializationRequest,
        SnapshotBatchPlan,
        SourceObject,
        input_bundle_fingerprint,
    )

    instrument = "aaaaaaaa-0000-4000-8000-000000000001"
    period_start = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    period_end = datetime(2026, 1, 5, 21, 0, tzinfo=UTC)
    source = SourceObject(
        dataset_object_id="cccccccc-0000-4000-8000-000000000001",
        dataset_manifest_id="dddddddd-0000-4000-8000-000000000001",
        content_hash="1" * 64,
        partition_start="2026-01-05",
        partition_end="2026-01-06",
        row_count=13,
    )
    series = tuple(
        BarPoint(
            bar_start_at=datetime(2026, 1, 5, 14 + (30 + 30 * index) // 60, (30 + 30 * index) % 60, tzinfo=UTC),
            open=Decimal(close) - 1,
            high=Decimal(close) + 1,
            low=Decimal(close) - 2,
            close=Decimal(close),
            volume=1000 + index,
        )
        for index, close in enumerate((100, 104, 108, 111, 107))
    )

    with tempfile.TemporaryDirectory() as directory:
        catalog = LocalCatalog(Path(directory) / "catalog")
        registry = FeatureDefinitionRegistry(catalog)
        definition = registry.publish(
            FeatureDefinition.create(
                element_catalog_version_id="0e5a1c9e-1111-4a11-8a11-000000000001",
                feature_code="SMA",
                calculator_version="1.0.0",
                resolution="30m",
                parameters={"window": 3, "price_field": "close"},
            )
        )
        plan = SnapshotBatchPlan(
            definition_hashes=(definition.definition_hash,),
            market_inputs=(
                MarketInput(
                    instrument_id=instrument,
                    input_dataset_set_hash=input_bundle_fingerprint((source,)),
                ),
            ),
            period_start=period_start,
            period_end=period_end,
            source_start_watermark="ALPACA_SIP_RAW_30M@2026-01-05T14:30:00Z",
            source_end_watermark="ALPACA_SIP_RAW_30M@2026-01-05T21:00:00Z",
        )
        builder = FeatureSnapshotBatchBuilder(catalog, registry)
        builder.open(plan)
        result = FeatureMaterializer(catalog, registry).materialize(
            MaterializationRequest(
                definition=definition,
                instrument_id=instrument,
                pipeline_run_id="11111111-0000-4000-8000-000000000001",
                sources=(source,),
                bars=series,
                period_start=period_start,
                period_end=period_end,
                source_watermark="ALPACA_SIP_RAW_30M@2026-01-05T21:00:00Z",
                output_dataset_manifest_id="dddddddd-0000-4000-8000-000000000011",
            )
        )
        return builder.seal(
            plan,
            results=(result,),
            snapshot_object_id="eeeeeeee-0000-4000-8000-000000000001",
        ).to_document()


def _evidence(suffix: str) -> Evidence:
    return Evidence(
        source_uri=f"https://investor.example.com/{suffix}",
        source_title=f"Press release {suffix}",
        content_sha256=suffix.encode().hex().ljust(64, "0")[:64],
        retrieved_at=datetime(2026, 8, 2, 5, 30, tzinfo=UTC),
    )


def _split_finding(suffix: str = "aapl", *, proposed: date = date(2026, 8, 20)) -> ResearchFinding:
    """A 1-for-2 split with the terms and claims DP-e's domain model requires.

    Every material field -- event_type, effective_date and both terms fields -- carries
    its own claim citing evidence the finding actually holds, because
    `ResearchCandidate` refuses anything less.
    """

    evidence = _evidence(suffix)
    terms = SplitTerms(from_shares=1, to_shares=2)
    material = {
        "event_type": terms.event_type,
        "effective_date": proposed.isoformat(),
        **terms.claim_fields(),
    }
    return ResearchFinding(
        event_type=terms.event_type,
        proposed_date=proposed,
        terms=terms,
        evidence=(evidence,),
        claims=tuple(
            Claim(
                field=field,
                value=value,
                source_uri=evidence.source_uri,
                confidence=Decimal("0.95"),
            )
            for field, value in material.items()
        ),
    )


# ---------------------------------------------------------------------------
# pipeline_trigger
# ---------------------------------------------------------------------------
class PipelineTriggerHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sink = _RecordingCommandSink()
        self.handler = PipelineTriggerHandler(
            command_sink=self.sink,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        self.event = {
            "triggerId": "nightly-2026-08-02",
            "command": "VALIDATE_CATALOG",
            "requestedAt": "2026-08-02T06:00:00Z",
        }

    def test_valid_event_enqueues_one_command(self) -> None:
        result = self.handler.handle(self.event, _FakeContext())
        self.assertEqual(result["handler"], "pipeline-trigger")
        self.assertEqual(result["status"], "ENQUEUED")
        self.assertEqual(result["idempotencyKey"], "nightly-2026-08-02")
        self.assertEqual(result["requestId"], "req-0001")
        self.assertEqual(result["result"]["providerMessageId"], "sqs-1")
        self.assertEqual(len(self.sink.sent), 1)
        self.assertEqual(self.sink.sent[0]["command"], "VALIDATE_CATALOG")
        self.assertEqual(self.sink.sent[0]["command_id"], "nightly-2026-08-02")
        json.dumps(result)

    def test_optional_payload_is_forwarded(self) -> None:
        event = dict(self.event, payload={"revision": 3})
        self.handler.handle(event, _FakeContext())
        self.assertEqual(self.sink.sent[0]["payload"], {"revision": 3})

    def test_duplicate_delivery_is_idempotent(self) -> None:
        first = self.handler.handle(self.event, _FakeContext())
        second = self.handler.handle(copy.deepcopy(self.event), _FakeContext())
        self.assertEqual(first["status"], "ENQUEUED")
        self.assertEqual(second["status"], "DUPLICATE")
        self.assertEqual(len(self.sink.sent), 1, "redelivery must not enqueue twice")
        self.assertEqual(second["result"]["providerMessageId"], "sqs-1")

    def test_malformed_events_are_rejected(self) -> None:
        cases: dict[str, Any] = {
            "not a mapping": ["triggerId"],
            "missing triggerId": {"command": "VALIDATE_CATALOG", "requestedAt": "2026-08-02T06:00:00Z"},
            "missing command": {"triggerId": "t", "requestedAt": "2026-08-02T06:00:00Z"},
            "missing requestedAt": {"triggerId": "t", "command": "VALIDATE_CATALOG"},
            "unknown field": dict(self.event, surprise=1),
            "unknown command": dict(self.event, command="DROP_EVERYTHING"),
            "non-UTC timestamp": dict(self.event, requestedAt="2026-08-02T06:00:00+09:00"),
            "blank triggerId": dict(self.event, triggerId="   "),
            "payload not an object": dict(self.event, payload=[1, 2]),
        }
        for label, event in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(MalformedEventError):
                    self.handler.handle(event, _FakeContext())
        self.assertEqual(self.sink.sent, [], "a rejected event must not enqueue anything")

    def test_unconfigured_command_sink_fails_loudly(self) -> None:
        unwired = PipelineTriggerHandler()
        with self.assertRaises(PortNotConfiguredError) as raised:
            unwired.handle(self.event, _FakeContext())
        self.assertIn("DP5", str(raised.exception))

    def test_failed_send_releases_the_idempotency_claim(self) -> None:
        class _BrokenSink:
            def send(self, command: Mapping[str, Any]) -> str:
                raise RuntimeError("sqs unavailable")

        store = InMemoryIdempotencyStore()
        broken = PipelineTriggerHandler(command_sink=_BrokenSink(), idempotency_store=store)
        with self.assertRaises(RuntimeError):
            broken.handle(self.event, _FakeContext())
        # The claim must be released, otherwise the retry is swallowed as a
        # duplicate and the command is lost.
        self.assertFalse(store.seen("nightly-2026-08-02"))

    def test_module_level_handler_is_the_unwired_default(self) -> None:
        with self.assertRaises(PortNotConfiguredError):
            pipeline_trigger_handler(self.event, _FakeContext())

    def test_every_supported_command_is_enqueued_and_actually_routable(self) -> None:
        """Producer/consumer agreement over the whole command set, not a pinned list.

        Three things per command, because "accepted" and "executed" are different
        claims and the gap between them is an empty-success path:

        1. `pipeline-trigger` enqueues it;
        2. the worker's own `Command.parse` accepts the message the trigger produced
           -- the same producer-consumer drift COM06 suffered, caught here instead;
        3. `PipelineCommandExecutor` has a real branch for it.  Reaching a typed
           refusal (an unwired port, a payload rule) proves the branch exists;
           `UnknownCommandError` proves it does not.

        Parameterised over `SUPPORTED_COMMANDS` itself, so a command added later is
        covered the day it lands rather than when someone remembers to edit a list.
        """

        from apps.common.errors import PipelineAppError, UnknownCommandError
        from apps.pipeline_worker.commands import (
            SUPPORTED_COMMANDS,
            Command,
            PipelineCommandExecutor,
        )

        fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        payloads: dict[str, dict[str, Any]] = {
            "VALIDATE_CATALOG": {},
            "VALIDATE_DATASET_MANIFEST": {"manifest": fixtures["dataset_manifest"]},
            "PUBLISH_DATASET": {"manifest_id": "m-1"},
            "INGEST_REALTIME_BARS": {
                "events": [{"symbol": "AAPL", "value": "1"}],
                "flush": False,
            },
            "APPLY_CORPORATE_ACTION_APPROVAL": {
                "candidateId": "10000000-0000-4000-8000-000000000001",
                "decision": "APPROVE",
                "decidedContentHash": "a" * 64,
                "evidenceBindings": ["b" * 64],
                "actorId": "20000000-0000-4000-8000-000000000001",
                "auditId": "30000000-0000-4000-8000-000000000001",
                "permissionId": "20000000-0000-4000-8000-000000000012",
                "requestSchemaVersion": "schema-v1",
                "decidedAt": "2026-08-04T15:00:00Z",
                "deliveryId": "40000000-0000-4000-8000-000000000001",
                "aggregateSequence": 1,
            },
        }
        missing = sorted(set(SUPPORTED_COMMANDS) - set(payloads))
        self.assertEqual(
            missing,
            [],
            f"SUPPORTED_COMMANDS gained {missing}; add a representative payload so this "
            "test exercises the new command's executor branch rather than skipping it",
        )

        with tempfile.TemporaryDirectory() as directory:
            executor = PipelineCommandExecutor(_worker_config(Path(directory)))
            executor.prepare()
            for name in SUPPORTED_COMMANDS:
                with self.subTest(command=name):
                    sink = _RecordingCommandSink()
                    trigger = PipelineTriggerHandler(
                        command_sink=sink, idempotency_store=InMemoryIdempotencyStore()
                    )
                    result = trigger.handle(
                        {
                            "triggerId": f"trigger-{name}",
                            "command": name,
                            "requestedAt": "2026-08-02T06:00:00Z",
                            "payload": payloads[name],
                        },
                        _FakeContext(),
                    )
                    self.assertEqual(result["status"], "ENQUEUED")

                    parsed = Command.parse(sink.sent[0], fallback_command_id="unused")
                    self.assertEqual(parsed.command, name)
                    self.assertEqual(parsed.command_id, f"trigger-{name}")
                    self.assertEqual(dict(parsed.payload), payloads[name])

                    try:
                        executor.execute(parsed)
                    except UnknownCommandError as error:
                        self.fail(
                            f"pipeline-trigger enqueues {name} but pipeline-worker has no "
                            f"executor branch for it, so the command would be accepted and "
                            f"then dropped: {error}"
                        )
                    except PipelineAppError:
                        # A typed refusal -- an unwired port or a payload rule.  The
                        # branch was reached, which is what this asserts.
                        pass


# ---------------------------------------------------------------------------
# lightweight_validation
# ---------------------------------------------------------------------------
class LightweightValidationHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        self.handler = LightweightValidationHandler(
            idempotency_store=InMemoryIdempotencyStore()
        )
        self.event = {
            "validationId": "val-0001",
            "documentType": "dataset-manifest",
            "document": self.fixtures["dataset_manifest"],
        }

    def test_valid_manifest_is_accepted(self) -> None:
        result = self.handler.handle(self.event, _FakeContext())
        self.assertEqual(result["handler"], "lightweight-validation")
        self.assertEqual(result["status"], "VALIDATED")
        self.assertEqual(result["result"]["decision"], "ACCEPTED")
        self.assertEqual(
            result["result"]["manifestId"], self.fixtures["dataset_manifest"]["manifest_id"]
        )
        json.dumps(result)

    def test_contract_violation_is_reported_as_a_rejection_not_a_crash(self) -> None:
        document = copy.deepcopy(self.fixtures["dataset_manifest"])
        document["dataset_hash"] = "0" * 64
        result = self.handler.handle(dict(self.event, document=document), _FakeContext())
        self.assertEqual(result["status"], "VALIDATED")
        self.assertEqual(result["result"]["decision"], "REJECTED")
        self.assertIn("dataset_hash", result["result"]["violation"])

    def test_duplicate_delivery_returns_the_first_decision(self) -> None:
        first = self.handler.handle(self.event, _FakeContext())
        second = self.handler.handle(copy.deepcopy(self.event), _FakeContext())
        self.assertEqual(first["status"], "VALIDATED")
        self.assertEqual(second["status"], "DUPLICATE")
        self.assertEqual(second["result"], first["result"])

    def test_malformed_events_are_rejected(self) -> None:
        cases: dict[str, Any] = {
            "not a mapping": "dataset-manifest",
            "missing validationId": {
                "documentType": "dataset-manifest",
                "document": self.fixtures["dataset_manifest"],
            },
            "missing documentType": {
                "validationId": "v",
                "document": self.fixtures["dataset_manifest"],
            },
            "missing document": {"validationId": "v", "documentType": "dataset-manifest"},
            "unknown documentType": dict(self.event, documentType="tea-leaves"),
            "unknown field": dict(self.event, extra=True),
            "document not an object": dict(self.event, document=[1]),
        }
        for label, event in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(MalformedEventError):
                    self.handler.handle(event, _FakeContext())

    def test_a_sealed_feature_snapshot_is_accepted(self) -> None:
        result = self.handler.handle(
            {
                "validationId": "val-0002",
                "documentType": "feature-snapshot",
                "document": _sealed_snapshot_document(),
            },
            _FakeContext(),
        )
        self.assertEqual(result["status"], "VALIDATED")
        self.assertEqual(result["result"]["decision"], "ACCEPTED")
        self.assertEqual(result["result"]["documentType"], "feature-snapshot")
        json.dumps(result)

    def test_a_partially_materialized_feature_snapshot_is_rejected(self) -> None:
        document = dict(_sealed_snapshot_document(), status="PENDING")
        result = self.handler.handle(
            {
                "validationId": "val-0003",
                "documentType": "feature-snapshot",
                "document": document,
            },
            _FakeContext(),
        )
        self.assertEqual(result["status"], "VALIDATED")
        self.assertEqual(result["result"]["decision"], "REJECTED")
        self.assertIn("SUCCEEDED", result["result"]["violation"])

    def test_a_placeholder_version_string_is_rejected(self) -> None:
        """The exact drift COM06 suffered: a text field carrying a label, not a pin."""

        document = dict(
            _sealed_snapshot_document(),
            feature_materialization_version="feature-materialization-v1",
        )
        result = self.handler.handle(
            {
                "validationId": "val-0004",
                "documentType": "feature-snapshot",
                "document": document,
            },
            _FakeContext(),
        )
        self.assertEqual(result["result"]["decision"], "REJECTED")
        self.assertIn("feature-materialization-v1", result["result"]["violation"])

    def test_a_failing_feature_port_does_not_consume_the_idempotency_key(self) -> None:
        """An infrastructure fault must leave the redelivery a real attempt."""

        class _BrokenPort:
            calls = 0

            def validate(self, document: Mapping[str, Any]) -> Mapping[str, Any]:
                type(self).calls += 1
                raise RuntimeError("feature store unreachable")

        port = _BrokenPort()
        store = InMemoryIdempotencyStore()
        handler = LightweightValidationHandler(feature_port=port, idempotency_store=store)
        event = {
            "validationId": "val-0005",
            "documentType": "feature-snapshot",
            "document": _sealed_snapshot_document(),
        }
        for _ in range(2):
            with self.assertRaises(RuntimeError):
                handler.handle(event, _FakeContext())
        self.assertEqual(_BrokenPort.calls, 2, "a released claim must let the retry run")
        self.assertFalse(store.seen("val-0005"))

    def test_an_injected_port_replaces_the_default_validator(self) -> None:
        class _AlwaysRejects:
            def validate(self, document: Mapping[str, Any]) -> Mapping[str, Any]:
                return {"documentType": "feature-snapshot", "decision": "REJECTED", "violation": "stub"}

        handler = LightweightValidationHandler(
            feature_port=_AlwaysRejects(), idempotency_store=InMemoryIdempotencyStore()
        )
        result = handler.handle(
            {
                "validationId": "val-0006",
                "documentType": "feature-snapshot",
                "document": _sealed_snapshot_document(),
            },
            _FakeContext(),
        )
        self.assertEqual(result["result"]["violation"], "stub")

    def test_the_module_level_handler_validates_a_feature_snapshot(self) -> None:
        result = lightweight_handler(
            {
                "validationId": "module-level-0002",
                "documentType": "feature-snapshot",
                "document": _sealed_snapshot_document(),
            },
            _FakeContext(),
        )
        self.assertEqual(result["result"]["decision"], "ACCEPTED")

    def test_module_level_handler_validates_a_real_manifest(self) -> None:
        result = lightweight_handler(
            {
                "validationId": "module-level-0001",
                "documentType": "dataset-manifest",
                "document": self.fixtures["dataset_manifest"],
            },
            _FakeContext(),
        )
        self.assertEqual(result["result"]["decision"], "ACCEPTED")


# ---------------------------------------------------------------------------
# corporate_action_research
# ---------------------------------------------------------------------------
class CorporateActionResearchHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store_path = Path(self._tmp.name) / "candidates.jsonl"
        self.port = _StubResearchPort({"AAPL": (_split_finding("aapl"),)})
        self.handler = CorporateActionResearchHandler(
            research_port=self.port,
            candidate_store_path=self.store_path,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        self.event = {
            "researchRunId": "car-2026-08-02T06",
            "slotScheduledAt": "2026-08-02T06:00:00Z",
            "tickers": ["AAPL", "MSFT"],
        }

    def test_valid_event_records_candidates(self) -> None:
        result = self.handler.handle(self.event, _FakeContext())
        self.assertEqual(result["handler"], "corporate-action-research")
        self.assertEqual(result["status"], "RESEARCHED")
        self.assertEqual(result["result"]["tickersResearched"], 2)
        self.assertEqual(result["result"]["candidatesRecorded"], 1)
        self.assertEqual(result["result"]["candidatesAlreadyKnown"], 0)
        self.assertEqual(self.port.calls, ["AAPL", "MSFT"])
        json.dumps(result)

        lines = self.store_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["ticker"], "AAPL")
        # DP-e's canonical action type, and the terms without which the split factor
        # could not be computed at all.
        self.assertEqual(record["event_type"], "STOCK_SPLIT")
        self.assertEqual(record["terms"], {"event_type": "STOCK_SPLIT", "from_shares": "1", "to_shares": "2"})
        self.assertEqual(record["confidence"], "0.9500")
        self.assertEqual(record["workflow_state"], "REVIEW_REQUIRED")
        self.assertEqual(len(record["candidate_id"]), 64)
        self.assertEqual(result["result"]["candidatesPerTicker"], {"AAPL": 1, "MSFT": 0})

    def test_duplicate_delivery_is_idempotent(self) -> None:
        first = self.handler.handle(self.event, _FakeContext())
        second = self.handler.handle(copy.deepcopy(self.event), _FakeContext())
        self.assertEqual(first["status"], "RESEARCHED")
        self.assertEqual(second["status"], "DUPLICATE")
        self.assertEqual(len(self.store_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(self.port.calls, ["AAPL", "MSFT"], "redelivery must not re-research")

    def test_same_finding_under_a_new_run_id_is_deduplicated_by_candidate_identity(self) -> None:
        self.handler.handle(self.event, _FakeContext())
        second = self.handler.handle(
            dict(self.event, researchRunId="car-2026-08-02T18"), _FakeContext()
        )
        self.assertEqual(second["status"], "RESEARCHED")
        self.assertEqual(second["result"]["candidatesRecorded"], 0)
        self.assertEqual(second["result"]["candidatesAlreadyKnown"], 1)
        self.assertEqual(len(self.store_path.read_text(encoding="utf-8").splitlines()), 1)

    def test_malformed_events_are_rejected(self) -> None:
        cases: dict[str, Any] = {
            "not a mapping": None,
            "missing researchRunId": {
                "slotScheduledAt": "2026-08-02T06:00:00Z",
                "tickers": ["AAPL"],
            },
            "missing slotScheduledAt": {"researchRunId": "r", "tickers": ["AAPL"]},
            "missing tickers": {
                "researchRunId": "r",
                "slotScheduledAt": "2026-08-02T06:00:00Z",
            },
            "tickers not a list": dict(self.event, tickers="AAPL"),
            "empty tickers": dict(self.event, tickers=[]),
            "non-string ticker": dict(self.event, tickers=[1]),
            "unnormalised ticker": dict(self.event, tickers=["aapl!"]),
            "unknown field": dict(self.event, region="US"),
            "non-UTC slot": dict(self.event, slotScheduledAt="2026-08-02T06:00:00-04:00"),
        }
        for label, event in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(MalformedEventError):
                    self.handler.handle(event, _FakeContext())
        self.assertFalse(self.store_path.exists())

    def test_unconfigured_research_port_fails_loudly(self) -> None:
        unwired = CorporateActionResearchHandler(candidate_store_path=self.store_path)
        with self.assertRaises(PortNotConfiguredError) as raised:
            unwired.handle(self.event, _FakeContext())
        self.assertIn("D14", str(raised.exception))
        self.assertFalse(self.store_path.exists())

    def test_missing_store_configuration_fails_loudly(self) -> None:
        from apps.common.errors import ConfigurationError

        unwired = CorporateActionResearchHandler(research_port=self.port)
        with self.assertRaises(ConfigurationError) as raised:
            unwired.handle(self.event, _FakeContext())
        self.assertIn("CORPORATE_ACTION_CANDIDATE_STORE", str(raised.exception))

    def test_port_returning_an_unclaimed_finding_is_reported_not_swallowed(self) -> None:
        """DP-e refuses a finding whose material fields are not all claimed."""

        finding = _split_finding("aapl")
        unclaimed = ResearchFinding(
            event_type=finding.event_type,
            proposed_date=finding.proposed_date,
            terms=finding.terms,
            evidence=finding.evidence,
            claims=finding.claims[:1],
        )
        handler = CorporateActionResearchHandler(
            research_port=_StubResearchPort({"AAPL": (unclaimed,)}),
            candidate_store_path=self.store_path,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        with self.assertRaises(PartialResearchSlotError) as raised:
            handler.handle(self.event, _FakeContext())
        self.assertIn("AAPL", str(raised.exception))
        self.assertIn("unclaimed material field", str(raised.exception))
        self.assertFalse(self.store_path.exists())

    def test_an_unsupported_event_type_is_refused_rather_than_stored(self) -> None:
        """Only STOCK_SPLIT and CASH_DIVIDEND can be adjusted for; the rest are refused.

        Storing an unactionable row would look like progress while leaving the price
        series wrong, so the refusal has to reach the invocation record.
        """

        split = _split_finding("aapl")
        mislabelled = ResearchFinding(
            event_type="DELISTING",
            proposed_date=split.proposed_date,
            terms=split.terms,
            evidence=split.evidence,
            claims=split.claims,
        )
        handler = CorporateActionResearchHandler(
            research_port=_StubResearchPort({"AAPL": (mislabelled,)}),
            candidate_store_path=self.store_path,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        with self.assertRaises(PartialResearchSlotError) as raised:
            handler.handle(self.event, _FakeContext())
        self.assertIn("DELISTING", str(raised.exception))
        self.assertFalse(self.store_path.exists())

    def test_one_failing_ticker_does_not_abandon_the_others_but_still_fails_the_slot(self) -> None:
        """The partial-failure contract: attempt everything, record what worked, fail loudly."""

        class _FlakyPort:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
                self.calls.append(ticker)
                if ticker == "MSFT":
                    raise RuntimeError("research provider timed out")
                return (_split_finding(ticker.lower()),)

        port = _FlakyPort()
        store = InMemoryIdempotencyStore()
        handler = CorporateActionResearchHandler(
            research_port=port,
            candidate_store_path=self.store_path,
            idempotency_store=store,
        )
        event = dict(self.event, tickers=["AAPL", "MSFT", "NVDA"])
        with self.assertRaises(PartialResearchSlotError) as raised:
            handler.handle(event, _FakeContext())

        error = raised.exception
        self.assertEqual(sorted(error.failures), ["MSFT"])
        self.assertIn("research provider timed out", error.failures["MSFT"])
        self.assertEqual(error.attempted, 3)
        self.assertEqual(error.recorded, 2)
        self.assertEqual(port.calls, ["AAPL", "MSFT", "NVDA"], "a failure must not stop the slot")
        # The two that worked are durable, and the claim is released so the retry runs.
        self.assertEqual(len(self.store_path.read_text(encoding="utf-8").splitlines()), 2)
        self.assertFalse(store.seen(self.event["researchRunId"]))

    def test_retrying_a_partial_slot_records_only_what_was_missing(self) -> None:
        class _HealsOnRetry:
            def __init__(self) -> None:
                self.attempts = 0

            def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
                if ticker == "MSFT":
                    self.attempts += 1
                    if self.attempts == 1:
                        raise RuntimeError("research provider timed out")
                return (_split_finding(ticker.lower()),)

        handler = CorporateActionResearchHandler(
            research_port=_HealsOnRetry(),
            candidate_store_path=self.store_path,
            idempotency_store=InMemoryIdempotencyStore(),
        )
        with self.assertRaises(PartialResearchSlotError):
            handler.handle(self.event, _FakeContext())
        result = handler.handle(self.event, _FakeContext())
        self.assertEqual(result["status"], "RESEARCHED")
        # AAPL was already stored on the first attempt; only MSFT is new.
        self.assertEqual(result["result"]["candidatesRecorded"], 1)
        self.assertEqual(result["result"]["candidatesAlreadyKnown"], 1)
        self.assertEqual(len(self.store_path.read_text(encoding="utf-8").splitlines()), 2)

    def test_a_schedule_executor_takes_over_the_whole_slot(self) -> None:
        """When a catalog is wired, DP-e's executor owns the orchestration."""

        from market_pipeline_lib.corporate_action_research import ResearchRunReport

        class _StubExecutor:
            def __init__(self) -> None:
                self.calls: list[tuple[tuple[str, ...], datetime]] = []

            def run_due_slot(self, tickers: Sequence[str], *, now: datetime) -> ResearchRunReport:
                self.calls.append((tuple(tickers), now))
                return ResearchRunReport(
                    slot_id="2026-08-02T06:00:00Z",
                    tickers_researched=2,
                    candidates_recorded=1,
                    candidates_already_known=0,
                    actions_persisted=1,
                    actions_already_present=0,
                    skipped_as_duplicate_slot=False,
                )

        executor = _StubExecutor()
        handler = CorporateActionResearchHandler(
            schedule_executor=executor,  # type: ignore[arg-type]
            idempotency_store=InMemoryIdempotencyStore(),
        )
        result = handler.handle(self.event, _FakeContext())
        self.assertEqual(result["status"], "RESEARCHED")
        self.assertEqual(result["result"]["actionsPersisted"], 1)
        self.assertEqual(result["result"]["slotId"], "2026-08-02T06:00:00Z")
        self.assertEqual(executor.calls[0][0], ("AAPL", "MSFT"))
        self.assertEqual(executor.calls[0][1], datetime(2026, 8, 2, 6, 0, tzinfo=UTC))
        # No candidate store is needed on this path.
        self.assertFalse(self.store_path.exists())
        json.dumps(result)

    def test_an_already_completed_slot_is_reported_as_a_duplicate(self) -> None:
        from market_pipeline_lib.corporate_action_research import ResearchRunReport

        class _AlreadyDone:
            def run_due_slot(self, tickers: Sequence[str], *, now: datetime) -> ResearchRunReport:
                return ResearchRunReport(
                    slot_id="2026-08-02T06:00:00Z",
                    tickers_researched=0,
                    candidates_recorded=0,
                    candidates_already_known=0,
                    actions_persisted=0,
                    actions_already_present=0,
                    skipped_as_duplicate_slot=True,
                )

        handler = CorporateActionResearchHandler(
            schedule_executor=_AlreadyDone(),  # type: ignore[arg-type]
            idempotency_store=InMemoryIdempotencyStore(),
        )
        result = handler.handle(self.event, _FakeContext())
        self.assertEqual(result["status"], "DUPLICATE")
        self.assertTrue(result["result"]["skippedAsDuplicateSlot"])

    def test_wiring_both_an_executor_and_a_port_is_refused(self) -> None:
        from apps.common.errors import ConfigurationError

        with self.assertRaises(ConfigurationError):
            CorporateActionResearchHandler(
                research_port=self.port,
                schedule_executor=object(),  # type: ignore[arg-type]
            )

    def test_module_level_handler_is_the_unwired_default(self) -> None:
        from apps.common.errors import PipelineAppError

        # Neither the D14 research adapter nor CORPORATE_ACTION_CANDIDATE_STORE
        # is configured in a plain test process, so the default wiring must
        # fail rather than report a successful research run with no findings.
        with self.assertRaises(PipelineAppError):
            corporate_action_handler(self.event, _FakeContext())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
