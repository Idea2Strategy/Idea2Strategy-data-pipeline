"""D14 -- corporate-action research: adapter, schedule executor, persistence.

No test here reaches the network.  The research adapter is driven through an
injected model callable, and persistence goes to a real `LocalCatalog` rooted
in a temporary directory, so the canonical column contract is genuinely
exercised without a database.
"""

from __future__ import annotations

import json
import logging
import tempfile
import unittest
from collections.abc import Sequence
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.contracts import deterministic_uuid
from market_pipeline_lib.corporate_action_research import (
    AiResearchAdapter,
    CandidateStore,
    CashDividendTerms,
    CatalogInstrumentResolver,
    CatalogSourceManifestResolver,
    Claim,
    ConflictingResearchError,
    Evidence,
    ResearchAdapterError,
    ResearchCandidate,
    ResearchFinding,
    ResearchScheduleExecutor,
    SplitTerms,
    TwiceDailySchedule,
    UnconfiguredResearchPort,
    UnknownInstrumentError,
    build_research_prompt,
    corporate_action_record,
)

UTC = timezone.utc

SOURCE = "https://issuer.example/investors/split-notice"
SECOND_SOURCE = "https://regulator.example/filings/8-K"

INSTRUMENT_ID = "11111111-1111-4111-8111-111111111111"
FEED_ID = "22222222-2222-4222-8222-222222222222"
MANIFEST_ID = "44444444-4444-4444-8444-444444444444"


def _evidence(uri: str = SOURCE, digest: str = "a" * 64) -> Evidence:
    return Evidence(
        source_uri=uri,
        source_title="Issuer split notice",
        content_sha256=digest,
        retrieved_at=datetime(2026, 8, 2, 0, 1, tzinfo=UTC),
    )


def _split_claims(source: str = SOURCE) -> tuple[Claim, ...]:
    return (
        Claim("event_type", "STOCK_SPLIT", source, Decimal("0.95")),
        Claim("effective_date", "2026-08-15", source, Decimal("0.90")),
        Claim("from_shares", "1", source, Decimal("0.99")),
        Claim("to_shares", "2", source, Decimal("0.99")),
    )


def _split_candidate(**overrides: Any) -> ResearchCandidate:
    arguments: dict[str, Any] = {
        "ticker": "AAPL",
        "event_type": "STOCK_SPLIT",
        "proposed_date": date(2026, 8, 15),
        "terms": SplitTerms(from_shares=1, to_shares=2),
        "evidence": (_evidence(),),
        "claims": _split_claims(),
        "researched_at": datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
    }
    arguments.update(overrides)
    return ResearchCandidate.create(**arguments)


# --------------------------------------------------------------------------------------
# Candidate identity and validation
# --------------------------------------------------------------------------------------
class ResearchCandidateTest(unittest.TestCase):
    def test_requires_public_evidence(self) -> None:
        with self.assertRaisesRegex(ValueError, "evidence"):
            _split_candidate(evidence=())

    def test_rejects_non_public_evidence_uri(self) -> None:
        with self.assertRaisesRegex(ValueError, "public HTTP"):
            Evidence(
                source_uri="s3://private-bucket/source",
                source_title="private object",
                content_sha256="a" * 64,
                retrieved_at=datetime(2026, 8, 2, 0, 1, tzinfo=UTC),
            )

    def test_identity_is_stable_for_the_same_research_result(self) -> None:
        first = _split_candidate(ticker=" aapl ", event_type="stock_split")
        second = _split_candidate()

        self.assertEqual(first.candidate_id, second.candidate_id)

    def test_identity_changes_when_the_terms_change(self) -> None:
        two_for_one = _split_candidate()
        three_for_one = _split_candidate(
            terms=SplitTerms(from_shares=1, to_shares=3),
            claims=(
                Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("0.95")),
                Claim("effective_date", "2026-08-15", SOURCE, Decimal("0.90")),
                Claim("from_shares", "1", SOURCE, Decimal("0.99")),
                Claim("to_shares", "3", SOURCE, Decimal("0.99")),
            ),
        )

        self.assertNotEqual(two_for_one.candidate_id, three_for_one.candidate_id)

    def test_candidate_export_is_review_only(self) -> None:
        exported = _split_candidate().to_record()

        self.assertEqual(exported["workflow_state"], "REVIEW_REQUIRED")
        forbidden = {"approved", "official", "strategy_decision", "dataset_manifest_id"}
        self.assertTrue(forbidden.isdisjoint(exported))

    def test_direct_construction_cannot_bypass_evidence_and_identity_checks(self) -> None:
        with self.assertRaisesRegex(ValueError, "evidence"):
            ResearchCandidate(
                candidate_id="0" * 64,
                ticker="AAPL",
                event_type="STOCK_SPLIT",
                proposed_date=date(2026, 8, 15),
                terms=SplitTerms(from_shares=1, to_shares=2),
                evidence=(),
                claims=_split_claims(),
                researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
            )

        with self.assertRaisesRegex(ValueError, "identity"):
            ResearchCandidate(
                candidate_id="0" * 64,
                ticker="AAPL",
                event_type="STOCK_SPLIT",
                proposed_date=date(2026, 8, 15),
                terms=SplitTerms(from_shares=1, to_shares=2),
                evidence=(_evidence(),),
                claims=_split_claims(),
                researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
            )

    def test_event_type_must_agree_with_the_terms(self) -> None:
        with self.assertRaisesRegex(ValueError, "event_type"):
            _split_candidate(
                terms=CashDividendTerms(amount=Decimal("1.00"), currency="USD")
            )

    def test_confidence_is_the_weakest_claim(self) -> None:
        self.assertEqual(_split_candidate().confidence, Decimal("0.9000"))

    def test_every_claim_must_cite_evidence_the_candidate_carries(self) -> None:
        with self.assertRaisesRegex(ValueError, "cites"):
            _split_candidate(claims=_split_claims(source=SECOND_SOURCE))

    def test_every_material_field_must_be_claimed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unclaimed"):
            _split_candidate(claims=_split_claims()[:3])

    def test_a_claim_must_match_the_value_it_claims(self) -> None:
        wrong = (
            Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("0.95")),
            Claim("effective_date", "2026-08-15", SOURCE, Decimal("0.90")),
            Claim("from_shares", "1", SOURCE, Decimal("0.99")),
            Claim("to_shares", "7", SOURCE, Decimal("0.99")),
        )
        with self.assertRaisesRegex(ValueError, "disagrees"):
            _split_candidate(claims=wrong)

    def test_claim_confidence_must_be_a_probability(self) -> None:
        with self.assertRaisesRegex(ValueError, "confidence"):
            Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("1.5"))
        with self.assertRaisesRegex(ValueError, "confidence"):
            Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("0"))

    def test_a_dividend_candidate_claims_its_amount_and_currency(self) -> None:
        candidate = ResearchCandidate.create(
            ticker="KO",
            event_type="CASH_DIVIDEND",
            proposed_date=date(2026, 9, 1),
            terms=CashDividendTerms(amount=Decimal("2.00"), currency="USD"),
            evidence=(_evidence(),),
            claims=(
                Claim("event_type", "CASH_DIVIDEND", SOURCE, Decimal("0.97")),
                Claim("effective_date", "2026-09-01", SOURCE, Decimal("0.93")),
                Claim("amount", "2.00", SOURCE, Decimal("0.99")),
                Claim("currency", "USD", SOURCE, Decimal("0.99")),
            ),
            researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
        )

        self.assertEqual(candidate.confidence, Decimal("0.9300"))
        self.assertEqual(candidate.to_record()["terms"]["amount"], "2.00")


# --------------------------------------------------------------------------------------
# Schedule
# --------------------------------------------------------------------------------------
class TwiceDailyScheduleTest(unittest.TestCase):
    def test_requires_exactly_two_distinct_utc_slots(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly two"):
            TwiceDailySchedule((time(0, 0),))
        with self.assertRaisesRegex(ValueError, "distinct"):
            TwiceDailySchedule((time(0, 0), time(0, 0)))
        with self.assertRaisesRegex(ValueError, "UTC"):
            TwiceDailySchedule((time(0, 0), time(12, 0, tzinfo=UTC)))

    def test_resolves_one_stable_slot_identity_per_due_window(self) -> None:
        schedule = TwiceDailySchedule((time(0, 0, tzinfo=UTC), time(12, 0, tzinfo=UTC)))

        first = schedule.latest_due_slot(datetime(2026, 8, 2, 11, 59, tzinfo=UTC))
        second = schedule.latest_due_slot(datetime(2026, 8, 2, 23, 59, tzinfo=UTC))

        self.assertEqual(first.slot_id, "2026-08-02T00:00:00Z")
        self.assertEqual(second.slot_id, "2026-08-02T12:00:00Z")


class CandidateStoreTest(unittest.TestCase):
    def test_appends_once_and_preserves_review_only_record(self) -> None:
        candidate = _split_candidate()

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "corporate-action-candidates.jsonl"
            store = CandidateStore(path)

            self.assertTrue(store.append(candidate))
            self.assertFalse(store.append(candidate))

            records = [json.loads(line) for line in path.read_text().splitlines()]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["candidate_id"], candidate.candidate_id)
        self.assertEqual(records[0]["workflow_state"], "REVIEW_REQUIRED")


# --------------------------------------------------------------------------------------
# Research adapter
# --------------------------------------------------------------------------------------
def _model_payload(*, confidence: str = "0.95", source: str = SOURCE) -> str:
    return json.dumps(
        {
            "findings": [
                {
                    "event_type": "STOCK_SPLIT",
                    "effective_date": "2026-08-15",
                    "terms": {"from_shares": 1, "to_shares": 2},
                    "evidence": [
                        {
                            "source_uri": source,
                            "source_title": "Issuer split notice",
                            "content_sha256": "a" * 64,
                            "retrieved_at": "2026-08-02T00:01:00Z",
                        }
                    ],
                    "claims": [
                        {
                            "field": "event_type",
                            "value": "STOCK_SPLIT",
                            "source_uri": source,
                            "confidence": confidence,
                        },
                        {
                            "field": "effective_date",
                            "value": "2026-08-15",
                            "source_uri": source,
                            "confidence": confidence,
                        },
                        {
                            "field": "from_shares",
                            "value": "1",
                            "source_uri": source,
                            "confidence": confidence,
                        },
                        {
                            "field": "to_shares",
                            "value": "2",
                            "source_uri": source,
                            "confidence": confidence,
                        },
                    ],
                }
            ]
        }
    )


class RecordingModel:
    """Stands in for the research LLM. Deterministic, and provably offline."""

    def __init__(self, *responses: str) -> None:
        self._responses = list(responses)
        self.prompts: list[str] = []

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return self._responses.pop(0) if self._responses else '{"findings": []}'


class AiResearchAdapterTest(unittest.TestCase):
    def _adapter(self, *responses: str, minimum: str = "0.70") -> AiResearchAdapter:
        self.model = RecordingModel(*responses)
        return AiResearchAdapter(self.model, min_confidence=Decimal(minimum))

    def test_a_well_formed_response_becomes_a_finding(self) -> None:
        adapter = self._adapter(_model_payload())

        (finding,) = adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

        self.assertEqual(finding.event_type, "STOCK_SPLIT")
        self.assertEqual(finding.proposed_date, date(2026, 8, 15))
        self.assertEqual(finding.terms, SplitTerms(from_shares=1, to_shares=2))
        self.assertEqual(len(finding.evidence), 1)
        self.assertEqual(len(finding.claims), 4)

    def test_the_prompt_names_the_ticker_and_the_slot(self) -> None:
        adapter = self._adapter(_model_payload())

        adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

        (prompt,) = self.model.prompts
        self.assertIn("AAPL", prompt)
        self.assertIn("2026-08-02T00:00:00Z", prompt)
        self.assertEqual(prompt, build_research_prompt("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC)))

    def test_a_quiet_slot_yields_no_findings(self) -> None:
        adapter = self._adapter('{"findings": []}')

        self.assertEqual(
            adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC)), ()
        )

    def test_non_json_output_is_an_adapter_error(self) -> None:
        adapter = self._adapter("I could not find anything, sorry!")

        with self.assertRaises(ResearchAdapterError):
            adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

    def test_an_unknown_event_type_is_an_adapter_error(self) -> None:
        payload = json.loads(_model_payload())
        payload["findings"][0]["event_type"] = "SPINOFF"
        adapter = self._adapter(json.dumps(payload))

        with self.assertRaisesRegex(ResearchAdapterError, "event_type"):
            adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

    def test_a_claim_citing_uncarried_evidence_is_an_adapter_error(self) -> None:
        payload = json.loads(_model_payload())
        payload["findings"][0]["claims"][0]["source_uri"] = SECOND_SOURCE
        adapter = self._adapter(json.dumps(payload))

        with self.assertRaises(ResearchAdapterError):
            adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

    def test_a_finding_with_no_evidence_is_an_adapter_error(self) -> None:
        payload = json.loads(_model_payload())
        payload["findings"][0]["evidence"] = []
        adapter = self._adapter(json.dumps(payload))

        with self.assertRaises(ResearchAdapterError):
            adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

    def test_a_low_confidence_finding_is_dropped_and_logged(self) -> None:
        adapter = self._adapter(_model_payload(confidence="0.40"), minimum="0.70")

        with self.assertLogs("market_pipeline_lib.corporate_action_research", "WARNING") as captured:
            findings = adapter.research("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))

        self.assertEqual(findings, ())
        self.assertTrue(any("0.4000" in line for line in captured.output))
        self.assertTrue(any("AAPL" in line for line in captured.output))

    def test_the_confidence_threshold_must_be_stated(self) -> None:
        with self.assertRaises(TypeError):
            AiResearchAdapter(RecordingModel())  # type: ignore[call-arg]

    def test_an_unconfigured_port_refuses_instead_of_reporting_a_quiet_slot(self) -> None:
        with self.assertRaises(Exception) as captured:
            UnconfiguredResearchPort().research("AAPL", datetime(2026, 8, 2, tzinfo=UTC))

        self.assertIn("adapter", str(captured.exception))


# --------------------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------------------
class CorporateActionRecordTest(unittest.TestCase):
    def test_the_record_carries_evidence_confidence_and_per_claim_sources(self) -> None:
        record = corporate_action_record(
            _split_candidate(),
            instrument_id=INSTRUMENT_ID,
            source_manifest_id=MANIFEST_ID,
        )

        self.assertEqual(record["action_type"], "STOCK_SPLIT")
        self.assertEqual(record["instrument_id"], INSTRUMENT_ID)
        self.assertEqual(record["source_manifest_id"], MANIFEST_ID)
        self.assertEqual(record["provider_event_key"], "RESEARCH:AAPL:STOCK_SPLIT:2026-08-15")
        # ET midnight on the effective date, expressed in UTC (EDT = UTC-4).
        self.assertEqual(record["effective_at"], "2026-08-15T04:00:00Z")

        document = record["terms_document"]
        self.assertEqual(document["confidence"], "0.9000")
        self.assertEqual(document["review"]["state"], "REVIEW_REQUIRED")
        self.assertEqual(document["evidence"][0]["content_sha256"], "a" * 64)
        self.assertEqual(
            {claim["field"] for claim in document["claims"]},
            {"event_type", "effective_date", "from_shares", "to_shares"},
        )

    def test_the_terms_hash_is_pinned_and_covers_the_document(self) -> None:
        record = corporate_action_record(
            _split_candidate(),
            instrument_id=INSTRUMENT_ID,
            source_manifest_id=MANIFEST_ID,
        )
        other = corporate_action_record(
            _split_candidate(
                terms=SplitTerms(from_shares=1, to_shares=3),
                claims=(
                    Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("0.95")),
                    Claim("effective_date", "2026-08-15", SOURCE, Decimal("0.90")),
                    Claim("from_shares", "1", SOURCE, Decimal("0.99")),
                    Claim("to_shares", "3", SOURCE, Decimal("0.99")),
                ),
            ),
            instrument_id=INSTRUMENT_ID,
            source_manifest_id=MANIFEST_ID,
        )

        self.assertEqual(len(record["terms_hash"]), 64)
        self.assertNotEqual(record["terms_hash"], other["terms_hash"])

    def test_the_record_is_accepted_by_the_canonical_column_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            catalog = LocalCatalog(Path(temporary))
            catalog.upsert(
                "market_data.corporate_actions",
                corporate_action_record(
                    _split_candidate(),
                    instrument_id=INSTRUMENT_ID,
                    source_manifest_id=MANIFEST_ID,
                ),
            )

            (row,) = catalog.records("market_data.corporate_actions")

        self.assertEqual(row["action_type"], "STOCK_SPLIT")


# --------------------------------------------------------------------------------------
# Schedule executor
# --------------------------------------------------------------------------------------
class StubPort:
    def __init__(self, findings_by_ticker: dict[str, Sequence[ResearchFinding]]) -> None:
        self._findings = findings_by_ticker
        self.calls: list[tuple[str, datetime]] = []

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        self.calls.append((ticker, scheduled_at))
        return tuple(self._findings.get(ticker, ()))


class ExplodingPort:
    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        raise RuntimeError("upstream research provider is down")


def _split_finding(to_shares: int = 2) -> ResearchFinding:
    return ResearchFinding(
        event_type="STOCK_SPLIT",
        proposed_date=date(2026, 8, 15),
        terms=SplitTerms(from_shares=1, to_shares=to_shares),
        evidence=(_evidence(),),
        claims=(
            Claim("event_type", "STOCK_SPLIT", SOURCE, Decimal("0.95")),
            Claim("effective_date", "2026-08-15", SOURCE, Decimal("0.90")),
            Claim("from_shares", "1", SOURCE, Decimal("0.99")),
            Claim("to_shares", str(to_shares), SOURCE, Decimal("0.99")),
        ),
    )


class ExecutorHarness:
    def __init__(self, root: Path, port: Any) -> None:
        self.catalog = LocalCatalog(root)
        self.catalog.upsert(
            "market_data.instruments",
            {
                "id": INSTRUMENT_ID,
                "asset_type": "EQUITY",
                "primary_exchange_mic": "XNAS",
                "currency_code": "USD",
                "created_at": "2026-01-01T00:00:00Z",
            },
        )
        self.catalog.append_unique(
            "market_data.instrument_symbols",
            {
                "id": deterministic_uuid("symbol", "AAPL"),
                "instrument_id": INSTRUMENT_ID,
                "exchange_mic": "XNAS",
                "symbol": "AAPL",
                "effective_from": "2020-01-01T00:00:00Z",
                "effective_to": None,
            },
            ("exchange_mic", "symbol", "effective_from"),
        )
        self.catalog.publish_manifest(
            {
                "id": MANIFEST_ID,
                "feed_id": FEED_ID,
                "instrument_id": INSTRUMENT_ID,
                "data_layer": "RAW",
                "resolution": "30m",
                "revision_number": 1,
                "status": "AVAILABLE",
                "period_start": "2026-01-01T00:00:00Z",
                "period_end": "2027-01-01T00:00:00Z",
                "schema_version": "market-bars-v2",
                "dataset_hash": "raw-hash-1",
                "supersedes_manifest_id": None,
                "created_at": "2026-01-01T00:00:00Z",
                "available_at": "2026-01-01T00:00:00Z",
            }
        )
        self.store_path = root / "candidates.jsonl"
        self.executor = ResearchScheduleExecutor(
            schedule=TwiceDailySchedule((time(0, 0, tzinfo=UTC), time(12, 0, tzinfo=UTC))),
            port=port,
            catalog=self.catalog,
            instrument_resolver=CatalogInstrumentResolver(self.catalog),
            manifest_resolver=CatalogSourceManifestResolver(
                self.catalog, feed_id=FEED_ID, resolution="30m"
            ),
            candidate_store=CandidateStore(self.store_path),
        )

    def action_rows(self) -> list[dict[str, Any]]:
        return self.catalog.records("market_data.corporate_actions")

    def runs(self) -> list[dict[str, Any]]:
        return self.catalog.records("market_data.pipeline_runs")


class ResearchScheduleExecutorTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name)
        self.port = StubPort({"AAPL": (_split_finding(),)})
        self.harness = ExecutorHarness(self.root, self.port)
        self.now = datetime(2026, 8, 2, 6, 0, tzinfo=UTC)

    def test_it_researches_the_due_slot_and_persists_the_finding(self) -> None:
        report = self.harness.executor.run_due_slot(("AAPL",), now=self.now)

        self.assertEqual(report.slot_id, "2026-08-02T00:00:00Z")
        self.assertEqual(report.tickers_researched, 1)
        self.assertEqual(report.actions_persisted, 1)
        self.assertFalse(report.skipped_as_duplicate_slot)
        self.assertEqual(self.port.calls, [("AAPL", datetime(2026, 8, 2, 0, 0, tzinfo=UTC))])

        (row,) = self.harness.action_rows()
        self.assertEqual(row["action_type"], "STOCK_SPLIT")
        self.assertEqual(row["instrument_id"], INSTRUMENT_ID)
        self.assertEqual(row["source_manifest_id"], MANIFEST_ID)
        self.assertEqual(row["terms_document"]["review"]["state"], "REVIEW_REQUIRED")
        self.assertEqual(row["terms_document"]["confidence"], "0.9000")

    def test_the_candidate_is_also_written_to_the_review_store(self) -> None:
        self.harness.executor.run_due_slot(("AAPL",), now=self.now)

        records = [
            json.loads(line)
            for line in self.harness.store_path.read_text().splitlines()
            if line.strip()
        ]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["workflow_state"], "REVIEW_REQUIRED")

    def test_re_running_the_same_slot_persists_nothing_further(self) -> None:
        first = self.harness.executor.run_due_slot(("AAPL",), now=self.now)
        second = self.harness.executor.run_due_slot(
            ("AAPL",), now=datetime(2026, 8, 2, 7, 30, tzinfo=UTC)
        )

        self.assertEqual(first.actions_persisted, 1)
        self.assertTrue(second.skipped_as_duplicate_slot)
        self.assertEqual(second.actions_persisted, 0)
        self.assertEqual(len(self.harness.action_rows()), 1)

    def test_the_next_slot_runs_again_and_re_discovery_is_not_duplicated(self) -> None:
        self.harness.executor.run_due_slot(("AAPL",), now=self.now)
        report = self.harness.executor.run_due_slot(
            ("AAPL",), now=datetime(2026, 8, 2, 12, 30, tzinfo=UTC)
        )

        self.assertFalse(report.skipped_as_duplicate_slot)
        self.assertEqual(report.slot_id, "2026-08-02T12:00:00Z")
        self.assertEqual(report.actions_persisted, 0)
        self.assertEqual(report.actions_already_present, 1)
        self.assertEqual(len(self.harness.action_rows()), 1)

    def test_a_slot_is_recorded_in_the_canonical_pipeline_run_ledger(self) -> None:
        self.harness.executor.run_due_slot(("AAPL",), now=self.now)

        (run,) = self.harness.runs()
        self.assertEqual(
            run["idempotency_key"], "corporate-action-research:2026-08-02T00:00:00Z"
        )
        self.assertEqual(run["status"], "SUCCEEDED")

    def test_a_failing_slot_is_marked_failed_and_the_error_propagates(self) -> None:
        harness = ExecutorHarness(self.root / "boom", ExplodingPort())

        with self.assertRaises(RuntimeError):
            harness.executor.run_due_slot(("AAPL",), now=self.now)

        (run,) = harness.runs()
        self.assertEqual(run["status"], "FAILED")
        self.assertEqual(harness.action_rows(), [])

    def test_a_failed_slot_can_be_retried(self) -> None:
        harness = ExecutorHarness(self.root / "retry", ExplodingPort())
        with self.assertRaises(RuntimeError):
            harness.executor.run_due_slot(("AAPL",), now=self.now)

        harness.executor._port = StubPort({"AAPL": (_split_finding(),)})  # noqa: SLF001
        report = harness.executor.run_due_slot(("AAPL",), now=self.now)

        self.assertFalse(report.skipped_as_duplicate_slot)
        self.assertEqual(report.actions_persisted, 1)

    def test_re_research_with_different_terms_is_a_conflict_not_a_silent_drop(self) -> None:
        self.harness.executor.run_due_slot(("AAPL",), now=self.now)
        self.harness.executor._port = StubPort(  # noqa: SLF001
            {"AAPL": (_split_finding(to_shares=3),)}
        )

        with self.assertRaises(ConflictingResearchError):
            self.harness.executor.run_due_slot(
                ("AAPL",), now=datetime(2026, 8, 2, 12, 30, tzinfo=UTC)
            )

    def test_an_unmapped_ticker_is_refused_rather_than_skipped(self) -> None:
        self.harness.executor._port = StubPort({"ZZZZ": (_split_finding(),)})  # noqa: SLF001

        with self.assertRaises(UnknownInstrumentError):
            self.harness.executor.run_due_slot(("ZZZZ",), now=self.now)

    def test_an_empty_ticker_list_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "ticker"):
            self.harness.executor.run_due_slot((), now=self.now)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main()
