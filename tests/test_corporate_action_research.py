import json
import tempfile
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path

from market_pipeline_lib.corporate_action_research import (
    CandidateStore,
    Evidence,
    ResearchCandidate,
    TwiceDailySchedule,
)


class ResearchCandidateTest(unittest.TestCase):
    def test_requires_public_evidence(self) -> None:
        with self.assertRaisesRegex(ValueError, "evidence"):
            ResearchCandidate.create(
                ticker="AAPL",
                event_type="STOCK_SPLIT",
                proposed_date=date(2026, 8, 15),
                evidence=(),
                researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
            )

    def test_rejects_non_public_evidence_uri(self) -> None:
        with self.assertRaisesRegex(ValueError, "public HTTP"):
            Evidence(
                source_uri="s3://private-bucket/source",
                source_title="private object",
                content_sha256="a" * 64,
                retrieved_at=datetime(2026, 8, 2, 0, 1, tzinfo=timezone.utc),
            )

    def test_identity_is_stable_for_the_same_research_result(self) -> None:
        evidence = self._evidence()
        first = ResearchCandidate.create(
            ticker=" aapl ",
            event_type="stock_split",
            proposed_date=date(2026, 8, 15),
            evidence=(evidence,),
            researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
        )
        second = ResearchCandidate.create(
            ticker="AAPL",
            event_type="STOCK_SPLIT",
            proposed_date=date(2026, 8, 15),
            evidence=(evidence,),
            researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(first.candidate_id, second.candidate_id)

    def test_candidate_export_is_review_only(self) -> None:
        candidate = ResearchCandidate.create(
            ticker="AAPL",
            event_type="STOCK_SPLIT",
            proposed_date=date(2026, 8, 15),
            evidence=(self._evidence(),),
            researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
        )

        exported = candidate.to_record()

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
                evidence=(),
                researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
            )

        with self.assertRaisesRegex(ValueError, "identity"):
            ResearchCandidate(
                candidate_id="0" * 64,
                ticker="AAPL",
                event_type="STOCK_SPLIT",
                proposed_date=date(2026, 8, 15),
                evidence=(self._evidence(),),
                researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
            )

    @staticmethod
    def _evidence() -> Evidence:
        return Evidence(
            source_uri="https://issuer.example/investors/split-notice",
            source_title="Issuer split notice",
            content_sha256="a" * 64,
            retrieved_at=datetime(2026, 8, 2, 0, 1, tzinfo=timezone.utc),
        )


class TwiceDailyScheduleTest(unittest.TestCase):
    def test_requires_exactly_two_distinct_utc_slots(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly two"):
            TwiceDailySchedule((time(0, 0),))
        with self.assertRaisesRegex(ValueError, "distinct"):
            TwiceDailySchedule((time(0, 0), time(0, 0)))
        with self.assertRaisesRegex(ValueError, "UTC"):
            TwiceDailySchedule((time(0, 0), time(12, 0, tzinfo=timezone.utc)))

    def test_resolves_one_stable_slot_identity_per_due_window(self) -> None:
        schedule = TwiceDailySchedule(
            (time(0, 0, tzinfo=timezone.utc), time(12, 0, tzinfo=timezone.utc))
        )

        first = schedule.latest_due_slot(
            datetime(2026, 8, 2, 11, 59, tzinfo=timezone.utc)
        )
        second = schedule.latest_due_slot(
            datetime(2026, 8, 2, 23, 59, tzinfo=timezone.utc)
        )

        self.assertEqual(first.slot_id, "2026-08-02T00:00:00Z")
        self.assertEqual(second.slot_id, "2026-08-02T12:00:00Z")


class CandidateStoreTest(unittest.TestCase):
    def test_appends_once_and_preserves_review_only_record(self) -> None:
        candidate = ResearchCandidate.create(
            ticker="AAPL",
            event_type="STOCK_SPLIT",
            proposed_date=date(2026, 8, 15),
            evidence=(ResearchCandidateTest._evidence(),),
            researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=timezone.utc),
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "corporate-action-candidates.jsonl"
            store = CandidateStore(path)

            self.assertTrue(store.append(candidate))
            self.assertFalse(store.append(candidate))

            records = [json.loads(line) for line in path.read_text().splitlines()]

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["candidate_id"], candidate.candidate_id)
        self.assertEqual(records[0]["workflow_state"], "REVIEW_REQUIRED")


if __name__ == "__main__":
    unittest.main()
