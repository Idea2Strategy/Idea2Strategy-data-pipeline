"""D11 -- `market_data.stream_watermarks` really tracked, really persisted.

Before DP5 the table had zero references anywhere in the codebase.  These tests
pin the four properties that make a watermark worth having:

* **advance-only** -- a stored watermark never moves backwards, not through the
  in-memory repository, not through SQL, not under a concurrent writer;
* **per-shard granularity** -- an out-of-order or replayed message is classified
  against *its own* shard head, not against a global one;
* **crash-safe resume** -- a ledger rebuilt from the persisted row alone replays
  the in-flight window and skips nothing that came after it;
* **honest floor semantics** -- the persisted `last_source_event_at` is the point
  every declared shard has passed, so a reader can treat it as "complete up to".

Every expectation is a pinned literal.  `first == second` would pass against an
implementation that returns a constant.
"""

from __future__ import annotations

import unittest
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import Engine, insert

from market_pipeline_lib.db.engine import create_market_data_engine
from market_pipeline_lib.db.tables import MARKET_DATA_SCHEMA, feeds, providers
from market_pipeline_lib.watermarks import (
    InMemoryWatermarkRepository,
    SqlWatermarkRepository,
    StreamPosition,
    StreamWatermark,
    UnknownShardError,
    WatermarkLedger,
    WatermarkOutcome,
)

FEED_ID = "3f1a6d0e-6b2c-4c0a-9b1a-1d2e3f4a5b6c"
SHARDS = ("s0-of-2", "s1-of-2")

#: (shard, source_event_at, sequence).  Two shards interleaved, strictly rising.
STREAM: tuple[tuple[str, str, int], ...] = (
    ("s0-of-2", "2026-07-31T14:30:00Z", 1),
    ("s1-of-2", "2026-07-31T14:30:00Z", 2),
    ("s0-of-2", "2026-07-31T14:31:00Z", 3),
    ("s1-of-2", "2026-07-31T14:31:00Z", 4),
    ("s0-of-2", "2026-07-31T14:32:00Z", 5),
    ("s1-of-2", "2026-07-31T14:32:00Z", 6),
    ("s0-of-2", "2026-07-31T14:33:00Z", 7),
    ("s1-of-2", "2026-07-31T14:33:00Z", 8),
)

#: The process is killed after this many stream entries have been handled.
CRASH_AFTER = 5

INGESTED_AT = datetime(2026, 7, 31, 14, 40, tzinfo=UTC)


def moment(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def position(text: str, sequence: int | None) -> StreamPosition:
    return StreamPosition(source_event_at=moment(text), sequence=sequence)


class StreamPositionTests(unittest.TestCase):
    def test_a_naive_timestamp_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            StreamPosition(source_event_at=datetime(2026, 7, 31, 14, 30), sequence=1)

    def test_timestamps_are_normalised_to_utc(self) -> None:
        eastern = datetime.fromisoformat("2026-07-31T10:30:00-04:00")
        self.assertEqual(
            StreamPosition(source_event_at=eastern, sequence=1).source_event_at,
            datetime(2026, 7, 31, 14, 30, tzinfo=UTC),
        )

    def test_ordering_prefers_event_time_then_sequence(self) -> None:
        self.assertLess(position("2026-07-31T14:30:00Z", 9), position("2026-07-31T14:31:00Z", 1))
        self.assertLess(position("2026-07-31T14:30:00Z", 1), position("2026-07-31T14:30:00Z", 2))
        self.assertEqual(position("2026-07-31T14:30:00Z", 2), position("2026-07-31T14:30:00Z", 2))

    def test_an_absent_sequence_sorts_before_any_sequence_at_the_same_instant(self) -> None:
        self.assertLess(position("2026-07-31T14:30:00Z", None), position("2026-07-31T14:30:00Z", 0))


class LedgerTests(unittest.TestCase):
    def test_an_undeclared_shard_is_refused_rather_than_silently_seeded(self) -> None:
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS)
        with self.assertRaises(UnknownShardError):
            ledger.observe("s7-of-2", position("2026-07-31T14:30:00Z", 1))

    def test_outcomes_are_decided_against_the_shards_own_head(self) -> None:
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS)
        first = ledger.observe("s0-of-2", position("2026-07-31T14:35:00Z", 10))
        self.assertEqual(first.outcome, WatermarkOutcome.ADVANCED)
        # The same position on the *other* shard is new work, not a duplicate.
        second = ledger.observe("s1-of-2", position("2026-07-31T14:35:00Z", 10))
        self.assertEqual(second.outcome, WatermarkOutcome.ADVANCED)
        self.assertEqual(ledger.observe("s0-of-2", position("2026-07-31T14:35:00Z", 10)).outcome,
                         WatermarkOutcome.DUPLICATE)
        self.assertEqual(ledger.observe("s0-of-2", position("2026-07-31T14:34:00Z", 9)).outcome,
                         WatermarkOutcome.STALE)
        # Neither the duplicate nor the stale message moved the head backwards.
        self.assertEqual(ledger.head("s0-of-2"), position("2026-07-31T14:35:00Z", 10))

    def test_only_an_advanced_decision_asks_for_processing(self) -> None:
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS)
        self.assertTrue(ledger.observe("s0-of-2", position("2026-07-31T14:30:00Z", 1)).should_process)
        self.assertFalse(ledger.observe("s0-of-2", position("2026-07-31T14:30:00Z", 1)).should_process)
        self.assertFalse(ledger.observe("s0-of-2", position("2026-07-31T14:29:00Z", 0)).should_process)

    def test_the_floor_waits_for_every_declared_shard(self) -> None:
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS)
        ledger.observe("s0-of-2", position("2026-07-31T14:30:00Z", 1))
        self.assertIsNone(ledger.completion_floor())
        ledger.observe("s1-of-2", position("2026-07-31T14:32:00Z", 4))
        self.assertEqual(ledger.completion_floor(), position("2026-07-31T14:30:00Z", 1))
        ledger.observe("s0-of-2", position("2026-07-31T14:33:00Z", 5))
        self.assertEqual(ledger.completion_floor(), position("2026-07-31T14:32:00Z", 4))

    def test_checkpoint_writes_nothing_while_the_floor_is_unknown(self) -> None:
        repository = InMemoryWatermarkRepository()
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS, repository=repository)
        ledger.observe("s0-of-2", position("2026-07-31T14:30:00Z", 1))
        self.assertIsNone(ledger.checkpoint(ingested_at=INGESTED_AT))
        self.assertIsNone(repository.load(FEED_ID))


class CrashResumeTests(unittest.TestCase):
    """Kill the ingester mid-stream, restart it, and replay the whole stream."""

    @staticmethod
    def _drive(ledger: WatermarkLedger, entries: tuple[tuple[str, str, int], ...]) -> list[str]:
        outcomes = []
        for shard, occurred_at, sequence in entries:
            decision = ledger.observe(shard, position(occurred_at, sequence))
            outcomes.append(decision.outcome.value)
            if decision.should_process:
                ledger.checkpoint(ingested_at=INGESTED_AT)
        return outcomes

    def test_resume_replays_only_the_in_flight_window_and_skips_nothing_after_it(self) -> None:
        repository = InMemoryWatermarkRepository()

        first = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS, repository=repository)
        self.assertEqual(
            self._drive(first, STREAM[:CRASH_AFTER]),
            ["ADVANCED", "ADVANCED", "ADVANCED", "ADVANCED", "ADVANCED"],
        )
        # s0 reached 14:32 but s1 only 14:31, so only 14:31 is durably complete.
        persisted = repository.load(FEED_ID)
        assert persisted is not None
        self.assertEqual(persisted.position, position("2026-07-31T14:31:00Z", 4))

        # -- the process dies here; `first` is discarded entirely ---------------
        restarted = WatermarkLedger.resume(repository, feed_id=FEED_ID, shard_keys=SHARDS)
        self.assertEqual(restarted.resumed_from, position("2026-07-31T14:31:00Z", 4))

        self.assertEqual(
            self._drive(restarted, STREAM),
            [
                "STALE",      # 14:30 seq 1  -- before the floor
                "STALE",      # 14:30 seq 2
                "STALE",      # 14:31 seq 3  -- same instant, lower sequence
                "DUPLICATE",  # 14:31 seq 4  -- exactly the floor
                "ADVANCED",   # 14:32 seq 5  -- in flight when the process died
                "ADVANCED",   # 14:32 seq 6  -- never processed before
                "ADVANCED",   # 14:33 seq 7
                "ADVANCED",   # 14:33 seq 8
            ],
        )
        self.assertEqual(repository.load(FEED_ID).position, position("2026-07-31T14:33:00Z", 7))

    def test_a_second_full_replay_after_a_clean_stop_processes_nothing(self) -> None:
        repository = InMemoryWatermarkRepository()
        self._drive(WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS, repository=repository), STREAM)

        restarted = WatermarkLedger.resume(repository, feed_id=FEED_ID, shard_keys=SHARDS)
        outcomes = self._drive(restarted, STREAM)
        self.assertEqual(outcomes.count("ADVANCED"), 1)
        # Only s1's last message is above the floor (s0 stopped one sequence earlier).
        self.assertEqual(outcomes, [
            "STALE", "STALE", "STALE", "STALE", "STALE", "STALE", "DUPLICATE", "ADVANCED",
        ])


class RepositoryContractTests(unittest.TestCase):
    """Applies to every `WatermarkRepository`; the SQL one re-runs it for real."""

    def repository(self) -> Any:
        return InMemoryWatermarkRepository()

    def test_absent_feed_reads_as_none(self) -> None:
        self.assertIsNone(self.repository().load(FEED_ID))

    def test_first_write_is_stored_verbatim(self) -> None:
        repository = self.repository()
        stored = repository.advance(
            StreamWatermark(
                feed_id=FEED_ID,
                position=position("2026-07-31T14:31:00Z", 4),
                ingested_at=INGESTED_AT,
            )
        )
        self.assertEqual(stored.position, position("2026-07-31T14:31:00Z", 4))
        self.assertEqual(repository.load(FEED_ID).position, position("2026-07-31T14:31:00Z", 4))

    def test_an_older_write_is_refused_and_the_row_is_unchanged(self) -> None:
        repository = self.repository()
        repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT)
        )
        stored = repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:31:00Z", 4), INGESTED_AT)
        )
        self.assertEqual(stored.position, position("2026-07-31T14:33:00Z", 7))
        self.assertEqual(repository.load(FEED_ID).position, position("2026-07-31T14:33:00Z", 7))

    def test_a_lower_sequence_at_the_same_instant_is_refused(self) -> None:
        repository = self.repository()
        repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT)
        )
        stored = repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 6), INGESTED_AT)
        )
        self.assertEqual(stored.position.sequence, 7)

    def test_a_higher_sequence_at_the_same_instant_advances(self) -> None:
        repository = self.repository()
        repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT)
        )
        stored = repository.advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 8), INGESTED_AT)
        )
        self.assertEqual(stored.position.sequence, 8)

    def test_replaying_the_identical_watermark_is_a_no_op_that_still_reads_back(self) -> None:
        repository = self.repository()
        watermark = StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT)
        repository.advance(watermark)
        self.assertEqual(repository.advance(watermark).position, watermark.position)


# --------------------------------------------------------------------------------------
# The same contract, against the real table
# --------------------------------------------------------------------------------------


@pytest.fixture
def watermark_engine(postgres_url: str, truncate_market_data: None) -> Iterator[Engine]:
    """A guarded engine with the one provider/feed row `stream_watermarks` needs."""

    engine = create_market_data_engine(postgres_url, writable_schemas=[MARKET_DATA_SCHEMA])
    provider_id = uuid.UUID("6d1f0b4c-0f2a-4a51-9d1e-7c8b9a0d1e2f")
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(providers).values(
                    id=provider_id,
                    code="DP5-TEST",
                    display_name="DP5 watermark test",
                    rights_version="UNVERIFIED",
                    status="REVIEW_REQUIRED",
                    created_at=INGESTED_AT,
                )
            )
            connection.execute(
                insert(feeds).values(
                    id=uuid.UUID(FEED_ID),
                    provider_id=provider_id,
                    code="DP5_REALTIME",
                    data_kind="BARS",
                    resolution="1m",
                    timezone_name="America/New_York",
                    feed_version="dp5-v1",
                    created_at=INGESTED_AT,
                )
            )
        yield engine
    finally:
        engine.dispose()


@pytest.mark.integration
class SqlRepositoryTests(RepositoryContractTests):
    """`RepositoryContractTests` re-run against PostgreSQL, plus SQL-only concerns."""

    engine: Engine

    @pytest.fixture(autouse=True)
    def _bind(self, watermark_engine: Engine) -> Iterator[None]:
        self.engine = watermark_engine
        yield

    def repository(self) -> Any:
        return SqlWatermarkRepository(self.engine)

    def test_the_row_lands_in_market_data_stream_watermarks(self) -> None:
        from sqlalchemy import select

        from market_pipeline_lib.db.tables import stream_watermarks

        SqlWatermarkRepository(self.engine).advance(
            StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT)
        )
        with self.engine.connect() as connection:
            rows = connection.execute(select(stream_watermarks)).mappings().all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["feed_id"]), FEED_ID)
        self.assertEqual(rows[0]["last_source_event_at"], moment("2026-07-31T14:33:00Z"))
        self.assertEqual(rows[0]["last_sequence"], 7)

    def test_a_regressing_writer_racing_an_advancing_one_cannot_win(self) -> None:
        repository = SqlWatermarkRepository(self.engine)
        repository.advance(StreamWatermark(FEED_ID, position("2026-07-31T14:33:00Z", 7), INGESTED_AT))
        for older in (
            position("2026-07-31T14:30:00Z", 1),
            position("2026-07-31T14:33:00Z", 6),
            position("2026-07-30T23:59:59Z", 9999),
        ):
            repository.advance(StreamWatermark(FEED_ID, older, INGESTED_AT))
        self.assertEqual(repository.load(FEED_ID).position, position("2026-07-31T14:33:00Z", 7))

    def test_a_crash_resume_survives_a_real_round_trip_through_the_table(self) -> None:
        repository = SqlWatermarkRepository(self.engine)
        ledger = WatermarkLedger(feed_id=FEED_ID, shard_keys=SHARDS, repository=repository)
        for shard, occurred_at, sequence in STREAM[:CRASH_AFTER]:
            if ledger.observe(shard, position(occurred_at, sequence)).should_process:
                ledger.checkpoint(ingested_at=INGESTED_AT)

        # A brand-new repository object, reading only what the table holds.
        restarted = WatermarkLedger.resume(
            SqlWatermarkRepository(self.engine), feed_id=FEED_ID, shard_keys=SHARDS
        )
        self.assertEqual(restarted.resumed_from, position("2026-07-31T14:31:00Z", 4))
        outcomes = [
            restarted.observe(shard, position(occurred_at, sequence)).outcome.value
            for shard, occurred_at, sequence in STREAM
        ]
        self.assertEqual(
            outcomes,
            ["STALE", "STALE", "STALE", "DUPLICATE", "ADVANCED", "ADVANCED", "ADVANCED", "ADVANCED"],
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
