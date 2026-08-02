"""D12/D90 -- the realtime path is a real consumer that publishes canonical objects.

Before DP5 `realtime_warmup.py` was a fixture converter with no consumer, it
hardcoded `PT1M` and `close`, and it wrote object keys of its own invention, so
nothing it produced could ever be read by `MarketPipelineEngine.compact`.

The load-bearing assertions here are:

* the key an ingested bar lands under is the canonical key of spec 2.5, pinned
  character for character;
* compaction, given nothing but what the realtime path published, produces a
  WEEK partition -- the property the old keys made impossible;
* granularity and the value-field mapping are inputs, proven by driving the same
  events through two different values and getting two different results;
* the SQS consumer really is a consumer: long poll, visibility timeout,
  at-least-once redelivery with a rising receive count, and a dead-letter queue
  after the configured number of receives -- checked against LocalStack.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from market_pipeline_lib.contracts import DATASET_CONTRACTS
from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.realtime_ingest import (
    DEDUP_SESSION_RETENTION,
    BarFieldMap,
    RealtimeDelivery,
    RealtimeIngestConsumer,
    RealtimeIngestError,
    RealtimeIngestor,
    RealtimeIngestSpec,
    SqsEventSource,
)
from market_pipeline_lib.watermarks import InMemoryWatermarkRepository, WatermarkLedger

ET = ZoneInfo("America/New_York")
LOCALSTACK_ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT_URL")

RAW_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]
#: C's own vocabulary, not D's `feed_code`.  `AlpacaMarketEventNormalizer.java:16-17`
#: emits `provider="ALPACA"`, `feed="SIP"`; D's RAW feed_code is `ALPACA_SIP_RAW_30M`.
SOURCE_PROVIDER = "ALPACA"
SOURCE_FEED = "SIP"
INSTRUMENTS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
}
SHARD_COUNT = 2
#: `stable_shard_key` puts AAPL on shard 0 and MSFT on shard 1 at shard_count=2.
SHARDS = ("s00-of-2", "s01-of-2")

SESSIONS = (
    date(2024, 1, 8),
    date(2024, 1, 9),
    date(2024, 1, 10),
    date(2024, 1, 11),
    date(2024, 1, 12),
)
BARS_PER_SESSION = 13  # 09:30 .. 15:30 ET inclusive, one every 30 minutes
TOTAL_EVENTS = len(SESSIONS) * BARS_PER_SESSION * len(INSTRUMENTS)

DATASET_ID = "2eab5266-777f-5cbb-8715-8e799b308cff"
CANONICAL_DAY_KEY = (
    "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_30M"
    f"/dataset={DATASET_ID}/revision=1/layer=RAW/resolution=30m"
    "/granularity=DAY/partition_start=2024-01-08/partition_end=2024-01-09"
    "/shard=s00-of-2/part-00001.parquet"
)
CANONICAL_WEEK_KEY = (
    "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_30M"
    f"/dataset={DATASET_ID}/revision=2/layer=RAW/resolution=30m"
    "/granularity=WEEK/partition_start=2024-01-08/partition_end=2024-01-15"
    "/shard=s00-of-2/part-00001.parquet"
)

FIELD_MAP = BarFieldMap(
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
    # Deliberately camelCase: a hardcoded column name cannot satisfy this.
    trade_count="tradeCount",
    vwap="vwap",
)


def temporary_root() -> str:
    return tempfile.mkdtemp()


def remove_root(path: str) -> None:
    # Canonical keys nest ten `key=value` directories; plain rmtree hits MAX_PATH.
    shutil.rmtree(long_path(path), ignore_errors=True)


def market_events(
    *,
    sessions: tuple[date, ...] = SESSIONS,
    event_type: str = "BAR_30M",
    first_sequence: int = 1,
) -> list[dict[str, Any]]:
    """One provider-neutral event per instrument per 30-minute bar, in time order."""

    events: list[dict[str, Any]] = []
    sequence = first_sequence
    for session in sessions:
        open_at = datetime.combine(session, datetime.min.time(), ET) + timedelta(hours=9, minutes=30)
        for index in range(BARS_PER_SESSION):
            bar_start = (open_at + timedelta(minutes=30 * index)).astimezone(UTC)
            for symbol, instrument_id in INSTRUMENTS.items():
                base = 100.0 + index / 10 + (0.5 if symbol == "MSFT" else 0.0)
                events.append(
                    {
                        "schemaVersion": 1,
                        "eventId": f"evt-{sequence:05d}",
                        "instrumentId": instrument_id,
                        "provider": SOURCE_PROVIDER,
                        "feed": SOURCE_FEED,
                        "eventType": event_type,
                        "providerEventId": f"{symbol}-{bar_start.isoformat()}",
                        "occurredAt": bar_start.isoformat().replace("+00:00", "Z"),
                        "receivedAt": (bar_start + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
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


def _session_of(event: Mapping[str, Any]) -> date:
    """The ET session an event's `occurredAt` falls in."""

    occurred = datetime.fromisoformat(str(event["occurredAt"]).replace("Z", "+00:00"))
    return occurred.astimezone(ET).date()


def build_engine(root: Path) -> MarketPipelineEngine:
    instrument_map = root / "instrument_map.csv"
    instrument_map.write_text(
        "provider_symbol,instrument_id\n"
        + "".join(f"{symbol},{identifier}\n" for symbol, identifier in INSTRUMENTS.items()),
        encoding="utf-8",
    )
    config = PipelineConfig(
        local_root=root / "objects",
        staging_root=root / "staging",
        instrument_map_path=instrument_map,
        shard_count=SHARD_COUNT,
        target_size_mib=1,
        max_size_mib=2,
    )
    return MarketPipelineEngine(config)


def build_ingestor(
    engine: MarketPipelineEngine,
    *,
    partition_granularity: str = "DAY",
    fields: BarFieldMap = FIELD_MAP,
    event_type: str = "BAR_30M",
    repository: InMemoryWatermarkRepository | None = None,
) -> RealtimeIngestor:
    spec = RealtimeIngestSpec(
        contract=RAW_CONTRACT,
        event_type=event_type,
        source_provider=SOURCE_PROVIDER,
        source_feed=SOURCE_FEED,
        source_resolution="PT30M",
        partition_granularity=partition_granularity,
        fields=fields,
    )
    ledger = WatermarkLedger(
        feed_id=engine.feed_ids[RAW_CONTRACT.feed_code],
        shard_keys=SHARDS,
        repository=repository or InMemoryWatermarkRepository(),
    )
    return RealtimeIngestor(engine, spec, ledger=ledger)


class SpecTests(unittest.TestCase):
    def test_granularity_event_type_and_fields_have_no_defaults(self) -> None:
        with self.assertRaises(TypeError):
            RealtimeIngestSpec(contract=RAW_CONTRACT)  # type: ignore[call-arg]

    def test_a_collection_granularity_is_refused(self) -> None:
        with self.assertRaises(RealtimeIngestError):
            RealtimeIngestSpec(
                contract=RAW_CONTRACT,
                event_type="BAR_30M",
                source_provider=SOURCE_PROVIDER,
                source_feed=SOURCE_FEED,
                source_resolution="PT30M",
                partition_granularity="MINUTE",
                fields=FIELD_MAP,
            )

    def test_a_resolution_that_disagrees_with_the_contract_is_refused(self) -> None:
        with self.assertRaisesRegex(RealtimeIngestError, "PT30M"):
            RealtimeIngestSpec(
                contract=RAW_CONTRACT,
                event_type="BAR_1M",
                source_provider=SOURCE_PROVIDER,
                source_feed=SOURCE_FEED,
                source_resolution="PT1M",
                partition_granularity="DAY",
                fields=FIELD_MAP,
            )

    def test_the_producers_provider_and_feed_have_no_defaults(self) -> None:
        """C's `provider`/`feed` are C's vocabulary; assuming them routes prices wrong."""

        for omitted in ("source_provider", "source_feed"):
            with self.subTest(field=omitted):
                kwargs: dict[str, Any] = {
                    "contract": RAW_CONTRACT,
                    "event_type": "BAR_30M",
                    "source_provider": SOURCE_PROVIDER,
                    "source_feed": SOURCE_FEED,
                    "source_resolution": "PT30M",
                    "partition_granularity": "DAY",
                    "fields": FIELD_MAP,
                }
                del kwargs[omitted]
                with self.assertRaises(TypeError):
                    RealtimeIngestSpec(**kwargs)
                with self.assertRaisesRegex(RealtimeIngestError, omitted):
                    RealtimeIngestSpec(**{**kwargs, omitted: "  "})

    def test_a_field_map_missing_a_price_is_refused(self) -> None:
        with self.assertRaises(RealtimeIngestError):
            BarFieldMap(open="open", high="high", low="low", close="", volume="volume")


class IngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.engine = build_engine(Path(self.root))

    def test_an_event_of_another_type_is_not_ingested_as_a_bar(self) -> None:
        """Two causes, two codes -- see `realtime_ingest.NON_BAR_EVENT_TYPES`.

        A `QUOTE` sharing C's stream is ordinary traffic that this stream does not
        turn into a row.  A `BAR_1M` on a 30-minute stream is a misrouted feed.
        Both are reported rather than dropped, under reasons an operator can tell
        apart.
        """

        ingestor = build_ingestor(self.engine)
        base = market_events(sessions=(SESSIONS[0],))[0]

        for event_type, expected in (
            ("QUOTE", "NON_BAR_EVENT_TYPE"),
            ("TRADE", "NON_BAR_EVENT_TYPE"),
            ("BAR_1M", "EVENT_TYPE_NOT_INGESTED"),
        ):
            with self.subTest(event_type=event_type):
                decision = ingestor.submit(dict(base, eventType=event_type))
                self.assertFalse(decision.accepted)
                self.assertEqual(decision.reason, expected)
        self.assertEqual(ingestor.pending_rows, 0)

    def test_a_malformed_event_is_refused_loudly(self) -> None:
        ingestor = build_ingestor(self.engine)
        event = market_events(sessions=(SESSIONS[0],))[0]
        del event["occurredAt"]
        with self.assertRaises(RealtimeIngestError):
            ingestor.submit(event)

    def test_an_unmapped_instrument_is_refused(self) -> None:
        ingestor = build_ingestor(self.engine)
        event = dict(
            market_events(sessions=(SESSIONS[0],))[0],
            instrumentId="99999999-9999-4999-8999-999999999999",
        )
        with self.assertRaisesRegex(RealtimeIngestError, "instrument"):
            ingestor.submit(event)

    def test_a_value_the_field_map_names_but_the_event_lacks_is_refused(self) -> None:
        ingestor = build_ingestor(self.engine)
        event = market_events(sessions=(SESSIONS[0],))[0]
        event["values"].pop("tradeCount")
        with self.assertRaisesRegex(RealtimeIngestError, "tradeCount"):
            ingestor.submit(event)

    def test_the_published_object_key_is_the_canonical_key(self) -> None:
        ingestor = build_ingestor(self.engine)
        for event in market_events(sessions=(SESSIONS[0],)):
            ingestor.submit(event)
        result = ingestor.flush()

        self.assertEqual(result.status, "AVAILABLE")
        self.assertEqual(result.row_count, BARS_PER_SESSION * len(INSTRUMENTS))
        self.assertIn(CANONICAL_DAY_KEY, result.object_keys)
        published = Path(long_path(str(Path(self.root) / "objects" / CANONICAL_DAY_KEY)))
        self.assertTrue(published.is_file())
        table = pq.read_table(published)
        self.assertEqual(table.num_rows, BARS_PER_SESSION)
        self.assertEqual(set(table.column("instrument_id").to_pylist()), {INSTRUMENTS["AAPL"]})
        # The field map, not a hardcoded column name, decided where these came from.
        self.assertEqual(table.column("trade_count").to_pylist()[:3], [10, 11, 12])
        self.assertEqual(table.column("close").to_pylist()[0], 100.5)

    def test_granularity_is_an_input_and_changes_the_partition(self) -> None:
        ingestor = build_ingestor(self.engine, partition_granularity="WEEK")
        for event in market_events():
            ingestor.submit(event)
        result = ingestor.flush()
        self.assertEqual(
            [key for key in result.object_keys if "shard=s00-of-2" in key],
            [
                "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_30M"
                f"/dataset={DATASET_ID}/revision=1/layer=RAW/resolution=30m"
                "/granularity=WEEK/partition_start=2024-01-08/partition_end=2024-01-15"
                "/shard=s00-of-2/part-00001.parquet"
            ],
        )

    def test_a_realtime_published_object_is_accepted_by_compaction(self) -> None:
        ingestor = build_ingestor(self.engine)
        for event in market_events():
            ingestor.submit(event)
        published = ingestor.flush()
        self.assertEqual(published.status, "AVAILABLE")
        # Five ET sessions x two shards, each a DAY partition.
        self.assertEqual(len(published.object_keys), 10)

        compacted = self.engine.compact(RAW_CONTRACT, granularity="WEEK", period=date(2024, 1, 8))

        self.assertEqual(compacted["status"], "SUCCEEDED")
        self.assertEqual(compacted["manifest"]["status"], "AVAILABLE")
        # Two WEEK objects replace the ten DAY objects the realtime path wrote.
        self.assertEqual(compacted["new_object_count"], 2)
        self.assertEqual(compacted["retained_object_count"], 0)
        week = Path(long_path(str(Path(self.root) / "objects" / CANONICAL_WEEK_KEY)))
        self.assertTrue(week.is_file(), f"compaction did not write {CANONICAL_WEEK_KEY}")
        self.assertEqual(pq.read_table(week).num_rows, len(SESSIONS) * BARS_PER_SESSION)

    def test_replayed_events_are_skipped_and_the_watermark_advances(self) -> None:
        repository = InMemoryWatermarkRepository()
        ingestor = build_ingestor(self.engine, repository=repository)
        everything = market_events()
        first = [ingestor.submit(event) for event in everything]
        self.assertEqual(sum(1 for decision in first if decision.accepted), TOTAL_EVENTS)
        published = ingestor.flush()
        self.assertEqual(published.status, "AVAILABLE")

        watermark = repository.load(self.engine.feed_ids[RAW_CONTRACT.feed_code])
        assert watermark is not None
        self.assertEqual(watermark.position.isoformat(), "2024-01-12T20:30:00Z")
        # The floor is the slower shard: AAPL's last event, one sequence behind MSFT's.
        self.assertEqual(watermark.position.sequence, TOTAL_EVENTS - 1)

        # Redelivery inside the stated de-duplication window is absorbed by C's
        # content-addressed `eventId`, whatever the watermark thinks of the position.
        # The *same* events are resubmitted, not regenerated ones.
        retained = set(SESSIONS[-DEDUP_SESSION_RETENTION:])
        redelivered = [event for event in everything if _session_of(event) in retained]
        self.assertEqual(len(redelivered), DEDUP_SESSION_RETENTION * BARS_PER_SESSION * len(INSTRUMENTS))
        replay = [ingestor.submit(event) for event in redelivered]
        self.assertEqual(sum(1 for decision in replay if decision.accepted), 0)
        self.assertEqual({decision.reason for decision in replay}, {"DUPLICATE_EVENT"})
        self.assertEqual(ingestor.pending_rows, 0)
        self.assertEqual(ingestor.flush().status, "NO_CHANGE")

    def test_a_replay_older_than_the_dedup_window_is_re_ingested_bar_for_bar(self) -> None:
        """The boundary of `DEDUP_SESSION_RETENTION`, stated rather than assumed.

        `_seen_event_ids` is bounded, so a redelivery of a session that has aged out
        of the window is *not* recognised by `eventId`.  What still holds -- and what
        actually protects the data -- is that the bar it rebuilds is keyed by
        `(instrument, bar_start)` and carries identical values, so the replay
        reproduces the same bars rather than adding any.
        """

        ingestor = build_ingestor(self.engine)
        for event in market_events():
            ingestor.submit(event)
        ingestor.flush()

        aged_out = SESSIONS[0]
        self.assertNotIn(aged_out, SESSIONS[-DEDUP_SESSION_RETENTION:])
        replay = [ingestor.submit(event) for event in market_events(sessions=(aged_out,))]
        self.assertTrue(all(decision.accepted for decision in replay))
        self.assertEqual({decision.reason for decision in replay}, {"ACCEPTED_LATE"})
        self.assertEqual(ingestor.pending_rows, BARS_PER_SESSION * len(INSTRUMENTS))

        republished = ingestor.flush()
        self.assertEqual(republished.status, "AVAILABLE")
        table = pq.read_table(
            Path(long_path(str(Path(self.root) / "objects" / CANONICAL_DAY_KEY)))
        )
        # One bar per 30-minute slot -- the replay rebuilt them, it did not add any.
        self.assertEqual(table.num_rows, BARS_PER_SESSION)
        self.assertEqual(len(set(table.column("bar_start_at").to_pylist())), BARS_PER_SESSION)
        self.assertEqual(table.column("close").to_pylist()[0], 100.5)

    def test_a_correction_arriving_out_of_order_never_rewinds_the_stream(self) -> None:
        """A revision-1 restatement of the first bar, delivered last.

        It replaces the bar it corrects and it is *not* refused, but the shard head
        stays where the newest event put it: a correction restates history, it does
        not un-consume the stream.
        """

        ingestor = build_ingestor(self.engine)
        events = market_events(sessions=(SESSIONS[0],))
        for event in events:
            ingestor.submit(event)
        head = ingestor.ledger.head("s00-of-2")
        assert head is not None

        correction = dict(
            events[0],
            eventId="evt-correction",
            sequence=1,
            revision=1,
            correctionOfEventId=events[0]["eventId"],
            values=dict(events[0]["values"], close=100.75),
        )
        decision = ingestor.submit(correction)

        self.assertTrue(decision.accepted)
        self.assertEqual(decision.reason, "CORRECTION_APPLIED")
        self.assertEqual(decision.outcome, "STALE")
        self.assertEqual(ingestor.ledger.head("s00-of-2"), head)
        # It replaced a bar rather than adding one.
        self.assertEqual(ingestor.pending_rows, BARS_PER_SESSION * len(INSTRUMENTS))

        self.assertEqual(ingestor.flush().status, "AVAILABLE")
        table = pq.read_table(
            Path(long_path(str(Path(self.root) / "objects" / CANONICAL_DAY_KEY)))
        )
        self.assertEqual(table.column("close").to_pylist()[0], 100.75)

    def test_a_message_is_ingested_whole_or_not_at_all(self) -> None:
        """`submit_batch` is the unit an SQS message is acknowledged in."""

        ingestor = build_ingestor(self.engine)
        events = market_events(sessions=(SESSIONS[0],))[:4]
        broken = [*events[:3], dict(events[3], schemaVersion=7)]

        with self.assertRaises(RealtimeIngestError):
            ingestor.submit_batch(broken)

        self.assertEqual(ingestor.pending_rows, 0)
        self.assertEqual(ingestor.flush().status, "NO_CHANGE")
        # And the three good events are still ingestable afterwards: the refusal
        # consumed nothing, not even their de-duplication identity.
        self.assertTrue(all(decision.accepted for decision in ingestor.submit_batch(events[:3])))
        self.assertEqual(ingestor.pending_rows, 3)


class ConsumerTests(unittest.TestCase):
    """The consumer loop, against a source that reproduces SQS delivery."""

    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.engine = build_engine(Path(self.root))

    def test_dead_letters_a_message_that_has_been_received_too_often(self) -> None:
        source = _RecordingSource(
            [
                RealtimeDelivery(
                    message_id="m1",
                    body={"events": market_events(sessions=(SESSIONS[0],))[:1]},
                    receipt_handle="rh1",
                    receive_count=3,
                )
            ]
        )
        consumer = RealtimeIngestConsumer(
            _AlwaysFailingIngestor(), source, max_receive_count=3, flush_every=1
        )
        cycle = consumer.run_once()
        self.assertEqual(cycle.dead_lettered, 1)
        self.assertEqual(cycle.retried, 0)
        self.assertEqual([reason for _, reason in source.dead_lettered], ["MAX_RECEIVES_EXCEEDED"])

    def test_retries_a_message_that_still_has_attempts_left(self) -> None:
        source = _RecordingSource(
            [
                RealtimeDelivery(
                    message_id="m1",
                    body={"events": market_events(sessions=(SESSIONS[0],))[:1]},
                    receipt_handle="rh1",
                    receive_count=1,
                )
            ]
        )
        consumer = RealtimeIngestConsumer(
            _AlwaysFailingIngestor(), source, max_receive_count=3, flush_every=1
        )
        cycle = consumer.run_once()
        self.assertEqual((cycle.dead_lettered, cycle.retried), (0, 1))
        self.assertEqual(source.dead_lettered, [])

    def test_a_malformed_body_is_dead_lettered_immediately(self) -> None:
        source = _RecordingSource(
            [RealtimeDelivery(message_id="m1", body={"nope": 1}, receipt_handle="rh1", receive_count=1)]
        )
        consumer = RealtimeIngestConsumer(
            build_ingestor(self.engine), source, max_receive_count=3, flush_every=1
        )
        cycle = consumer.run_once()
        self.assertEqual(cycle.dead_lettered, 1)
        self.assertEqual([reason for _, reason in source.dead_lettered], ["MALFORMED_EVENT"])

    def test_an_unimplemented_schema_version_parks_under_its_own_reason(self) -> None:
        """A newer C is a deployment fact, not a bad payload, and reads differently."""

        events = market_events(sessions=(SESSIONS[0],))[:2]
        source = _RecordingSource(
            [
                RealtimeDelivery(
                    message_id="m1",
                    body={"events": [events[0], dict(events[1], schemaVersion=2)]},
                    receipt_handle="rh1",
                    receive_count=1,
                )
            ]
        )
        ingestor = build_ingestor(self.engine)
        consumer = RealtimeIngestConsumer(ingestor, source, max_receive_count=3, flush_every=1)

        cycle = consumer.run_once()

        self.assertEqual((cycle.dead_lettered, cycle.retried, cycle.acknowledged), (1, 0, 0))
        self.assertEqual(
            [reason for _, reason in source.dead_lettered], ["UNSUPPORTED_EVENT_VERSION"]
        )
        self.assertEqual(ingestor.pending_rows, 0)


class _RecordingSource:
    def __init__(self, deliveries: list[RealtimeDelivery]) -> None:
        self._deliveries = list(deliveries)
        self.acknowledged: list[str] = []
        self.retried: list[tuple[str, float]] = []
        self.dead_lettered: list[tuple[str, str]] = []
        self.closed = False

    def poll(self, max_messages: int, wait_seconds: float) -> list[RealtimeDelivery]:
        batch, self._deliveries = self._deliveries[:max_messages], self._deliveries[max_messages:]
        return batch

    def acknowledge(self, delivery: RealtimeDelivery) -> None:
        self.acknowledged.append(delivery.message_id)

    def retry_later(self, delivery: RealtimeDelivery, *, delay_seconds: float) -> None:
        self.retried.append((delivery.message_id, delay_seconds))

    def dead_letter(self, delivery: RealtimeDelivery, *, reason: str) -> None:
        self.dead_lettered.append((delivery.message_id, reason))

    def close(self) -> None:
        self.closed = True


class _AlwaysFailingIngestor:
    """Stands in for an ingestor whose backing store is down."""

    pending_rows = 0

    def submit(self, event: Any) -> Any:
        raise OSError("object store unreachable")

    def submit_batch(self, events: Any) -> Any:
        raise OSError("object store unreachable")

    def flush(self) -> Any:  # pragma: no cover - never reached in these tests
        raise OSError("object store unreachable")


# --------------------------------------------------------------------------------------
# LocalStack
# --------------------------------------------------------------------------------------


@pytest.fixture
def sqs_queues() -> Iterator[tuple[Any, str, str]]:
    if not LOCALSTACK_ENDPOINT_URL:
        pytest.skip("set LOCALSTACK_ENDPOINT_URL to run the LocalStack SQS integration tests")
    import boto3

    client = boto3.client(
        "sqs",
        endpoint_url=LOCALSTACK_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    suffix = uuid.uuid4().hex[:10]
    dead_letter_url = client.create_queue(QueueName=f"dp5-dlq-{suffix}")["QueueUrl"]
    queue_url = client.create_queue(
        QueueName=f"dp5-main-{suffix}", Attributes={"VisibilityTimeout": "0"}
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
class TestSqsEventSourceAgainstLocalStack:
    def test_long_poll_returns_empty_on_an_idle_queue(self, sqs_queues: tuple[Any, str, str]) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
        assert source.poll(max_messages=10, wait_seconds=1.0) == []

    def test_a_message_round_trips_and_is_deleted_by_acknowledge(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": []}))
        source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)

        received = source.poll(max_messages=10, wait_seconds=5.0)
        assert len(received) == 1
        assert received[0].body == {"events": []}
        assert received[0].receive_count == 1

        source.acknowledge(received[0])
        assert source.poll(max_messages=10, wait_seconds=1.0) == []

    def test_an_unacknowledged_message_comes_back_with_a_higher_receive_count(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": []}))
        source = SqsEventSource(
            client,
            queue_url=queue_url,
            dead_letter_queue_url=dead_letter_url,
            visibility_timeout_seconds=30,
        )
        first = source.poll(max_messages=1, wait_seconds=5.0)[0]
        assert first.receive_count == 1
        # While it is invisible nobody else can take it.
        assert source.poll(max_messages=1, wait_seconds=1.0) == []
        source.retry_later(first, delay_seconds=0.0)
        second = source.poll(max_messages=1, wait_seconds=5.0)[0]
        assert second.receive_count == 2
        assert second.body == first.body

    def test_dead_letter_moves_the_body_and_removes_it_from_the_main_queue(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": ["poison"]}))
        source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
        delivery = source.poll(max_messages=1, wait_seconds=5.0)[0]

        source.dead_letter(delivery, reason="MALFORMED_EVENT")

        assert source.poll(max_messages=1, wait_seconds=1.0) == []
        parked = client.receive_message(
            QueueUrl=dead_letter_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"],
        )["Messages"]
        assert len(parked) == 1
        assert json.loads(parked[0]["Body"]) == {"events": ["poison"]}
        assert parked[0]["MessageAttributes"]["DeadLetterReason"]["StringValue"] == "MALFORMED_EVENT"

    def test_a_source_without_a_dead_letter_queue_refuses_to_park(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, _ = sqs_queues
        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": []}))
        source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=None)
        delivery = source.poll(max_messages=1, wait_seconds=5.0)[0]
        with pytest.raises(RealtimeIngestError):
            source.dead_letter(delivery, reason="MALFORMED_EVENT")


@pytest.mark.integration
class TestRealtimeIngestEndToEndOverLocalStack:
    def test_events_published_to_sqs_become_a_canonical_compaction_ready_object(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            repository = InMemoryWatermarkRepository()
            ingestor = build_ingestor(engine, repository=repository)
            source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
            consumer = RealtimeIngestConsumer(ingestor, source, max_receive_count=3, flush_every=10_000)

            events = market_events()
            for start in range(0, len(events), 10):
                client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({"events": events[start : start + 10]}),
                )

            report = consumer.drain(max_empty_cycles=2, wait_seconds=2.0)
            assert report.accepted == TOTAL_EVENTS
            assert report.dead_lettered == 0
            assert report.acknowledged == 13

            published = ingestor.flush()
            assert published.status == "AVAILABLE"
            assert CANONICAL_DAY_KEY in published.object_keys

            compacted = engine.compact(RAW_CONTRACT, granularity="WEEK", period=date(2024, 1, 8))
            assert compacted["status"] == "SUCCEEDED"
            week = Path(long_path(str(Path(root) / "objects" / CANONICAL_WEEK_KEY)))
            assert week.is_file()

            watermark = repository.load(engine.feed_ids[RAW_CONTRACT.feed_code])
            assert watermark is not None
            assert watermark.position.isoformat() == "2024-01-12T20:30:00Z"
        finally:
            remove_root(root)

    def test_a_failing_handler_parks_the_message_after_the_configured_receives(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"events": market_events(sessions=(SESSIONS[0],))[:1]}),
        )
        source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
        consumer = RealtimeIngestConsumer(
            _AlwaysFailingIngestor(), source, max_receive_count=3, flush_every=1, retry_delay_seconds=0.0
        )

        outcomes = [consumer.run_once() for _ in range(3)]

        assert [cycle.retried for cycle in outcomes] == [1, 1, 0]
        assert [cycle.dead_lettered for cycle in outcomes] == [0, 0, 1]
        parked = client.receive_message(
            QueueUrl=dead_letter_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )["Messages"]
        assert len(parked) == 1
        assert source.poll(max_messages=1, wait_seconds=1.0) == []


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
