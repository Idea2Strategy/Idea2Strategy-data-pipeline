"""D90 -- C 실시간 적재·warm-up 실제 연동.

Two halves, both of which the previous D90 claimed and neither of which it had.

**Half one: D consumes C's real event.**  C's market-gateway emits
``com.idea2strategy.trading.messaging.market.MarketEventEnvelope`` -- a *flat*,
camelCase, thirteen-field record with no ``metadata`` envelope.  D's realtime
ingest had invented a seven-field subset of it and validated nothing else, so a
``schemaVersion: 2`` event from a newer C deployment was ingested as if it were a
version D understood.  The producer's format wins; the fixtures pinned here are
C's own published contract fixture, copied field for field from
``modules/trading-messaging/src/testFixtures/resources/contracts/v1/provider-neutral-market-events.json``.

**Half two: 누락 시 실행을 차단.**  A block that only exists inside D's process is
not a block.  What C actually reads is the warm-up ``manifest.json``
(``ManifestBoundWarmupDataSource``), and what the canonical schema says a
pre-evaluation gate reads is ``market_data.stream_watermarks`` together with
``market_data.quality_incidents`` (the DBML ``Note`` on ``stream_watermarks``).
So every block proved here is asserted in *both* projections, with a specific
reason code -- never a generic "not ready".
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import unittest
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from market_pipeline_lib.contracts import DATASET_CONTRACTS
from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.realtime_ingest import (
    MARKET_EVENT_SCHEMA_VERSION,
    BarFieldMap,
    RealtimeIngestConsumer,
    RealtimeIngestError,
    RealtimeIngestor,
    RealtimeIngestSpec,
    SqsEventSource,
    UnsupportedEventVersion,
    parse_market_event,
)
from market_pipeline_lib.realtime_warmup import (
    WarmupBlockReason,
    WarmupReadiness,
)
from market_pipeline_lib.warmup_gate import (
    WarmupCoverage,
    WarmupReadinessGate,
)
from market_pipeline_lib.watermarks import (
    InMemoryWatermarkRepository,
    SqlWatermarkRepository,
    StreamPosition,
    StreamWatermark,
    WatermarkLedger,
)

ET = ZoneInfo("America/New_York")
LOCALSTACK_ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT_URL")

# ----------------------------------------------------------------------------------
# C's published contract fixture, verbatim.
#
# Source of truth (READ-ONLY reference repository):
#   trading-engine/modules/trading-messaging/src/testFixtures/resources/
#     contracts/v1/provider-neutral-market-events.json
# Producer:  AlpacaMarketEventNormalizer.java:28-52   (constructs the envelope)
# Record:    MarketEventEnvelope.java:10-63           (field order and invariants)
# Wire keys: RedisMarketEventPublisher.java:50-64     (the literal key names)
#
# There is no JSON Schema, Avro or protobuf definition anywhere in trading-engine,
# so this literal *is* the contract as far as a consumer can pin it.
# ----------------------------------------------------------------------------------

C_INSTRUMENT_ID = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"

C_QUOTE_EVENT: dict[str, Any] = {
    "eventId": "evt_cffe872742cbbc8c471e84cca929ab3922d636f881f676792cd8a3886a49a008",
    "schemaVersion": 1,
    "instrumentId": C_INSTRUMENT_ID,
    "provider": "ALPACA",
    "feed": "SIP",
    "eventType": "QUOTE",
    "providerEventId": "quote-42",
    "occurredAt": "2026-07-31T14:30:00Z",
    "receivedAt": "2026-07-31T14:30:00.125Z",
    "sequence": 42,
    "revision": 0,
    "correctionOfEventId": None,
    "values": {"bidPrice": 210.10, "bidSize": 100, "askPrice": 210.12, "askSize": 80},
}

C_TRADE_EVENT: dict[str, Any] = {
    "eventId": "evt_5bb3d64d6ac9c1be4e58b738054f455d5a453c315a8db745b73176ab17fa988e",
    "schemaVersion": 1,
    "instrumentId": C_INSTRUMENT_ID,
    "provider": "ALPACA",
    "feed": "SIP",
    "eventType": "TRADE",
    "providerEventId": "trade-43",
    "occurredAt": "2026-07-31T14:30:00.010Z",
    "receivedAt": "2026-07-31T14:30:00.135Z",
    "sequence": 43,
    "revision": 0,
    "correctionOfEventId": None,
    "values": {"price": 210.11, "size": 25},
}

C_BAR_EVENT: dict[str, Any] = {
    "eventId": "evt_96e3d1d3e45d92ab162eee652010595bae90825203db15590d2ee3afcac3c834",
    "schemaVersion": 1,
    "instrumentId": C_INSTRUMENT_ID,
    "provider": "ALPACA",
    "feed": "SIP",
    "eventType": "BAR_1M",
    "providerEventId": "bar-20260731T143000Z",
    "occurredAt": "2026-07-31T14:30:00Z",
    "receivedAt": "2026-07-31T14:31:00.050Z",
    "sequence": 45,
    "revision": 0,
    "correctionOfEventId": None,
    "values": {"open": 210.10, "high": 210.25, "low": 210.05, "close": 210.20, "volume": 2500},
}

C_FIXTURE_EVENTS = (C_QUOTE_EVENT, C_TRADE_EVENT, C_BAR_EVENT)

#: Every wire key C emits, in `MarketEventEnvelope` declaration order.
C_WIRE_FIELDS = (
    "eventId",
    "schemaVersion",
    "instrumentId",
    "provider",
    "feed",
    "eventType",
    "providerEventId",
    "occurredAt",
    "receivedAt",
    "sequence",
    "revision",
    "correctionOfEventId",
    "values",
)

# ----------------------------------------------------------------------------------
# D's landing zone for that stream.
#
# C emits BAR_1M at PT1M; D's canonical `DATASET_CONTRACTS` has no 1-minute RAW
# contract (RAW is 30m; 1h/4h/1d are DERIVED).  The ingest spec below therefore
# declares the 30-minute stream, and `test_cs_native_bar_cadence_has_no_landing_
# zone_and_says_so` pins the fact that binding C's PT1M cadence to D's 30m
# contract fails closed instead of silently mis-labelling the resolution.
# ----------------------------------------------------------------------------------

RAW_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]
SOURCE_PROVIDER = "ALPACA"
SOURCE_FEED = "SIP"
INGESTED_EVENT_TYPE = "BAR_30M"

INSTRUMENTS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
}
SHARD_COUNT = 2
SHARDS = ("s00-of-2", "s01-of-2")
AAPL_SHARD = "s00-of-2"

SESSION = date(2024, 1, 8)
WEEK_SESSIONS = (
    date(2024, 1, 8),
    date(2024, 1, 9),
    date(2024, 1, 10),
    date(2024, 1, 11),
    date(2024, 1, 12),
)
BARS_PER_SESSION = 13
TOTAL_EVENTS = BARS_PER_SESSION * len(INSTRUMENTS)

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
    open="open", high="high", low="low", close="close", volume="volume",
    trade_count="tradeCount", vwap="vwap",
)

#: The 2024-01-08 session's final 30-minute bar (15:30 ET).  The feed has delivered
#: the session once it has consumed past this; there is no calendar-derived default.
SESSION_LAST_BAR_UTC = datetime(2024, 1, 8, 20, 30, tzinfo=UTC)


def temporary_root() -> str:
    return tempfile.mkdtemp()


def remove_root(path: str) -> None:
    shutil.rmtree(long_path(path), ignore_errors=True)


def c_shaped_event(
    *,
    instrument_id: str,
    symbol: str,
    bar_start: datetime,
    sequence: int,
    index: int,
    revision: int = 0,
    correction_of: str | None = None,
    event_id: str | None = None,
    close_override: float | None = None,
) -> dict[str, Any]:
    """One event carrying C's exact thirteen wire fields, in C's exact casing."""

    base = 100.0 + index / 10 + (0.5 if symbol == "MSFT" else 0.0)
    close = base + 0.5 if close_override is None else close_override
    # A correction restates the whole bar, so the OHLC relation still has to hold --
    # otherwise the pipeline's own quality check quarantines it and the test would be
    # measuring the wrong thing.
    high = max(base + 1.0, close)
    low = min(base - 1.0, close)
    return {
        "eventId": event_id or f"evt_{symbol.lower()}_{sequence:05d}_r{revision}",
        "schemaVersion": MARKET_EVENT_SCHEMA_VERSION,
        "instrumentId": instrument_id,
        "provider": SOURCE_PROVIDER,
        "feed": SOURCE_FEED,
        "eventType": INGESTED_EVENT_TYPE,
        "providerEventId": f"{symbol}-{bar_start.isoformat()}",
        "occurredAt": bar_start.isoformat().replace("+00:00", "Z"),
        "receivedAt": (bar_start + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "sequence": sequence,
        "revision": revision,
        "correctionOfEventId": correction_of,
        "values": {
            "open": base,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1000 + index,
            "tradeCount": 10 + index,
            "vwap": base + 0.25,
        },
    }


def session_events(
    *sessions: date, first_sequence: int = 1
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    sequence = first_sequence
    for session in sessions or (SESSION,):
        open_at = datetime.combine(session, datetime.min.time(), ET) + timedelta(hours=9, minutes=30)
        for index in range(BARS_PER_SESSION):
            bar_start = (open_at + timedelta(minutes=30 * index)).astimezone(UTC)
            for symbol, instrument_id in INSTRUMENTS.items():
                events.append(
                    c_shaped_event(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        bar_start=bar_start,
                        sequence=sequence,
                        index=index,
                    )
                )
                sequence += 1
    return events


def build_engine(root: Path, catalog: Any = None) -> MarketPipelineEngine:
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
    return MarketPipelineEngine(config, catalog=catalog)


def ingest_spec(
    *,
    event_type: str = INGESTED_EVENT_TYPE,
    partition_granularity: str = "DAY",
    fields: BarFieldMap = FIELD_MAP,
) -> RealtimeIngestSpec:
    return RealtimeIngestSpec(
        contract=RAW_CONTRACT,
        event_type=event_type,
        source_provider=SOURCE_PROVIDER,
        source_feed=SOURCE_FEED,
        source_resolution="PT30M",
        partition_granularity=partition_granularity,
        fields=fields,
    )


def build_ingestor(
    engine: MarketPipelineEngine,
    *,
    repository: Any = None,
    **spec_kwargs: Any,
) -> RealtimeIngestor:
    ledger = WatermarkLedger(
        feed_id=engine.feed_ids[RAW_CONTRACT.feed_code],
        shard_keys=SHARDS,
        repository=repository or InMemoryWatermarkRepository(),
    )
    return RealtimeIngestor(engine, ingest_spec(**spec_kwargs), ledger=ledger)


# ======================================================================================
# 1. D consumes C's real event format
# ======================================================================================


class CEventContractTests(unittest.TestCase):
    """`parse_market_event` against C's own published fixture, field by field."""

    def test_parses_every_event_in_cs_published_contract_fixture(self) -> None:
        parsed = [parse_market_event(event) for event in C_FIXTURE_EVENTS]

        self.assertEqual([event.event_type for event in parsed], ["QUOTE", "TRADE", "BAR_1M"])
        self.assertEqual([event.sequence for event in parsed], [42, 43, 45])
        self.assertEqual({event.provider for event in parsed}, {"ALPACA"})
        self.assertEqual({event.feed for event in parsed}, {"SIP"})
        self.assertEqual({event.instrument_id for event in parsed}, {C_INSTRUMENT_ID})
        self.assertEqual({event.revision for event in parsed}, {0})
        self.assertEqual({event.correction_of_event_id for event in parsed}, {None})
        self.assertEqual({event.schema_version for event in parsed}, {1})

        bar = parsed[2]
        self.assertEqual(bar.event_id, C_BAR_EVENT["eventId"])
        self.assertEqual(bar.provider_event_id, "bar-20260731T143000Z")
        self.assertEqual(bar.occurred_at, datetime(2026, 7, 31, 14, 30, tzinfo=UTC))
        self.assertEqual(bar.received_at, datetime(2026, 7, 31, 14, 31, 0, 50_000, tzinfo=UTC))
        self.assertEqual(
            dict(bar.values),
            {"open": 210.10, "high": 210.25, "low": 210.05, "close": 210.20, "volume": 2500},
        )

    def test_every_wire_field_c_emits_is_required(self) -> None:
        """Dropping any one of C's thirteen fields is refused, one at a time."""

        for field_name in C_WIRE_FIELDS:
            with self.subTest(field=field_name):
                event = {key: value for key, value in C_BAR_EVENT.items() if key != field_name}
                with self.assertRaises(RealtimeIngestError):
                    parse_market_event(event)

    def test_an_unknown_event_schema_version_is_rejected_explicitly(self) -> None:
        event = dict(C_BAR_EVENT, schemaVersion=2)
        with self.assertRaises(UnsupportedEventVersion) as caught:
            parse_market_event(event)
        self.assertIn("schemaVersion", str(caught.exception))
        self.assertIn("2", str(caught.exception))

    def test_a_schema_version_below_cs_floor_is_rejected(self) -> None:
        with self.assertRaises(UnsupportedEventVersion):
            parse_market_event(dict(C_BAR_EVENT, schemaVersion=0))

    def test_received_at_may_not_precede_occurred_at(self) -> None:
        """`MarketEventEnvelope.java:37-39` -- C refuses to construct one."""

        event = dict(C_BAR_EVENT, receivedAt="2026-07-31T14:29:59Z")
        with self.assertRaisesRegex(RealtimeIngestError, "receivedAt"):
            parse_market_event(event)

    def test_revision_zero_may_not_carry_a_correction_pointer(self) -> None:
        event = dict(C_BAR_EVENT, revision=0, correctionOfEventId="evt_something")
        with self.assertRaisesRegex(RealtimeIngestError, "correctionOfEventId"):
            parse_market_event(event)

    def test_a_correction_must_name_the_event_it_corrects(self) -> None:
        event = dict(C_BAR_EVENT, revision=1, correctionOfEventId=None)
        with self.assertRaisesRegex(RealtimeIngestError, "correctionOfEventId"):
            parse_market_event(event)

    def test_a_negative_revision_is_rejected(self) -> None:
        with self.assertRaises(RealtimeIngestError):
            parse_market_event(dict(C_BAR_EVENT, revision=-1, correctionOfEventId=None))


class CEventIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.engine = build_engine(Path(self.root))

    def test_an_event_from_another_provider_or_feed_is_refused(self) -> None:
        ingestor = build_ingestor(self.engine)
        first = session_events()[0]
        with self.assertRaisesRegex(RealtimeIngestError, "provider"):
            ingestor.submit(dict(first, provider="POLYGON"))
        with self.assertRaisesRegex(RealtimeIngestError, "feed"):
            ingestor.submit(dict(first, feed="IEX"))
        self.assertEqual(ingestor.pending_rows, 0)

    def test_an_unknown_schema_version_never_reaches_the_buffer(self) -> None:
        ingestor = build_ingestor(self.engine)
        with self.assertRaises(UnsupportedEventVersion):
            ingestor.submit(dict(session_events()[0], schemaVersion=99))
        self.assertEqual(ingestor.pending_rows, 0)
        self.assertEqual(ingestor.flush().status, "NO_CHANGE")

    def test_cs_native_bar_cadence_has_no_landing_zone_and_says_so(self) -> None:
        """C emits BAR_1M at PT1M; D's RAW contract is 30m.  Fail closed, loudly."""

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

    def test_an_event_type_this_stream_does_not_ingest_is_reported_not_dropped(self) -> None:
        ingestor = build_ingestor(self.engine)
        decisions = [ingestor.submit(dict(event, eventType="QUOTE")) for event in session_events()[:2]]
        self.assertEqual([d.accepted for d in decisions], [False, False])
        self.assertEqual({d.reason for d in decisions}, {"EVENT_TYPE_NOT_INGESTED"})
        self.assertEqual(ingestor.pending_rows, 0)

    def test_an_exact_redelivery_is_skipped_and_does_not_duplicate_a_bar(self) -> None:
        ingestor = build_ingestor(self.engine)
        events = session_events()
        first = [ingestor.submit(event) for event in events]
        self.assertEqual(sum(1 for d in first if d.accepted), TOTAL_EVENTS)

        replay = [ingestor.submit(event) for event in events]
        self.assertEqual(sum(1 for d in replay if d.accepted), 0)
        self.assertEqual({d.reason for d in replay}, {"DUPLICATE_EVENT"})
        self.assertEqual(ingestor.pending_rows, TOTAL_EVENTS)

    def test_a_late_arriving_bar_is_ingested_and_the_watermark_does_not_rewind(self) -> None:
        """Out-of-order is a delivery fact, not a reason to lose a bar.

        C publishes an out-of-order event rather than dropping it
        (`MarketEventOrderingProcessor.java:31-34`, status ``OUT_OF_ORDER``,
        published=true).  D therefore records the bar and leaves the shard head
        where it was.
        """

        ingestor = build_ingestor(self.engine)
        events = session_events()
        newest = events[-2]  # AAPL 15:30
        ingestor.submit(newest)
        head_after_newest = ingestor.ledger.head(AAPL_SHARD)
        assert head_after_newest is not None

        late = events[0]  # AAPL 09:30, arriving after the 15:30 bar
        decision = ingestor.submit(late)

        self.assertTrue(decision.accepted)
        self.assertEqual(decision.reason, "ACCEPTED_LATE")
        self.assertEqual(decision.outcome, "STALE")
        self.assertEqual(ingestor.pending_rows, 2)
        self.assertEqual(ingestor.ledger.head(AAPL_SHARD), head_after_newest)

        result = ingestor.flush()
        table = pq.read_table(
            Path(long_path(str(Path(self.root) / "objects" / CANONICAL_DAY_KEY)))
        )
        self.assertIn(CANONICAL_DAY_KEY, result.object_keys)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(
            table.column("bar_start_at").to_pylist(),
            [
                datetime(2024, 1, 8, 14, 30, tzinfo=UTC),
                datetime(2024, 1, 8, 20, 30, tzinfo=UTC),
            ],
        )

    def _published_aapl_closes(self) -> list[float]:
        table = pq.read_table(
            Path(long_path(str(Path(self.root) / "objects" / CANONICAL_DAY_KEY)))
        )
        return list(table.column("close").to_pylist())

    def test_a_correction_replaces_the_bar_it_corrects(self) -> None:
        ingestor = build_ingestor(self.engine)
        events = session_events()
        for event in events:
            ingestor.submit(event)
        original = events[0]
        self.assertEqual(original["values"]["close"], 100.5)

        correction = c_shaped_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2024, 1, 8, 14, 30, tzinfo=UTC),
            sequence=999,
            index=0,
            revision=1,
            correction_of=original["eventId"],
            event_id="evt_aapl_correction_r1",
            close_override=123.75,
        )
        decision = ingestor.submit(correction)

        self.assertTrue(decision.accepted)
        self.assertEqual(decision.reason, "CORRECTION_APPLIED")
        self.assertEqual(decision.revision, 1)
        # The correction replaces a bar; it does not add one.
        self.assertEqual(ingestor.pending_rows, TOTAL_EVENTS)

        self.assertEqual(ingestor.flush().status, "AVAILABLE")
        closes = self._published_aapl_closes()
        self.assertEqual(len(closes), BARS_PER_SESSION)
        self.assertEqual(closes[0], 123.75)
        self.assertEqual(closes[1], 100.6)

    def test_an_older_revision_arriving_after_a_correction_is_not_applied(self) -> None:
        ingestor = build_ingestor(self.engine)
        events = session_events()
        for event in events:
            ingestor.submit(event)
        original = events[0]
        bar_start = datetime(2024, 1, 8, 14, 30, tzinfo=UTC)
        ingestor.submit(
            c_shaped_event(
                instrument_id=INSTRUMENTS["AAPL"], symbol="AAPL", bar_start=bar_start,
                sequence=999, index=0, revision=2, correction_of=original["eventId"],
                event_id="evt_aapl_correction_r2", close_override=200.5,
            )
        )

        decision = ingestor.submit(
            c_shaped_event(
                instrument_id=INSTRUMENTS["AAPL"], symbol="AAPL", bar_start=bar_start,
                sequence=998, index=0, revision=1, correction_of=original["eventId"],
                event_id="evt_aapl_correction_r1", close_override=17.25,
            )
        )

        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "SUPERSEDED_REVISION")

        self.assertEqual(ingestor.flush().status, "AVAILABLE")
        self.assertEqual(self._published_aapl_closes()[0], 200.5)

    def test_two_different_events_claiming_one_bar_at_one_revision_is_a_conflict(self) -> None:
        ingestor = build_ingestor(self.engine)
        first = session_events()[0]
        rival = dict(first, eventId="evt_aapl_rival_r0", values=dict(first["values"], close=1.0))
        ingestor.submit(first)
        with self.assertRaisesRegex(RealtimeIngestError, "conflict"):
            ingestor.submit(rival)

    def test_c_format_events_publish_the_canonical_key_and_compact(self) -> None:
        ingestor = build_ingestor(self.engine)
        for event in session_events(*WEEK_SESSIONS):
            ingestor.submit(event)
        result = ingestor.flush()

        self.assertEqual(result.status, "AVAILABLE")
        self.assertEqual(result.row_count, TOTAL_EVENTS * len(WEEK_SESSIONS))
        self.assertIn(CANONICAL_DAY_KEY, result.object_keys)
        self.assertEqual(len(result.object_keys), len(WEEK_SESSIONS) * len(SHARDS))

        compacted = self.engine.compact(RAW_CONTRACT, granularity="WEEK", period=date(2024, 1, 8))
        self.assertEqual(compacted["status"], "SUCCEEDED")
        self.assertEqual(compacted["new_object_count"], 2)
        self.assertEqual(compacted["retained_object_count"], 0)
        week = Path(long_path(str(Path(self.root) / "objects" / CANONICAL_WEEK_KEY)))
        self.assertTrue(week.is_file())
        self.assertEqual(pq.read_table(week).num_rows, BARS_PER_SESSION * len(WEEK_SESSIONS))


# ======================================================================================
# 2. The blocking gate
# ======================================================================================


def gate_for(
    engine: MarketPipelineEngine,
    repository: Any,
    *,
    now: datetime,
    freshness_budget: timedelta = timedelta(minutes=15),
) -> WarmupReadinessGate:
    return WarmupReadinessGate(
        engine.catalog,
        feed_id=engine.feed_ids[RAW_CONTRACT.feed_code],
        watermarks=repository,
        freshness_budget=freshness_budget,
        now=lambda: now,
    )


COVERAGE = WarmupCoverage(
    contract=RAW_CONTRACT,
    session=SESSION,
    granularity="DAY",
    required_shards=SHARDS,
    required_watermark_at=SESSION_LAST_BAR_UTC,
)


class WarmupBlockingTests(unittest.TestCase):
    """누락 시 실행을 차단 -- each miss produces its own reason code."""

    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.engine = build_engine(Path(self.root))
        self.repository = InMemoryWatermarkRepository()
        self.feed_id = self.engine.feed_ids[RAW_CONTRACT.feed_code]
        self.now = datetime(2024, 1, 8, 21, 5, tzinfo=UTC)

    def ingest_the_session(self) -> None:
        ingestor = build_ingestor(self.engine, repository=self.repository)
        for event in session_events():
            ingestor.submit(event)
        ingestor.flush(ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC))

    # -- missing daily object ---------------------------------------------------------

    def test_a_missing_daily_object_blocks_with_its_own_reason_code(self) -> None:
        gate = gate_for(self.engine, self.repository, now=self.now)
        readiness = gate.evaluate(COVERAGE)

        self.assertEqual(readiness.state, "BLOCKED")
        self.assertEqual(readiness.reason_code, WarmupBlockReason.DAILY_OBJECT_MISSING)
        self.assertEqual(readiness.reason_code, "D90_DAILY_OBJECT_MISSING")
        self.assertIn("2024-01-08", readiness.detail or "")
        self.assertEqual(readiness.manifest_status, "QUARANTINED")

    def test_a_session_covering_only_one_of_two_required_shards_blocks(self) -> None:
        ingestor = build_ingestor(self.engine, repository=self.repository)
        for event in session_events():
            if event["instrumentId"] == INSTRUMENTS["AAPL"]:
                ingestor.submit(event)
        ingestor.flush(ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC))

        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(COVERAGE)

        self.assertEqual(readiness.reason_code, "D90_DAILY_OBJECT_MISSING")
        self.assertIn("s01-of-2", readiness.detail or "")

    def test_a_quarantined_manifest_blocks_with_a_different_reason_than_a_missing_one(
        self,
    ) -> None:
        self.ingest_the_session()
        rows = self.engine.catalog.records("market_data.dataset_manifests")
        self.assertEqual(len(rows), 1)
        self.engine.catalog.upsert(
            "market_data.dataset_manifests", {**rows[0], "status": "QUARANTINED"}
        )

        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(COVERAGE)

        self.assertEqual(readiness.reason_code, "D90_MANIFEST_NOT_AVAILABLE")
        self.assertIn("QUARANTINED", readiness.detail or "")

    # -- watermark --------------------------------------------------------------------

    def test_a_missing_watermark_blocks(self) -> None:
        self.ingest_the_session()
        readiness = gate_for(self.engine, InMemoryWatermarkRepository(), now=self.now).evaluate(
            COVERAGE
        )
        self.assertEqual(readiness.reason_code, "D90_WATERMARK_MISSING")

    def test_a_stale_watermark_blocks(self) -> None:
        self.ingest_the_session()
        late = datetime(2024, 1, 8, 23, 30, tzinfo=UTC)  # 2h29m after last ingest
        readiness = gate_for(self.engine, self.repository, now=late).evaluate(COVERAGE)

        self.assertEqual(readiness.reason_code, "D90_WATERMARK_STALE")
        self.assertIn("PT15M", readiness.detail or "")

    def test_a_watermark_that_has_not_reached_the_session_close_blocks(self) -> None:
        self.ingest_the_session()
        behind = InMemoryWatermarkRepository(
            [
                StreamWatermark(
                    feed_id=self.feed_id,
                    position=StreamPosition(
                        source_event_at=datetime(2024, 1, 8, 17, 0, tzinfo=UTC), sequence=5
                    ),
                    ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC),
                )
            ]
        )
        readiness = gate_for(self.engine, behind, now=self.now).evaluate(COVERAGE)

        self.assertEqual(readiness.reason_code, "D90_WATERMARK_BEHIND_SESSION")
        self.assertIn("2024-01-08T17:00:00Z", readiness.detail or "")
        self.assertIn("2024-01-08T20:30:00Z", readiness.detail or "")

    def test_a_completion_target_outside_the_partition_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside"):
            WarmupCoverage(
                contract=RAW_CONTRACT,
                session=SESSION,
                granularity="DAY",
                required_shards=SHARDS,
                required_watermark_at=datetime(2024, 1, 10, 20, 30, tzinfo=UTC),
            )

    # -- features ---------------------------------------------------------------------

    def test_an_opened_but_unsealed_feature_batch_blocks(self) -> None:
        """PENDING with no `batch_hash` -- the real builder's own refusal."""

        self.ingest_the_session()
        batch = _RealFeatureBatch(self.engine.catalog, INSTRUMENTS["AAPL"]).open()
        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(
            COVERAGE, feature_batch=batch
        )
        self.assertEqual(readiness.reason_code, "D90_FEATURE_BATCH_INCOMPLETE")
        self.assertIn(batch.plan.id, readiness.detail or "")
        self.assertIn("PENDING", readiness.detail or "")
        self.assertIn("batch_hash=None", readiness.detail or "")

    def test_a_batch_whose_planned_members_were_never_materialized_blocks(self) -> None:
        """`PartialSnapshotBatch` out of `seal`, classified as incomplete."""

        self.ingest_the_session()
        batch = _SealingFeatureBatch(self.engine.catalog, INSTRUMENTS["AAPL"])
        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(
            COVERAGE, feature_batch=batch
        )
        self.assertEqual(readiness.reason_code, "D90_FEATURE_BATCH_INCOMPLETE")
        self.assertIn(batch.plan.id, readiness.detail or "")

    def test_a_feature_batch_that_was_never_opened_blocks(self) -> None:
        self.ingest_the_session()
        batch = _RealFeatureBatch(self.engine.catalog, INSTRUMENTS["AAPL"])
        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(
            COVERAGE, feature_batch=batch
        )
        self.assertEqual(readiness.reason_code, "D90_FEATURE_BATCH_MISSING")
        self.assertIn("has never been opened", readiness.detail or "")
        # Never opened and merely unfinished are different facts and different codes.
        self.assertNotEqual(
            WarmupBlockReason.FEATURE_BATCH_MISSING, WarmupBlockReason.FEATURE_BATCH_INCOMPLETE
        )

    # -- the ready path ---------------------------------------------------------------

    def test_everything_present_is_ready_and_carries_no_reason_code(self) -> None:
        self.ingest_the_session()
        batch = _RealFeatureBatch(self.engine.catalog, INSTRUMENTS["AAPL"]).seal()
        self.assertEqual(batch.consume().status, "SUCCEEDED")
        readiness = gate_for(self.engine, self.repository, now=self.now).evaluate(
            COVERAGE, feature_batch=batch
        )

        self.assertEqual(readiness.state, "READY")
        self.assertIsNone(readiness.reason_code)
        self.assertIsNone(readiness.detail)
        self.assertEqual(readiness.manifest_status, "AVAILABLE")
        self.assertEqual(readiness.session_date_et, "2024-01-08")
        self.assertEqual(
            readiness.observed["watermark_source_event_at"], "2024-01-08T20:30:00Z"
        )

    def test_a_readiness_verdict_cannot_be_blocked_without_a_reason(self) -> None:
        with self.assertRaises(ValueError):
            WarmupReadiness(
                state="BLOCKED",
                session_date_et="2024-01-08",
                feed_id=self.feed_id,
                evaluated_at=self.now,
            )

    def test_a_ready_verdict_cannot_smuggle_a_reason_code(self) -> None:
        with self.assertRaises(ValueError):
            WarmupReadiness(
                state="READY",
                session_date_et="2024-01-08",
                feed_id=self.feed_id,
                evaluated_at=self.now,
                reason_code="D90_WATERMARK_STALE",
                detail="x",
            )

    # -- the block is observable ------------------------------------------------------

    def test_a_block_is_recorded_as_a_quality_incident_with_the_specific_code(self) -> None:
        self.ingest_the_session()
        gate = gate_for(self.engine, self.repository, now=datetime(2024, 1, 9, 4, 0, tzinfo=UTC))
        readiness = gate.evaluate(COVERAGE)
        self.assertEqual(readiness.reason_code, "D90_WATERMARK_STALE")

        incident_id = gate.record(readiness, COVERAGE)
        self.assertIsNotNone(incident_id)

        rows = self.engine.catalog.records("market_data.quality_incidents")
        blocking = [row for row in rows if str(row["incident_code"]).startswith("D90_")]
        self.assertEqual(len(blocking), 1)
        self.assertEqual(blocking[0]["incident_code"], "D90_WATERMARK_STALE")
        self.assertEqual(blocking[0]["severity"], "ERROR")
        self.assertEqual(blocking[0]["status"], "ACTIVE")
        self.assertEqual(blocking[0]["id"], incident_id)
        self.assertIsNotNone(blocking[0]["dataset_manifest_id"])

    def test_recording_a_block_quarantines_the_manifest_c_would_otherwise_read(self) -> None:
        self.ingest_the_session()
        gate = gate_for(self.engine, self.repository, now=datetime(2024, 1, 9, 4, 0, tzinfo=UTC))
        readiness = gate.evaluate(COVERAGE)
        gate.record(readiness, COVERAGE)

        statuses = [row["status"] for row in self.engine.catalog.records("market_data.dataset_manifests")]
        self.assertEqual(statuses, ["QUARANTINED"])

    def test_recording_a_ready_verdict_writes_no_incident_and_says_so(self) -> None:
        self.ingest_the_session()
        gate = gate_for(self.engine, self.repository, now=self.now)
        batch = _RealFeatureBatch(self.engine.catalog, INSTRUMENTS["AAPL"]).seal()
        readiness = gate.evaluate(COVERAGE, feature_batch=batch)

        self.assertIsNone(gate.record(readiness, COVERAGE))
        rows = self.engine.catalog.records("market_data.quality_incidents")
        self.assertEqual([row for row in rows if str(row["incident_code"]).startswith("D90_")], [])
        statuses = [row["status"] for row in self.engine.catalog.records("market_data.dataset_manifests")]
        self.assertEqual(statuses, ["AVAILABLE"])

    def test_the_block_reason_codes_are_all_distinct_and_d90_scoped(self) -> None:
        codes = [reason.value for reason in WarmupBlockReason]
        self.assertEqual(len(codes), len(set(codes)))
        self.assertTrue(all(code.startswith("D90_") for code in codes))
        self.assertEqual(
            sorted(codes),
            [
                "D90_DAILY_OBJECT_MISSING",
                "D90_FEATURE_BATCH_INCOMPLETE",
                "D90_FEATURE_BATCH_MISSING",
                "D90_MANIFEST_NOT_AVAILABLE",
                "D90_WATERMARK_BEHIND_SESSION",
                "D90_WATERMARK_MISSING",
                "D90_WATERMARK_STALE",
            ],
        )


# --------------------------------------------------------------------------------------
# A real D13 snapshot batch, not a stand-in.
#
# The gate's feature check is only worth anything if the exception it classifies is the
# one `FeatureSnapshotBatchBuilder` actually raises.  A hand-written double raising
# `SnapshotBatchNotConsumable("...")` would keep passing after the production message,
# the status set or the exception type changed, so the batch below is built, opened and
# sealed through the real registry and builder.
# --------------------------------------------------------------------------------------

FEATURE_CATALOG_VERSION_ID = "0e5a1c9e-1111-4a11-8a11-000000000001"
FEATURE_PERIOD_START = datetime(2024, 1, 8, 14, 30, tzinfo=UTC)
FEATURE_PERIOD_END = datetime(2024, 1, 8, 21, 0, tzinfo=UTC)
FEATURE_WATERMARK = "ALPACA_SIP_RAW_30M@2024-01-08T21:00:00Z"
FEATURE_RUN_ID = "11111111-0000-4000-8000-0000000000a1"
FEATURE_OUTPUT_MANIFEST = "dddddddd-0000-4000-8000-0000000000a1"
FEATURE_SNAPSHOT_OBJECT = "eeeeeeee-0000-4000-8000-0000000000a1"


def _feature_bars() -> tuple[Any, ...]:
    from market_pipeline_lib.features import BarPoint

    return tuple(
        BarPoint(
            bar_start_at=FEATURE_PERIOD_START + timedelta(minutes=30 * index),
            open=Decimal(close) - 1,
            high=Decimal(close) + 1,
            low=Decimal(close) - 2,
            close=Decimal(close),
            volume=1000 + index,
        )
        for index, close in enumerate((100, 104, 108, 111, 107))
    )


def _feature_sources() -> tuple[Any, ...]:
    from market_pipeline_lib.features import SourceObject

    return (
        SourceObject(
            dataset_object_id="cccccccc-0000-4000-8000-0000000000a1",
            dataset_manifest_id="dddddddd-0000-4000-8000-0000000000b1",
            content_hash="1" * 64,
            partition_start="2024-01-08",
            partition_end="2024-01-09",
            row_count=BARS_PER_SESSION,
        ),
    )


class _RealFeatureBatch:
    """Binds a real `FeatureSnapshotBatchBuilder` to its plan.

    `WarmupReadinessGate` needs a zero-argument `consume()`; the builder's takes the
    plan.  This is the binding and nothing else -- every failure it surfaces comes out
    of production code.
    """

    def __init__(self, catalog: Any, instrument_id: str) -> None:
        from market_pipeline_lib.features import (
            FeatureDefinition,
            FeatureDefinitionRegistry,
            FeatureMaterializer,
            FeatureSnapshotBatchBuilder,
            MarketInput,
            MaterializationRequest,
            SnapshotBatchPlan,
            input_bundle_fingerprint,
        )

        self._registry = FeatureDefinitionRegistry(catalog)
        self._definition = self._registry.publish(
            FeatureDefinition.create(
                element_catalog_version_id=FEATURE_CATALOG_VERSION_ID,
                feature_code="SMA",
                calculator_version="1.0.0",
                resolution="30m",
                parameters={"window": 3, "price_field": "close"},
            )
        )
        self._materializer = FeatureMaterializer(catalog, self._registry)
        self._builder = FeatureSnapshotBatchBuilder(catalog, self._registry)
        self._request = MaterializationRequest(
            definition=self._definition,
            instrument_id=instrument_id,
            pipeline_run_id=FEATURE_RUN_ID,
            sources=_feature_sources(),
            bars=_feature_bars(),
            period_start=FEATURE_PERIOD_START,
            period_end=FEATURE_PERIOD_END,
            source_watermark=FEATURE_WATERMARK,
            output_dataset_manifest_id=FEATURE_OUTPUT_MANIFEST,
        )
        self.plan = SnapshotBatchPlan(
            definition_hashes=(self._definition.definition_hash,),
            market_inputs=(
                MarketInput(
                    instrument_id=instrument_id,
                    input_dataset_set_hash=input_bundle_fingerprint(_feature_sources()),
                ),
            ),
            period_start=FEATURE_PERIOD_START,
            period_end=FEATURE_PERIOD_END,
            source_start_watermark="ALPACA_SIP_RAW_30M@2024-01-08T14:30:00Z",
            source_end_watermark=FEATURE_WATERMARK,
        )

    def open(self) -> _RealFeatureBatch:
        self._builder.open(self.plan)
        return self

    def seal(self) -> _RealFeatureBatch:
        self._builder.open(self.plan)
        result = self._materializer.materialize(self._request)
        self._builder.seal(
            self.plan, results=(result,), snapshot_object_id=FEATURE_SNAPSHOT_OBJECT
        )
        return self

    def consume(self) -> Any:
        return self._builder.consume(self.plan)


class _SealingFeatureBatch(_RealFeatureBatch):
    """A consumer that seals before reading -- the shape that surfaces a partial batch.

    `consume` alone can never raise `PartialSnapshotBatch`; `seal` is what refuses a
    batch whose planned members were never materialized.  A caller whose projection
    seals on demand therefore hits that failure, and the gate has to classify it.
    """

    def consume(self) -> Any:
        self._builder.open(self.plan)
        self._builder.seal(self.plan, results=(), snapshot_object_id=FEATURE_SNAPSHOT_OBJECT)
        return super().consume()


# ======================================================================================
# 3. The bundle C actually reads carries the verdict
# ======================================================================================


class WarmupBundleReadinessTests(unittest.TestCase):
    """`manifest.json` is the only thing C's `ManifestBoundWarmupDataSource` reads."""

    def setUp(self) -> None:
        self.root = Path(temporary_root())
        self.addCleanup(remove_root, str(self.root))

    def _spec(self) -> Any:
        from market_pipeline_lib.realtime_warmup import WarmupPublicationSpec

        return WarmupPublicationSpec(
            contract=RAW_CONTRACT, event_type=INGESTED_EVENT_TYPE, granularity="DAY"
        )

    def _requirement(self) -> Any:
        from market_pipeline_lib.realtime_warmup import FeatureRequirement

        return FeatureRequirement(
            requirement_id="req-close",
            feature_id="close",
            feature_version="1.0.0",
            resolution="PT30M",
            value_field="close",
            instruments=(INSTRUMENTS["AAPL"],),
            required_observations=1,
        )

    def _document(self) -> dict[str, Any]:
        events = [
            event for event in session_events() if event["instrumentId"] == INSTRUMENTS["AAPL"]
        ]
        return {"schemaVersion": MARKET_EVENT_SCHEMA_VERSION, "events": events}

    def test_a_ready_bundle_is_available_and_states_it_was_evaluated(self) -> None:
        from market_pipeline_lib.realtime_warmup import publish_realtime_warmup_bundle

        readiness = WarmupReadiness(
            state="READY",
            session_date_et="2024-01-08",
            feed_id="feed-1",
            evaluated_at=datetime(2024, 1, 8, 21, 5, tzinfo=UTC),
        )
        bundle = publish_realtime_warmup_bundle(
            self._document(), self.root / "bundle", (self._requirement(),),
            spec=self._spec(), readiness=readiness,
        )
        self.assertEqual(bundle.manifest["status"], "AVAILABLE")
        self.assertEqual(bundle.manifest["readiness"]["state"], "READY")
        self.assertIsNone(bundle.manifest["readiness"]["reason_code"])
        self.assertEqual(
            bundle.manifest["readiness"]["evaluated_at"], "2024-01-08T21:05:00Z"
        )

    def test_a_blocked_bundle_is_quarantined_and_names_the_reason(self) -> None:
        from market_pipeline_lib.realtime_warmup import publish_realtime_warmup_bundle

        readiness = WarmupReadiness(
            state="BLOCKED",
            session_date_et="2024-01-08",
            feed_id="feed-1",
            evaluated_at=datetime(2024, 1, 8, 21, 5, tzinfo=UTC),
            reason_code=WarmupBlockReason.WATERMARK_STALE,
            detail="last_ingested_at 2024-01-08T21:01:00Z is older than PT15M",
        )
        bundle = publish_realtime_warmup_bundle(
            self._document(), self.root / "bundle", (self._requirement(),),
            spec=self._spec(), readiness=readiness,
        )
        manifest = json.loads((self.root / "bundle" / "manifest.json").read_text(encoding="utf-8"))

        # C's StartupWarmupCoordinator.java:67-69 refuses any status but AVAILABLE.
        self.assertEqual(manifest["status"], "QUARANTINED")
        self.assertEqual(manifest["readiness"]["state"], "BLOCKED")
        self.assertEqual(manifest["readiness"]["reason_code"], "D90_WATERMARK_STALE")
        self.assertIn("PT15M", manifest["readiness"]["detail"])
        self.assertEqual(bundle.manifest["status"], "QUARANTINED")

    def test_a_bundle_with_no_data_at_all_still_publishes_a_blocked_manifest(self) -> None:
        from market_pipeline_lib.realtime_warmup import publish_blocked_warmup_manifest

        readiness = WarmupReadiness(
            state="BLOCKED",
            session_date_et="2024-01-08",
            feed_id="feed-1",
            evaluated_at=datetime(2024, 1, 8, 21, 5, tzinfo=UTC),
            reason_code=WarmupBlockReason.DAILY_OBJECT_MISSING,
            detail="no AVAILABLE object for shard s01-of-2 on 2024-01-08",
        )
        path = publish_blocked_warmup_manifest(self.root / "bundle", spec=self._spec(), readiness=readiness)
        manifest = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["contract_id"], "d90.realtime-warmup-manifest")
        self.assertEqual(manifest["status"], "QUARANTINED")
        self.assertEqual(manifest["objects"], [])
        self.assertEqual(manifest["readiness"]["reason_code"], "D90_DAILY_OBJECT_MISSING")
        self.assertIn("s01-of-2", manifest["readiness"]["detail"])

    def test_publishing_a_ready_manifest_through_the_blocked_writer_is_refused(self) -> None:
        from market_pipeline_lib.realtime_warmup import (
            RealtimeWarmupError,
            publish_blocked_warmup_manifest,
        )

        readiness = WarmupReadiness(
            state="READY", session_date_et="2024-01-08", feed_id="feed-1",
            evaluated_at=datetime(2024, 1, 8, 21, 5, tzinfo=UTC),
        )
        with self.assertRaises(RealtimeWarmupError):
            publish_blocked_warmup_manifest(self.root / "bundle", spec=self._spec(), readiness=readiness)

    def test_the_verifier_refuses_a_manifest_whose_status_contradicts_its_readiness(self) -> None:
        from market_pipeline_lib.realtime_warmup import (
            RealtimeWarmupError,
            publish_realtime_warmup_bundle,
            verify_realtime_warmup_bundle,
        )

        readiness = WarmupReadiness(
            state="BLOCKED", session_date_et="2024-01-08", feed_id="feed-1",
            evaluated_at=datetime(2024, 1, 8, 21, 5, tzinfo=UTC),
            reason_code=WarmupBlockReason.WATERMARK_STALE, detail="stale",
        )
        output = self.root / "bundle"
        publish_realtime_warmup_bundle(
            self._document(), output, (self._requirement(),), spec=self._spec(), readiness=readiness
        )
        manifest_path = output / "manifest.json"
        tampered = json.loads(manifest_path.read_text(encoding="utf-8"))
        tampered["status"] = "AVAILABLE"
        manifest_path.write_text(
            json.dumps(tampered, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RealtimeWarmupError, "readiness"):
            verify_realtime_warmup_bundle(output)


# ======================================================================================
# 4. Real infrastructure
# ======================================================================================


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
    dead_letter_url = client.create_queue(QueueName=f"d90-dlq-{suffix}")["QueueUrl"]
    queue_url = client.create_queue(
        QueueName=f"d90-main-{suffix}", Attributes={"VisibilityTimeout": "0"}
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
class TestCEventsOverLocalStack:
    def test_c_format_events_become_a_validated_compaction_ready_object(
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

            # The whole trading week, because a WEEK compaction of a partially
            # delivered week is a data gap and is quarantined -- correctly.
            events = session_events(*WEEK_SESSIONS)
            for start in range(0, len(events), 10):
                client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({"events": events[start : start + 10]}),
                )

            report = consumer.drain(max_empty_cycles=2, wait_seconds=2.0)
            assert report.accepted == TOTAL_EVENTS * len(WEEK_SESSIONS)
            assert report.dead_lettered == 0
            assert report.retried == 0

            published = ingestor.flush(ingested_at=datetime(2024, 1, 12, 21, 1, tzinfo=UTC))
            assert published.status == "AVAILABLE"
            assert CANONICAL_DAY_KEY in published.object_keys
            assert len(published.object_keys) == len(WEEK_SESSIONS) * len(SHARDS)

            compacted = engine.compact(RAW_CONTRACT, granularity="WEEK", period=date(2024, 1, 8))
            assert compacted["status"] == "SUCCEEDED"
            assert compacted["manifest"]["status"] == "AVAILABLE"
            assert compacted["new_object_count"] == 2
            week = Path(long_path(str(Path(root) / "objects" / CANONICAL_WEEK_KEY)))
            assert week.is_file()
            assert pq.read_table(week).num_rows == BARS_PER_SESSION * len(WEEK_SESSIONS)

            watermark = repository.load(engine.feed_ids[RAW_CONTRACT.feed_code])
            assert watermark is not None
            assert watermark.position.isoformat() == "2024-01-12T20:30:00Z"
        finally:
            remove_root(root)

    def test_a_real_redelivery_produces_no_duplicate_bar_and_no_second_object(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        """At-least-once, driven through the queue rather than simulated."""

        client, queue_url, dead_letter_url = sqs_queues
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            source = SqsEventSource(
                client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url,
                visibility_timeout_seconds=0,
            )
            body = json.dumps({"events": session_events()})
            client.send_message(QueueUrl=queue_url, MessageBody=body)

            # Take the message without acknowledging it, so SQS really redelivers it.
            first = source.poll(max_messages=1, wait_seconds=5.0)
            assert len(first) == 1
            assert first[0].receive_count == 1
            decisions = [ingestor.submit(event) for event in first[0].body["events"]]
            assert sum(1 for d in decisions if d.accepted) == TOTAL_EVENTS

            second = source.poll(max_messages=1, wait_seconds=5.0)
            assert len(second) == 1
            assert second[0].receive_count == 2
            assert second[0].message_id == first[0].message_id
            replay = [ingestor.submit(event) for event in second[0].body["events"]]
            assert sum(1 for d in replay if d.accepted) == 0
            assert {d.reason for d in replay} == {"DUPLICATE_EVENT"}
            source.acknowledge(second[0])

            assert ingestor.pending_rows == TOTAL_EVENTS
            published = ingestor.flush(ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC))
            assert published.row_count == TOTAL_EVENTS
            assert len(published.object_keys) == 2
            assert published.object_keys.count(CANONICAL_DAY_KEY) == 1
            table = pq.read_table(
                Path(long_path(str(Path(root) / "objects" / CANONICAL_DAY_KEY)))
            )
            assert table.num_rows == BARS_PER_SESSION
            assert len(set(table.column("bar_start_at").to_pylist())) == BARS_PER_SESSION

            # Nothing left on either queue; the redelivery was absorbed, not parked.
            assert source.poll(max_messages=1, wait_seconds=1.0) == []
            assert "Messages" not in client.receive_message(
                QueueUrl=dead_letter_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
            )
            # A second flush republishes nothing.
            assert ingestor.flush().status == "NO_CHANGE"
        finally:
            remove_root(root)

    def test_out_of_order_delivery_loses_no_bar_and_never_rewinds_the_watermark(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        """The session delivered newest-message-first, through the real queue.

        C publishes an out-of-order event rather than dropping it
        (`MarketEventOrderingProcessor.java:31-34`), so the declared semantics are:
        the bar is kept, and the shard head does not go backwards.  Both halves are
        asserted against the durable `market_data.stream_watermarks` row, not against
        the in-process ledger.
        """

        client, queue_url, dead_letter_url = sqs_queues
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            repository = InMemoryWatermarkRepository()
            ingestor = build_ingestor(engine, repository=repository)
            source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
            consumer = RealtimeIngestConsumer(
                ingestor, source, max_receive_count=3, flush_every=10_000
            )

            events = session_events()
            # Four messages, sent newest chunk first.  SQS preserves neither the send
            # order across messages nor anything else here -- the point is that D is
            # handed the session out of order and must not lose the early bars.
            chunks = [events[start : start + 7] for start in range(0, len(events), 7)]
            for chunk in reversed(chunks):
                client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": chunk}))

            report = consumer.drain(max_empty_cycles=3, wait_seconds=2.0)
            assert report.accepted == TOTAL_EVENTS
            assert report.skipped == 0
            assert report.dead_lettered == 0

            published = ingestor.flush(ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC))
            assert published.status == "AVAILABLE"
            assert published.row_count == TOTAL_EVENTS
            table = pq.read_table(
                Path(long_path(str(Path(root) / "objects" / CANONICAL_DAY_KEY)))
            )
            # No silent bar loss: every 30-minute slot of the session is present, and
            # the rows are written in time order regardless of arrival order.
            starts = table.column("bar_start_at").to_pylist()
            assert len(starts) == BARS_PER_SESSION
            assert starts == sorted(starts)
            assert starts[0] == datetime(2024, 1, 8, 14, 30, tzinfo=UTC)
            assert starts[-1] == SESSION_LAST_BAR_UTC

            # The head is the newest position seen, never rewound by the late chunks.
            watermark = repository.load(engine.feed_ids[RAW_CONTRACT.feed_code])
            assert watermark is not None
            assert watermark.position.source_event_at == SESSION_LAST_BAR_UTC
        finally:
            remove_root(root)

    def test_an_unknown_version_event_is_parked_with_no_partial_ingest(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        client, queue_url, dead_letter_url = sqs_queues
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
            consumer = RealtimeIngestConsumer(
                ingestor, source, max_receive_count=3, flush_every=10_000
            )
            events = session_events()[:3]
            events[2] = dict(events[2], schemaVersion=2)
            client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": events}))

            cycle = consumer.run_once(wait_seconds=5.0)

            assert cycle.dead_lettered == 1
            assert cycle.retried == 0
            assert cycle.acknowledged == 0
            parked = client.receive_message(
                QueueUrl=dead_letter_url, MaxNumberOfMessages=1, WaitTimeSeconds=5,
                MessageAttributeNames=["All"],
            )["Messages"]
            assert len(parked) == 1
            # Its own reason, not the generic one: C deployed ahead of D is fixed by
            # shipping D, and an operator cannot tell that from "MALFORMED_EVENT".
            assert parked[0]["MessageAttributes"]["DeadLetterReason"]["StringValue"] == (
                "UNSUPPORTED_EVENT_VERSION"
            )
            assert json.loads(parked[0]["Body"])["events"][2]["schemaVersion"] == 2
            # No empty success and no partial substitution: the two events *before*
            # the bad one are not buffered either -- the message is the unit.
            assert ingestor.pending_rows == 0
            assert ingestor.flush().status == "NO_CHANGE"
            assert source.poll(max_messages=1, wait_seconds=1.0) == []
        finally:
            remove_root(root)

    def test_a_malformed_event_is_parked_under_the_generic_reason(
        self, sqs_queues: tuple[Any, str, str]
    ) -> None:
        """A field C always emits, missing.  Distinct from a version it does not."""

        client, queue_url, dead_letter_url = sqs_queues
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            source = SqsEventSource(client, queue_url=queue_url, dead_letter_queue_url=dead_letter_url)
            consumer = RealtimeIngestConsumer(
                ingestor, source, max_receive_count=3, flush_every=10_000
            )
            events = session_events()[:3]
            events[2] = {key: value for key, value in events[2].items() if key != "occurredAt"}
            client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({"events": events}))

            cycle = consumer.run_once(wait_seconds=5.0)

            assert (cycle.dead_lettered, cycle.retried, cycle.acknowledged) == (1, 0, 0)
            parked = client.receive_message(
                QueueUrl=dead_letter_url, MaxNumberOfMessages=1, WaitTimeSeconds=5,
                MessageAttributeNames=["All"],
            )["Messages"]
            assert parked[0]["MessageAttributes"]["DeadLetterReason"]["StringValue"] == (
                "MALFORMED_EVENT"
            )
            assert ingestor.pending_rows == 0
        finally:
            remove_root(root)


@pytest.mark.integration
class TestWatermarksUnderConcurrency:
    def test_advance_only_holds_under_an_actual_race(self, postgres_url: str, postgres_catalog: Any) -> None:
        """Concurrent writers, not a sequential loop.

        The advance-only rule lives in one `ON CONFLICT ... WHERE` statement, so
        the property under test is that two connections interleaving on the same
        row can never leave it older than a value either of them already saw.
        """

        from sqlalchemy import create_engine

        root = temporary_root()
        try:
            engine = build_engine(Path(root), catalog=postgres_catalog)
            feed_id = engine.feed_ids[RAW_CONTRACT.feed_code]

            db = create_engine(postgres_url, future=True, pool_size=12, max_overflow=12)
            try:
                repository = SqlWatermarkRepository(db)
                base = datetime(2024, 1, 8, 14, 30, tzinfo=UTC)
                worker_count = 8
                per_worker = 25
                barrier = threading.Barrier(worker_count)
                observed: dict[int, list[tuple[datetime, int]]] = {}
                errors: list[BaseException] = []

                def run(worker: int) -> None:
                    seen: list[tuple[datetime, int]] = []
                    try:
                        barrier.wait(timeout=30)
                        # Interleaved, not partitioned: every worker walks the whole
                        # range, so writers genuinely collide on the same positions.
                        for step in range(per_worker):
                            offset = (step * worker_count + worker) % (worker_count * per_worker)
                            stored = repository.advance(
                                StreamWatermark(
                                    feed_id=feed_id,
                                    position=StreamPosition(
                                        source_event_at=base + timedelta(seconds=offset),
                                        sequence=offset,
                                    ),
                                    ingested_at=base + timedelta(minutes=1),
                                )
                            )
                            seen.append((stored.position.source_event_at, stored.position.sequence or -1))
                    except BaseException as error:  # noqa: BLE001 - surfaced below
                        errors.append(error)
                    finally:
                        observed[worker] = seen

                threads = [threading.Thread(target=run, args=(index,)) for index in range(worker_count)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=120)

                assert errors == []
                assert all(not thread.is_alive() for thread in threads)

                # No worker ever saw the row go backwards.
                for worker, seen in observed.items():
                    assert seen == sorted(seen), f"worker {worker} observed a regression: {seen}"

                highest = worker_count * per_worker - 1
                final = repository.load(feed_id)
                assert final is not None
                assert final.position.source_event_at == base + timedelta(seconds=highest)
                assert final.position.sequence == highest

                # And a genuinely older write after the race still cannot move it.
                stored = repository.advance(
                    StreamWatermark(
                        feed_id=feed_id,
                        position=StreamPosition(source_event_at=base, sequence=0),
                        ingested_at=base + timedelta(minutes=2),
                    )
                )
                assert stored.position.sequence == highest
            finally:
                db.dispose()
        finally:
            remove_root(root)


@pytest.mark.integration
class TestBlockIsVisibleInThePostgresProjection:
    def test_a_stale_watermark_block_is_readable_from_the_database_alone(
        self, postgres_url: str, postgres_catalog: Any
    ) -> None:
        """The DBML `Note` on stream_watermarks names exactly this join.

        A pre-evaluation gate reads `stream_watermarks` together with
        `quality_incidents`; both rows must be there, written by the real
        SQLAlchemy Core path against the real central schema.
        """

        from sqlalchemy import create_engine, text

        root = temporary_root()
        try:
            engine = build_engine(Path(root), catalog=postgres_catalog)
            feed_id = engine.feed_ids[RAW_CONTRACT.feed_code]
            db = create_engine(postgres_url, future=True)
            try:
                repository = SqlWatermarkRepository(db)
                ingestor = build_ingestor(engine, repository=repository)
                for event in session_events():
                    ingestor.submit(event)
                ingestor.flush(ingested_at=datetime(2024, 1, 8, 21, 1, tzinfo=UTC))

                stored = repository.load(feed_id)
                assert stored is not None
                assert stored.position.isoformat() == "2024-01-08T20:30:00Z"

                gate = WarmupReadinessGate(
                    postgres_catalog,
                    feed_id=feed_id,
                    watermarks=repository,
                    freshness_budget=timedelta(minutes=15),
                    now=lambda: datetime(2024, 1, 9, 4, 0, tzinfo=UTC),
                )
                readiness = gate.evaluate(COVERAGE)
                assert readiness.reason_code == "D90_WATERMARK_STALE"
                incident_id = gate.record(readiness, COVERAGE)
                assert incident_id is not None

                incidents = [
                    row
                    for row in postgres_catalog.records("market_data.quality_incidents")
                    if str(row["incident_code"]).startswith("D90_")
                ]
                assert len(incidents) == 1
                assert incidents[0]["incident_code"] == "D90_WATERMARK_STALE"
                assert incidents[0]["status"] == "ACTIVE"
                assert incidents[0]["severity"] == "ERROR"

                manifests = postgres_catalog.records("market_data.dataset_manifests")
                assert [row["status"] for row in manifests] == ["QUARANTINED"]

                # The block is effective for a reader that only knows the schema:
                # there is no AVAILABLE manifest left to start from.
                assert postgres_catalog.latest_available_manifest(
                    feed_id=feed_id, data_layer="RAW", resolution="30m", year=2024
                ) is None

                # And this is the join C's Java gate has to run -- plain SQL over the
                # central schema, no D code in the path.  If this query cannot see
                # the block, the block does not exist as far as C is concerned.
                with db.connect() as connection:
                    gate = connection.execute(
                        text(
                            """
                            SELECT w.last_source_event_at,
                                   w.last_ingested_at,
                                   i.incident_code,
                                   i.severity,
                                   i.status,
                                   m.status AS manifest_status
                              FROM market_data.stream_watermarks w
                              JOIN market_data.quality_incidents i
                                ON i.dataset_manifest_id IS NOT NULL
                              JOIN market_data.dataset_manifests m
                                ON m.id = i.dataset_manifest_id
                             WHERE w.feed_id = :feed_id
                               AND m.feed_id = :feed_id
                               AND i.status = 'ACTIVE'
                               AND i.severity = 'ERROR'
                            """
                        ),
                        {"feed_id": uuid.UUID(feed_id)},
                    ).mappings().all()
                assert len(gate) == 1
                assert gate[0]["incident_code"] == "D90_WATERMARK_STALE"
                assert gate[0]["manifest_status"] == "QUARANTINED"
                assert gate[0]["last_source_event_at"] == datetime(
                    2024, 1, 8, 20, 30, tzinfo=UTC
                )
                assert gate[0]["last_ingested_at"] == datetime(2024, 1, 8, 21, 1, tzinfo=UTC)
            finally:
                db.dispose()
        finally:
            remove_root(root)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
