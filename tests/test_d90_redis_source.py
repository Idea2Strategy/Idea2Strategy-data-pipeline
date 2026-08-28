"""D90 -- D consumes the Redis Stream C actually publishes.

Why this file exists
--------------------
C's market gateway publishes every market event with
``RedisMarketEventPublisher`` (Redis Streams, one Lua script, five keys).  D's
realtime ingest consumed **SQS**.  The two halves were never connected: no event
C emits could reach D, by construction, whatever either side's tests said.

The project authority's decision is that **D conforms to C**.  So this file does
not describe a format D would like; it pins the format C already emits and drives
D's new :class:`~market_pipeline_lib.redis_ingest.RedisMarketEventSource` with
**C's own Lua script**, copied verbatim out of C's Java and cross-checked against
it whenever the superproject checkout is reachable
(:class:`TestCsPublishScriptIsMirroredVerbatim`).

What is pinned, and where each fact was read
--------------------------------------------
``trading-engine/modules/market-data-adapter/src/main/java/com/idea2strategy/trading/market/``

* ``redis/RedisMarketEventPublisher.java``
    - ``:310-318``  key base is ``"{" + keyPrefix + ":market}"``; braces in the
      prefix are refused, blank prefixes are refused
    - ``:252-254``  stream key   ``<base>:events``
    - ``:256-258``  dedup set    ``<base>:seen``     (member = ``eventId``)
    - ``:260-264``  latest hash  ``<base>:latest:<instrumentId>:<EVENT_TYPE>``
    - ``:46-48``    ``SADD`` on the set is the publish gate: an ``eventId`` already
      in the set is never appended to the stream a second time
    - ``:50-64``    the thirteen ``XADD`` field names, in order
    - ``:66-92``    the latest hash only moves forward -- higher ``sequence``, or the
      same ``sequence`` at a higher ``revision``
* ``../messaging/market/MarketEventEnvelope.java`` -- the record and its invariants
* ``../messaging/market/MarketEventType.java``     -- exactly ``{QUOTE, TRADE, BAR_1M}``
* ``alpaca/MarketEventOrderingProcessor.java``     -- what C publishes and in what order
* ``redis/RedisMarketEventPublisherTest.java``     -- C's own statement of the contract;
  :class:`TestOrderingAndDedupAgainstCsKeys` mirrors its four expectations

Everything below that says "real Redis" runs against a real ``redis:7.4-alpine``
container -- the same image C's own test uses.  Nothing here is faked.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import time
import unittest
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from market_pipeline_lib.contracts import (
    DATASET_CONTRACTS,
    ET,
    RAW_1M_FEED,
    logical_dataset_id,
)
from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.realtime_ingest import (
    BarFieldMap,
    RealtimeIngestConsumer,
    RealtimeIngestError,
    RealtimeIngestor,
    RealtimeIngestSpec,
)
from market_pipeline_lib.redis_ingest import (
    C_MARKET_EVENT_TYPES,
    C_PUBLISH_KEY_ROLES,
    C_STREAM_FIELDS,
    RedisMarketEventSource,
    RedisStreamDecodeError,
    bar_updates_channel,
    decode_stream_entry,
    deduplication_key,
    latest_key,
    market_key_base,
    recent_bars_key,
    stream_key,
)
from market_pipeline_lib.warmup_gate import WarmupCoverage, WarmupReadinessGate
from market_pipeline_lib.watermarks import InMemoryWatermarkRepository, WatermarkLedger

# ======================================================================================
# C's publish script, copied verbatim from RedisMarketEventPublisher.java:25-95.
#
# D cannot run C's Java here, and a hand-written Python "equivalent" of the producer
# would only prove that D agrees with D.  So the tests below execute *C's own Lua*.
# `TestCsPublishScriptIsMirroredVerbatim` fails if this copy ever drifts from the
# Java text block it was taken from.
# ======================================================================================

C_PUBLISH_SCRIPT = """local function assert_type(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual ~= 'none' and actual ~= expected then
    return redis.error_reply('WRONGTYPE key ' .. key .. ' must be ' .. expected)
  end
end

local type_error = assert_type(KEYS[1], 'stream')
if type_error ~= nil then
  return type_error
end
type_error = assert_type(KEYS[2], 'hash')
if type_error ~= nil then
  return type_error
end
type_error = assert_type(KEYS[3], 'zset')
if type_error ~= nil then
  return type_error
end
if string.sub(ARGV[6], 1, 4) == 'BAR_' then
  type_error = assert_type(KEYS[4], 'zset')
  if type_error ~= nil then
    return type_error
  end
end

redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[18])
if redis.call('ZADD', KEYS[3], 'NX', ARGV[17], ARGV[1]) == 0 then
  return {0, '', 0}
end

local stream_id = redis.call(
  'XADD', KEYS[1], '*',
  'eventId', ARGV[1],
  'schemaVersion', ARGV[2],
  'instrumentId', ARGV[3],
  'provider', ARGV[4],
  'feed', ARGV[5],
  'eventType', ARGV[6],
  'providerEventId', ARGV[7],
  'occurredAt', ARGV[8],
  'receivedAt', ARGV[9],
  'sequence', ARGV[10],
  'revision', ARGV[11],
  'correctionOfEventId', ARGV[12],
  'values', ARGV[13])
redis.call('XTRIM', KEYS[1], 'MAXLEN', '=', tonumber(ARGV[19]))

local latest_updated = 0
if ARGV[14] == '1' then
  local stored_sequence = redis.call('HGET', KEYS[2], 'sequence')
  local stored_revision = redis.call('HGET', KEYS[2], 'revision')
  if stored_sequence == false
      or tonumber(ARGV[10]) > tonumber(stored_sequence)
      or (tonumber(ARGV[10]) == tonumber(stored_sequence)
          and tonumber(ARGV[11]) > tonumber(stored_revision)) then
    redis.call(
      'HSET', KEYS[2],
      'eventId', ARGV[1],
      'schemaVersion', ARGV[2],
      'instrumentId', ARGV[3],
      'provider', ARGV[4],
      'feed', ARGV[5],
      'eventType', ARGV[6],
      'providerEventId', ARGV[7],
      'occurredAt', ARGV[8],
      'receivedAt', ARGV[9],
      'sequence', ARGV[10],
      'revision', ARGV[11],
      'correctionOfEventId', ARGV[12],
      'values', ARGV[13],
      'streamEntryId', stream_id)
    latest_updated = 1
  end
end

if string.sub(ARGV[6], 1, 4) == 'BAR_' then
  redis.call('ZREMRANGEBYSCORE', KEYS[4], ARGV[10], ARGV[10])
  redis.call('ZADD', KEYS[4], ARGV[10], ARGV[16])
  local bar_count = redis.call('ZCARD', KEYS[4])
  local capacity = tonumber(ARGV[15])
  if bar_count > capacity then
    redis.call('ZREMRANGEBYRANK', KEYS[4], 0, bar_count - capacity - 1)
  end
end

return {1, stream_id, latest_updated}
"""

#: Where C's Java lives inside the superproject, for the drift check.
C_PUBLISHER_JAVA = Path(
    "trading-engine/modules/market-data-adapter/src/main/java/com/idea2strategy/"
    "trading/market/redis/RedisMarketEventPublisher.java"
)

# ======================================================================================
# The landing zone: C's BAR_1M against D's new 1-minute RAW contract
# ======================================================================================

RAW_1M_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "1m")]
RAW_30M_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]

#: C's own vocabulary (`AlpacaMarketEventNormalizer.java:16-17`, and `feed` is the
#: normalized Alpaca feed token), which is *not* D's `feed_code`.
SOURCE_PROVIDER = "ALPACA"
SOURCE_FEED = "SIP"
SCHEMA_VERSION = 1

INSTRUMENTS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
}
SHARD_COUNT = 2
SHARDS = ("s00-of-2", "s01-of-2")
AAPL_SHARD = "s00-of-2"
MSFT_SHARD = "s01-of-2"

#: 2026-07-06 .. 2026-07-10 is a Monday-to-Friday week lying wholly inside one month.
#: `compact` deliberately declines a week that straddles a MONTH boundary, so a week
#: chosen carelessly would make the compaction assertions below vacuous.
SESSIONS = (
    date(2026, 7, 6),
    date(2026, 7, 7),
    date(2026, 7, 8),
    date(2026, 7, 9),
    date(2026, 7, 10),
)
BARS_PER_SESSION = 5

#: `logical_dataset_id(RAW_1M_CONTRACT, 2026)`, pinned as a literal so a change to
#: the feed code, the layer or the resolution shows up here instead of silently
#: relocating every 1-minute object.
DATASET_ID_2026 = "ccb73618-0c37-5a6d-9ca0-c385f6e2bb70"

CANONICAL_1M_DAY_KEY = (
    "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_1M"
    f"/dataset={DATASET_ID_2026}/revision=1/layer=RAW/resolution=1m"
    "/granularity=DAY/partition_start=2026-07-06/partition_end=2026-07-07"
    "/shard=s00-of-2/part-00001.parquet"
)
CANONICAL_1M_WEEK_KEY = (
    "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_1M"
    f"/dataset={DATASET_ID_2026}/revision=2/layer=RAW/resolution=1m"
    "/granularity=WEEK/partition_start=2026-07-06/partition_end=2026-07-13"
    "/shard=s00-of-2/part-00001.parquet"
)

#: C's `BAR_1M` values carry no trade count or VWAP (`test_d90_c_integration.C_BAR_EVENT`,
#: taken from C's published fixture), so the map declares those columns absent rather
#: than inventing a source field for them.
BAR_1M_FIELDS = BarFieldMap(open="open", high="high", low="low", close="close", volume="volume")

# ======================================================================================
# Redis
# ======================================================================================

REDIS_IMAGE = os.environ.get("D90_REDIS_IMAGE", "redis:7.4-alpine")
REDIS_URL_ENV = "D90_REDIS_URL"

GROUP = "d-market-pipeline"
CONSUMER = "worker-1"
CLAIM_MIN_IDLE_SECONDS = 0.20


def _docker_is_available() -> bool:
    try:
        import docker
    except ImportError:  # pragma: no cover - testcontainers depends on docker
        return False
    try:
        docker.from_env().ping()
    except Exception:  # pragma: no cover - depends on the developer's machine
        return False
    return True


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A real Redis, from `D90_REDIS_URL` or a container of C's own image.

    Never a fake and never an in-process stub: consumer groups, the pending-entries
    list and `XAUTOCLAIM`-style redelivery are the behaviour under test, and a stub
    of them would only assert that the stub behaves like the stub.
    """

    override = os.environ.get(REDIS_URL_ENV)
    if override:
        yield override
        return
    if not _docker_is_available():
        pytest.skip(
            f"no real Redis: Docker is unavailable and {REDIS_URL_ENV} is unset. "
            "This suite is not meaningful against a fake."
        )
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(REDIS_IMAGE).with_exposed_ports(6379)
    with container:
        url = f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}"
        _await_redis(url)
        yield url


def _await_redis(url: str, *, timeout: float = 30.0) -> None:
    import redis as redis_py

    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        client = redis_py.Redis.from_url(url, decode_responses=True)
        try:
            client.ping()
            return
        except Exception as error:  # pragma: no cover - startup race
            last = error
            time.sleep(0.2)
        finally:
            client.close()
    raise AssertionError(f"Redis at {url} never became ready: {last}")


@pytest.fixture
def redis_client(redis_url: str) -> Iterator[Any]:
    import redis as redis_py

    client = redis_py.Redis.from_url(redis_url, decode_responses=True)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def key_prefix() -> str:
    """A fresh prefix per test, exactly as C's own test does (`prefix()`)."""

    return f"test:{uuid.uuid4()}"


# ======================================================================================
# Producing events the way C produces them
# ======================================================================================


def java_instant(moment: datetime) -> str:
    """`java.time.Instant.toString()`: UTC, `Z`, milliseconds only when non-zero."""

    utc = moment.astimezone(UTC)
    if utc.microsecond:
        return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def values_json(values: Mapping[str, Decimal]) -> str:
    """Jackson's rendering of a `Map<String, BigDecimal>`: numbers, `BigDecimal.toString()`."""

    body = ",".join(f'{json.dumps(name)}:{value}' for name, value in values.items())
    return "{" + body + "}"


class CPublisher:
    """C's `RedisMarketEventPublisher.publish`, driven through C's own Lua."""

    def __init__(self, client: Any, prefix: str) -> None:
        self._client = client
        self._prefix = prefix

    @property
    def stream_key(self) -> str:
        return stream_key(self._prefix)

    def publish(self, event: Mapping[str, Any], *, update_latest: bool = True) -> list[Any]:
        received_at_ms = int(
            datetime.fromisoformat(str(event["receivedAt"]).replace("Z", "+00:00")).timestamp()
            * 1000
        )
        keys = [
            stream_key(self._prefix),
            latest_key(self._prefix, event["instrumentId"], event["eventType"]),
            deduplication_key(self._prefix),
            recent_bars_key(self._prefix, event["instrumentId"]),
        ]
        argv = [
            event["eventId"],
            str(event["schemaVersion"]),
            event["instrumentId"],
            event["provider"],
            event["feed"],
            event["eventType"],
            event["providerEventId"],
            event["occurredAt"],
            event["receivedAt"],
            str(event["sequence"]),
            str(event["revision"]),
            event["correctionOfEventId"] or "",
            event["valuesJson"],
            "1" if update_latest else "0",
            "1000",
            self._bar_update_json(event),
            str(received_at_ms),
            str(received_at_ms - 86_400_000),
            "10000",
        ]
        return list(self._client.eval(C_PUBLISH_SCRIPT, len(keys), *keys, *argv))

    @staticmethod
    def _bar_update_json(event: Mapping[str, Any]) -> str:
        fields = [
            ("schemaVersion", event["schemaVersion"]),
            ("eventId", event["eventId"]),
            ("instrumentId", event["instrumentId"]),
            ("provider", event["provider"]),
            ("feed", event["feed"]),
            ("eventType", event["eventType"]),
            ("occurredAt", event["occurredAt"]),
            ("receivedAt", event["receivedAt"]),
            ("sequence", event["sequence"]),
            ("revision", event["revision"]),
        ]
        prefix = ",".join(
            f"{json.dumps(name)}:{json.dumps(value, separators=(',', ':'))}"
            for name, value in fields
        )
        values = str(event["valuesJson"])[1:-1]
        return "{" + prefix + ("," + values if values else "") + "}"


def c_event(
    *,
    instrument_id: str,
    symbol: str,
    bar_start: datetime,
    sequence: int,
    index: int,
    event_type: str = "BAR_1M",
    revision: int = 0,
    correction_of: str | None = None,
    event_id: str | None = None,
    values: Mapping[str, Decimal] | None = None,
) -> dict[str, Any]:
    """One event in C's wire shape, with `values` already serialized as C sends it."""

    base = Decimal("100") + Decimal(index) / 10 + (Decimal("0.5") if symbol == "MSFT" else Decimal(0))
    resolved = values or {
        "close": base + Decimal("0.5"),
        "high": base + Decimal("1"),
        "low": base - Decimal("1"),
        "open": base,
        "volume": Decimal(1000 + index),
    }
    return {
        "eventId": event_id or f"evt_{symbol.lower()}_{sequence:05d}_r{revision}",
        "schemaVersion": SCHEMA_VERSION,
        "instrumentId": instrument_id,
        "provider": SOURCE_PROVIDER,
        "feed": SOURCE_FEED,
        "eventType": event_type,
        "providerEventId": f"{symbol}-{java_instant(bar_start)}",
        "occurredAt": java_instant(bar_start),
        "receivedAt": java_instant(bar_start + timedelta(seconds=1)),
        "sequence": sequence,
        "revision": revision,
        "correctionOfEventId": correction_of,
        "valuesJson": values_json(dict(sorted(resolved.items()))),
    }


def session_events(*sessions: date, first_sequence: int = 1) -> list[dict[str, Any]]:
    """One `BAR_1M` per instrument per minute from 09:30 ET, in publish order."""

    events: list[dict[str, Any]] = []
    sequence = first_sequence
    for session in sessions or SESSIONS:
        open_at = datetime.combine(session, datetime.min.time(), ET) + timedelta(hours=9, minutes=30)
        for index in range(BARS_PER_SESSION):
            bar_start = (open_at + timedelta(minutes=index)).astimezone(UTC)
            for symbol, instrument_id in INSTRUMENTS.items():
                events.append(
                    c_event(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        bar_start=bar_start,
                        sequence=sequence,
                        index=index,
                    )
                )
                sequence += 1
    return events


def entry_fields(event: Mapping[str, Any]) -> dict[str, str]:
    """The exact flat string hash C's `XADD` writes for `event`."""

    return {
        "eventId": event["eventId"],
        "schemaVersion": str(event["schemaVersion"]),
        "instrumentId": event["instrumentId"],
        "provider": event["provider"],
        "feed": event["feed"],
        "eventType": event["eventType"],
        "providerEventId": event["providerEventId"],
        "occurredAt": event["occurredAt"],
        "receivedAt": event["receivedAt"],
        "sequence": str(event["sequence"]),
        "revision": str(event["revision"]),
        "correctionOfEventId": event["correctionOfEventId"] or "",
        "values": event["valuesJson"],
    }


# ======================================================================================
# D-side fixtures
# ======================================================================================


def temporary_root() -> str:
    return tempfile.mkdtemp()


def remove_root(path: str) -> None:
    shutil.rmtree(long_path(path), ignore_errors=True)


def build_engine(root: Path) -> MarketPipelineEngine:
    instrument_map = root / "instrument_map.csv"
    instrument_map.write_text(
        "provider_symbol,instrument_id\n"
        + "".join(f"{symbol},{identifier}\n" for symbol, identifier in INSTRUMENTS.items()),
        encoding="utf-8",
    )
    return MarketPipelineEngine(
        PipelineConfig(
            local_root=root / "objects",
            staging_root=root / "staging",
            instrument_map_path=instrument_map,
            shard_count=SHARD_COUNT,
            target_size_mib=1,
            max_size_mib=2,
        )
    )


def bar_1m_spec(*, partition_granularity: str = "DAY") -> RealtimeIngestSpec:
    return RealtimeIngestSpec(
        contract=RAW_1M_CONTRACT,
        event_type="BAR_1M",
        source_provider=SOURCE_PROVIDER,
        source_feed=SOURCE_FEED,
        source_resolution="PT1M",
        partition_granularity=partition_granularity,
        fields=BAR_1M_FIELDS,
    )


def build_ingestor(
    engine: MarketPipelineEngine,
    *,
    repository: Any = None,
    partition_granularity: str = "DAY",
) -> RealtimeIngestor:
    ledger = WatermarkLedger(
        feed_id=engine.feed_ids[RAW_1M_FEED],
        shard_keys=SHARDS,
        repository=repository or InMemoryWatermarkRepository(),
    )
    return RealtimeIngestor(
        engine, bar_1m_spec(partition_granularity=partition_granularity), ledger=ledger
    )


def build_source(
    client: Any,
    prefix: str,
    *,
    dead_letter_stream_key: str | None = "d:market:dead-letter",
    claim_min_idle_seconds: float = CLAIM_MIN_IDLE_SECONDS,
    consumer_name: str = CONSUMER,
) -> RedisMarketEventSource:
    source = RedisMarketEventSource(
        client,
        key_prefix=prefix,
        consumer_group=GROUP,
        consumer_name=consumer_name,
        dead_letter_stream_key=dead_letter_stream_key,
        claim_min_idle_seconds=claim_min_idle_seconds,
    )
    source.ensure_group()
    return source


# ======================================================================================
# 1. C's key contract  (no Redis required -- these are pure key-construction facts)
# ======================================================================================

C_INSTRUMENT_ID = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"


class TestCsKeyContract(unittest.TestCase):
    """`RedisMarketEventPublisher.java:252-264, 310-318`, character for character."""

    def test_the_key_base_is_cs_hash_tagged_base(self) -> None:
        self.assertEqual(market_key_base("i2s"), "{i2s:market}")

    def test_the_five_keys_are_cs_five_keys(self) -> None:
        self.assertEqual(stream_key("i2s"), "{i2s:market}:events")
        self.assertEqual(deduplication_key("i2s"), "{i2s:market}:seen:v2")
        self.assertEqual(
            latest_key("i2s", C_INSTRUMENT_ID, "QUOTE"),
            "{i2s:market}:latest:8a35e6b5-cf84-4f63-920d-57c1f1b95df0:QUOTE",
        )
        self.assertEqual(
            recent_bars_key("i2s", C_INSTRUMENT_ID),
            "{i2s:market}:bars:8a35e6b5-cf84-4f63-920d-57c1f1b95df0:1m",
        )
        self.assertEqual(bar_updates_channel("i2s"), "{i2s:market}:bar-updates")

    def test_the_keys_share_one_hash_slot_so_cs_lua_stays_atomic_in_cluster_mode(self) -> None:
        tag = re.compile(r"\{([^}]*)\}")
        tags = {
            tag.search(key).group(1)  # type: ignore[union-attr]
            for key in (
                stream_key("i2s"),
                deduplication_key("i2s"),
                latest_key("i2s", C_INSTRUMENT_ID, "BAR_1M"),
                recent_bars_key("i2s", C_INSTRUMENT_ID),
                bar_updates_channel("i2s"),
            )
        }
        self.assertEqual(tags, {"i2s:market"})

    def test_a_prefix_carrying_hash_tag_braces_is_refused_as_c_refuses_it(self) -> None:
        for bad in ("{i2s}", "i2s}", "{i2s"):
            with self.assertRaisesRegex(RealtimeIngestError, "brace"):
                market_key_base(bad)

    def test_a_blank_prefix_is_refused_as_c_refuses_it(self) -> None:
        for bad in ("", "   "):
            with self.assertRaisesRegex(RealtimeIngestError, "blank"):
                market_key_base(bad)

    def test_the_key_roles_are_cs_five_lua_keys_in_cs_order(self) -> None:
        """C's stream, latest, dedup, recent-bars and notification roles stay ordered."""

        self.assertEqual(C_PUBLISH_KEY_ROLES, ("stream", "hash", "zset", "zset"))

    def test_the_stream_fields_are_cs_xadd_fields_in_cs_order(self) -> None:
        self.assertEqual(
            C_STREAM_FIELDS,
            (
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
            ),
        )

    def test_cs_event_type_vocabulary_is_exactly_three_values(self) -> None:
        """`MarketEventType.java:3-7`."""

        self.assertEqual(C_MARKET_EVENT_TYPES, ("QUOTE", "TRADE", "BAR_1M"))


# ======================================================================================
# 2. Decoding C's flat string entry back into an envelope
# ======================================================================================


class TestStreamEntryDecoding(unittest.TestCase):
    def setUp(self) -> None:
        self.event = c_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            sequence=42,
            index=0,
        )

    def test_cs_strings_decode_into_the_types_the_envelope_declares(self) -> None:
        decoded = decode_stream_entry(entry_fields(self.event))

        self.assertEqual(decoded["eventId"], self.event["eventId"])
        self.assertEqual(decoded["schemaVersion"], 1)
        self.assertIsInstance(decoded["schemaVersion"], int)
        self.assertEqual(decoded["sequence"], 42)
        self.assertIsInstance(decoded["sequence"], int)
        self.assertEqual(decoded["revision"], 0)
        self.assertEqual(decoded["instrumentId"], INSTRUMENTS["AAPL"])
        self.assertEqual(decoded["provider"], "ALPACA")
        self.assertEqual(decoded["feed"], "SIP")
        self.assertEqual(decoded["eventType"], "BAR_1M")
        self.assertEqual(decoded["occurredAt"], "2026-07-06T13:30:00Z")

    def test_cs_empty_string_for_a_missing_correction_pointer_decodes_to_null(self) -> None:
        """C writes `""`, never a Redis nil (`RedisMarketEventPublisher.java:205`)."""

        decoded = decode_stream_entry(entry_fields(self.event))
        self.assertIsNone(decoded["correctionOfEventId"])

    def test_a_correction_pointer_survives_decoding(self) -> None:
        correction = c_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            sequence=42,
            index=0,
            revision=2,
            correction_of=self.event["eventId"],
        )
        decoded = decode_stream_entry(entry_fields(correction))
        self.assertEqual(decoded["revision"], 2)
        self.assertEqual(decoded["correctionOfEventId"], self.event["eventId"])

    def test_a_bigdecimal_value_is_decimal_and_never_passes_through_binary_float(self) -> None:
        """C's `values` is `Map<String, BigDecimal>`; a float round trip loses digits."""

        event = c_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            sequence=1,
            index=0,
            values={
                "open": Decimal("210.10"),
                "high": Decimal("210.25"),
                "low": Decimal("210.05"),
                "close": Decimal("210.12345678901234567890"),
                "volume": Decimal("2500"),
            },
        )
        decoded = decode_stream_entry(entry_fields(event))

        close = decoded["values"]["close"]
        self.assertIsInstance(close, Decimal)
        self.assertEqual(close, Decimal("210.12345678901234567890"))
        # The digits a `float` cannot hold are still there.
        self.assertNotEqual(close, Decimal(repr(float("210.12345678901234567890"))))

    def test_the_bar_row_quantizes_to_eight_places_with_bankers_rounding(self) -> None:
        """`precision:1.0.0`: eight places, ROUND_HALF_EVEN, at one point only.

        The two inputs are exact ties at the ninth place.  ROUND_HALF_UP would send
        them to ...79 and ...78; ROUND_HALF_EVEN sends both to ...78.
        """

        root = temporary_root()
        self.addCleanup(remove_root, root)
        ingestor = build_ingestor(build_engine(Path(root)))

        cases = (
            (0, Decimal("210.123456785"), Decimal("210.12345678")),
            (1, Decimal("210.123456775"), Decimal("210.12345678")),
        )
        for offset, raw, _ in cases:
            event = c_event(
                instrument_id=INSTRUMENTS["AAPL"],
                symbol="AAPL",
                bar_start=datetime(2026, 7, 6, 13, 30 + offset, tzinfo=UTC),
                sequence=1 + offset,
                index=offset,
                values={
                    "open": Decimal("210"),
                    "high": Decimal("300"),
                    "low": Decimal("100"),
                    "close": raw,
                    "volume": Decimal("2500"),
                },
            )
            decision = ingestor.submit(decode_stream_entry(entry_fields(event)))
            self.assertTrue(decision.accepted, decision.reason)

        result = ingestor.flush()
        self.assertEqual(result.status, "AVAILABLE")
        table = pq.read_table(
            Path(long_path(str(Path(root) / "objects" / CANONICAL_1M_DAY_KEY)))
        )
        self.assertEqual(
            table.column("close").to_pylist(),
            [float(expected) for _, _, expected in cases],
        )

    def test_a_whole_number_volume_arrives_as_an_integer_column(self) -> None:
        decoded = decode_stream_entry(entry_fields(self.event))
        self.assertEqual(decoded["values"]["volume"], Decimal("1000"))

    def test_a_fractional_volume_is_refused_rather_than_truncated(self) -> None:
        root = temporary_root()
        self.addCleanup(remove_root, root)
        ingestor = build_ingestor(build_engine(Path(root)))
        event = c_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            sequence=1,
            index=0,
            values={
                "open": Decimal("210"),
                "high": Decimal("300"),
                "low": Decimal("100"),
                "close": Decimal("210.5"),
                "volume": Decimal("2500.5"),
            },
        )
        with self.assertRaisesRegex(RealtimeIngestError, "volume"):
            ingestor.submit(decode_stream_entry(entry_fields(event)))

    def test_value_key_order_does_not_matter(self) -> None:
        """`Map.copyOf` in `MarketEventEnvelope` does not preserve iteration order."""

        forward = entry_fields(self.event)
        reversed_json = values_json(
            dict(reversed(list(json.loads(forward["values"], parse_float=Decimal).items())))
        )
        self.assertNotEqual(reversed_json, forward["values"])
        self.assertEqual(
            decode_stream_entry(forward)["values"],
            decode_stream_entry({**forward, "values": reversed_json})["values"],
        )

    def test_an_entry_missing_a_field_c_always_writes_is_refused_by_name(self) -> None:
        fields = entry_fields(self.event)
        del fields["occurredAt"]
        with self.assertRaisesRegex(RedisStreamDecodeError, "occurredAt"):
            decode_stream_entry(fields)

    def test_a_non_numeric_sequence_is_refused(self) -> None:
        fields = {**entry_fields(self.event), "sequence": "not-a-number"}
        with self.assertRaisesRegex(RedisStreamDecodeError, "sequence"):
            decode_stream_entry(fields)

    def test_values_that_are_not_valid_json_are_refused(self) -> None:
        fields = {**entry_fields(self.event), "values": "{oops"}
        with self.assertRaisesRegex(RedisStreamDecodeError, "values"):
            decode_stream_entry(fields)

    def test_a_non_numeric_value_is_refused_because_c_only_sends_bigdecimal(self) -> None:
        fields = {**entry_fields(self.event), "values": '{"close":"210.12"}'}
        with self.assertRaisesRegex(RedisStreamDecodeError, "close"):
            decode_stream_entry(fields)


# ======================================================================================
# 3. The copy of C's Lua is a copy
# ======================================================================================


def _superproject_root() -> Path | None:
    from tests.conftest import superproject_root

    return superproject_root()


def _java_text_block(source: str, field: str) -> str:
    """Extract and un-indent a Java text block, per JLS 3.10.6."""

    marker = f'{field} = """\n'
    start = source.index(marker) + len(marker)
    end = source.index('""";', start)
    raw = source[start:end]
    lines = raw.split("\n")
    content, closing = lines[:-1], lines[-1]
    measured = [line for line in content if line.strip()] + [closing]
    indent = min(len(line) - len(line.lstrip(" ")) for line in measured)
    return "".join(f"{line[indent:].rstrip()}\n" if line.strip() else "\n" for line in content)


class TestCsPublishScriptIsMirroredVerbatim(unittest.TestCase):
    def test_the_embedded_lua_is_byte_for_byte_cs_java_text_block(self) -> None:
        root = _superproject_root()
        if root is None:
            self.skipTest(
                "the superproject checkout is not reachable, so C's Java cannot be "
                "cross-checked from here; the behavioural tests still run against the "
                "embedded copy"
            )
        java = root / C_PUBLISHER_JAVA
        if not java.is_file():
            self.skipTest(f"{java} is not present in this checkout")
        self.assertEqual(
            _java_text_block(java.read_text(encoding="utf-8"), "PUBLISH_SCRIPT"),
            C_PUBLISH_SCRIPT,
        )


# ======================================================================================
# 4. QUOTE and TRADE:  ignored, with a reason, and acknowledged
# ======================================================================================


class TestQuoteAndTradeAreIgnoredWithAReason(unittest.TestCase):
    """C multiplexes QUOTE, TRADE and BAR_1M onto one stream (`MarketEventType.java`).

    A bar ingest cannot turn a quote into an OHLCV row -- a quote has no open, high,
    low or volume.  D therefore *reports* the two non-bar types under their own reason
    and acknowledges them.  Dropping them silently would leave an operator unable to
    tell "C sent 40k quotes we correctly ignored" from "C sent nothing"; parking them
    would fill the dead-letter stream during ordinary trading.
    """

    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.ingestor = build_ingestor(build_engine(Path(self.root)))

    def _submit(self, event_type: str) -> Any:
        event = c_event(
            instrument_id=INSTRUMENTS["AAPL"],
            symbol="AAPL",
            bar_start=datetime(2026, 7, 6, 13, 30, tzinfo=UTC),
            sequence=1,
            index=0,
            event_type=event_type,
            values={"askPrice": Decimal("210.12"), "bidPrice": Decimal("210.10")},
        )
        return self.ingestor.submit(decode_stream_entry(entry_fields(event)))

    def test_a_quote_is_reported_under_the_non_bar_reason_and_buffers_nothing(self) -> None:
        decision = self._submit("QUOTE")
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "NON_BAR_EVENT_TYPE")
        self.assertEqual(self.ingestor.pending_rows, 0)

    def test_a_trade_is_reported_under_the_non_bar_reason_and_buffers_nothing(self) -> None:
        decision = self._submit("TRADE")
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "NON_BAR_EVENT_TYPE")
        self.assertEqual(self.ingestor.pending_rows, 0)

    def test_a_bar_of_another_cadence_gets_the_routing_reason_not_the_non_bar_one(self) -> None:
        """The two are different faults and must not share one code.

        A `QUOTE` on a bar stream is normal.  A `BAR_30M` on the 1-minute stream is a
        misrouted stream, and an operator has to be able to tell them apart.
        """

        decision = self._submit("BAR_30M")
        self.assertFalse(decision.accepted)
        self.assertEqual(decision.reason, "EVENT_TYPE_NOT_INGESTED")

    def test_a_quote_carrying_none_of_the_bar_fields_is_still_not_an_error(self) -> None:
        """C's real QUOTE payload has bid/ask only -- no `open`, no `volume`.

        The field map must never be applied to a non-bar event; if it were, every
        quote would raise "values is missing the mapped field 'open'" and the stream
        would park itself into the dead-letter queue within a second of market open.
        """

        decision = self._submit("QUOTE")
        self.assertEqual(decision.reason, "NON_BAR_EVENT_TYPE")
        self.assertEqual(self.ingestor.flush().status, "NO_CHANGE")


# ======================================================================================
# 5. The 1-minute RAW landing zone
# ======================================================================================


class TestOneMinuteRawLandingZone(unittest.TestCase):
    def setUp(self) -> None:
        self.root = temporary_root()
        self.addCleanup(remove_root, self.root)
        self.engine = build_engine(Path(self.root))

    def test_the_one_minute_raw_contract_exists_and_is_not_derived(self) -> None:
        self.assertEqual(RAW_1M_CONTRACT.resolution, "1m")
        self.assertEqual(RAW_1M_CONTRACT.data_layer, "RAW")
        self.assertEqual(RAW_1M_CONTRACT.feed_code, "ALPACA_SIP_RAW_1M")
        self.assertFalse(RAW_1M_CONTRACT.has_source_minutes)
        self.assertEqual(RAW_1M_CONTRACT.logical_code, "ALPACA_SIP_RAW_1M:RAW:1m")

    def test_the_one_minute_feed_is_registered_with_its_own_id_and_resolution(self) -> None:
        feed_id = self.engine.feed_ids[RAW_1M_FEED]
        self.assertEqual(feed_id, "6dfe5595-67eb-5dad-9864-9d4ab32043c1")
        rows = [
            row
            for row in self.engine.catalog.records("market_data.feeds")
            if row["code"] == RAW_1M_FEED
        ]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["resolution"], "1m")
        self.assertEqual(rows[0]["id"], feed_id)

    def test_the_dataset_id_is_the_pinned_deterministic_id(self) -> None:
        self.assertEqual(logical_dataset_id(RAW_1M_CONTRACT, 2026), DATASET_ID_2026)

    def test_pt1m_bound_to_the_thirty_minute_contract_still_fails_closed(self) -> None:
        """Adding a 1-minute contract must not make mislabelling possible."""

        with self.assertRaisesRegex(RealtimeIngestError, "PT30M"):
            RealtimeIngestSpec(
                contract=RAW_30M_CONTRACT,
                event_type="BAR_1M",
                source_provider=SOURCE_PROVIDER,
                source_feed=SOURCE_FEED,
                source_resolution="PT1M",
                partition_granularity="DAY",
                fields=BAR_1M_FIELDS,
            )

    def test_pt30m_bound_to_the_one_minute_contract_fails_closed_too(self) -> None:
        with self.assertRaisesRegex(RealtimeIngestError, "PT1M"):
            RealtimeIngestSpec(
                contract=RAW_1M_CONTRACT,
                event_type="BAR_1M",
                source_provider=SOURCE_PROVIDER,
                source_feed=SOURCE_FEED,
                source_resolution="PT30M",
                partition_granularity="DAY",
                fields=BAR_1M_FIELDS,
            )

    def test_bar_1m_lands_under_the_canonical_key_and_compacts_into_a_week(self) -> None:
        ingestor = build_ingestor(self.engine)
        for event in session_events():
            self.assertTrue(ingestor.submit(decode_stream_entry(entry_fields(event))).accepted)
        result = ingestor.flush()

        self.assertEqual(result.status, "AVAILABLE")
        self.assertEqual(result.row_count, len(SESSIONS) * BARS_PER_SESSION * len(INSTRUMENTS))
        self.assertIn(CANONICAL_1M_DAY_KEY, result.object_keys)
        self.assertEqual(len(result.object_keys), len(SESSIONS) * len(SHARDS))

        compacted = self.engine.compact(
            RAW_1M_CONTRACT, granularity="WEEK", period=date(2026, 7, 6)
        )
        self.assertEqual(compacted["status"], "SUCCEEDED")
        self.assertEqual(compacted["manifest"]["status"], "AVAILABLE")
        self.assertEqual(compacted["new_object_count"], len(SHARDS))
        self.assertEqual(compacted["retained_object_count"], 0)
        week = Path(long_path(str(Path(self.root) / "objects" / CANONICAL_1M_WEEK_KEY)))
        self.assertTrue(week.is_file(), f"compaction did not write {CANONICAL_1M_WEEK_KEY}")
        self.assertEqual(pq.read_table(week).num_rows, len(SESSIONS) * BARS_PER_SESSION)

    def test_one_minute_bars_do_not_land_in_the_thirty_minute_dataset(self) -> None:
        ingestor = build_ingestor(self.engine)
        for event in session_events(SESSIONS[0]):
            ingestor.submit(decode_stream_entry(entry_fields(event)))
        keys = ingestor.flush().object_keys

        self.assertTrue(keys)
        self.assertTrue(all("/resolution=1m/" in key for key in keys), keys)
        self.assertFalse(any("RAW_30M" in key for key in keys), keys)


# ======================================================================================
# 6. Real Redis: consumer-group semantics
# ======================================================================================


@pytest.mark.integration
class TestConsumerGroupSemantics:
    def test_creating_the_group_twice_reports_that_it_already_existed(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        source = RedisMarketEventSource(
            redis_client,
            key_prefix=key_prefix,
            consumer_group=GROUP,
            consumer_name=CONSUMER,
            dead_letter_stream_key="d:market:dead-letter",
            claim_min_idle_seconds=CLAIM_MIN_IDLE_SECONDS,
        )
        assert source.ensure_group() is True
        assert source.ensure_group() is False

    def test_the_group_reads_from_the_start_so_events_published_first_are_not_lost(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """C publishes before D's group exists; those entries must still arrive."""

        publisher = CPublisher(redis_client, key_prefix)
        events = session_events(SESSIONS[0])[:4]
        for event in events:
            publisher.publish(event)

        source = build_source(redis_client, key_prefix)
        deliveries = source.poll(max_messages=10, wait_seconds=0.1)

        assert [d.body["events"][0]["eventId"] for d in deliveries] == [
            event["eventId"] for event in events
        ]

    def test_two_consumers_in_one_group_split_the_stream_and_never_share_an_entry(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        events = session_events(SESSIONS[0])[:6]
        for event in events:
            publisher.publish(event)

        first = build_source(redis_client, key_prefix, consumer_name="worker-1")
        second = build_source(redis_client, key_prefix, consumer_name="worker-2")
        left = first.poll(max_messages=3, wait_seconds=0.1)
        right = second.poll(max_messages=3, wait_seconds=0.1)

        left_ids = [d.message_id for d in left]
        right_ids = [d.message_id for d in right]
        assert len(left_ids) == 3
        assert len(right_ids) == 3
        assert set(left_ids).isdisjoint(right_ids)
        assert sorted(left_ids + right_ids) == sorted(
            entry[0] for entry in redis_client.xrange(stream_key(key_prefix))
        )

    def test_a_second_group_gets_its_own_copy_because_cs_stream_has_other_readers(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        for event in session_events(SESSIONS[0])[:3]:
            publisher.publish(event)

        pipeline = build_source(redis_client, key_prefix)
        consumed = pipeline.poll(max_messages=10, wait_seconds=0.1)
        for delivery in consumed:
            pipeline.acknowledge(delivery)

        other = RedisMarketEventSource(
            redis_client,
            key_prefix=key_prefix,
            consumer_group="trading-workers",
            consumer_name="c-1",
            dead_letter_stream_key=None,
            claim_min_idle_seconds=CLAIM_MIN_IDLE_SECONDS,
        )
        other.ensure_group()

        assert len(other.poll(max_messages=10, wait_seconds=0.1)) == 3

    def test_acknowledge_clears_the_pending_entry_and_leaves_cs_stream_intact(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        for event in session_events(SESSIONS[0])[:2]:
            publisher.publish(event)
        source = build_source(redis_client, key_prefix)

        deliveries = source.poll(max_messages=10, wait_seconds=0.1)
        assert source.pending_count() == 2
        for delivery in deliveries:
            source.acknowledge(delivery)

        assert source.pending_count() == 0
        # Acknowledging must never XDEL: another group still has to read these.
        assert redis_client.xlen(stream_key(key_prefix)) == 2


# ======================================================================================
# 7. Real Redis: at-least-once redelivery and idempotent reprocessing
# ======================================================================================


@pytest.mark.integration
class TestAtLeastOnceRedelivery:
    def test_an_unacknowledged_entry_comes_back_with_a_higher_delivery_count(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        publisher.publish(event)
        source = build_source(redis_client, key_prefix)

        first = source.poll(max_messages=10, wait_seconds=0.1)
        assert [d.receive_count for d in first] == [1]

        time.sleep(CLAIM_MIN_IDLE_SECONDS * 1.5)
        second = source.poll(max_messages=10, wait_seconds=0.1)

        assert [d.message_id for d in second] == [d.message_id for d in first]
        assert [d.receive_count for d in second] == [2]
        assert second[0].body["events"][0]["eventId"] == event["eventId"]

    def test_a_stalled_consumer_entry_is_reclaimed_by_a_sibling_consumer(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        publisher.publish(event)

        stalled = build_source(redis_client, key_prefix, consumer_name="worker-dead")
        taken = stalled.poll(max_messages=10, wait_seconds=0.1)
        assert len(taken) == 1

        time.sleep(CLAIM_MIN_IDLE_SECONDS * 1.5)
        survivor = build_source(redis_client, key_prefix, consumer_name="worker-alive")
        reclaimed = survivor.poll(max_messages=10, wait_seconds=0.1)

        assert [d.message_id for d in reclaimed] == [d.message_id for d in taken]
        assert reclaimed[0].receive_count == 2

    def test_retry_later_makes_the_entry_reclaimable_only_after_the_requested_delay(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        publisher.publish(session_events(SESSIONS[0])[0])
        source = build_source(redis_client, key_prefix, claim_min_idle_seconds=1.0)

        delivery = source.poll(max_messages=10, wait_seconds=0.1)[0]
        source.retry_later(delivery, delay_seconds=0.5)

        assert source.poll(max_messages=10, wait_seconds=0.05) == []
        time.sleep(0.7)
        assert [d.message_id for d in source.poll(max_messages=10, wait_seconds=0.05)] == [
            delivery.message_id
        ]

    def test_a_delay_longer_than_the_reclaim_threshold_is_refused_not_silently_shortened(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        publisher.publish(session_events(SESSIONS[0])[0])
        source = build_source(redis_client, key_prefix, claim_min_idle_seconds=1.0)
        delivery = source.poll(max_messages=10, wait_seconds=0.1)[0]

        with pytest.raises(RealtimeIngestError, match="claim_min_idle_seconds"):
            source.retry_later(delivery, delay_seconds=5.0)

    def test_a_redelivered_event_produces_no_duplicate_bar_and_no_second_object(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """At-least-once delivery, exactly-once effect.

        The engine is driven twice over the same entries; the second pass must be
        absorbed by C's content-addressed `eventId`, not written again.
        """

        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            publisher = CPublisher(redis_client, key_prefix)
            events = session_events(SESSIONS[0])
            for event in events:
                publisher.publish(event)

            source = build_source(redis_client, key_prefix)
            consumer = RealtimeIngestConsumer(
                ingestor=ingestor, source=source, max_receive_count=3, flush_every=10_000
            )
            # First pass: consume without acknowledging, by never letting the loop ack.
            first = source.poll(max_messages=10, wait_seconds=0.1)
            for delivery in first:
                ingestor.submit_batch(delivery.body["events"])
            accepted_first = ingestor.pending_rows

            time.sleep(CLAIM_MIN_IDLE_SECONDS * 1.5)
            report = consumer.drain(max_empty_cycles=2, wait_seconds=0.1)

            assert report.received >= len(first)
            assert report.accepted == 0, "a redelivery must not be accepted a second time"
            assert report.skipped == report.received
            assert ingestor.pending_rows == accepted_first

            result = ingestor.flush()
            assert result.row_count == BARS_PER_SESSION * len(INSTRUMENTS)
            assert len(result.object_keys) == len(SHARDS)
            assert source.pending_count() == 0
        finally:
            remove_root(root)


# ======================================================================================
# 8. Real Redis: dead-lettering
# ======================================================================================


@pytest.mark.integration
class TestDeadLettering:
    def test_an_entry_is_parked_after_the_configured_number_of_deliveries(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        dead_letter = f"d:{key_prefix}:dead-letter"
        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        publisher.publish(event)
        source = build_source(redis_client, key_prefix, dead_letter_stream_key=dead_letter)
        consumer = RealtimeIngestConsumer(
            ingestor=_AlwaysFailingIngestor(),
            source=source,
            max_receive_count=2,
            retry_delay_seconds=0.0,
            flush_every=10_000,
        )

        first = consumer.run_once(wait_seconds=0.1)
        assert (first.received, first.retried, first.dead_lettered) == (1, 1, 0)

        time.sleep(CLAIM_MIN_IDLE_SECONDS * 1.5)
        second = consumer.run_once(wait_seconds=0.1)
        assert (second.received, second.retried, second.dead_lettered) == (1, 0, 1)

        parked = redis_client.xrange(dead_letter)
        assert len(parked) == 1
        fields = parked[0][1]
        assert fields["eventId"] == event["eventId"]
        assert fields["deadLetterReason"] == "MAX_RECEIVES_EXCEEDED"
        assert fields["sourceStream"] == stream_key(key_prefix)
        assert fields["deliveryCount"] == "2"
        # Parking acknowledges on C's stream but never deletes from it.
        assert source.pending_count() == 0
        assert redis_client.xlen(stream_key(key_prefix)) == 1

    def test_a_parked_entry_keeps_every_field_c_wrote_so_it_can_be_replayed(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        dead_letter = f"d:{key_prefix}:dead-letter"
        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        publisher.publish(event)
        source = build_source(redis_client, key_prefix, dead_letter_stream_key=dead_letter)
        delivery = source.poll(max_messages=10, wait_seconds=0.1)[0]

        source.dead_letter(delivery, reason="MALFORMED_EVENT")

        fields = redis_client.xrange(dead_letter)[0][1]
        assert {name: fields[name] for name in C_STREAM_FIELDS} == entry_fields(event)

    def test_a_source_without_a_dead_letter_stream_refuses_to_park(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        publisher.publish(session_events(SESSIONS[0])[0])
        source = build_source(redis_client, key_prefix, dead_letter_stream_key=None)
        delivery = source.poll(max_messages=10, wait_seconds=0.1)[0]

        with pytest.raises(RealtimeIngestError, match="dead-letter"):
            source.dead_letter(delivery, reason="MALFORMED_EVENT")
        assert source.pending_count() == 1

    def test_an_entry_c_never_wrote_is_parked_rather_than_crashing_the_loop(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """Anything can XADD to a stream; a consumer must survive it."""

        dead_letter = f"d:{key_prefix}:dead-letter"
        redis_client.xadd(stream_key(key_prefix), {"hello": "world"})
        source = build_source(redis_client, key_prefix, dead_letter_stream_key=dead_letter)
        consumer = RealtimeIngestConsumer(
            ingestor=_AlwaysFailingIngestor(),
            source=source,
            max_receive_count=5,
            flush_every=10_000,
        )

        cycle = consumer.run_once(wait_seconds=0.1)

        assert (cycle.received, cycle.dead_lettered, cycle.retried) == (1, 1, 0)
        parked = redis_client.xrange(dead_letter)
        assert len(parked) == 1
        assert parked[0][1]["deadLetterReason"] == "MALFORMED_EVENT"


class _AlwaysFailingIngestor:
    """Fails every batch, so the retry and park policy is what is under test."""

    def submit_batch(self, events: Any) -> Any:
        raise RuntimeError("the ingestor is down")

    def flush(self) -> Any:  # pragma: no cover - never reached, nothing is accepted
        raise AssertionError("flush must not be called when nothing was accepted")


# ======================================================================================
# 9. Real Redis: ordering and de-duplication against C's own set and hash
# ======================================================================================


@pytest.mark.integration
class TestOrderingAndDedupAgainstCsKeys:
    """Mirrors `RedisMarketEventPublisherTest` and then consumes what it produced."""

    def test_publishing_one_event_twice_appends_one_entry_because_of_cs_set(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """`RedisMarketEventPublisherTest.publishesOnceAndAdvancesLatest...`."""

        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]

        first = publisher.publish(event)
        duplicate = publisher.publish(event)

        assert first[0] == 1
        assert first[2] == 1
        assert duplicate[0] == 0
        assert redis_client.xlen(stream_key(key_prefix)) == 1
        assert redis_client.zcard(deduplication_key(key_prefix)) == 1

        source = build_source(redis_client, key_prefix)
        deliveries = source.poll(max_messages=10, wait_seconds=0.1)
        assert len(deliveries) == 1

    def test_the_source_can_tell_whether_c_published_an_event_id(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        publisher.publish(event)
        source = build_source(redis_client, key_prefix)

        assert source.producer_published(event["eventId"]) is True
        assert source.producer_published("evt_never_published") is False

    def test_a_historical_correction_never_moves_cs_latest_observation_backward(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """`RedisMarketEventPublisherTest.publishesHistoricalCorrection...`."""

        publisher = CPublisher(redis_client, key_prefix)
        bar_start = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)
        older = c_event(
            instrument_id=INSTRUMENTS["AAPL"], symbol="AAPL", bar_start=bar_start,
            sequence=41, index=0,
        )
        newest = c_event(
            instrument_id=INSTRUMENTS["AAPL"], symbol="AAPL",
            bar_start=bar_start + timedelta(minutes=1), sequence=42, index=1,
        )
        correction = c_event(
            instrument_id=INSTRUMENTS["AAPL"], symbol="AAPL", bar_start=bar_start,
            sequence=41, index=0, revision=1, correction_of=older["eventId"],
            event_id="evt_aapl_correction_r1",
        )

        publisher.publish(older)
        publisher.publish(newest)
        # C's ordering processor reports `shouldUpdateLatestValue=false` for a
        # correction to a bar that is no longer the head.
        result = publisher.publish(correction, update_latest=False)

        assert result[0] == 1
        assert result[2] == 0
        assert redis_client.xlen(stream_key(key_prefix)) == 3
        latest = redis_client.hgetall(latest_key(key_prefix, INSTRUMENTS["AAPL"], "BAR_1M"))
        assert latest["eventId"] == newest["eventId"]
        assert latest["sequence"] == "42"

        source = build_source(redis_client, key_prefix)
        observed = source.latest_observation(INSTRUMENTS["AAPL"], "BAR_1M")
        assert observed is not None
        assert observed["eventId"] == newest["eventId"]
        assert observed["sequence"] == 42
        assert source.latest_observation(INSTRUMENTS["MSFT"], "BAR_1M") is None

    def test_a_wrong_type_key_aborts_cs_publish_before_anything_is_written(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """`RedisMarketEventPublisherTest.rejectsWrongTypeBefore...`."""

        import redis as redis_py

        publisher = CPublisher(redis_client, key_prefix)
        event = session_events(SESSIONS[0])[0]
        redis_client.set(latest_key(key_prefix, event["instrumentId"], "BAR_1M"), "wrong-type")

        with pytest.raises(redis_py.exceptions.ResponseError, match="WRONGTYPE"):
            publisher.publish(event)

        assert redis_client.xlen(stream_key(key_prefix)) == 0
        assert redis_client.zcard(deduplication_key(key_prefix)) == 0

    def test_events_are_consumed_in_cs_publish_order_across_several_polls(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        publisher = CPublisher(redis_client, key_prefix)
        events = session_events(SESSIONS[0])
        for event in events:
            publisher.publish(event)
        source = build_source(redis_client, key_prefix)

        consumed: list[str] = []
        while True:
            deliveries = source.poll(max_messages=3, wait_seconds=0.05)
            if not deliveries:
                break
            for delivery in deliveries:
                consumed.append(delivery.body["events"][0]["eventId"])
                source.acknowledge(delivery)

        assert consumed == [event["eventId"] for event in events]

    def test_the_published_bars_are_written_in_bar_order_whatever_the_poll_size(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            publisher = CPublisher(redis_client, key_prefix)
            for event in session_events(SESSIONS[0]):
                publisher.publish(event)

            source = build_source(redis_client, key_prefix)
            consumer = RealtimeIngestConsumer(
                ingestor=ingestor,
                source=source,
                max_receive_count=3,
                max_messages_per_poll=3,
                flush_every=10_000,
            )
            consumer.drain(max_empty_cycles=2, wait_seconds=0.05)
            result = ingestor.flush()

            assert result.status == "AVAILABLE"
            table = pq.read_table(
                Path(long_path(str(Path(root) / "objects" / CANONICAL_1M_DAY_KEY)))
            )
            starts = table.column("bar_start_at").to_pylist()
            assert starts == [
                datetime(2026, 7, 6, 13, 30, tzinfo=UTC) + timedelta(minutes=index)
                for index in range(BARS_PER_SESSION)
            ]
        finally:
            remove_root(root)


# ======================================================================================
# 10. Real Redis: graceful shutdown
# ======================================================================================


@pytest.mark.integration
class TestGracefulShutdown:
    def test_shutdown_publishes_everything_that_was_acknowledged(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """An acknowledged event that is still only buffered is data at risk.

        The loop acknowledges a message as soon as the ingestor accepts it, and the
        ingestor holds the bar in memory until a flush.  A consumer that stopped
        between those two points would have told Redis "done" about rows nobody ever
        wrote.  `shutdown` is what closes that window.
        """

        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            publisher = CPublisher(redis_client, key_prefix)
            for event in session_events(SESSIONS[0]):
                publisher.publish(event)

            source = build_source(redis_client, key_prefix)
            consumer = RealtimeIngestConsumer(
                ingestor=ingestor, source=source, max_receive_count=3, flush_every=10_000
            )
            consumer.drain(max_empty_cycles=2, wait_seconds=0.05)

            # Everything is acknowledged, and nothing is on disk yet.
            assert source.pending_count() == 0
            assert ingestor.pending_rows == BARS_PER_SESSION * len(INSTRUMENTS)
            assert not Path(long_path(str(Path(root) / "objects" / CANONICAL_1M_DAY_KEY))).exists()

            flushed = consumer.shutdown()

            assert flushed.status == "AVAILABLE"
            assert flushed.row_count == BARS_PER_SESSION * len(INSTRUMENTS)
            assert ingestor.pending_rows == 0
            assert Path(long_path(str(Path(root) / "objects" / CANONICAL_1M_DAY_KEY))).is_file()
        finally:
            remove_root(root)

    def test_shutdown_on_an_idle_consumer_reports_no_change_rather_than_success(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        root = temporary_root()
        try:
            ingestor = build_ingestor(build_engine(Path(root)))
            source = build_source(redis_client, key_prefix)
            consumer = RealtimeIngestConsumer(
                ingestor=ingestor, source=source, max_receive_count=3
            )
            assert consumer.shutdown().status == "NO_CHANGE"
        finally:
            remove_root(root)

    def test_the_loop_stops_when_asked_and_flushes_before_returning(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        root = temporary_root()
        try:
            engine = build_engine(Path(root))
            ingestor = build_ingestor(engine)
            publisher = CPublisher(redis_client, key_prefix)
            events = session_events(SESSIONS[0])
            for event in events:
                publisher.publish(event)

            source = build_source(redis_client, key_prefix)
            consumer = RealtimeIngestConsumer(
                ingestor=ingestor, source=source, max_receive_count=3, flush_every=10_000
            )
            stop = threading.Event()
            outcome: dict[str, Any] = {}

            def run() -> None:
                drain, flush = consumer.run_until_stopped(stop.is_set, wait_seconds=0.05)
                outcome["drain"] = drain
                outcome["flush"] = flush

            worker = threading.Thread(target=run, daemon=True)
            worker.start()
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline and source.pending_count() + 0 == 0:
                if ingestor.pending_rows >= len(events):
                    break
                time.sleep(0.05)
            stop.set()
            worker.join(timeout=20.0)

            assert not worker.is_alive(), "run_until_stopped did not return after the stop signal"
            assert outcome["drain"].acknowledged == len(events)
            assert outcome["flush"].status == "AVAILABLE"
            assert outcome["flush"].row_count == len(events)
            assert ingestor.pending_rows == 0
            assert source.pending_count() == 0
        finally:
            remove_root(root)


# ======================================================================================
# 11. Real Redis: the warm-up gate still fails closed on what Redis did not deliver
# ======================================================================================


@pytest.mark.integration
class TestWarmupGateStillFailsClosedOnTheRedisPath:
    def _coverage(self, shards: tuple[str, ...]) -> WarmupCoverage:
        return WarmupCoverage(
            contract=RAW_1M_CONTRACT,
            session=SESSIONS[0],
            granularity="DAY",
            required_shards=shards,
            # 09:34 ET, the last minute this test's session publishes.
            required_watermark_at=datetime(2026, 7, 6, 13, 34, tzinfo=UTC),
        )

    def _run(
        self, redis_client: Any, key_prefix: str, root: str, *, only_aapl: bool
    ) -> tuple[MarketPipelineEngine, Any, Any]:
        engine = build_engine(Path(root))
        repository = InMemoryWatermarkRepository()
        ingestor = build_ingestor(engine, repository=repository)
        publisher = CPublisher(redis_client, key_prefix)
        for event in session_events(SESSIONS[0]):
            if only_aapl and event["instrumentId"] != INSTRUMENTS["AAPL"]:
                continue
            publisher.publish(event)

        source = build_source(redis_client, key_prefix)
        consumer = RealtimeIngestConsumer(
            ingestor=ingestor, source=source, max_receive_count=3, flush_every=10_000
        )
        consumer.drain(max_empty_cycles=2, wait_seconds=0.05)
        consumer.shutdown()
        return engine, repository, ingestor

    def test_a_session_the_redis_path_fully_delivered_is_ready(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        root = temporary_root()
        try:
            engine, repository, _ = self._run(redis_client, key_prefix, root, only_aapl=False)
            gate = WarmupReadinessGate(
                engine.catalog,
                feed_id=engine.feed_ids[RAW_1M_FEED],
                watermarks=repository,
                freshness_budget=timedelta(days=3650),
                now=lambda: datetime(2026, 7, 6, 21, 0, tzinfo=UTC),
            )
            readiness = gate.evaluate(self._coverage(SHARDS))
            assert readiness.reason_code is None, readiness.detail
            assert readiness.state == "READY"

        finally:
            remove_root(root)

    def test_a_shard_redis_never_delivered_blocks_the_start(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        root = temporary_root()
        try:
            engine, repository, _ = self._run(redis_client, key_prefix, root, only_aapl=True)
            gate = WarmupReadinessGate(
                engine.catalog,
                feed_id=engine.feed_ids[RAW_1M_FEED],
                watermarks=repository,
                freshness_budget=timedelta(days=3650),
                now=lambda: datetime(2026, 7, 6, 21, 0, tzinfo=UTC),
            )

            readiness = gate.evaluate(self._coverage(SHARDS))

            assert readiness.blocked
            assert readiness.reason_code == "D90_DAILY_OBJECT_MISSING"
            assert MSFT_SHARD in (readiness.detail or "")
            incident_id = gate.record(readiness, self._coverage(SHARDS))
            assert incident_id is not None
            statuses = {
                row["status"]
                for row in engine.catalog.records("market_data.dataset_manifests")
            }
            assert statuses == {"QUARANTINED"}
        finally:
            remove_root(root)

    def test_a_feed_that_never_ingested_anything_blocks_on_the_missing_watermark(
        self, redis_client: Any, key_prefix: str
    ) -> None:
        """The Redis path being wired up does not make an empty feed ready."""

        root = temporary_root()
        try:
            engine, _, _ = self._run(redis_client, key_prefix, root, only_aapl=False)
            gate = WarmupReadinessGate(
                engine.catalog,
                feed_id=engine.feed_ids[RAW_1M_FEED],
                watermarks=InMemoryWatermarkRepository(),
                freshness_budget=timedelta(days=3650),
                now=lambda: datetime(2026, 7, 6, 21, 0, tzinfo=UTC),
            )

            readiness = gate.evaluate(self._coverage(SHARDS))

            assert readiness.blocked
            assert readiness.reason_code == "D90_WATERMARK_MISSING"
        finally:
            remove_root(root)
