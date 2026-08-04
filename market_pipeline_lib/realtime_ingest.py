"""D12/D90 -- realtime market-event ingestion as a genuine queue consumer.

What this replaces
------------------
`realtime_warmup.py` used to be the whole "realtime" path: a function that read a
JSON fixture off disk, hardcoded ``PT1M`` and ``close``, and wrote objects under
``warmup/session_date_et=.../…`` -- a key of its own invention.  Nothing consumed
it, and because the key was not the canonical one from
:func:`market_pipeline_lib.contracts.object_key`, an object it produced could
never be found by ``MarketPipelineEngine.compact``.  Realtime data was a dead
end by construction.

What this is
------------
Three separable pieces, so none of them has to know about the others' transport:

``SqsEventSource``
    The adapter.  Long-poll receive, an explicit visibility timeout, the SQS
    receive count carried through on every delivery, and an explicit dead-letter
    hop (send to the DLQ, then delete from the source) so parking a message does
    not depend on a redrive policy being configured on the queue.

``RealtimeIngestor``
    The domain.  Validates a provider-neutral market event, asks the D11
    :class:`~market_pipeline_lib.watermarks.WatermarkLedger` whether it is new,
    buffers accepted bars, and publishes them through
    ``MarketPipelineEngine.publish_dataset`` -- which means the canonical object
    key, the canonical `storage.objects` row, and the canonical manifest.  The
    resolution, the event type, the partition granularity and the mapping from
    event ``values`` keys to bar columns are all required inputs
    (:class:`RealtimeIngestSpec`); none of them has a default.

``RealtimeIngestConsumer``
    The loop.  At-least-once with idempotent processing (the watermark makes a
    redelivery a no-op), retry with a delay while attempts remain, and a
    dead-letter hop once the receive count reaches the configured maximum.

Because publication goes through the same engine call the batch pipeline uses,
an object this module writes is indistinguishable from a backfilled one, which
is what makes it compaction-eligible.  ``tests/test_realtime_ingest.py`` proves
that by compacting a week out of nothing but realtime output.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from typing import Any, Protocol, runtime_checkable

import pyarrow as pa

from .contracts import (
    ET,
    DatasetContract,
    Granularity,
    bar_schema,
    canonical_provider_symbol,
    deterministic_uuid,
    partition_bounds,
    stable_shard_key,
)
from .engine import MarketPipelineEngine
from .features.calculators import quantize
from .watermarks import StreamPosition, WatermarkLedger, WatermarkOutcome

__all__ = [
    "DEDUP_SESSION_RETENTION",
    "MARKET_EVENT_SCHEMA_VERSION",
    "NON_BAR_EVENT_TYPES",
    "BarFieldMap",
    "ConsumerCycle",
    "DrainReport",
    "IngestDecision",
    "MarketEvent",
    "RealtimeDelivery",
    "RealtimeEventSource",
    "RealtimeFlushResult",
    "RealtimeIngestConsumer",
    "RealtimeIngestError",
    "RealtimeIngestSpec",
    "RealtimeIngestor",
    "SqsEventSource",
    "UnsupportedEventVersion",
    "parse_market_event",
]

LOGGER = logging.getLogger("market_pipeline_lib.realtime_ingest")

#: Granularities `market_data.partition_granularity` actually has.
PARTITION_GRANULARITIES: tuple[str, ...] = ("DAY", "WEEK", "MONTH", "YEAR")

#: The pipeline run code recorded for a realtime publication.
REALTIME_RUN_CODE = "MARKET_DATA_REALTIME_INGEST"

#: How the contract's own resolution reads as an ISO-8601 duration, so a spec
#: cannot silently declare `PT1M` for a 30-minute dataset.
_RESOLUTION_DURATIONS: dict[str, str] = {
    # C's native cadence (`MarketEventType.BAR_1M`).  Present so `BAR_1M` has a
    # dataset contract to land in; see `contracts.RAW_1M_FEED`.
    "1m": "PT1M",
    "30m": "PT30M",
    "1h": "PT1H",
    "4h": "PT4H",
    "1d": "P1D",
}

_ISO_DURATION = re.compile(r"^P(?!$)(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")

#: The only ``schemaVersion`` C's market-gateway emits.
#: ``AlpacaMarketEventNormalizer.java:15`` -- ``private static final int SCHEMA_VERSION = 1``.
MARKET_EVENT_SCHEMA_VERSION = 1

#: Every wire field of C's ``MarketEventEnvelope``, in declaration order
#: (``MarketEventEnvelope.java:10-24``; the literal key names are the ones the
#: publisher writes, ``RedisMarketEventPublisher.java:50-64``).
#:
#: There is no ``metadata`` envelope: C's market event is flat and camelCase.  The
#: ``metadata``-wrapped, ``sha256:``-prefixed convention of the D-REBUILD-SPEC applies
#: to the *bot-control* contracts, not to this one.  D is the consumer here, so the
#: producer's shape wins verbatim.
MARKET_EVENT_FIELDS: tuple[str, ...] = (
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

#: How many ET session dates of de-duplication state an ingestor keeps.
#:
#: Stated rather than assumed: redelivery de-duplication is keyed on C's
#: content-addressed ``eventId``, and that set has to be bounded or a long-running
#: consumer leaks.  Two sessions covers an overnight redelivery and a next-morning
#: correction; a redelivery older than that is still absorbed by the bar slot key
#: whenever its partition is still buffered, and is otherwise republished as the same
#: row under the same canonical object key.
DEDUP_SESSION_RETENTION = 2

#: Working precision for the one quantization step in :func:`_number`.  Wide enough
#: that ``quantize`` is the only rounding a value ever sees, even for a
#: ``BigDecimal`` C sent with twenty significant digits.
_DECIMAL_WORKING_PRECISION = 50

#: The two members of C's ``MarketEventType`` that are not bars
#: (``MarketEventType.java``: ``{QUOTE, TRADE, BAR_1M}``).
#:
#: C multiplexes all three types onto one stream, so a bar ingest sees quotes and
#: trades constantly.  They get their own decision reason, ``NON_BAR_EVENT_TYPE``,
#: rather than sharing ``EVENT_TYPE_NOT_INGESTED`` with a misrouted bar:
#:
#: * a ``QUOTE`` on a bar stream is **normal traffic** -- it carries bid/ask, has no
#:   OHLCV, and there is no dataset contract in which a quote is a row.  It is
#:   reported, counted and acknowledged.  Never parked: parking normal traffic would
#:   fill the dead-letter stream within a second of the opening bell.  Never silently
#:   dropped either, or an operator could not distinguish "40k quotes correctly
#:   ignored" from "the feed is dead";
#: * a ``BAR_30M`` arriving on the 1-minute stream is a **routing mistake**, and
#:   collapsing the two into one code would hide it.
NON_BAR_EVENT_TYPES: frozenset[str] = frozenset({"QUOTE", "TRADE"})


class RealtimeIngestError(ValueError):
    """A realtime event, spec or delivery could not be handled."""


class UnsupportedEventVersion(RealtimeIngestError):
    """The event declares a ``schemaVersion`` this build does not implement.

    Separate from a malformed event on purpose.  A newer C deployment raising
    ``schemaVersion`` is a deployment-ordering fact an operator must see; guessing that
    version 2 means the same thing as version 1 is how a silently mis-parsed field
    becomes a wrong bar.
    """


# ------------------------------------------------------------------------------------
# Spec
# ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class BarFieldMap:
    """Which key in an event's ``values`` object supplies which bar column.

    Every name is required except the two nullable columns.  The old warm-up
    converter read ``values["close"]`` directly; here the caller says so, and a
    provider that names the field differently is a configuration change rather
    than a code change.
    """

    open: str
    high: str
    low: str
    close: str
    volume: str
    trade_count: str | None = None
    vwap: str | None = None

    def __post_init__(self) -> None:
        required = {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            raise RealtimeIngestError(f"BarFieldMap needs a source field for {missing}")
        optional = {"trade_count": self.trade_count, "vwap": self.vwap}
        sources = [value for value in (*required.values(), *optional.values()) if value]
        if len(set(sources)) != len(sources):
            raise RealtimeIngestError(f"BarFieldMap source fields must be distinct, got {sources}")

    def required_columns(self) -> dict[str, str]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    def optional_columns(self) -> dict[str, str | None]:
        return {"trade_count": self.trade_count, "vwap": self.vwap}


@dataclass(frozen=True)
class RealtimeIngestSpec:
    """Everything the realtime path used to assume, stated explicitly."""

    contract: DatasetContract
    event_type: str
    source_provider: str
    source_feed: str
    source_resolution: str
    partition_granularity: Granularity
    fields: BarFieldMap

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise RealtimeIngestError("event_type must name the event this stream ingests")
        for name in ("source_provider", "source_feed"):
            if not str(getattr(self, name)).strip():
                raise RealtimeIngestError(
                    f"{name} must name the C-side value this stream accepts; C's `provider` "
                    "and `feed` are its own vocabulary and do not equal D's feed_code"
                )
        if self.partition_granularity not in PARTITION_GRANULARITIES:
            raise RealtimeIngestError(
                f"partition_granularity must be one of {list(PARTITION_GRANULARITIES)}, "
                f"got {self.partition_granularity!r}"
            )
        if not _ISO_DURATION.match(self.source_resolution):
            raise RealtimeIngestError(
                f"source_resolution must be an ISO-8601 duration, got {self.source_resolution!r}"
            )
        expected = _RESOLUTION_DURATIONS.get(self.contract.resolution)
        if expected is not None and self.source_resolution != expected:
            raise RealtimeIngestError(
                f"source_resolution {self.source_resolution!r} does not match the "
                f"{self.contract.resolution} dataset contract, which is {expected}"
            )
        if self.contract.has_source_minutes:
            raise RealtimeIngestError(
                "a DERIVED contract is produced by resampling, not ingested from a stream; "
                f"got {self.contract.logical_code}"
            )


# ------------------------------------------------------------------------------------
# Results
# ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class IngestDecision:
    """What the ingestor did with one event.

    ``reason`` is the ingest decision; ``outcome`` is the *watermark's* separate
    opinion about the position.  They are deliberately not the same field: a bar can
    be new (``ACCEPTED_LATE``) while its position is older than the shard head
    (``STALE``), and collapsing the two is what used to throw the bar away.
    """

    event_id: str
    instrument_id: str
    shard_key: str
    accepted: bool
    reason: str
    revision: int = 0
    outcome: WatermarkOutcome | None = None


@dataclass(frozen=True)
class RealtimeFlushResult:
    """One publication attempt.

    ``status`` is ``"NO_CHANGE"`` when there was nothing buffered.  That is a
    distinct, reportable outcome -- never an empty success dressed up as one.
    """

    status: str
    row_count: int
    object_keys: tuple[str, ...]
    manifest_ids: tuple[str, ...]
    partitions: tuple[str, ...]
    incident_count: int
    watermark_position: str | None


# ------------------------------------------------------------------------------------
# Event validation
# ------------------------------------------------------------------------------------


def _text(document: Mapping[str, Any], key: str, label: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RealtimeIngestError(f"{label}.{key} must be a non-empty string")
    return value.strip()


def _utc(document: Mapping[str, Any], key: str, label: str) -> datetime:
    raw = _text(document, key, label)
    text = f"{raw[:-1]}+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RealtimeIngestError(f"{label}.{key} must be an ISO-8601 timestamp, got {raw!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RealtimeIngestError(f"{label}.{key} must carry a timezone offset")
    return parsed.astimezone(UTC)


def _number(values: Mapping[str, Any], key: str, label: str) -> float:
    """One bar column, as the parquet schema's `float64`.

    A `Decimal` gets here when the event came off C's Redis stream, where `values`
    is a `Map<String, BigDecimal>` serialized as JSON number text.  It is quantized
    *once*, here, through the project's single `precision:1.0.0` quantum -- eight
    places, ``ROUND_HALF_EVEN`` -- and only then narrowed to `float`.  Going
    `text -> float -> Decimal` instead would bake a binary-float approximation in
    before the rounding rule ever ran, which is exactly the precision loss the
    ``BigDecimal`` on C's side exists to avoid.
    """

    if key not in values:
        raise RealtimeIngestError(f"{label}.values is missing the mapped field {key!r}")
    value = values[key]
    if isinstance(value, Decimal):
        with localcontext() as context:
            context.prec = _DECIMAL_WORKING_PRECISION
            return float(quantize(value))
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RealtimeIngestError(f"{label}.values.{key} must be numeric, got {type(value).__name__}")
    return float(value)


def _integer(values: Mapping[str, Any], key: str, label: str) -> int:
    """One bar column, as the parquet schema's `int64`.

    A `Decimal` is accepted only when it is a whole number.  `volume` and
    `trade_count` are counts, so there is no eighth decimal place to round to and
    a fractional one means the mapping points at the wrong `values` field --
    truncating it would turn that mistake into a plausible-looking bar.
    """

    if key not in values:
        raise RealtimeIngestError(f"{label}.values is missing the mapped field {key!r}")
    value = values[key]
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            raise RealtimeIngestError(
                f"{label}.values.{key} must be a whole number, got {value}; a count "
                "cannot be fractional and rounding one would invent volume"
            )
        return int(value)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RealtimeIngestError(f"{label}.values.{key} must be an integer, got {type(value).__name__}")
    return value


@dataclass(frozen=True)
class MarketEvent:
    """One parsed ``MarketEventEnvelope``, in Python names.

    Every field C emits is carried: dropping ``revision`` or ``correctionOfEventId``
    on the way in is how a corrected bar becomes indistinguishable from a duplicate.
    """

    event_id: str
    schema_version: int
    instrument_id: str
    provider: str
    feed: str
    event_type: str
    provider_event_id: str
    occurred_at: datetime
    received_at: datetime
    sequence: int
    revision: int
    correction_of_event_id: str | None
    values: Mapping[str, Any]

    @property
    def is_correction(self) -> bool:
        return self.revision > 0

    def position(self) -> StreamPosition:
        return StreamPosition(source_event_at=self.occurred_at, sequence=self.sequence)


def parse_market_event(event: Mapping[str, Any]) -> MarketEvent:
    """Validate one event against C's ``MarketEventEnvelope`` and return it typed.

    The invariants are C's own, not ones D invented:

    * ``schemaVersion`` must be exactly :data:`MARKET_EVENT_SCHEMA_VERSION`
      (``MarketEventEnvelope.java:27-29`` rejects ``< 1``; D additionally refuses a
      version above the one it implements rather than reading unknown fields);
    * ``receivedAt`` must not precede ``occurredAt`` (``MarketEventEnvelope.java:37-39``);
    * ``sequence`` and ``revision`` must be non-negative (``:31-35``);
    * ``correctionOfEventId`` is ``null`` exactly when ``revision == 0`` (``:46-51``);
    * ``values`` must be a non-empty object (``:55-63``).
    """

    if not isinstance(event, Mapping):
        raise RealtimeIngestError(f"a market event must be an object, got {type(event).__name__}")
    label = "market event"
    missing = [name for name in MARKET_EVENT_FIELDS if name not in event]
    if missing:
        raise RealtimeIngestError(
            f"{label} is missing required field(s) {missing}; C emits all of "
            f"{list(MARKET_EVENT_FIELDS)}"
        )

    schema_version = event["schemaVersion"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise UnsupportedEventVersion(
            f"{label}.schemaVersion must be an integer, got {type(schema_version).__name__}"
        )
    if schema_version != MARKET_EVENT_SCHEMA_VERSION:
        raise UnsupportedEventVersion(
            f"{label}.schemaVersion is {schema_version}; this build implements only "
            f"{MARKET_EVENT_SCHEMA_VERSION} and will not guess what a different version means"
        )

    occurred_at = _utc(event, "occurredAt", label)
    received_at = _utc(event, "receivedAt", label)
    if received_at < occurred_at:
        raise RealtimeIngestError(
            f"{label}.receivedAt {received_at.isoformat()} precedes occurredAt "
            f"{occurred_at.isoformat()}; C refuses to construct such an envelope"
        )

    sequence = event["sequence"]
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise RealtimeIngestError(f"{label}.sequence must be a non-negative integer")
    revision = event["revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise RealtimeIngestError(f"{label}.revision must be a non-negative integer")

    correction = event["correctionOfEventId"]
    if revision == 0 and correction is not None:
        raise RealtimeIngestError(
            f"{label}.correctionOfEventId must be null when revision is 0, got {correction!r}"
        )
    if revision > 0 and (not isinstance(correction, str) or not correction.strip()):
        raise RealtimeIngestError(
            f"{label}.correctionOfEventId must name the event this revision {revision} corrects"
        )

    values = event["values"]
    if not isinstance(values, Mapping) or not values:
        raise RealtimeIngestError(f"{label}.values must be a non-empty object")

    return MarketEvent(
        event_id=_text(event, "eventId", label),
        schema_version=schema_version,
        instrument_id=_text(event, "instrumentId", label),
        provider=_text(event, "provider", label),
        feed=_text(event, "feed", label),
        event_type=_text(event, "eventType", label),
        provider_event_id=_text(event, "providerEventId", label),
        occurred_at=occurred_at,
        received_at=received_at,
        sequence=sequence,
        revision=revision,
        correction_of_event_id=None if correction is None else str(correction).strip(),
        values=dict(values),
    )


@dataclass(frozen=True)
class _BarSlot:
    """What is already buffered for one ``(instrument, bar_start)``."""

    revision: int
    event_id: str
    session: date


@dataclass
class _BatchOverlay:
    """The effect earlier events of one batch would have, before any is applied.

    Without it, two events of a single message claiming the same bar at the same
    revision would both validate -- the conflict only appears once the first is
    written -- and the second would silently overwrite the first.
    """

    event_ids: set[str] = field(default_factory=set)
    slots: dict[tuple[str, datetime], _BarSlot] = field(default_factory=dict)


@dataclass(frozen=True)
class _PreparedEvent:
    """A validated event, and what applying it would do.

    ``rejection`` and ``row`` are mutually exclusive: a rejected event is reported
    and changes nothing, an accepted one carries the bar it becomes.
    """

    parsed: MarketEvent
    shard_key: str
    rejection: str | None = None
    row: dict[str, Any] | None = None
    session: date | None = None
    slot_key: tuple[str, datetime] | None = None


# ------------------------------------------------------------------------------------
# The ingestor
# ------------------------------------------------------------------------------------


class RealtimeIngestor:
    """Buffers validated realtime bars and publishes them as canonical objects."""

    def __init__(
        self,
        engine: MarketPipelineEngine,
        spec: RealtimeIngestSpec,
        *,
        ledger: WatermarkLedger,
        dedup_session_retention: int = DEDUP_SESSION_RETENTION,
    ) -> None:
        if dedup_session_retention < 1:
            raise RealtimeIngestError("dedup_session_retention must be at least 1")
        self._engine = engine
        self._spec = spec
        self._ledger = ledger
        self._shard_count = engine.config.shard_count
        self._dedup_session_retention = dedup_session_retention
        self._symbols = {
            mapping.instrument_id: canonical_provider_symbol(mapping.provider_symbol)
            for mapping in engine.mappings.values()
        }
        # (partition_start, partition_end, shard_key) -> {(instrument, bar_start): row}
        self._buffer: dict[tuple[date, date, str], dict[tuple[str, datetime], dict[str, Any]]] = {}
        # C's `eventId` is a SHA-256 of the event's own content, so it is the exact
        # identity a redelivery repeats and a correction does not.
        self._seen_event_ids: dict[str, date] = {}
        # Survives `flush`, so a correction arriving after its original was published
        # is still recognised as newer rather than as a rival for the same bar.
        self._slots: dict[tuple[str, datetime], _BarSlot] = {}

    @property
    def spec(self) -> RealtimeIngestSpec:
        return self._spec

    @property
    def ledger(self) -> WatermarkLedger:
        return self._ledger

    @property
    def pending_rows(self) -> int:
        return sum(len(rows) for rows in self._buffer.values())

    # -- ingestion --------------------------------------------------------------
    def submit(self, event: Mapping[str, Any]) -> IngestDecision:
        """Validate, classify and buffer one of C's market events.

        Three separate questions, answered in order, because conflating them is how
        the previous implementation lost data:

        1. *Is this the same event again?*  Answered by C's content-addressed
           ``eventId``.  Yes means ``DUPLICATE_EVENT`` -- an at-least-once
           redelivery, absorbed.
        2. *Is this a better version of a bar we hold?*  Answered by ``revision``.
           A higher revision replaces the bar in place (``CORRECTION_APPLIED``); a
           lower one is ``SUPERSEDED_REVISION``; an equal one from a different event
           is a genuine conflict and raises.
        3. *Where has the shard got to?*  Answered by the watermark, which is a
           freshness projection -- **not** an admission gate.  An event whose
           position is behind the shard head is still a bar
           (``ACCEPTED_LATE``); the head simply does not rewind.  C itself
           publishes out-of-order events rather than dropping them
           (``MarketEventOrderingProcessor.java:31-34``), so a consumer that
           dropped them would silently disagree with its producer.
        """

        return self._commit(self._prepare(event, _BatchOverlay()))

    def submit_batch(self, events: Iterable[Mapping[str, Any]]) -> list[IngestDecision]:
        """Ingest a whole queue message, all of it or none of it.

        One SQS message carries many events, and a message is acknowledged, retried
        or parked *as a unit*.  Buffering the events before the bad one and then
        parking the message left those bars in the buffer with nothing left to
        acknowledge them -- they would be published as if they had been delivered
        cleanly, and the redelivered message would then be a conflicting second
        claim on the same bars.  So validation happens for every event first, and
        the first refusal leaves this ingestor exactly as it was.
        """

        overlay = _BatchOverlay()
        prepared = [self._prepare(event, overlay) for event in events]
        return [self._commit(item) for item in prepared]

    def _prepare(self, event: Mapping[str, Any], overlay: _BatchOverlay) -> _PreparedEvent:
        """Decide everything that can fail, without mutating this ingestor.

        ``overlay`` carries the effect of earlier events in the same batch, so two
        events of one message are judged against each other as well as against what
        is already buffered.
        """

        parsed = parse_market_event(event)
        spec = self._spec
        if parsed.provider != spec.source_provider:
            raise RealtimeIngestError(
                f"market event.provider {parsed.provider!r} is not the provider this stream "
                f"ingests ({spec.source_provider!r}); routing it here would file another "
                "provider's prices under this dataset"
            )
        if parsed.feed != spec.source_feed:
            raise RealtimeIngestError(
                f"market event.feed {parsed.feed!r} is not the feed this stream ingests "
                f"({spec.source_feed!r}); its watermark belongs to a different shard set"
            )

        symbol = self._symbols.get(parsed.instrument_id)
        if symbol is None:
            raise RealtimeIngestError(
                f"market event.instrumentId {parsed.instrument_id} is not in the instrument "
                "map; an unmapped instrument cannot be sharded or published"
            )
        shard_key = stable_shard_key(parsed.instrument_id, self._shard_count)

        if parsed.event_type != spec.event_type:
            # A different event type on the same stream is not an error -- this
            # stream simply does not turn it into a bar.  It is still reported, and
            # the two causes get two codes; see `NON_BAR_EVENT_TYPES`.
            rejection = (
                "NON_BAR_EVENT_TYPE"
                if parsed.event_type in NON_BAR_EVENT_TYPES
                else "EVENT_TYPE_NOT_INGESTED"
            )
            LOGGER.debug(
                "realtime.event.not_ingested",
                extra={
                    "event_id": parsed.event_id,
                    "event_type": parsed.event_type,
                    "ingested_event_type": spec.event_type,
                    "reason": rejection,
                },
            )
            return _PreparedEvent(parsed, shard_key, rejection=rejection)

        if parsed.event_id in self._seen_event_ids or parsed.event_id in overlay.event_ids:
            return _PreparedEvent(parsed, shard_key, rejection="DUPLICATE_EVENT")

        slot_key = (parsed.instrument_id, parsed.occurred_at)
        held = overlay.slots.get(slot_key) or self._slots.get(slot_key)
        if held is not None:
            if parsed.revision < held.revision:
                return _PreparedEvent(parsed, shard_key, rejection="SUPERSEDED_REVISION")
            if parsed.revision == held.revision:
                raise RealtimeIngestError(
                    f"market event {parsed.event_id} conflicts with {held.event_id}: both claim "
                    f"instrument {parsed.instrument_id} at {parsed.occurred_at.isoformat()} at "
                    f"revision {parsed.revision}. Two different events for one bar at one "
                    "revision cannot both be true, and picking one would be a silent guess"
                )

        session_date = parsed.occurred_at.astimezone(ET).date()
        row = self._bar_row(parsed.instrument_id, symbol, parsed.occurred_at, parsed.values, "market event")
        overlay.event_ids.add(parsed.event_id)
        overlay.slots[slot_key] = _BarSlot(
            revision=parsed.revision, event_id=parsed.event_id, session=session_date
        )
        return _PreparedEvent(parsed, shard_key, row=row, session=session_date, slot_key=slot_key)

    def _commit(self, prepared: _PreparedEvent) -> IngestDecision:
        """Apply a prepared event.  Nothing here may raise, and nothing here decides."""

        parsed = prepared.parsed
        if prepared.rejection is not None:
            return self._decision(parsed, prepared.shard_key, False, prepared.rejection)

        assert prepared.row is not None  # noqa: S101 - guaranteed by `_prepare`
        assert prepared.session is not None  # noqa: S101
        assert prepared.slot_key is not None  # noqa: S101
        session_date = prepared.session
        self._retain(session_date)

        decision = self._ledger.observe(prepared.shard_key, parsed.position())
        start, end = partition_bounds(session_date, self._spec.partition_granularity)
        self._buffer.setdefault((start, end, prepared.shard_key), {})[prepared.slot_key] = prepared.row
        self._slots[prepared.slot_key] = _BarSlot(
            revision=parsed.revision, event_id=parsed.event_id, session=session_date
        )
        self._seen_event_ids[parsed.event_id] = session_date

        if parsed.is_correction:
            reason = "CORRECTION_APPLIED"
        elif decision.outcome is WatermarkOutcome.ADVANCED:
            reason = "ACCEPTED_NEW"
        else:
            reason = "ACCEPTED_LATE"
        return self._decision(parsed, prepared.shard_key, True, reason, outcome=decision.outcome)

    @staticmethod
    def _decision(
        parsed: MarketEvent,
        shard_key: str,
        accepted: bool,
        reason: str,
        *,
        outcome: WatermarkOutcome | None = None,
    ) -> IngestDecision:
        return IngestDecision(
            event_id=parsed.event_id,
            instrument_id=parsed.instrument_id,
            shard_key=shard_key,
            accepted=accepted,
            reason=reason,
            revision=parsed.revision,
            outcome=outcome,
        )

    def _retain(self, session: date) -> None:
        """Drop de-duplication state older than the retention window."""

        sessions = {session, *self._seen_event_ids.values(), *(slot.session for slot in self._slots.values())}
        if len(sessions) <= self._dedup_session_retention:
            return
        keep = set(sorted(sessions)[-self._dedup_session_retention :])
        self._seen_event_ids = {
            event_id: seen for event_id, seen in self._seen_event_ids.items() if seen in keep
        }
        self._slots = {key: slot for key, slot in self._slots.items() if slot.session in keep}

    def _bar_row(
        self,
        instrument_id: str,
        symbol: str,
        bar_start: datetime,
        values: Mapping[str, Any],
        label: str,
    ) -> dict[str, Any]:
        fields = self._spec.fields
        row: dict[str, Any] = {
            "instrument_id": instrument_id,
            "provider_symbol": symbol,
            "bar_start_at": bar_start,
            "session_date_et": bar_start.astimezone(ET).date(),
        }
        for column, source in fields.required_columns().items():
            row[column] = (
                _integer(values, source, label) if column == "volume" else _number(values, source, label)
            )
        for column, source in fields.optional_columns().items():
            if source is None:
                row[column] = None
            elif column == "trade_count":
                row[column] = _integer(values, source, label)
            else:
                row[column] = _number(values, source, label)
        return row

    # -- publication ------------------------------------------------------------
    def flush(self, *, ingested_at: datetime | None = None) -> RealtimeFlushResult:
        """Publish everything buffered, then checkpoint the watermark."""

        if not self._buffer:
            return RealtimeFlushResult(
                status="NO_CHANGE",
                row_count=0,
                object_keys=(),
                manifest_ids=(),
                partitions=(),
                incident_count=0,
                watermark_position=self._checkpoint(ingested_at),
            )

        schema = bar_schema(False)
        by_year: dict[int, list[tuple[Granularity, date, date, str, pa.Table, list[str]]]] = {}
        row_count = 0
        partitions: set[str] = set()
        for (start, end, shard_key), rows in sorted(self._buffer.items(), key=lambda item: item[0]):
            ordered = [rows[key] for key in sorted(rows)]
            table = pa.Table.from_pylist(ordered, schema=schema)
            by_year.setdefault(start.year, []).append(
                (self._spec.partition_granularity, start, end, shard_key, table, [])
            )
            row_count += len(ordered)
            partitions.add(f"{start.isoformat()}/{end.isoformat()}")

        object_keys: list[str] = []
        manifest_ids: list[str] = []
        incident_count = 0
        statuses: list[str] = []
        for year, groups in sorted(by_year.items()):
            result = self._publish_year(year, groups)
            manifest = result["manifest"]
            manifest_ids.append(manifest["id"])
            statuses.append(manifest["status"])
            incident_count += int(result["incident_count"])
            object_keys.extend(
                item["storage"]["object_key"]
                for item in self._engine.catalog.objects_for_manifest(manifest["id"])
            )

        self._buffer.clear()
        status = "AVAILABLE" if statuses and set(statuses) == {"AVAILABLE"} else "QUARANTINED"
        return RealtimeFlushResult(
            status=status,
            row_count=row_count,
            object_keys=tuple(sorted(object_keys)),
            manifest_ids=tuple(manifest_ids),
            partitions=tuple(sorted(partitions)),
            incident_count=incident_count,
            watermark_position=self._checkpoint(ingested_at),
        )

    def _publish_year(
        self,
        year: int,
        groups: list[tuple[Granularity, date, date, str, pa.Table, list[str]]],
    ) -> dict[str, Any]:
        replace_periods = sorted({(start, end) for _, start, end, _, _, _ in groups})
        idempotency_key = ":".join(
            (
                self._engine.config.fingerprint,
                "realtime",
                self._spec.contract.logical_code,
                self._spec.partition_granularity,
                str(year),
                deterministic_uuid(
                    *(f"{start}:{end}:{shard}" for _, start, end, shard, _, _ in sorted(groups, key=_group_order))
                ),
            )
        )
        run = self._engine._run_record(REALTIME_RUN_CODE, idempotency_key)  # noqa: SLF001
        self._engine._active_run_id = run["id"]  # noqa: SLF001
        result: dict[str, Any] = self._engine.publish_dataset(
            self._spec.contract,
            year,
            groups,
            replace_periods=list(replace_periods),
            relation_type="INGESTED_FROM",
        )
        succeeded = result["manifest"]["status"] == "AVAILABLE"
        self._engine.catalog.finish_pipeline_run(
            run["id"],
            status="SUCCEEDED" if succeeded else "FAILED",
            output_hash=result["manifest"]["dataset_hash"],
            failure_code=None if succeeded else "REALTIME_INGEST_QUARANTINED",
        )
        return result

    def _checkpoint(self, ingested_at: datetime | None) -> str | None:
        watermark = self._ledger.checkpoint(ingested_at=ingested_at or datetime.now(UTC))
        return None if watermark is None else watermark.position.isoformat()


def _group_order(
    group: tuple[Granularity, date, date, str, pa.Table, list[str]],
) -> tuple[date, date, str]:
    _, start, end, shard, _, _ = group
    return start, end, shard


# ------------------------------------------------------------------------------------
# The transport port and its SQS adapter
# ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RealtimeDelivery:
    """One delivery attempt of one queue message."""

    message_id: str
    body: Mapping[str, Any]
    receipt_handle: str
    receive_count: int


@runtime_checkable
class RealtimeEventSource(Protocol):
    """Durable-queue port for realtime market events."""

    def poll(self, max_messages: int, wait_seconds: float) -> Sequence[RealtimeDelivery]:
        """Receive up to `max_messages`, waiting at most `wait_seconds`."""

    def acknowledge(self, delivery: RealtimeDelivery) -> None:
        """Delete the message; it must never be delivered again."""

    def retry_later(self, delivery: RealtimeDelivery, *, delay_seconds: float) -> None:
        """Make the message visible again after `delay_seconds`."""

    def extend_visibility(
        self, delivery: RealtimeDelivery, *, timeout_seconds: int
    ) -> None:
        """Renew an in-flight delivery while its work is still running."""

    def dead_letter(self, delivery: RealtimeDelivery, *, reason: str) -> None:
        """Park the message on the dead-letter queue and remove it from this one."""

    def close(self) -> None:
        """Release adapter resources."""


class SqsEventSource:
    """Amazon SQS adapter, exercised against LocalStack.

    The dead-letter hop is performed by this adapter rather than left to a queue
    redrive policy: the worker must be able to state *why* a message was parked,
    and an operator who forgot to configure redrive would otherwise get an
    infinite retry loop instead of a parked message.
    """

    #: SQS caps a single receive at ten messages and a long poll at twenty seconds.
    MAX_BATCH = 10
    MAX_WAIT_SECONDS = 20

    def __init__(
        self,
        client: Any,
        *,
        queue_url: str,
        dead_letter_queue_url: str | None,
        visibility_timeout_seconds: int = 60,
    ) -> None:
        if not queue_url:
            raise RealtimeIngestError("queue_url must not be empty")
        if visibility_timeout_seconds < 0:
            raise RealtimeIngestError("visibility_timeout_seconds must not be negative")
        self._client = client
        self._queue_url = queue_url
        self._dead_letter_queue_url = dead_letter_queue_url
        self._visibility_timeout_seconds = visibility_timeout_seconds

    @property
    def queue_url(self) -> str:
        return self._queue_url

    @property
    def dead_letter_queue_url(self) -> str | None:
        return self._dead_letter_queue_url

    def poll(self, max_messages: int, wait_seconds: float) -> list[RealtimeDelivery]:
        response = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=max(1, min(int(max_messages), self.MAX_BATCH)),
            WaitTimeSeconds=max(0, min(int(wait_seconds), self.MAX_WAIT_SECONDS)),
            VisibilityTimeout=self._visibility_timeout_seconds,
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        deliveries: list[RealtimeDelivery] = []
        for message in response.get("Messages", []):
            deliveries.append(
                RealtimeDelivery(
                    message_id=message["MessageId"],
                    body=self._decode(message),
                    receipt_handle=message["ReceiptHandle"],
                    receive_count=int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
                )
            )
        return deliveries

    @staticmethod
    def _decode(message: Mapping[str, Any]) -> Mapping[str, Any]:
        raw = message.get("Body", "")
        try:
            document = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            # Not raised: an undecodable body is a message the consumer must be
            # able to park, and it can only do that if it is handed the delivery.
            return {"__undecodable__": raw}
        if not isinstance(document, Mapping):
            return {"__undecodable__": raw}
        return document

    def acknowledge(self, delivery: RealtimeDelivery) -> None:
        self._client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=delivery.receipt_handle)

    def retry_later(self, delivery: RealtimeDelivery, *, delay_seconds: float) -> None:
        self._client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=delivery.receipt_handle,
            VisibilityTimeout=max(0, int(delay_seconds)),
        )

    def extend_visibility(
        self, delivery: RealtimeDelivery, *, timeout_seconds: int
    ) -> None:
        self._client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=delivery.receipt_handle,
            VisibilityTimeout=max(0, min(int(timeout_seconds), 43_200)),
        )

    def dead_letter(self, delivery: RealtimeDelivery, *, reason: str) -> None:
        if not self._dead_letter_queue_url:
            raise RealtimeIngestError(
                f"message {delivery.message_id} must be parked ({reason}) but no dead-letter "
                "queue is configured; refusing to drop it or to retry it forever"
            )
        self._client.send_message(
            QueueUrl=self._dead_letter_queue_url,
            MessageBody=json.dumps(
                delivery.body.get("__undecodable__", delivery.body)
                if "__undecodable__" in delivery.body
                else delivery.body,
                separators=(",", ":"),
                sort_keys=True,
            ),
            MessageAttributes={
                "DeadLetterReason": {"DataType": "String", "StringValue": reason},
                "SourceQueueUrl": {"DataType": "String", "StringValue": self._queue_url},
                "ReceiveCount": {"DataType": "Number", "StringValue": str(delivery.receive_count)},
            },
        )
        self.acknowledge(delivery)

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()


# ------------------------------------------------------------------------------------
# The loop
# ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsumerCycle:
    """What one receive/handle cycle did."""

    received: int = 0
    accepted: int = 0
    skipped: int = 0
    acknowledged: int = 0
    retried: int = 0
    dead_lettered: int = 0


@dataclass
class DrainReport:
    """The sum of every cycle in a drain."""

    cycles: int = 0
    received: int = 0
    accepted: int = 0
    skipped: int = 0
    acknowledged: int = 0
    retried: int = 0
    dead_lettered: int = 0

    def add(self, cycle: ConsumerCycle) -> None:
        self.cycles += 1
        self.received += cycle.received
        self.accepted += cycle.accepted
        self.skipped += cycle.skipped
        self.acknowledged += cycle.acknowledged
        self.retried += cycle.retried
        self.dead_lettered += cycle.dead_lettered


@dataclass
class RealtimeIngestConsumer:
    """Drains a realtime event source into an ingestor.

    Delivery policy, stated once:

    * a body that is not ``{"events": [...]}``, or an event the ingestor refuses
      to parse, is **parked immediately** -- another delivery cannot fix it.  An
      event declaring a ``schemaVersion`` this build does not implement is parked
      under its own reason, ``UNSUPPORTED_EVENT_VERSION``: it means C was deployed
      ahead of D, which an operator fixes by shipping D, not by fixing a payload;
    * any other failure is **retried** until ``receive_count`` reaches
      ``max_receive_count``, then parked with ``MAX_RECEIVES_EXCEEDED``;
    * a handled message is acknowledged, and redelivery of one already handled is
      absorbed by the ingestor's de-duplication rather than duplicating a row;
    * a message is ingested **whole or not at all** -- see
      :meth:`RealtimeIngestor.submit_batch`.
    """

    ingestor: Any
    source: RealtimeEventSource
    max_receive_count: int
    flush_every: int = 1_000
    max_messages_per_poll: int = 10
    retry_delay_seconds: float = 5.0
    _since_flush: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_receive_count < 1:
            raise RealtimeIngestError("max_receive_count must be at least 1")
        if self.flush_every < 1:
            raise RealtimeIngestError("flush_every must be at least 1")

    def run_once(self, *, wait_seconds: float = 1.0) -> ConsumerCycle:
        deliveries = self.source.poll(
            max_messages=self.max_messages_per_poll, wait_seconds=wait_seconds
        )
        if not deliveries:
            return ConsumerCycle()

        received = accepted = skipped = acknowledged = retried = dead_lettered = 0
        for delivery in deliveries:
            received += 1
            try:
                events = _events_of(delivery.body)
            except RealtimeIngestError as error:
                self.source.dead_letter(delivery, reason="MALFORMED_EVENT")
                dead_lettered += 1
                LOGGER.warning(
                    "realtime.message.parked",
                    extra={"message_id": delivery.message_id, "reason": str(error)},
                )
                continue

            try:
                # Atomic: nothing is buffered unless every event in the message is
                # acceptable, so a parked message leaves no half-ingested session.
                decisions = self.ingestor.submit_batch(events)
            except UnsupportedEventVersion as error:
                self.source.dead_letter(delivery, reason="UNSUPPORTED_EVENT_VERSION")
                dead_lettered += 1
                LOGGER.error(
                    "realtime.message.parked",
                    extra={"message_id": delivery.message_id, "reason": str(error)},
                )
                continue
            except RealtimeIngestError as error:
                self.source.dead_letter(delivery, reason="MALFORMED_EVENT")
                dead_lettered += 1
                LOGGER.warning(
                    "realtime.message.parked",
                    extra={"message_id": delivery.message_id, "reason": str(error)},
                )
                continue
            except Exception as error:  # noqa: BLE001 - one bad message must not stop the loop
                if delivery.receive_count >= self.max_receive_count:
                    self.source.dead_letter(delivery, reason="MAX_RECEIVES_EXCEEDED")
                    dead_lettered += 1
                    LOGGER.error(
                        "realtime.message.parked",
                        extra={
                            "message_id": delivery.message_id,
                            "receive_count": delivery.receive_count,
                            "reason": str(error),
                        },
                    )
                else:
                    self.source.retry_later(delivery, delay_seconds=self.retry_delay_seconds)
                    retried += 1
                    LOGGER.warning(
                        "realtime.message.retry",
                        extra={
                            "message_id": delivery.message_id,
                            "receive_count": delivery.receive_count,
                            "reason": str(error),
                        },
                    )
                continue

            accepted += sum(1 for decision in decisions if decision.accepted)
            skipped += sum(1 for decision in decisions if not decision.accepted)
            self.source.acknowledge(delivery)
            acknowledged += 1

        self._since_flush += accepted
        if self._since_flush >= self.flush_every:
            self.ingestor.flush()
            self._since_flush = 0
        return ConsumerCycle(
            received=received,
            accepted=accepted,
            skipped=skipped,
            acknowledged=acknowledged,
            retried=retried,
            dead_lettered=dead_lettered,
        )

    def drain(self, *, max_empty_cycles: int = 2, wait_seconds: float = 1.0) -> DrainReport:
        """Run cycles until `max_empty_cycles` consecutive polls come back empty."""

        report = DrainReport()
        empty = 0
        while empty < max_empty_cycles:
            cycle = self.run_once(wait_seconds=wait_seconds)
            report.add(cycle)
            empty = empty + 1 if cycle.received == 0 else 0
        return report

    def shutdown(self) -> RealtimeFlushResult:
        """Publish everything the loop acknowledged but has not yet written.

        The loop acknowledges a message the moment the ingestor *accepts* it, and an
        accepted bar sits in the ingestor's buffer until a flush.  Between those two
        points the queue believes the work is done and nothing durable exists -- so a
        process that stopped there would lose exactly the rows it had promised to
        keep.  Closing that window is the whole job of this method, which is why a
        graceful stop calls it and a crash is what the unflushed-buffer risk is
        measured against.

        Returns the flush result rather than a boolean: ``NO_CHANGE`` on an idle
        consumer is a real, reportable outcome and must not read as a success.
        """

        result: RealtimeFlushResult = self.ingestor.flush()
        self._since_flush = 0
        return result

    def run_until_stopped(
        self,
        should_stop: Callable[[], bool],
        *,
        wait_seconds: float = 1.0,
    ) -> tuple[DrainReport, RealtimeFlushResult]:
        """Poll until `should_stop()` answers true, then flush and return.

        `should_stop` is a predicate rather than a flag on this object so the signal
        can come from wherever the host already keeps it -- ``threading.Event.is_set``
        satisfies it directly, and so does a supervisor's own shutdown check.

        The check happens *between* cycles, never inside one: a cycle that has already
        received messages finishes handling them, so a stop can never strand a message
        that was taken from the queue but neither acknowledged nor made visible again.
        """

        report = DrainReport()
        while not should_stop():
            report.add(self.run_once(wait_seconds=wait_seconds))
        return report, self.shutdown()


def _events_of(body: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if not isinstance(body, Mapping) or "__undecodable__" in body:
        raise RealtimeIngestError("message body is not a JSON object")
    events = body.get("events")
    if not isinstance(events, list) or not events:
        raise RealtimeIngestError("message body must carry a non-empty 'events' array")
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            raise RealtimeIngestError(f"events[{index}] must be an object")
    return list(events)


def session_window_utc(session: date) -> tuple[datetime, datetime]:
    """The ET calendar day `session` expressed as a UTC half-open interval."""

    start = datetime.combine(session, datetime.min.time(), ET).astimezone(UTC)
    end = datetime.combine(session + timedelta(days=1), datetime.min.time(), ET).astimezone(UTC)
    return start, end
