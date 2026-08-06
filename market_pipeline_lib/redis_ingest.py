"""D90 -- the Redis Streams transport C actually publishes on.

The gap this closes
-------------------
C's market gateway publishes every market event through
``RedisMarketEventPublisher`` -- Redis Streams, one Lua script, five keys.  D's
realtime ingest (:mod:`market_pipeline_lib.realtime_ingest`) consumed **SQS**.
No event C emits could ever reach D; the two halves of D90 were never connected,
whatever either side's tests said.

The decision is that **D conforms to C**: C's repository is not touched, C's
format is not renegotiated, and D's existing SQS path is left working.  This
module is therefore a second implementation of the *existing*
:class:`~market_pipeline_lib.realtime_ingest.RealtimeEventSource` protocol -- the
same ``poll``/``acknowledge``/``retry_later``/``dead_letter``/``close`` surface
:class:`~market_pipeline_lib.realtime_ingest.SqsEventSource` implements, so
:class:`~market_pipeline_lib.realtime_ingest.RealtimeIngestConsumer` and the
ingestor are unchanged and untouched by the transport swap.

C's contract, and where each line of it was read
------------------------------------------------
``trading-engine/modules/market-data-adapter/src/main/java/com/idea2strategy/trading/market/``

``redis/RedisMarketEventPublisher.java``
    ``:310-318``
        The key base is ``"{" + keyPrefix + ":market}"``.  The braces are a Redis
        Cluster hash tag: all three keys of the publish script land in one slot,
        which is what lets the script be atomic on a clustered deployment.  A
        prefix containing braces of its own is refused, and so is a blank one.
    ``:252-264``
        ``<base>:events`` (stream), ``<base>:seen`` (set), and
        ``<base>:latest:<instrumentId>:<EVENT_TYPE>`` (hash).
    ``:46-48``
        ``SADD`` on ``:seen`` is the **publish gate**.  The member is the
        ``eventId``, and an ``eventId`` already in the set is never appended to the
        stream again.  So duplicate suppression happens *at the producer*: any
        repeat D sees is a redelivery of one stream entry, never a second entry.
    ``:50-64``
        The thirteen ``XADD`` fields, in order, all Redis strings -- see
        :data:`C_STREAM_FIELDS`.  ``correctionOfEventId`` is written as ``""``
        when it is absent (``:205``), never as a missing field.
    ``:66-92``
        The ``:latest:`` hash is a monotone projection: it moves only to a higher
        ``sequence``, or to a higher ``revision`` at the same ``sequence``.  It is
        the producer's own "where has this instrument got to" marker, and it
        carries an extra ``streamEntryId`` field the stream entries do not.

``alpaca/MarketEventOrderingProcessor.java``
    The ordering C guarantees, per ``(provider, feed, instrumentId, eventType)``:
    a repeated ``eventId`` is dropped; an event whose ``sequence`` goes backwards
    is still **published** but does not advance ``:latest:`` (``:31-34``); a
    second event at an already-seen ``sequence`` is not published at all
    (``:35-38``); a correction is published only when its ``revision`` beats the
    one on record (``:45-62``).  Because every publish is one ``XADD *``, the
    stream's entry-ID order *is* C's publish order, and reading a consumer group
    in ID order therefore replays exactly what C decided to emit.

``../messaging/market/MarketEventType.java``
    Exactly ``{QUOTE, TRADE, BAR_1M}`` -- :data:`C_MARKET_EVENT_TYPES`.  All three
    share one stream, which is why a bar ingest must handle quotes and trades as
    ordinary traffic rather than as faults; see
    :data:`~market_pipeline_lib.realtime_ingest.NON_BAR_EVENT_TYPES`.

Where Redis Streams differ from SQS, and what this adapter does about it
------------------------------------------------------------------------
*There is no visibility timeout.*  An entry a consumer group hands out stays in
that group's pending-entries list until it is acknowledged.  Redelivery is
therefore explicit: :meth:`RedisMarketEventSource.poll` first reclaims entries
that have been pending longer than ``claim_min_idle_seconds`` and only then reads
new ones, so an entry whose consumer died is picked up by whichever consumer polls
next.  ``XPENDING`` supplies the delivery count that drives dead-lettering, which
is the same number SQS calls ``ApproximateReceiveCount``.

*There is no per-message delay.*  :meth:`retry_later` sets the entry's idle clock
with ``XCLAIM ... IDLE`` so that it becomes reclaim-eligible after exactly the
requested delay.  A delay longer than ``claim_min_idle_seconds`` cannot be
expressed that way, and is refused rather than quietly shortened.

*There is no redrive policy, and the stream has other readers.*  Parking writes
the entry -- every field C wrote, plus why it was parked -- to a **D-owned**
dead-letter stream, and then ``XACK``s the original.  It never ``XDEL``s: C's
stream is a shared log and other consumer groups still have to read it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from .realtime_ingest import RealtimeDelivery, RealtimeIngestError

__all__ = [
    "C_MARKET_EVENT_TYPES",
    "C_PUBLISH_KEY_ROLES",
    "C_STREAM_FIELDS",
    "DEAD_LETTER_FIELDS",
    "RedisMarketEventSource",
    "RedisStreamDecodeError",
    "decode_stream_entry",
    "deduplication_key",
    "bar_updates_channel",
    "latest_key",
    "market_key_base",
    "recent_bars_key",
    "stream_key",
]

LOGGER = logging.getLogger("market_pipeline_lib.redis_ingest")

#: The five ``KEYS`` of C's publish script and their Redis roles
#: (``RedisMarketEventPublisher.java:36-53``).  Pinned because the script asserts
#: the types before it writes anything, so a D-side helper that built the keys in
#: another order would make C's own script fail closed against its own data.
C_PUBLISH_KEY_ROLES: tuple[str, str, str, str, str] = (
    "stream",
    "hash",
    "set",
    "zset",
    "channel",
)

#: Every field C's ``XADD`` writes, in C's order
#: (``RedisMarketEventPublisher.java:50-64``).  These are also the wire names of
#: ``MarketEventEnvelope``; the ``:latest:`` hash adds ``streamEntryId`` on top.
C_STREAM_FIELDS: tuple[str, ...] = (
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

#: ``MarketEventType.java`` in declaration order.
C_MARKET_EVENT_TYPES: tuple[str, ...] = ("QUOTE", "TRADE", "BAR_1M")

#: The fields D adds when it parks an entry.  Namespaced away from C's thirteen so
#: a parked entry can be replayed by stripping exactly these.
DEAD_LETTER_FIELDS: tuple[str, ...] = (
    "deadLetterReason",
    "sourceStream",
    "sourceStreamEntryId",
    "sourceConsumerGroup",
    "deliveryCount",
)

#: The fields C writes as decimal integer text.
_INTEGER_FIELDS: tuple[str, ...] = ("schemaVersion", "sequence", "revision")

#: What a body carries when the entry cannot be decoded.  The consumer parks a
#: delivery shaped like this; it is deliberately the same marker
#: :class:`~market_pipeline_lib.realtime_ingest.SqsEventSource` uses, so both
#: transports reach the same ``MALFORMED_EVENT`` branch.
_UNDECODABLE = "__undecodable__"


class RedisStreamDecodeError(RealtimeIngestError):
    """A stream entry is not shaped like something C's publisher wrote."""


# --------------------------------------------------------------------------------------
# C's keys
# --------------------------------------------------------------------------------------


def market_key_base(key_prefix: str) -> str:
    """``"{" + keyPrefix + ":market}"`` -- ``RedisMarketEventPublisher.java:310-318``."""

    if not key_prefix or not key_prefix.strip():
        raise RealtimeIngestError("key_prefix must not be blank; C refuses a blank prefix")
    if "{" in key_prefix or "}" in key_prefix:
        raise RealtimeIngestError(
            f"key_prefix {key_prefix!r} must not contain Redis hash-tag braces: C adds the "
            "hash tag itself, and a second one would split its five keys across cluster "
            "slots and break the atomicity of its publish script"
        )
    return "{" + key_prefix + ":market}"


def stream_key(key_prefix: str) -> str:
    """The event stream -- ``RedisMarketEventPublisher.java:252-254``."""

    return f"{market_key_base(key_prefix)}:events"


def deduplication_key(key_prefix: str) -> str:
    """C's producer-side de-duplication set of ``eventId``s -- ``:256-258``."""

    return f"{market_key_base(key_prefix)}:seen"


def latest_key(key_prefix: str, instrument_id: str, event_type: str) -> str:
    """C's monotone latest-observation hash -- ``:260-264``."""

    if not str(instrument_id).strip():
        raise RealtimeIngestError("instrument_id must not be blank")
    if not str(event_type).strip():
        raise RealtimeIngestError("event_type must not be blank")
    return f"{market_key_base(key_prefix)}:latest:{instrument_id}:{event_type}"


def recent_bars_key(key_prefix: str, instrument_id: str) -> str:
    """C's bounded one-minute bar projection -- ``RedisMarketEventPublisher.java:363-366``."""

    if not str(instrument_id).strip():
        raise RealtimeIngestError("instrument_id must not be blank")
    return f"{market_key_base(key_prefix)}:bars:{instrument_id}:1m"


def bar_updates_channel(key_prefix: str) -> str:
    """C's one-minute bar notification channel -- ``RedisMarketEventPublisher.java:368-370``."""

    return f"{market_key_base(key_prefix)}:bar-updates"


# --------------------------------------------------------------------------------------
# Decoding one entry
# --------------------------------------------------------------------------------------


def decode_values(raw: str, *, label: str = "values") -> dict[str, Decimal]:
    """C's ``Map<String, BigDecimal>``, as JSON text, back into exact `Decimal`.

    ``parse_float``/``parse_int`` hand :class:`~decimal.Decimal` the *token text*, so
    the digits never touch a binary float.  A JSON string, boolean or null in here
    is refused rather than coerced: C's map cannot hold one, so its presence means
    the entry did not come from C's publisher and guessing what it meant would put
    an invented number into a bar.
    """

    try:
        document = json.loads(raw, parse_float=Decimal, parse_int=Decimal)
    except (TypeError, ValueError) as error:
        raise RedisStreamDecodeError(f"{label} is not valid JSON: {error}") from error
    if not isinstance(document, Mapping) or not document:
        raise RedisStreamDecodeError(
            f"{label} must be a non-empty JSON object; C's MarketEventEnvelope refuses "
            "an empty values map (MarketEventEnvelope.java:59-61)"
        )
    decoded: dict[str, Decimal] = {}
    for name, value in document.items():
        if isinstance(value, bool) or not isinstance(value, Decimal):
            raise RedisStreamDecodeError(
                f"{label}.{name} must be a JSON number: C serializes a BigDecimal, and "
                f"a {type(value).__name__} here is not something its publisher wrote"
            )
        decoded[str(name)] = value
    return decoded


def decode_stream_entry(fields: Mapping[str, str]) -> dict[str, Any]:
    """One ``XADD`` entry, as the flat-JSON document ``parse_market_event`` reads.

    Purely a transport translation.  Every *semantic* invariant -- the schema
    version, ``receivedAt`` not preceding ``occurredAt``, the correction pointer
    matching the revision -- stays in
    :func:`~market_pipeline_lib.realtime_ingest.parse_market_event`, which is the
    one place that decides what a valid C event is, so both transports get the
    identical judgement.
    """

    missing = [name for name in C_STREAM_FIELDS if name not in fields]
    if missing:
        raise RedisStreamDecodeError(
            f"stream entry is missing field(s) {missing}; C's XADD always writes all of "
            f"{list(C_STREAM_FIELDS)} (RedisMarketEventPublisher.java:50-64)"
        )

    document: dict[str, Any] = {name: fields[name] for name in C_STREAM_FIELDS}
    for name in _INTEGER_FIELDS:
        text = str(fields[name])
        try:
            document[name] = int(text)
        except ValueError as error:
            raise RedisStreamDecodeError(
                f"stream entry field {name} must be decimal integer text, got {text!r}"
            ) from error

    # C writes "" for a null correction pointer (RedisMarketEventPublisher.java:205);
    # Redis has no null hash value to write instead.
    document["correctionOfEventId"] = fields["correctionOfEventId"] or None

    try:
        document["values"] = decode_values(fields["values"], label="stream entry values")
    except InvalidOperation as error:  # pragma: no cover - Decimal rejects pathological text
        raise RedisStreamDecodeError(f"stream entry values is not decodable: {error}") from error
    return document


def _decode_latest(fields: Mapping[str, str]) -> dict[str, Any]:
    """C's ``:latest:`` hash, which is a stream entry plus ``streamEntryId``."""

    decoded = decode_stream_entry(fields)
    entry_id = fields.get("streamEntryId")
    if not entry_id:
        raise RedisStreamDecodeError(
            "the latest-observation hash must carry streamEntryId "
            "(RedisMarketEventPublisher.java:89)"
        )
    decoded["streamEntryId"] = entry_id
    return decoded


# --------------------------------------------------------------------------------------
# The adapter
# --------------------------------------------------------------------------------------


class RedisMarketEventSource:
    """Consumes C's market-event stream as a
    :class:`~market_pipeline_lib.realtime_ingest.RealtimeEventSource`.

    One stream entry is one event, because C publishes one ``XADD`` per event, so a
    delivery's body is ``{"events": [event]}`` -- the shape
    :class:`~market_pipeline_lib.realtime_ingest.RealtimeIngestConsumer` already
    reads, which is what keeps the loop and the ingestor transport-agnostic.
    """

    def __init__(
        self,
        client: Any,
        *,
        key_prefix: str,
        consumer_group: str,
        consumer_name: str,
        dead_letter_stream_key: str | None,
        claim_min_idle_seconds: float,
    ) -> None:
        if not consumer_group.strip():
            raise RealtimeIngestError("consumer_group must not be blank")
        if not consumer_name.strip():
            raise RealtimeIngestError(
                "consumer_name must not be blank; it is the name Redis records against "
                "every pending entry, and it is what identifies the stalled worker"
            )
        if claim_min_idle_seconds <= 0:
            raise RealtimeIngestError(
                "claim_min_idle_seconds must be positive: a zero threshold would let a "
                "consumer reclaim the entry it is still working on and process it twice"
            )
        self._client = client
        # `market_key_base` raises here on a bad prefix, exactly as C's constructor does.
        self._stream_key = stream_key(key_prefix)
        self._deduplication_key = deduplication_key(key_prefix)
        self._key_prefix = key_prefix
        self._consumer_group = consumer_group
        self._consumer_name = consumer_name
        self._dead_letter_stream_key = dead_letter_stream_key
        self._claim_min_idle_ms = int(claim_min_idle_seconds * 1000)
        # Raw fields of the entries currently held by this source, so a parked entry
        # can be written to the dead-letter stream exactly as C wrote it.  Entries
        # leave on acknowledge or dead_letter, so this cannot grow without bound.
        self._held: dict[str, Mapping[str, str]] = {}

    # -- identity ----------------------------------------------------------------
    @property
    def stream_key(self) -> str:
        return self._stream_key

    @property
    def deduplication_key(self) -> str:
        return self._deduplication_key

    @property
    def consumer_group(self) -> str:
        return self._consumer_group

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    @property
    def dead_letter_stream_key(self) -> str | None:
        return self._dead_letter_stream_key

    @property
    def claim_min_idle_seconds(self) -> float:
        return self._claim_min_idle_ms / 1000

    def latest_key(self, instrument_id: str, event_type: str) -> str:
        return latest_key(self._key_prefix, instrument_id, event_type)

    # -- the group ----------------------------------------------------------------
    def ensure_group(self, *, start_id: str = "0") -> bool:
        """Create the consumer group if it is absent.  ``True`` when it was created.

        ``start_id="0"``, not ``"$"``: C publishes whether or not D is running, and a
        group created at the tail would silently skip every event that arrived before
        the consumer first started -- a data loss that looks exactly like an idle feed.
        ``MKSTREAM`` so D can start before C has published anything.

        The return value distinguishes "created" from "already there".  It is never a
        bare ``True``: an operator reading a startup log has to be able to tell a
        first deployment from a restart.
        """

        try:
            self._client.xgroup_create(
                name=self._stream_key,
                groupname=self._consumer_group,
                id=start_id,
                mkstream=True,
            )
        except Exception as error:  # noqa: BLE001 - redis-py raises a bare ResponseError
            if "BUSYGROUP" not in str(error):
                raise
            return False
        return True

    def pending_count(self) -> int:
        """How many entries this group has handed out and not had acknowledged."""

        summary = self._client.xpending(self._stream_key, self._consumer_group)
        return int(summary["pending"]) if summary else 0

    # -- reading ------------------------------------------------------------------
    def poll(self, max_messages: int, wait_seconds: float) -> list[RealtimeDelivery]:
        """Reclaim stale pending entries first, then read new ones.

        Reclaim-before-read is deliberate.  Redis hands ``>`` entries out forever, so
        a consumer that only ever read new entries would leave everything a crashed
        sibling was holding pending for good -- at-least-once would degrade to
        at-most-once with no error anywhere.  Taking the oldest work first also keeps
        a retried entry from being starved by a busy stream.
        """

        wanted = max(1, int(max_messages))
        deliveries = self._reclaim(wanted)
        remaining = wanted - len(deliveries)
        if remaining <= 0:
            return deliveries

        response = self._client.xreadgroup(
            groupname=self._consumer_group,
            consumername=self._consumer_name,
            streams={self._stream_key: ">"},
            count=remaining,
            block=max(0, int(wait_seconds * 1000)) or None,
        )
        for _, entries in response or []:
            for entry_id, fields in entries:
                deliveries.append(self._delivery(entry_id, fields, receive_count=1))
        return deliveries

    def _reclaim(self, count: int) -> list[RealtimeDelivery]:
        """Take over entries pending longer than the idle threshold."""

        pending = self._client.xpending_range(
            name=self._stream_key,
            groupname=self._consumer_group,
            min="-",
            max="+",
            count=count,
            idle=self._claim_min_idle_ms,
        )
        if not pending:
            return []
        # `times_delivered` is the count *before* this claim; XCLAIM increments it.
        counts = {str(item["message_id"]): int(item["times_delivered"]) + 1 for item in pending}
        claimed = self._client.xclaim(
            name=self._stream_key,
            groupname=self._consumer_group,
            consumername=self._consumer_name,
            min_idle_time=self._claim_min_idle_ms,
            message_ids=list(counts),
        )
        deliveries: list[RealtimeDelivery] = []
        for entry_id, fields in claimed or []:
            key = str(entry_id)
            deliveries.append(
                self._delivery(key, fields, receive_count=counts.get(key, 1))
            )
        return deliveries

    def _delivery(
        self, entry_id: str, fields: Mapping[str, str] | None, *, receive_count: int
    ) -> RealtimeDelivery:
        raw: Mapping[str, str] = fields or {}
        self._held[str(entry_id)] = raw
        return RealtimeDelivery(
            message_id=str(entry_id),
            body=self._body(entry_id, raw),
            receipt_handle=str(entry_id),
            receive_count=receive_count,
        )

    def _body(self, entry_id: str, fields: Mapping[str, str]) -> dict[str, Any]:
        """One entry as a consumer body, or an undecodable marker.

        Never raises.  A stream anyone can ``XADD`` to will eventually contain
        something C did not write, and a poll that threw on it would take down the
        loop instead of parking the one bad entry.
        """

        if not fields:
            # XCLAIM returns a claimed id with no fields when the entry was XDELed
            # out from under the group.  It can never be processed, and it must not
            # be silently acknowledged either.
            return {_UNDECODABLE: f"stream entry {entry_id} no longer exists"}
        try:
            return {"events": [decode_stream_entry(fields)]}
        except RealtimeIngestError as error:
            LOGGER.warning(
                "redis.entry.undecodable",
                extra={"stream": self._stream_key, "entry_id": str(entry_id), "reason": str(error)},
            )
            return {_UNDECODABLE: f"{entry_id}: {error}"}

    # -- completing ---------------------------------------------------------------
    def acknowledge(self, delivery: RealtimeDelivery) -> None:
        """``XACK``, never ``XDEL``.

        C's stream is a shared log: the trading workers read it through their own
        consumer group (``RedisMarketEventPublisherTest`` creates ``trading-workers``
        on the same key).  Deleting an entry D has finished with would delete it out
        from under them.  Retention is C's decision, made with ``XADD MAXLEN``, not a
        consumer's.
        """

        self._client.xack(self._stream_key, self._consumer_group, delivery.receipt_handle)
        self._held.pop(delivery.receipt_handle, None)

    def retry_later(self, delivery: RealtimeDelivery, *, delay_seconds: float) -> None:
        """Make the entry reclaimable again after `delay_seconds`.

        Redis Streams has no visibility timeout, so "later" is expressed by winding
        the entry's idle clock: ``poll`` reclaims at ``claim_min_idle_seconds`` of
        idleness, so setting the idle time to ``claim_min_idle - delay`` makes it
        eligible exactly ``delay`` from now.

        ``JUSTID`` so this does not increment the delivery counter -- this delivery
        has already been counted, and counting the parking of it as well would park
        the entry after half the configured attempts.

        A delay longer than ``claim_min_idle_seconds`` cannot be expressed this way
        (it would need a negative idle time) and is refused.  Quietly retrying sooner
        than asked is how a backoff turns into a hot loop against a failing dependency.
        """

        delay_ms = max(0, int(delay_seconds * 1000))
        if delay_ms > self._claim_min_idle_ms:
            raise RealtimeIngestError(
                f"cannot delay {delivery.message_id} by {delay_seconds}s: this source "
                f"reclaims at claim_min_idle_seconds={self.claim_min_idle_seconds}s, and a "
                "longer delay would need a negative idle time. Raise claim_min_idle_seconds "
                "to at least the retry delay"
            )
        self._client.xclaim(
            name=self._stream_key,
            groupname=self._consumer_group,
            consumername=self._consumer_name,
            min_idle_time=0,
            message_ids=[delivery.receipt_handle],
            idle=self._claim_min_idle_ms - delay_ms,
            justid=True,
        )
        self._held.pop(delivery.receipt_handle, None)

    def dead_letter(self, delivery: RealtimeDelivery, *, reason: str) -> None:
        """Park the entry on D's dead-letter stream, then acknowledge the original.

        The parked entry carries **every field C wrote**, unchanged, plus
        :data:`DEAD_LETTER_FIELDS`.  Replaying it is then a matter of stripping
        exactly those five, rather than reconstructing an envelope from a log line.

        Parking is done here rather than left to any Redis-side mechanism because
        there is no such mechanism: Redis has no redrive policy, so an adapter that
        did not park would retry a poisonous entry until someone noticed.
        """

        if not self._dead_letter_stream_key:
            raise RealtimeIngestError(
                f"stream entry {delivery.message_id} must be parked ({reason}) but no "
                "dead-letter stream is configured; refusing to drop it or to retry it forever"
            )
        raw = self._held.get(delivery.receipt_handle)
        if raw is None:
            raw = {_UNDECODABLE: str(delivery.body.get(_UNDECODABLE, delivery.body))}
        parked: dict[str, str] = {str(name): str(value) for name, value in raw.items()}
        parked.update(
            {
                "deadLetterReason": reason,
                "sourceStream": self._stream_key,
                "sourceStreamEntryId": delivery.message_id,
                "sourceConsumerGroup": self._consumer_group,
                "deliveryCount": str(delivery.receive_count),
            }
        )
        self._client.xadd(self._dead_letter_stream_key, parked)
        LOGGER.error(
            "redis.entry.parked",
            extra={
                "stream": self._stream_key,
                "entry_id": delivery.message_id,
                "dead_letter_stream": self._dead_letter_stream_key,
                "reason": reason,
                "delivery_count": delivery.receive_count,
            },
        )
        self.acknowledge(delivery)

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    # -- reading C's own projections ----------------------------------------------
    def producer_published(self, event_id: str) -> bool:
        """Whether C's de-duplication set records having published `event_id`.

        C's ``SADD`` gate (``RedisMarketEventPublisher.java:46-48``) means this set is
        the producer's own record of what it emitted.  Read-only from D: writing to it
        would make C's next publish of that event a no-op and lose the event outright.
        """

        return bool(self._client.sismember(self._deduplication_key, event_id))

    def latest_observation(self, instrument_id: str, event_type: str) -> dict[str, Any] | None:
        """C's ``:latest:`` hash for one instrument and type, or ``None``.

        The producer's monotone head marker.  It is *not* what D ingests from -- the
        stream is -- but it is what tells an operator how far behind a consumer is
        without replaying anything, and it is the only place ``streamEntryId`` exists.
        """

        fields = self._client.hgetall(self.latest_key(instrument_id, event_type))
        if not fields:
            return None
        return _decode_latest(fields)
