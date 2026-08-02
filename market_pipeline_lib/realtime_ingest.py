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
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
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
from .watermarks import StreamPosition, WatermarkLedger, WatermarkOutcome

__all__ = [
    "BarFieldMap",
    "ConsumerCycle",
    "DrainReport",
    "IngestDecision",
    "RealtimeDelivery",
    "RealtimeEventSource",
    "RealtimeFlushResult",
    "RealtimeIngestConsumer",
    "RealtimeIngestError",
    "RealtimeIngestSpec",
    "RealtimeIngestor",
    "SqsEventSource",
]

LOGGER = logging.getLogger("market_pipeline_lib.realtime_ingest")

#: Granularities `market_data.partition_granularity` actually has.
PARTITION_GRANULARITIES: tuple[str, ...] = ("DAY", "WEEK", "MONTH", "YEAR")

#: The pipeline run code recorded for a realtime publication.
REALTIME_RUN_CODE = "MARKET_DATA_REALTIME_INGEST"

#: How the contract's own resolution reads as an ISO-8601 duration, so a spec
#: cannot silently declare `PT1M` for a 30-minute dataset.
_RESOLUTION_DURATIONS: dict[str, str] = {
    "30m": "PT30M",
    "1h": "PT1H",
    "4h": "PT4H",
    "1d": "P1D",
}

_ISO_DURATION = re.compile(r"^P(?!$)(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")

_EVENT_FIELDS = (
    "eventId",
    "instrumentId",
    "eventType",
    "providerEventId",
    "occurredAt",
    "sequence",
    "values",
)


class RealtimeIngestError(ValueError):
    """A realtime event, spec or delivery could not be handled."""


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
    source_resolution: str
    partition_granularity: Granularity
    fields: BarFieldMap

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise RealtimeIngestError("event_type must name the event this stream ingests")
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
    """What the ingestor did with one event."""

    event_id: str
    instrument_id: str
    shard_key: str
    accepted: bool
    reason: str
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
    if key not in values:
        raise RealtimeIngestError(f"{label}.values is missing the mapped field {key!r}")
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RealtimeIngestError(f"{label}.values.{key} must be numeric, got {type(value).__name__}")
    return float(value)


def _integer(values: Mapping[str, Any], key: str, label: str) -> int:
    if key not in values:
        raise RealtimeIngestError(f"{label}.values is missing the mapped field {key!r}")
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise RealtimeIngestError(f"{label}.values.{key} must be an integer, got {type(value).__name__}")
    return value


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
    ) -> None:
        self._engine = engine
        self._spec = spec
        self._ledger = ledger
        self._shard_count = engine.config.shard_count
        self._symbols = {
            mapping.instrument_id: canonical_provider_symbol(mapping.provider_symbol)
            for mapping in engine.mappings.values()
        }
        # (partition_start, partition_end, shard_key) -> {(instrument, bar_start): row}
        self._buffer: dict[tuple[date, date, str], dict[tuple[str, datetime], dict[str, Any]]] = {}

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
        """Validate, classify and buffer one provider-neutral market event."""

        if not isinstance(event, Mapping):
            raise RealtimeIngestError(f"a market event must be an object, got {type(event).__name__}")
        label = "market event"
        missing = [name for name in _EVENT_FIELDS if name not in event]
        if missing:
            raise RealtimeIngestError(f"{label} is missing required field(s) {missing}")

        event_id = _text(event, "eventId", label)
        instrument_id = _text(event, "instrumentId", label)
        event_type = _text(event, "eventType", label)
        occurred_at = _utc(event, "occurredAt", label)
        sequence = event.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise RealtimeIngestError(f"{label}.sequence must be a non-negative integer")

        symbol = self._symbols.get(instrument_id)
        if symbol is None:
            raise RealtimeIngestError(
                f"{label}.instrumentId {instrument_id} is not in the instrument map; "
                "an unmapped instrument cannot be sharded or published"
            )
        shard_key = stable_shard_key(instrument_id, self._shard_count)

        if event_type != self._spec.event_type:
            # A different event type on the same stream is not an error -- this
            # stream simply does not turn it into a bar.  It is still reported.
            return IngestDecision(
                event_id=event_id,
                instrument_id=instrument_id,
                shard_key=shard_key,
                accepted=False,
                reason="EVENT_TYPE_NOT_INGESTED",
            )

        values = event.get("values")
        if not isinstance(values, Mapping):
            raise RealtimeIngestError(f"{label}.values must be an object")
        row = self._bar_row(instrument_id, symbol, occurred_at, values, label)

        decision = self._ledger.observe(
            shard_key, StreamPosition(source_event_at=occurred_at, sequence=sequence)
        )
        if not decision.should_process:
            return IngestDecision(
                event_id=event_id,
                instrument_id=instrument_id,
                shard_key=shard_key,
                accepted=False,
                reason=decision.outcome.value,
                outcome=decision.outcome,
            )

        session_date = occurred_at.astimezone(ET).date()
        start, end = partition_bounds(session_date, self._spec.partition_granularity)
        self._buffer.setdefault((start, end, shard_key), {})[(instrument_id, occurred_at)] = row
        return IngestDecision(
            event_id=event_id,
            instrument_id=instrument_id,
            shard_key=shard_key,
            accepted=True,
            reason=decision.outcome.value,
            outcome=decision.outcome,
        )

    def submit_batch(self, events: Iterable[Mapping[str, Any]]) -> list[IngestDecision]:
        return [self.submit(event) for event in events]

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
      to parse, is **parked immediately** -- another delivery cannot fix it;
    * any other failure is **retried** until ``receive_count`` reaches
      ``max_receive_count``, then parked with ``MAX_RECEIVES_EXCEEDED``;
    * a handled message is acknowledged, and redelivery of one already handled is
      absorbed by the watermark rather than duplicating a row.
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
                decisions = [self.ingestor.submit(event) for event in events]
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
