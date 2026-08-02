"""Message-source port for `pipeline-worker`, and its two adapters.

The worker is a durable-queue consumer.  The queue is behind the
:class:`MessageSource` port -- receive / acknowledge / retry_later / dead_letter
/ close -- so the loop is identical whichever adapter is in play.

:class:`InProcessMessageSource`
    A real, working local queue that deliberately reproduces the awkward parts of
    SQS: redelivery of unacknowledged messages, a growing receive count, delayed
    visibility, and a dead-letter side channel.

:class:`SqsMessageSource`
    The DP5 adapter.  It delegates the wire protocol to
    :class:`market_pipeline_lib.realtime_ingest.SqsEventSource` -- long poll,
    explicit visibility timeout, `ApproximateReceiveCount`, and an explicit
    dead-letter hop -- so the worker and the realtime ingestion path share one
    SQS implementation rather than two that drift.

`build_message_source` never degrades to the in-process queue: a worker that
quietly consumed a local queue while operators believed it was draining SQS
would lose every production message.
"""

from __future__ import annotations

import itertools
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from apps.common.errors import ConfigurationError, PortNotConfiguredError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from apps.pipeline_worker.config import WorkerConfig
    from market_pipeline_lib.realtime_ingest import RealtimeDelivery, SqsEventSource


@dataclass(frozen=True)
class Message:
    """One delivery attempt of one queue message."""

    message_id: str
    body: Mapping[str, Any]
    receipt_handle: str
    receive_count: int
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@runtime_checkable
class MessageSource(Protocol):
    """Durable-queue consumer port."""

    def poll(self, max_messages: int, wait_seconds: float) -> Sequence[Message]:
        """Receive up to `max_messages`, waiting at most `wait_seconds`."""

    def acknowledge(self, message: Message) -> None:
        """Delete the message: it must never be delivered again."""

    def retry_later(self, message: Message, *, delay_seconds: float) -> None:
        """Return the message to the queue for another delivery attempt."""

    def dead_letter(self, message: Message, *, reason: str) -> None:
        """Park the message where an operator can find it, and stop delivering it."""

    def close(self) -> None:
        """Release adapter resources."""


@dataclass
class _Envelope:
    message_id: str
    body: Mapping[str, Any]
    receive_count: int = 0
    visible_at: float = 0.0
    in_flight: bool = False


class InProcessMessageSource:
    """Thread-safe in-memory queue with SQS-shaped delivery semantics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._envelopes: dict[str, _Envelope] = {}
        self._order: list[str] = []
        self._receipts = itertools.count(1)
        self._closed = False
        #: `(message, reason)` for everything this queue parked, in order.
        self.dead_letters: list[tuple[Message, str]] = []

    # -- producer side ---------------------------------------------------
    def submit(self, body: Mapping[str, Any], *, message_id: str | None = None) -> str:
        """Enqueue one message.  Returns its message id."""

        if self._closed:
            raise RuntimeError("cannot submit to a closed message source")
        identifier = message_id or f"msg-{uuid.uuid4().hex[:12]}"
        with self._lock:
            if identifier in self._envelopes:
                raise ValueError(f"message id already queued: {identifier}")
            self._envelopes[identifier] = _Envelope(message_id=identifier, body=dict(body))
            self._order.append(identifier)
        return identifier

    def pending(self) -> int:
        """Messages that have not been acknowledged, in flight or not."""

        with self._lock:
            return len(self._envelopes)

    # -- consumer side ---------------------------------------------------
    def poll(self, max_messages: int, wait_seconds: float) -> Sequence[Message]:
        deadline = time.monotonic() + max(wait_seconds, 0.0)
        while True:
            batch = self._take(max_messages)
            if batch or time.monotonic() >= deadline or self._closed:
                return batch
            time.sleep(0.005)

    def _take(self, max_messages: int) -> list[Message]:
        now = time.monotonic()
        taken: list[Message] = []
        with self._lock:
            for identifier in self._order:
                if len(taken) >= max_messages:
                    break
                envelope = self._envelopes.get(identifier)
                if envelope is None or envelope.in_flight or envelope.visible_at > now:
                    continue
                envelope.in_flight = True
                envelope.receive_count += 1
                taken.append(
                    Message(
                        message_id=envelope.message_id,
                        body=envelope.body,
                        receipt_handle=f"rh-{next(self._receipts)}",
                        receive_count=envelope.receive_count,
                    )
                )
        return taken

    def acknowledge(self, message: Message) -> None:
        with self._lock:
            self._envelopes.pop(message.message_id, None)
            if message.message_id in self._order:
                self._order.remove(message.message_id)

    def retry_later(self, message: Message, *, delay_seconds: float) -> None:
        with self._lock:
            envelope = self._envelopes.get(message.message_id)
            if envelope is None:
                return
            envelope.in_flight = False
            envelope.visible_at = time.monotonic() + max(delay_seconds, 0.0)

    def dead_letter(self, message: Message, *, reason: str) -> None:
        with self._lock:
            self._envelopes.pop(message.message_id, None)
            if message.message_id in self._order:
                self._order.remove(message.message_id)
            self.dead_letters.append((message, reason))

    def close(self) -> None:
        self._closed = True


class SqsMessageSource:
    """`MessageSource` over Amazon SQS, sharing the DP5 wire adapter.

    Command envelopes are carried in the message body, so `Message.body` is the
    decoded JSON object.  A body that is not a JSON object is delivered as-is and
    the worker rejects it -- which is what lets a poison message reach the
    dead-letter queue instead of being retried until the queue's retention
    expires.
    """

    def __init__(self, source: SqsEventSource) -> None:
        self._source = source

    @property
    def queue_url(self) -> str:
        # `market_pipeline_lib` is `follow_imports = "skip"` under this repo's mypy
        # configuration until DP2 finishes, so the adapter's own types are opaque here.
        return str(self._source.queue_url)

    @property
    def dead_letter_queue_url(self) -> str | None:
        url = self._source.dead_letter_queue_url
        return None if url is None else str(url)

    @staticmethod
    def _delivery(message: Message) -> RealtimeDelivery:
        # Imported here rather than at module scope: `market_pipeline_lib`
        # transitively imports pyarrow and pandas, and a worker configured for the
        # in-process queue should not pay that on every boot.
        from market_pipeline_lib.realtime_ingest import RealtimeDelivery

        return RealtimeDelivery(
            message_id=message.message_id,
            body=message.body,
            receipt_handle=message.receipt_handle,
            receive_count=message.receive_count,
        )

    def poll(self, max_messages: int, wait_seconds: float) -> Sequence[Message]:
        return [
            Message(
                message_id=delivery.message_id,
                body=delivery.body,
                receipt_handle=delivery.receipt_handle,
                receive_count=delivery.receive_count,
            )
            for delivery in self._source.poll(max_messages=max_messages, wait_seconds=wait_seconds)
        ]

    def acknowledge(self, message: Message) -> None:
        self._source.acknowledge(self._delivery(message))

    def retry_later(self, message: Message, *, delay_seconds: float) -> None:
        self._source.retry_later(self._delivery(message), delay_seconds=delay_seconds)

    def dead_letter(self, message: Message, *, reason: str) -> None:
        self._source.dead_letter(self._delivery(message), reason=reason)

    def close(self) -> None:
        self._source.close()


def build_message_source(config: WorkerConfig) -> MessageSource:
    """Resolve the configured message-source adapter.

    Never degrades to the in-process queue.  A missing AWS SDK is reported as
    :class:`PortNotConfiguredError` -- the port is named and selected, its adapter
    simply cannot be constructed on this machine.
    """

    if config.message_source == "inprocess":
        return InProcessMessageSource()
    if config.message_source == "sqs":
        if not config.queue_url or not config.dead_letter_queue_url:  # pragma: no cover - config guards it
            raise ConfigurationError(
                "PIPELINE_WORKER_MESSAGE_SOURCE=sqs needs both PIPELINE_WORKER_QUEUE_URL and "
                "PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL"
            )
        try:
            import boto3
        except ImportError as error:
            raise PortNotConfiguredError(
                "PIPELINE_WORKER_MESSAGE_SOURCE=sqs needs the AWS SDK, but importing boto3 "
                f"failed ({error}). This worker will not fall back to the in-process queue; "
                "install boto3 or set PIPELINE_WORKER_MESSAGE_SOURCE=inprocess."
            ) from error
        if boto3 is None:  # `sys.modules['boto3'] = None` is how an absent SDK is simulated
            raise PortNotConfiguredError(
                "PIPELINE_WORKER_MESSAGE_SOURCE=sqs needs the AWS SDK, but boto3 is not importable."
            )
        from market_pipeline_lib.realtime_ingest import SqsEventSource

        client = boto3.client(
            "sqs",
            endpoint_url=config.aws_endpoint_url,
            region_name=config.aws_region,
        )
        return SqsMessageSource(
            SqsEventSource(
                client,
                queue_url=config.queue_url,
                dead_letter_queue_url=config.dead_letter_queue_url,
                visibility_timeout_seconds=config.visibility_timeout_seconds,
            )
        )
    raise ConfigurationError(f"unknown message source: {config.message_source!r}")
