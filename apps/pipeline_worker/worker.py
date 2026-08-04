"""The `pipeline-worker` consumer loop.

Lifecycle
---------
1. Resolve the message-source adapter (fails loudly if it is not implemented).
2. Prepare the catalog and object-store roots, start the readiness endpoint,
   then report READY.
3. Receive -> validate -> claim idempotency key -> execute -> acknowledge.
4. On SIGINT/SIGTERM: stop receiving, finish the in-flight batch within the
   configured grace period, return anything unfinished to the queue, report
   STOPPED, stop the readiness endpoint and remove the health file.

Outcomes are explicit:

===================  ==========================================================
outcome              what happened to the message
===================  ==========================================================
``SUCCEEDED``        executed, then deleted
``DUPLICATE``        already handled by this worker, deleted
``REJECTED``         structurally invalid; parked on the dead-letter queue
``FAILED``           execution failed with attempts remaining; made visible again
``DEAD_LETTERED``    failed on its ``max_receive_count``-th delivery; parked
===================  ==========================================================

Nothing is deleted without being executed, rejected or parked, and nothing is
retried forever.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections import deque
from collections.abc import Mapping
from datetime import UTC, datetime
from types import FrameType
from typing import Any

from apps.common.errors import (
    ConfigurationError,
    MalformedEventError,
    PipelineAppError,
    PortNotConfiguredError,
)
from apps.common.idempotency import IdempotencyStore, InMemoryIdempotencyStore
from apps.pipeline_worker.commands import Command, PipelineCommandExecutor
from apps.pipeline_worker.config import WorkerConfig
from apps.pipeline_worker.health import HealthEndpoint, HealthState
from apps.pipeline_worker.messaging import Message, MessageSource, build_message_source

LOGGER = logging.getLogger("apps.pipeline_worker")

#: How many recent command results are retained for inspection and probes.
RESULT_HISTORY = 1_000

OUTCOME_SUCCEEDED = "SUCCEEDED"
OUTCOME_REJECTED = "REJECTED"
OUTCOME_DUPLICATE = "DUPLICATE"
OUTCOME_FAILED = "FAILED"
OUTCOME_DEAD_LETTERED = "DEAD_LETTERED"


class PipelineWorker:
    """Long-running durable-queue consumer for market-data pipeline commands."""

    def __init__(
        self,
        config: WorkerConfig,
        *,
        message_source: MessageSource | None = None,
        executor: PipelineCommandExecutor | None = None,
        idempotency_store: IdempotencyStore | None = None,
    ) -> None:
        self.config = config
        self.health = HealthState()
        self._source = message_source
        self._executor = executor or PipelineCommandExecutor(config)
        self._idempotency: IdempotencyStore = idempotency_store or InMemoryIdempotencyStore(
            max_entries=config.idempotency_cache_size
        )
        self._stop = threading.Event()
        self._stop_reason = "not-stopped"
        self._results: deque[dict[str, Any]] = deque(maxlen=RESULT_HISTORY)
        self._start_lock = threading.Lock()
        self._started = False
        self._health_endpoint: HealthEndpoint | None = (
            None
            if config.health_port is None
            else HealthEndpoint(self.health, host=config.health_host, port=config.health_port)
        )

    # -- public API -------------------------------------------------------
    @property
    def results(self) -> list[dict[str, Any]]:
        return list(self._results)

    @property
    def health_endpoint_port(self) -> int | None:
        """The port the readiness endpoint bound to, or `None` when it is off."""

        return None if self._health_endpoint is None else self._health_endpoint.port

    def request_stop(self, reason: str) -> None:
        """Ask the loop to drain and exit.  Safe from a signal handler."""

        self._stop_reason = reason
        if not self._stop.is_set():
            self._stop.set()
            self.health.mark_draining(reason)

    def run(self) -> int:
        """Run until stopped.  Returns a process exit code."""

        with self._start_lock:
            if self._started:
                raise RuntimeError(
                    "PipelineWorker.run() is not re-entrant; construct a new worker"
                )
            self._started = True

        self._install_signal_handlers()
        try:
            source = self._source or build_message_source(self.config)
        except (ConfigurationError, PortNotConfiguredError) as error:
            self.health.mark_failed(f"{type(error).__name__}: {error}")
            LOGGER.error("worker.message_source.unavailable", exc_info=error)
            raise
        self._source = source

        exit_code = 0
        try:
            self._executor.prepare()
            if self._health_endpoint is not None:
                self._health_endpoint.start()
            self.health.mark_ready()
            self._publish_health()
            LOGGER.info("worker.started", extra={"config": self.config.describe()})
            self._consume(source)
        except PipelineAppError as error:
            exit_code = 1
            self.health.mark_failed(f"{type(error).__name__}: {error}")
            LOGGER.error("worker.failed", exc_info=error)
        finally:
            self._shutdown(source)
        return exit_code

    # -- loop -------------------------------------------------------------
    def _consume(self, source: MessageSource) -> None:
        idle_polls = 0
        while not self._stop.is_set():
            messages = source.poll(
                max_messages=self.config.max_messages_per_poll,
                wait_seconds=self.config.poll_interval_seconds,
            )
            if not messages:
                idle_polls += 1
                if (
                    self.config.exit_after_idle_polls > 0
                    and idle_polls >= self.config.exit_after_idle_polls
                ):
                    self.request_stop("idle-poll-limit")
                    LOGGER.info(
                        "worker.idle_limit_reached",
                        extra={"idle_polls": idle_polls},
                    )
                continue
            idle_polls = 0
            self._process_batch(source, messages, deadline=None)
            self._publish_health()

    def _drain(self, source: MessageSource) -> None:
        """Finish work already received, then hand the rest back to the queue."""

        deadline = time.monotonic() + self.config.shutdown_grace_seconds
        messages = source.poll(max_messages=self.config.max_messages_per_poll, wait_seconds=0.0)
        if messages:
            LOGGER.info("worker.drain.started", extra={"in_flight": len(messages)})
            self._process_batch(source, messages, deadline=deadline)

    def _process_batch(
        self,
        source: MessageSource,
        messages: Any,
        *,
        deadline: float | None,
    ) -> None:
        for message in messages:
            deadline_expired = deadline is not None and time.monotonic() >= deadline
            if self._stop.is_set() or deadline_expired:
                source.retry_later(message, delay_seconds=0.0)
                LOGGER.warning(
                    "worker.drain.requeued",
                    extra={
                        "message_id": message.message_id,
                        "reason": self._stop_reason if self._stop.is_set() else "deadline-expired",
                    },
                )
                continue
            self._process(source, message)

    def _process(self, source: MessageSource, message: Message) -> None:
        started = time.perf_counter()
        try:
            command = Command.parse(message.body, fallback_command_id=message.message_id)
        except MalformedEventError as error:
            # A poison message: another delivery attempt cannot help.  It is
            # parked on the dead-letter queue rather than deleted, so the producer
            # defect is recoverable, and recorded as REJECTED so it is never
            # mistaken for a success.
            self._record(
                command="<malformed>",
                command_id=message.message_id,
                outcome=OUTCOME_REJECTED,
                detail={"code": error.code, "reason": str(error)},
                message=message,
                started=started,
            )
            self._park(source, message, reason=error.code)
            return

        if not self._idempotency.claim(command.command_id):
            self._record(
                command=command.command,
                command_id=command.command_id,
                outcome=OUTCOME_DUPLICATE,
                detail={"reason": "command_id already processed by this worker"},
                message=message,
                started=started,
            )
            source.acknowledge(message)
            return

        try:
            detail = self._executor.execute(command)
        except MalformedEventError as error:
            self._idempotency.forget(command.command_id)
            self._record(
                command=command.command,
                command_id=command.command_id,
                outcome=OUTCOME_REJECTED,
                detail={"code": error.code, "reason": str(error)},
                message=message,
                started=started,
            )
            self._park(source, message, reason=error.code)
            return
        except Exception as error:  # noqa: BLE001 - the loop must survive one bad command
            # Includes PortNotConfiguredError: the command is valid, the worker
            # simply cannot complete it.  It goes back on the queue rather than
            # being answered with an empty success -- until it has used up its
            # deliveries, at which point it is parked rather than retried forever.
            self._idempotency.forget(command.command_id)
            detail = {"code": getattr(error, "code", type(error).__name__), "reason": str(error)}
            exhausted = message.receive_count >= self.config.max_receive_count
            self._record(
                command=command.command,
                command_id=command.command_id,
                outcome=OUTCOME_DEAD_LETTERED if exhausted else OUTCOME_FAILED,
                detail=detail,
                message=message,
                started=started,
            )
            if exhausted:
                self._park(source, message, reason="MAX_RECEIVES_EXCEEDED")
            else:
                source.retry_later(message, delay_seconds=self.config.retry_delay_seconds)
            return

        self._record(
            command=command.command,
            command_id=command.command_id,
            outcome=OUTCOME_SUCCEEDED,
            detail=detail,
            message=message,
            started=started,
        )
        source.acknowledge(message)

    def _park(self, source: MessageSource, message: Message, *, reason: str) -> None:
        """Move a message off the queue for good, preferring the dead-letter queue.

        A source without a dead-letter hop (or one that cannot reach it) must
        still stop cycling the message, so the fallback is an acknowledgement --
        logged at ERROR, because the payload is then only in the log.
        """

        try:
            source.dead_letter(message, reason=reason)
        except Exception as error:  # noqa: BLE001 - parking must never break the loop
            LOGGER.error(
                "worker.dead_letter.unavailable",
                extra={"message_id": message.message_id, "reason": reason, "body": dict(message.body)},
                exc_info=error,
            )
            source.acknowledge(message)
            return
        LOGGER.warning(
            "worker.message.parked",
            extra={
                "message_id": message.message_id,
                "reason": reason,
                "attempt": message.receive_count,
            },
        )

    # -- reporting --------------------------------------------------------
    def _record(
        self,
        *,
        command: str,
        command_id: str,
        outcome: str,
        detail: Mapping[str, Any],
        message: Message,
        started: float,
    ) -> None:
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        result: dict[str, Any] = {
            "command": command,
            "command_id": command_id,
            "outcome": outcome,
            "detail": dict(detail),
            "message_id": message.message_id,
            "attempt": message.receive_count,
            "duration_ms": duration_ms,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        self._results.append(result)
        self.health.record(outcome)
        level = logging.INFO if outcome in {OUTCOME_SUCCEEDED, OUTCOME_DUPLICATE} else logging.ERROR
        LOGGER.log(
            level,
            f"worker.command.{outcome.lower()}",
            extra={
                "command": command,
                "command_id": command_id,
                "message_id": message.message_id,
                "attempt": message.receive_count,
                "duration_ms": duration_ms,
                "detail": dict(detail),
            },
        )

    def _publish_health(self) -> None:
        if self.config.health_file is not None:
            self.health.write_to(self.config.health_file)

    def _remove_health_file(self) -> None:
        if self.config.health_file is not None:
            HealthState.remove(self.config.health_file)

    # -- lifecycle --------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        # Only the main thread may install handlers; tests and embedded uses
        # run the loop in a worker thread and stop it via request_stop().
        if threading.current_thread() is not threading.main_thread():
            return
        # SIGBREAK is the Windows equivalent of a console stop request; without
        # it the worker cannot be drained gracefully on Windows.
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            handler_signal = getattr(signal, name, None)
            if handler_signal is None:  # pragma: no cover - platform dependent
                continue
            try:
                signal.signal(handler_signal, self._handle_signal)
            except (ValueError, OSError):  # pragma: no cover - restricted runtimes
                LOGGER.warning("worker.signal.unavailable", extra={"signal": name})

    def _handle_signal(self, signal_number: int, frame: FrameType | None) -> None:
        self.request_stop(signal.Signals(signal_number).name)

    def _shutdown(self, source: MessageSource) -> None:
        self.health.mark_draining(self._stop_reason)
        try:
            self._drain(source)
        except Exception as error:  # noqa: BLE001 - shutdown must always complete
            LOGGER.error("worker.drain.failed", exc_info=error)
        finally:
            try:
                source.close()
            finally:
                self.health.mark_stopped()
                if self._health_endpoint is not None:
                    self._health_endpoint.stop()
                self._remove_health_file()
                LOGGER.info(
                    "worker.stopped",
                    extra={"reason": self._stop_reason, "health": self.health.snapshot()},
                )
