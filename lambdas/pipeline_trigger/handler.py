"""`pipeline-trigger` Lambda.

Turns an EventBridge schedule (or a manual invocation) into exactly one command
on the `pipeline-worker` queue.

Event (camelCase, matching the project's messaging convention)::

    {
      "triggerId":   "nightly-2026-08-02",       # required, the idempotency key
      "command":     "VALIDATE_CATALOG",         # required, one of SUPPORTED_COMMANDS
      "requestedAt": "2026-08-02T06:00:00Z",     # required, UTC ISO-8601
      "payload":     { ... }                     # optional command arguments
    }

Any other shape is rejected with :class:`MalformedEventError`.  Unknown fields
are rejected too: a producer that starts sending a field this version does not
understand must fail visibly rather than have it dropped.

The queue itself is behind :class:`CommandSink`.  The SQS adapter is DP5; until
then the default adapter raises :class:`PortNotConfiguredError`, so a trigger
fired against an unwired deployment fails the invocation instead of reporting a
scheduled run that never happened.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from apps.common.errors import PortNotConfiguredError
from apps.common.events import (
    reject_unknown_fields,
    require_enum,
    require_identifier,
    require_mapping,
    require_utc_timestamp,
)
from apps.common.idempotency import IdempotencyStore, InMemoryIdempotencyStore
from apps.pipeline_worker.commands import SUPPORTED_COMMANDS
from lambdas.common import STATUS_DUPLICATE, ResultCache, lambda_result

LOGGER = logging.getLogger("lambdas.pipeline_trigger")

HANDLER_NAME = "pipeline-trigger"
EVENT_FIELDS: tuple[str, ...] = ("triggerId", "command", "requestedAt", "payload")
STATUS_ENQUEUED = "ENQUEUED"


@runtime_checkable
class CommandSink(Protocol):
    """Port for the durable queue the worker consumes."""

    def send(self, command: Mapping[str, Any]) -> str:
        """Enqueue one command and return the provider's message id."""


class UnconfiguredCommandSink:
    """Default adapter: refuses, loudly, naming the stage that supplies one."""

    def send(self, command: Mapping[str, Any]) -> str:
        raise PortNotConfiguredError(
            "pipeline-trigger has no CommandSink adapter. The SQS queue that "
            "pipeline-worker consumes is delivered in DP5 (D11/D12/D90). This Lambda "
            "will not pretend a command was scheduled: inject a CommandSink into "
            "PipelineTriggerHandler to enable it."
        )


class PipelineTriggerHandler:
    """Validates a trigger event and enqueues exactly one worker command."""

    def __init__(
        self,
        *,
        command_sink: CommandSink | None = None,
        idempotency_store: IdempotencyStore | None = None,
        result_cache: ResultCache | None = None,
    ) -> None:
        self._sink: CommandSink = command_sink or UnconfiguredCommandSink()
        self._idempotency: IdempotencyStore = idempotency_store or InMemoryIdempotencyStore()
        self._results = result_cache or ResultCache()

    def handle(self, event: Any, context: Any = None) -> dict[str, Any]:
        document = require_mapping(event, "pipeline-trigger event")
        reject_unknown_fields(document, EVENT_FIELDS, "pipeline-trigger event")
        trigger_id = require_identifier(document, "triggerId", "pipeline-trigger event")
        command = require_enum(
            document, "command", SUPPORTED_COMMANDS, "pipeline-trigger event"
        )
        requested_at = require_utc_timestamp(
            document, "requestedAt", "pipeline-trigger event"
        )
        payload = require_mapping(
            document.get("payload", {}), "pipeline-trigger event.payload"
        )

        if not self._idempotency.claim(trigger_id):
            remembered = self._results.recall(trigger_id) or {
                "reason": "triggerId already enqueued by this container"
            }
            LOGGER.info(
                "pipeline_trigger.duplicate",
                extra={"trigger_id": trigger_id, "command": command},
            )
            return lambda_result(
                handler=HANDLER_NAME,
                status=STATUS_DUPLICATE,
                idempotency_key=trigger_id,
                context=context,
                result=remembered,
            )

        message: dict[str, Any] = {
            "command": command,
            "command_id": trigger_id,
            "issued_at": requested_at.isoformat().replace("+00:00", "Z"),
            "payload": dict(payload),
        }
        try:
            provider_message_id = self._sink.send(message)
        except Exception:
            # Release the claim so the retry is a real attempt, not a duplicate.
            self._idempotency.forget(trigger_id)
            LOGGER.error(
                "pipeline_trigger.enqueue_failed",
                extra={"trigger_id": trigger_id, "command": command},
                exc_info=True,
            )
            raise

        result = {
            "command": command,
            "providerMessageId": provider_message_id,
            "requestedAt": message["issued_at"],
        }
        self._results.remember(trigger_id, result)
        LOGGER.info(
            "pipeline_trigger.enqueued",
            extra={
                "trigger_id": trigger_id,
                "command": command,
                "provider_message_id": provider_message_id,
            },
        )
        return lambda_result(
            handler=HANDLER_NAME,
            status=STATUS_ENQUEUED,
            idempotency_key=trigger_id,
            context=context,
            result=result,
        )


#: Warm-container singleton so redelivery to the same container is idempotent.
_DEFAULT_HANDLER = PipelineTriggerHandler()


def handler(event: Any, context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point."""

    return _DEFAULT_HANDLER.handle(event, context)
