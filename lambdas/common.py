"""Shared result envelope and redelivery cache for the D-bundle Lambdas.

Every handler returns the same JSON-serialisable envelope so a consumer can
route on `handler`/`status` without knowing which Lambda produced it.  Failures
are *not* expressed in the envelope: a malformed event or an unwired port
raises, so the invocation is recorded as failed and the message is retried or
dead-lettered instead of being silently accepted.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

#: Envelope schema version.  Bump when a field is removed or its meaning changes.
RESULT_SCHEMA_VERSION = 1

STATUS_DUPLICATE = "DUPLICATE"


def request_id(context: Any) -> str:
    """Best-effort AWS request id.  Local invocations report ``local``."""

    value = getattr(context, "aws_request_id", None)
    return value if isinstance(value, str) and value else "local"


def lambda_result(
    *,
    handler: str,
    status: str,
    idempotency_key: str,
    context: Any,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schemaVersion": RESULT_SCHEMA_VERSION,
        "handler": handler,
        "status": status,
        "idempotencyKey": idempotency_key,
        "requestId": request_id(context),
        "completedAt": datetime.now(UTC).isoformat(),
        "result": dict(result),
    }


class ResultCache:
    """Bounded key -> result map so a redelivery replays the first decision.

    Process-local, which is the correct scope for Lambda warm-container
    redelivery.  Cross-container idempotency needs a durable store; that lands
    with the `market_data` persistence work and slots in behind the same two
    methods.
    """

    def __init__(self, max_entries: int = 1_000) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, Mapping[str, Any]] = OrderedDict()

    def remember(self, key: str, value: Mapping[str, Any]) -> None:
        with self._lock:
            self._entries[key] = dict(value)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def recall(self, key: str) -> Mapping[str, Any] | None:
        with self._lock:
            value = self._entries.get(key)
            if value is None:
                return None
            self._entries.move_to_end(key)
            return dict(value)

    def forget(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)
