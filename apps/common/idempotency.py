"""At-least-once delivery protection.

Both SQS and EventBridge deliver at least once, so every handler in this bundle
claims an idempotency key before it does any work and reports `DUPLICATE`
instead of repeating a side effect.

`InMemoryIdempotencyStore` is process-local and is correct for a single worker
process and for Lambda warm-container redelivery within one container.  The
durable store (a `market_data` table or DynamoDB) is a later stage; it drops in
behind the same `IdempotencyStore` protocol.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Protocol, runtime_checkable


@runtime_checkable
class IdempotencyStore(Protocol):
    """Claim-once semantics for a delivery key."""

    def claim(self, key: str) -> bool:
        """Return True when `key` was claimed by this call, False if already seen."""

    def seen(self, key: str) -> bool:
        """Return True when `key` has already been claimed."""

    def forget(self, key: str) -> None:
        """Release a claim so the message can be retried."""


class InMemoryIdempotencyStore:
    """Thread-safe bounded LRU of claimed keys."""

    def __init__(self, max_entries: int = 10_000) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._lock = threading.Lock()
        self._keys: OrderedDict[str, None] = OrderedDict()

    def claim(self, key: str) -> bool:
        if not key:
            raise ValueError("idempotency key must not be empty")
        with self._lock:
            if key in self._keys:
                self._keys.move_to_end(key)
                return False
            self._keys[key] = None
            while len(self._keys) > self._max_entries:
                self._keys.popitem(last=False)
            return True

    def seen(self, key: str) -> bool:
        with self._lock:
            return key in self._keys

    def forget(self, key: str) -> None:
        with self._lock:
            self._keys.pop(key, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._keys)
