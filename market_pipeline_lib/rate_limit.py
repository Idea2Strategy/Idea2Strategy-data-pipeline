"""Proactive, thread-safe token-bucket rate limiting with an injectable clock.

The provider adapter this replaces slept a fixed 0.35s *after* a successful
call, which is not rate limiting: it costs latency on every request, does no
requests-per-minute accounting, and silently stops throttling the moment a
call raises. A token bucket instead blocks *before* the request is issued, so
the configured requests-per-minute ceiling holds across retries, failures and
concurrent workers alike.

Time is reached only through the `Clock` protocol. Production passes
`SYSTEM_CLOCK`; tests pass `ManualClock`, which makes the admission schedule
exactly reproducible and keeps the suite free of real sleeping.
"""

from __future__ import annotations

import threading
import time
from typing import Protocol, runtime_checkable

__all__ = [
    "SYSTEM_CLOCK",
    "Clock",
    "ManualClock",
    "SystemClock",
    "TokenBucketRateLimiter",
]


@runtime_checkable
class Clock(Protocol):
    """The only time surface the rate limiter and the Alpaca client may use."""

    def monotonic(self) -> float:
        """Return a monotonically non-decreasing time in seconds."""

    def sleep(self, seconds: float) -> None:
        """Block for `seconds`; a non-positive duration must return at once."""


class SystemClock:
    """The production clock: `time.monotonic` plus `time.sleep`."""

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


SYSTEM_CLOCK = SystemClock()


class ManualClock:
    """A deterministic clock driven by the caller instead of by wall time.

    `sleep` records the requested duration and, unless `advance_on_sleep` is
    disabled, moves the clock forward by it. A frozen clock is what makes the
    concurrency test deterministic: no refill can occur between admissions, so
    the wait every caller receives depends only on the admission order.

    This lives beside the limiter rather than in the test tree because
    determinism under an injected clock is part of the limiter's contract, and
    two separate test modules plus any future engine-level test need one
    shared implementation of it.
    """

    def __init__(self, *, start: float = 0.0, advance_on_sleep: bool = True) -> None:
        self._now = float(start)
        self._advance_on_sleep = advance_on_sleep
        self._lock = threading.Lock()
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        with self._lock:
            return self._now

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("sleep 시간은 음수일 수 없습니다.")
        with self._lock:
            self.sleeps.append(seconds)
            if self._advance_on_sleep:
                self._now += seconds

    def advance(self, seconds: float) -> None:
        """Move the clock forward without recording a sleep."""
        if seconds < 0:
            raise ValueError("advance 시간은 음수일 수 없습니다.")
        with self._lock:
            self._now += seconds


class TokenBucketRateLimiter:
    """Admit at most `requests_per_minute` calls per minute, bursting to `burst`.

    `acquire` reserves its tokens under the lock and sleeps outside it, so a
    waiting caller never blocks the accounting of another. Reservations are
    allowed to drive the balance negative: that is what keeps concurrent
    callers from all measuring the same deficit and then all being admitted.
    """

    def __init__(
        self,
        requests_per_minute: int,
        *,
        burst: int | None = None,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute는 1 이상이어야 합니다.")
        capacity = requests_per_minute if burst is None else burst
        if capacity < 1:
            raise ValueError("burst는 1 이상이어야 합니다.")
        self._requests_per_minute = requests_per_minute
        self._capacity = float(capacity)
        self._rate = requests_per_minute / 60.0
        self._tokens = float(capacity)
        self._clock = clock
        self._updated = clock.monotonic()
        self._lock = threading.Lock()

    @property
    def requests_per_minute(self) -> int:
        return self._requests_per_minute

    @property
    def capacity(self) -> int:
        return int(self._capacity)

    def available_tokens(self) -> float:
        """Current balance after refill; negative while reservations are queued."""
        with self._lock:
            self._refill_locked()
            return self._tokens

    def acquire(self, tokens: int = 1) -> float:
        """Block until `tokens` are available and return the seconds waited."""
        if tokens < 1:
            raise ValueError("tokens는 1 이상이어야 합니다.")
        if tokens > self._capacity:
            raise ValueError(
                f"한 번에 burst({self.capacity})보다 많은 토큰을 요청할 수 없습니다: {tokens}"
            )
        with self._lock:
            self._refill_locked()
            deficit = tokens - self._tokens
            self._tokens -= tokens
            wait = deficit / self._rate if deficit > 0 else 0.0
        if wait > 0:
            self._clock.sleep(wait)
        return wait

    def _refill_locked(self) -> None:
        now = self._clock.monotonic()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._updated = now
