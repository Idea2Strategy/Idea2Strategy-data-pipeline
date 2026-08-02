"""Deterministic coverage for the proactive token-bucket rate limiter.

Every assertion here is against a hardcoded expected value produced by hand
from the bucket parameters, never by re-running the production formula, and no
test touches a real clock: `ManualClock` is injected everywhere.
"""

from __future__ import annotations

import threading
import unittest

from market_pipeline_lib.rate_limit import ManualClock, TokenBucketRateLimiter


class ManualClockTests(unittest.TestCase):
    def test_sleep_advances_the_clock_and_is_recorded(self) -> None:
        clock = ManualClock()

        self.assertEqual(clock.monotonic(), 0.0)
        clock.sleep(1.5)
        clock.sleep(0.5)

        self.assertEqual(clock.monotonic(), 2.0)
        self.assertEqual(clock.sleeps, [1.5, 0.5])

    def test_frozen_clock_records_sleeps_without_advancing(self) -> None:
        clock = ManualClock(advance_on_sleep=False)

        clock.sleep(3.0)

        self.assertEqual(clock.monotonic(), 0.0)
        self.assertEqual(clock.sleeps, [3.0])

    def test_negative_sleep_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "음수"):
            ManualClock().sleep(-1.0)


class TokenBucketConstructionTests(unittest.TestCase):
    def test_requests_per_minute_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "requests_per_minute"):
            TokenBucketRateLimiter(0, clock=ManualClock())

    def test_burst_must_be_at_least_one(self) -> None:
        with self.assertRaisesRegex(ValueError, "burst"):
            TokenBucketRateLimiter(60, burst=0, clock=ManualClock())

    def test_burst_defaults_to_one_minute_of_requests(self) -> None:
        limiter = TokenBucketRateLimiter(120, clock=ManualClock())

        self.assertEqual(limiter.capacity, 120)
        self.assertEqual(limiter.requests_per_minute, 120)

    def test_acquiring_more_than_the_burst_can_never_succeed(self) -> None:
        limiter = TokenBucketRateLimiter(60, burst=4, clock=ManualClock())

        with self.assertRaisesRegex(ValueError, "burst"):
            limiter.acquire(5)

    def test_acquire_rejects_non_positive_token_counts(self) -> None:
        limiter = TokenBucketRateLimiter(60, clock=ManualClock())

        with self.assertRaisesRegex(ValueError, "tokens"):
            limiter.acquire(0)


class TokenBucketAdmissionTests(unittest.TestCase):
    def test_admits_the_whole_window_then_blocks_the_next_request(self) -> None:
        # 60 requests/minute == one token per second, burst 5.
        clock = ManualClock()
        limiter = TokenBucketRateLimiter(60, burst=5, clock=clock)

        immediate = [limiter.acquire() for _ in range(5)]

        self.assertEqual(immediate, [0.0, 0.0, 0.0, 0.0, 0.0])
        self.assertEqual(clock.sleeps, [])
        self.assertEqual(clock.monotonic(), 0.0)

        blocked = limiter.acquire()

        self.assertEqual(blocked, 1.0)
        self.assertEqual(clock.sleeps, [1.0])
        self.assertEqual(clock.monotonic(), 1.0)

    def test_blocking_happens_before_the_call_is_admitted(self) -> None:
        clock = ManualClock()
        limiter = TokenBucketRateLimiter(60, burst=1, clock=clock)
        admitted_at: list[float] = []

        for _ in range(3):
            limiter.acquire()
            admitted_at.append(clock.monotonic())

        # The first call is free; each later call is admitted only after the
        # clock has been advanced past its token's refill time.
        self.assertEqual(admitted_at, [0.0, 1.0, 2.0])

    def test_waiting_longer_than_the_window_refills_only_up_to_the_burst(self) -> None:
        clock = ManualClock()
        limiter = TokenBucketRateLimiter(60, burst=3, clock=clock)

        for _ in range(3):
            limiter.acquire()
        clock.advance(600.0)  # ten idle minutes: 600 tokens of refill offered

        immediate = [limiter.acquire() for _ in range(3)]
        blocked = limiter.acquire()

        self.assertEqual(immediate, [0.0, 0.0, 0.0])
        self.assertEqual(blocked, 1.0)

    def test_partial_refill_shortens_the_wait(self) -> None:
        clock = ManualClock()
        limiter = TokenBucketRateLimiter(60, burst=1, clock=clock)

        limiter.acquire()
        clock.advance(0.25)

        self.assertEqual(limiter.acquire(), 0.75)

    def test_multi_token_acquire_waits_for_every_token(self) -> None:
        clock = ManualClock()
        limiter = TokenBucketRateLimiter(120, burst=4, clock=clock)  # 2 tokens/s

        self.assertEqual(limiter.acquire(4), 0.0)
        self.assertEqual(limiter.acquire(3), 1.5)

    def test_available_tokens_reports_the_reservation_deficit(self) -> None:
        clock = ManualClock(advance_on_sleep=False)
        limiter = TokenBucketRateLimiter(60, burst=2, clock=clock)

        limiter.acquire()
        limiter.acquire()
        limiter.acquire()

        self.assertEqual(limiter.available_tokens(), -1.0)


class TokenBucketThreadSafetyTests(unittest.TestCase):
    def test_concurrent_acquires_issue_every_token_exactly_once(self) -> None:
        # The clock is frozen, so refill contributes nothing and the wait each
        # caller receives is fully determined by its position in the admission
        # order. 8 burst tokens are free; the remaining 12 callers must be
        # spaced by 1/rate = 0.1s each. Any lost update or double-issued token
        # would change this multiset.
        clock = ManualClock(advance_on_sleep=False)
        limiter = TokenBucketRateLimiter(600, burst=8, clock=clock)
        waits: list[float] = []
        waits_lock = threading.Lock()
        start = threading.Barrier(20)

        def worker() -> None:
            start.wait()
            wait = limiter.acquire()
            with waits_lock:
                waits.append(wait)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        expected = [0.0] * 8 + [round(index / 10.0, 6) for index in range(1, 13)]
        self.assertEqual(sorted(round(value, 6) for value in waits), expected)
        self.assertEqual(sorted(round(value, 6) for value in clock.sleeps), expected[8:])


if __name__ == "__main__":
    unittest.main()
