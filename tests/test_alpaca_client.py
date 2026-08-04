"""Coverage for the canonical Alpaca bars client (card D05).

The adapter this replaces (`market_pipeline_lib.engine.AlpacaBarSource`) had no
tests at all: every engine test injected a fake source, so its retry loop, its
`except Exception` swallow and its post-hoc `time.sleep(0.35)` were never
exercised. Everything here drives the real client through an injected
`httpx.MockTransport` and an injected `ManualClock`, so no test sleeps on a
real clock and no test reaches the network.
"""

from __future__ import annotations

import json
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from market_pipeline_lib.alpaca import (
    AlpacaAuthError,
    AlpacaBarsClient,
    AlpacaBarSource,
    AlpacaClientConfig,
    AlpacaCorporateActionsClient,
    AlpacaRequestError,
    AlpacaResponseError,
    AlpacaRetriesExhausted,
    PermanentAlpacaError,
    backoff_seconds,
    map_page,
    parse_retry_after,
    parse_utc_timestamp,
)
from market_pipeline_lib.rate_limit import ManualClock, TokenBucketRateLimiter

API_KEY = "AKTESTKEYID000001"
API_SECRET = "s3cr3t-alpaca-secret-value"

START = datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
END = datetime(2024, 1, 3, 14, 30, tzinfo=UTC)


def bar_payload(timestamp: str, close: float) -> dict[str, Any]:
    return {
        "t": timestamp,
        "o": 100.0,
        "h": 101.5,
        "l": 99.5,
        "c": close,
        "v": 1200,
        "n": 42,
        "vw": 100.25,
    }


def page(bars: dict[str, list[dict[str, Any]]], token: str | None) -> dict[str, Any]:
    return {"bars": bars, "next_page_token": token}


class ScriptedTransport(httpx.MockTransport):
    """Replay a fixed script of responses and record every request made."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses)
        super().__init__(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError(
                f"unscripted Alpaca request #{len(self.requests)}: {request.url}"
            )
        return self._responses.pop(0)

    @property
    def remaining(self) -> int:
        return len(self._responses)


def json_response(status: int, payload: dict[str, Any]) -> httpx.Response:
    return httpx.Response(status, json=payload)


def error_response(status: int, *, retry_after: str | None = None) -> httpx.Response:
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return httpx.Response(status, json={"message": "boom"}, headers=headers)


def make_client(
    responses: list[httpx.Response],
    *,
    clock: ManualClock | None = None,
    config: AlpacaClientConfig | None = None,
    jitter: float = 0.0,
    requests_per_minute: int = 6000,
    burst: int | None = None,
) -> tuple[AlpacaBarsClient, ScriptedTransport, ManualClock]:
    clock = clock or ManualClock()
    transport = ScriptedTransport(responses)
    limiter = TokenBucketRateLimiter(
        requests_per_minute,
        burst=burst,
        clock=clock,
    )
    client = AlpacaBarsClient(
        API_KEY,
        API_SECRET,
        config=config or AlpacaClientConfig(),
        transport=transport,
        clock=clock,
        rate_limiter=limiter,
        jitter=lambda: jitter,
    )
    return client, transport, clock


class BackoffScheduleTests(unittest.TestCase):
    def test_schedule_matches_the_pinned_values_without_jitter(self) -> None:
        schedule = [
            backoff_seconds(
                attempt,
                base=1.0,
                maximum=16.0,
                jitter_span=0.25,
                jitter_fraction=0.0,
            )
            for attempt in range(1, 8)
        ]

        self.assertEqual(schedule, [1.0, 2.0, 4.0, 8.0, 16.0, 16.0, 16.0])

    def test_schedule_matches_the_pinned_values_with_jitter(self) -> None:
        schedule = [
            backoff_seconds(
                attempt,
                base=1.0,
                maximum=16.0,
                jitter_span=0.25,
                jitter_fraction=0.5,
            )
            for attempt in range(1, 6)
        ]

        self.assertEqual(schedule, [1.125, 2.125, 4.125, 8.125, 16.125])

    def test_attempt_and_jitter_fraction_are_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "attempt"):
            backoff_seconds(0, base=1.0, maximum=16.0, jitter_span=0.25, jitter_fraction=0.0)
        with self.assertRaisesRegex(ValueError, "jitter_fraction"):
            backoff_seconds(1, base=1.0, maximum=16.0, jitter_span=0.25, jitter_fraction=1.0)


class RetryAfterParsingTests(unittest.TestCase):
    def test_positive_numeric_header_is_honoured(self) -> None:
        self.assertEqual(parse_retry_after("7"), 7.0)
        self.assertEqual(parse_retry_after("2.5"), 2.5)

    def test_absent_zero_negative_and_http_date_headers_fall_back(self) -> None:
        # Alpaca sends delta-seconds. An HTTP-date form is deliberately *not*
        # honoured; it falls back to the backoff schedule rather than being
        # mis-parsed into a wrong (possibly enormous) delay.
        self.assertIsNone(parse_retry_after(None))
        self.assertIsNone(parse_retry_after(""))
        self.assertIsNone(parse_retry_after("0"))
        self.assertIsNone(parse_retry_after("-5"))
        self.assertIsNone(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"))


class ThrottlingAndRetryTests(unittest.TestCase):
    def test_429_with_retry_after_waits_exactly_that_long(self) -> None:
        client, transport, clock = make_client(
            [
                error_response(429, retry_after="7"),
                json_response(200, page({"AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)]}, None)),
            ]
        )

        pages = list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(pages), 1)
        self.assertEqual(clock.sleeps, [7.0])
        self.assertEqual(len(transport.requests), 2)

    def test_429_retry_after_is_capped_by_configuration(self) -> None:
        config = AlpacaClientConfig(max_retry_after_seconds=30.0)
        client, _transport, clock = make_client(
            [
                error_response(429, retry_after="900"),
                json_response(200, page({"AAPL": []}, None)),
            ],
            config=config,
        )

        list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(clock.sleeps, [30.0])

    def test_429_without_retry_after_uses_the_backoff_schedule(self) -> None:
        client, _transport, clock = make_client(
            [
                error_response(429),
                json_response(200, page({"AAPL": []}, None)),
            ],
            jitter=0.5,
        )

        list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(clock.sleeps, [1.125])

    def test_500_is_retried_and_then_succeeds(self) -> None:
        client, transport, clock = make_client(
            [
                error_response(500),
                error_response(500),
                json_response(200, page({"AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)]}, None)),
            ]
        )

        pages = list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(pages), 1)
        self.assertEqual(clock.sleeps, [1.0, 2.0])
        self.assertEqual(len(transport.requests), 3)

    def test_retry_budget_is_exhausted_with_the_pinned_backoff_schedule(self) -> None:
        config = AlpacaClientConfig(max_attempts=5)
        client, transport, clock = make_client(
            [error_response(503) for _ in range(5)],
            config=config,
            jitter=0.5,
        )

        with self.assertRaises(AlpacaRetriesExhausted) as caught:
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertFalse(isinstance(caught.exception, PermanentAlpacaError))
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(len(transport.requests), 5)
        self.assertEqual(clock.sleeps, [1.125, 2.125, 4.125, 8.125])

    def test_transport_timeout_is_retried_then_surfaces_as_transient(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectTimeout("connect timed out", request=request)

        clock = ManualClock()
        client = AlpacaBarsClient(
            API_KEY,
            API_SECRET,
            config=AlpacaClientConfig(max_attempts=3),
            transport=httpx.MockTransport(handler),
            clock=clock,
            rate_limiter=TokenBucketRateLimiter(6000, clock=clock),
            jitter=lambda: 0.0,
        )

        with self.assertRaises(AlpacaRetriesExhausted):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(clock.sleeps, [1.0, 2.0])


class AuthAndPermanentFailureTests(unittest.TestCase):
    def test_401_raises_immediately_without_retry(self) -> None:
        client, transport, clock = make_client([error_response(401)])

        with self.assertRaises(AlpacaAuthError) as caught:
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(clock.sleeps, [])
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(caught.exception.code, "ALPACA_AUTH_FAILED")

    def test_403_raises_immediately_without_retry(self) -> None:
        client, transport, clock = make_client([error_response(403)])

        with self.assertRaises(AlpacaAuthError):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(clock.sleeps, [])

    def test_auth_error_is_not_a_transient_error(self) -> None:
        self.assertTrue(issubclass(AlpacaAuthError, PermanentAlpacaError))
        self.assertFalse(AlpacaAuthError("x").retryable)

    def test_400_is_a_permanent_request_error(self) -> None:
        client, transport, _clock = make_client([error_response(400)])

        with self.assertRaises(AlpacaRequestError):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 1)

    def test_unexpected_client_status_is_permanent(self) -> None:
        client, transport, _clock = make_client([error_response(404)])

        with self.assertRaises(AlpacaResponseError):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 1)

    def test_non_json_body_is_a_permanent_response_error(self) -> None:
        client, _transport, _clock = make_client(
            [httpx.Response(200, content=b"<html>not json</html>")]
        )

        with self.assertRaises(AlpacaResponseError):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

    def test_malformed_bars_container_is_a_permanent_response_error(self) -> None:
        client, _transport, _clock = make_client(
            [json_response(200, {"bars": [], "next_page_token": None})]
        )

        with self.assertRaises(AlpacaResponseError):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

    def test_missing_credentials_fail_before_any_request(self) -> None:
        with self.assertRaises(AlpacaAuthError):
            AlpacaBarsClient("", API_SECRET, transport=ScriptedTransport([]))

    def test_unsupported_adjustment_and_range_are_rejected(self) -> None:
        client, transport, _clock = make_client([])

        with self.assertRaises(AlpacaRequestError):
            list(client.iter_bar_pages(["AAPL"], START, END, "sideways"))
        with self.assertRaises(AlpacaRequestError):
            list(client.iter_bar_pages(["AAPL"], END, START, "raw"))
        with self.assertRaises(AlpacaRequestError):
            list(client.iter_bar_pages(["AAPL"], START.replace(tzinfo=None), END, "raw"))
        with self.assertRaises(AlpacaRequestError):
            list(client.iter_bar_pages([], START, END, "raw"))

        self.assertEqual(transport.requests, [])


class CredentialLeakTests(unittest.TestCase):
    def test_credentials_are_sent_as_headers_but_never_rendered_or_logged(self) -> None:
        client, transport, _clock = make_client(
            [
                error_response(503),
                json_response(200, page({"AAPL": []}, None)),
            ]
        )

        with self.assertLogs("market_pipeline_lib.alpaca.client", level="DEBUG") as logs:
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(transport.requests[0].headers["APCA-API-KEY-ID"], API_KEY)
        rendered = "\n".join(
            [repr(client), str(client), *logs.output, str(transport.requests[0].url)]
        )
        self.assertNotIn(API_KEY, rendered)
        self.assertNotIn(API_SECRET, rendered)

    def test_auth_error_message_carries_no_credential(self) -> None:
        client, _transport, _clock = make_client([error_response(401)])

        with self.assertRaises(AlpacaAuthError) as caught:
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertNotIn(API_KEY, str(caught.exception))
        self.assertNotIn(API_SECRET, str(caught.exception))


class RateLimiterIntegrationTests(unittest.TestCase):
    def test_the_limiter_blocks_before_the_request_that_exceeds_the_window(self) -> None:
        # 60 requests/minute, burst 2: the third HTTP call must wait one second
        # *before* being issued.
        clock = ManualClock()
        client, transport, _clock = make_client(
            [
                json_response(200, page({"AAPL": []}, "t1")),
                json_response(200, page({"AAPL": []}, "t2")),
                json_response(200, page({"AAPL": []}, None)),
            ],
            clock=clock,
            requests_per_minute=60,
            burst=2,
        )

        list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 3)
        self.assertEqual(clock.sleeps, [1.0])

    def test_retries_also_consume_rate_limiter_tokens(self) -> None:
        # burst 1: request #1 is free, the 503 retry must be throttled too.
        clock = ManualClock()
        client, transport, _clock = make_client(
            [
                error_response(503, retry_after="2"),
                json_response(200, page({"AAPL": []}, None)),
            ],
            clock=clock,
            requests_per_minute=60,
            burst=1,
        )

        list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(transport.requests), 2)
        # Retry-After 2s is served first, which refills 2 tokens, so the
        # limiter admits the retry without an additional wait.
        self.assertEqual(clock.sleeps, [2.0])


class PaginationTests(unittest.TestCase):
    def test_pages_are_followed_including_an_empty_final_page(self) -> None:
        client, transport, _clock = make_client(
            [
                json_response(
                    200,
                    page({"AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)]}, "tok-1"),
                ),
                json_response(
                    200,
                    page({"AAPL": [bar_payload("2024-01-02T15:00:00Z", 102.0)]}, "tok-2"),
                ),
                json_response(200, page({}, None)),
            ]
        )

        pages = list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

        self.assertEqual(len(pages), 3)
        self.assertEqual(pages[2], {"bars": {}, "next_page_token": None})
        tokens = [
            dict(httpx.QueryParams(request.url.query.decode())).get("page_token")
            for request in transport.requests
        ]
        self.assertEqual(tokens, [None, "tok-1", "tok-2"])

    def test_first_request_carries_the_canonical_query_parameters(self) -> None:
        client, transport, _clock = make_client([json_response(200, page({}, None))])

        list(client.iter_bar_pages(["AAPL", "MSFT"], START, END, "all"))

        params = dict(httpx.QueryParams(transport.requests[0].url.query.decode()))
        self.assertEqual(
            params,
            {
                "symbols": "AAPL,MSFT",
                "timeframe": "30Min",
                "start": "2024-01-02T14:30:00Z",
                "end": "2024-01-03T14:30:00Z",
                "adjustment": "all",
                "feed": "sip",
                "sort": "asc",
                "limit": "10000",
            },
        )
        self.assertEqual(transport.requests[0].url.path, "/v2/stocks/bars")

    def test_repeated_page_token_is_rejected_instead_of_looping_forever(self) -> None:
        client, _transport, _clock = make_client(
            [
                json_response(200, page({}, "same")),
                json_response(200, page({}, "same")),
            ]
        )

        with self.assertRaisesRegex(PermanentAlpacaError, "next_page_token"):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))

    def test_non_string_page_token_is_rejected(self) -> None:
        client, _transport, _clock = make_client([json_response(200, page({}, 17))])  # type: ignore[arg-type]

        with self.assertRaisesRegex(PermanentAlpacaError, "next_page_token"):
            list(client.iter_bar_pages(["AAPL"], START, END, "raw"))


class CorporateActionsClientTests(unittest.TestCase):
    def _client(
        self, responses: list[httpx.Response]
    ) -> tuple[AlpacaCorporateActionsClient, ScriptedTransport, ManualClock]:
        clock = ManualClock()
        transport = ScriptedTransport(responses)
        client = AlpacaCorporateActionsClient(
            API_KEY,
            API_SECRET,
            transport=transport,
            clock=clock,
            rate_limiter=TokenBucketRateLimiter(6000, clock=clock),
            jitter=lambda: 0.0,
            now=lambda: datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
        )
        return client, transport, clock

    def test_fetches_the_27_symbol_window_and_follows_page_tokens(self) -> None:
        fixture = json.loads(
            Path("tests/fixtures/alpaca/corporate-actions-pages.v1.json").read_text(
                encoding="utf-8"
            )
        )
        first, second = fixture["pages"]
        client, transport, _ = self._client(
            [json_response(200, first), json_response(200, second)]
        )
        symbols = ("SPY", "QQQ", *(f"ETF{index}" for index in range(25)))

        pages = tuple(
            client.iter_corporate_action_pages(
                symbols, date(2026, 8, 1), date(2026, 8, 4)
            )
        )

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0].retrieved_at, datetime(2026, 8, 4, 12, 0, tzinfo=UTC))
        self.assertTrue(pages[0].raw_bytes)
        self.assertEqual(transport.requests[0].url.path, "/v1/corporate-actions")
        self.assertEqual(transport.requests[0].url.params["symbols"], ",".join(symbols))
        self.assertEqual(transport.requests[0].url.params["limit"], "1000")
        self.assertEqual(transport.requests[0].url.params["sort"], "asc")
        self.assertNotIn("cas_region", transport.requests[0].url.params)
        self.assertEqual(transport.requests[1].url.params["page_token"], "page-2")
        self.assertNotIn(API_KEY, str(transport.requests[0].url))
        self.assertNotIn(API_SECRET, str(transport.requests[0].url))
        self.assertEqual(fixture["request"]["path"], transport.requests[0].url.path)
        self.assertEqual(
            fixture["request"]["types"],
            transport.requests[0].url.params["types"].split(","),
        )

    def test_retries_a_transient_page_without_losing_the_pagination_token(self) -> None:
        client, transport, clock = self._client(
            [
                error_response(503),
                json_response(200, {"corporate_actions": {}, "next_page_token": None}),
            ]
        )

        pages = tuple(
            client.iter_corporate_action_pages(
                ("SPY",), date(2026, 8, 1), date(2026, 8, 4)
            )
        )

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(transport.requests), 2)
        self.assertEqual(clock.sleeps, [1.0])

    def test_zero_events_is_a_successful_page_not_a_provider_failure(self) -> None:
        client, _, _ = self._client(
            [json_response(200, {"corporate_actions": {}, "next_page_token": None})]
        )

        (page,) = tuple(
            client.iter_corporate_action_pages(
                ("SPY",), date(2026, 8, 1), date(2026, 8, 4)
            )
        )

        self.assertEqual(page.corporate_actions, {})


class MapperTests(unittest.TestCase):
    def test_bar_payload_is_mapped_to_the_pipeline_row_shape(self) -> None:
        rows = map_page(page({"AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)]}, None))

        self.assertEqual(
            rows,
            [
                {
                    "symbol": "AAPL",
                    "timestamp": datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
                    "open": 100.0,
                    "high": 101.5,
                    "low": 99.5,
                    "close": 101.0,
                    "volume": 1200,
                    "trade_count": 42,
                    "vwap": 100.25,
                }
            ],
        )

    def test_optional_fields_map_to_none_rather_than_zero(self) -> None:
        payload = bar_payload("2024-01-02T14:30:00Z", 101.0)
        del payload["n"]
        del payload["vw"]

        rows = map_page(page({"AAPL": [payload]}, None))

        self.assertIsNone(rows[0]["trade_count"])
        self.assertIsNone(rows[0]["vwap"])

    def test_rows_are_ordered_by_symbol_then_timestamp(self) -> None:
        rows = map_page(
            page(
                {
                    "MSFT": [bar_payload("2024-01-02T15:00:00Z", 1.0)],
                    "AAPL": [
                        bar_payload("2024-01-02T15:00:00Z", 2.0),
                        bar_payload("2024-01-02T14:30:00Z", 3.0),
                    ],
                },
                None,
            )
        )

        self.assertEqual(
            [(row["symbol"], row["timestamp"].isoformat()) for row in rows],
            [
                ("AAPL", "2024-01-02T14:30:00+00:00"),
                ("AAPL", "2024-01-02T15:00:00+00:00"),
                ("MSFT", "2024-01-02T15:00:00+00:00"),
            ],
        )

    def test_missing_required_field_is_permanent(self) -> None:
        payload = bar_payload("2024-01-02T14:30:00Z", 101.0)
        del payload["h"]

        with self.assertRaisesRegex(PermanentAlpacaError, "'h'"):
            map_page(page({"AAPL": [payload]}, None))

    def test_naive_and_unparseable_timestamps_are_permanent(self) -> None:
        with self.assertRaises(PermanentAlpacaError):
            parse_utc_timestamp("2024-01-02T14:30:00")
        with self.assertRaises(PermanentAlpacaError):
            parse_utc_timestamp("not-a-timestamp")

    def test_offset_timestamps_are_normalised_to_utc(self) -> None:
        self.assertEqual(
            parse_utc_timestamp("2024-01-02T09:30:00-05:00"),
            datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        )


class BarSourceTests(unittest.TestCase):
    def make_source(
        self,
        responses: list[httpx.Response],
        **kwargs: Any,
    ) -> tuple[AlpacaBarSource, ScriptedTransport, ManualClock]:
        clock = ManualClock()
        transport = ScriptedTransport(responses)
        source = AlpacaBarSource(
            API_KEY,
            API_SECRET,
            transport=transport,
            clock=clock,
            rate_limiter=TokenBucketRateLimiter(6000, clock=clock),
            jitter=lambda: 0.0,
            **kwargs,
        )
        return source, transport, clock

    def test_fetch_returns_a_frame_the_pipeline_normalizer_accepts(self) -> None:
        from market_pipeline_lib.contracts import InstrumentMapping
        from market_pipeline_lib.processing import normalize_provider_frame

        source, _transport, _clock = self.make_source(
            [
                json_response(
                    200,
                    page(
                        {
                            "AAPL": [
                                bar_payload("2024-01-02T14:30:00Z", 101.0),
                                bar_payload("2024-01-02T15:00:00Z", 102.0),
                            ]
                        },
                        None,
                    ),
                )
            ]
        )

        frame = source.fetch("AAPL", START, END, "raw")
        table = normalize_provider_frame(
            frame,
            InstrumentMapping("AAPL", "11111111-1111-4111-8111-111111111111"),
        )

        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("close").to_pylist(), [101.0, 102.0])
        self.assertEqual(table.column("trade_count").to_pylist(), [42, 42])

    def test_fetch_spans_pages(self) -> None:
        source, transport, _clock = self.make_source(
            [
                json_response(
                    200,
                    page({"AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)]}, "tok-1"),
                ),
                json_response(
                    200,
                    page({"AAPL": [bar_payload("2024-01-02T15:00:00Z", 102.0)]}, None),
                ),
            ]
        )

        frame = source.fetch("AAPL", START, END, "raw")

        self.assertEqual(len(transport.requests), 2)
        self.assertEqual(list(frame["close"]), [101.0, 102.0])

    def test_no_bars_yields_an_empty_frame_never_none(self) -> None:
        source, _transport, _clock = self.make_source(
            [json_response(200, page({}, None))]
        )

        frame = source.fetch("AAPL", START, END, "raw")

        self.assertIsNotNone(frame)
        self.assertTrue(frame.empty)
        self.assertEqual(
            list(frame.columns),
            [
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_count",
                "vwap",
            ],
        )

    def test_auth_failure_propagates_instead_of_becoming_none(self) -> None:
        source, transport, _clock = self.make_source([error_response(401)])

        with self.assertRaises(AlpacaAuthError):
            source.fetch("AAPL", START, END, "raw")

        self.assertEqual(len(transport.requests), 1)

    def test_exhausted_retries_propagate_instead_of_becoming_none(self) -> None:
        source, _transport, _clock = self.make_source(
            [error_response(503) for _ in range(5)]
        )

        with self.assertRaises(AlpacaRetriesExhausted):
            source.fetch("AAPL", START, END, "raw")

    def test_price_type_selects_the_alpaca_adjustment(self) -> None:
        source, transport, _clock = self.make_source(
            [
                json_response(200, page({}, None)),
                json_response(200, page({}, None)),
            ]
        )

        source.fetch("AAPL", START, END, "raw")
        source.fetch("AAPL", START, END, "adjusted")

        adjustments = [
            dict(httpx.QueryParams(request.url.query.decode()))["adjustment"]
            for request in transport.requests
        ]
        self.assertEqual(adjustments, ["raw", "all"])

    def test_unknown_price_type_is_rejected(self) -> None:
        source, transport, _clock = self.make_source([])

        with self.assertRaises(AlpacaRequestError):
            source.fetch("AAPL", START, END, "nominal")

        self.assertEqual(transport.requests, [])

    def test_other_symbols_in_a_shared_page_are_not_leaked_into_the_frame(self) -> None:
        source, _transport, _clock = self.make_source(
            [
                json_response(
                    200,
                    page(
                        {
                            "AAPL": [bar_payload("2024-01-02T14:30:00Z", 101.0)],
                            "MSFT": [bar_payload("2024-01-02T14:30:00Z", 300.0)],
                        },
                        None,
                    ),
                )
            ]
        )

        frame = source.fetch("AAPL", START, END, "raw")

        self.assertEqual(list(frame["symbol"]), ["AAPL"])

    def test_inactivity_probe_is_exposed_only_when_one_is_injected(self) -> None:
        # The engine feature-detects with `hasattr(source, "should_skip_inactive")`.
        # Without a probe the attribute must be absent, so `check_inactive`
        # cannot silently degrade into "never skip".
        plain, _transport, _clock = self.make_source([])
        self.assertFalse(hasattr(plain, "should_skip_inactive"))

        calls: list[tuple[str, datetime, datetime]] = []

        def probe(symbol: str, last_bar: datetime, end: datetime) -> bool:
            calls.append((symbol, last_bar, end))
            return True

        probed, _t, _c = self.make_source([], inactivity_probe=probe)

        self.assertTrue(probed.should_skip_inactive("AAPL", START, END))
        self.assertEqual(calls, [("AAPL", START, END)])


if __name__ == "__main__":
    unittest.main()
