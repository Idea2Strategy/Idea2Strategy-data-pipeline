"""The canonical Alpaca market-data HTTP client.

Ported from the orphaned `idea2strategy-market-loader` client, which already
honoured `Retry-After`, used jittered exponential backoff and separated
permanent from transient failures, with the two things it was missing added:

* a **proactive** token-bucket rate limiter that blocks *before* the request,
  so the requests-per-minute ceiling holds across retries and threads; and
* a fully injectable clock and jitter source, so the retry schedule is
  asserted against pinned values instead of slept through.

Credentials are held in the header map only. They are never logged, never
placed in a query string, and never rendered by `repr`.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import httpx

from ..rate_limit import SYSTEM_CLOCK, Clock, TokenBucketRateLimiter
from .errors import (
    AlpacaAuthError,
    AlpacaRequestError,
    AlpacaResponseError,
    AlpacaRetriesExhausted,
    TransientAlpacaError,
)
from .pagination import iter_pages

__all__ = [
    "BARS_PATH",
    "CORPORATE_ACTIONS_PATH",
    "RETRYABLE_STATUS",
    "AlpacaBarsClient",
    "AlpacaClientConfig",
    "AlpacaCorporateActionPage",
    "AlpacaCorporateActionsClient",
    "backoff_seconds",
    "parse_retry_after",
]

LOGGER = logging.getLogger(__name__)

BARS_PATH = "/v2/stocks/bars"
CORPORATE_ACTIONS_PATH = "/v1/corporate-actions"
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
AUTH_STATUS = frozenset({401, 403})
_ADJUSTMENTS = frozenset({"raw", "split", "dividend", "all"})


@dataclass(frozen=True)
class AlpacaClientConfig:
    """Everything about the client that an operator may tune.

    The defaults are the free-tier-safe values: 200 requests/minute is Alpaca's
    documented basic-plan ceiling, and the backoff tops out at 16s so a long
    outage does not park a worker for minutes at a time.
    """

    base_url: str = "https://data.alpaca.markets"
    feed: str = "sip"
    timeframe: str = "30Min"
    page_limit: int = 10_000
    max_attempts: int = 5
    requests_per_minute: int = 200
    burst: int | None = None
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 60.0
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 16.0
    backoff_jitter_seconds: float = 0.25
    max_retry_after_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not self.base_url:
            raise ValueError("base_url이 필요합니다.")
        if self.max_attempts < 1:
            raise ValueError("max_attempts는 1 이상이어야 합니다.")
        if not 0 < self.page_limit <= 10_000:
            raise ValueError("page_limit은 1..10000 범위여야 합니다.")
        if self.requests_per_minute < 1:
            raise ValueError("requests_per_minute는 1 이상이어야 합니다.")
        if self.backoff_base_seconds <= 0:
            raise ValueError("backoff_base_seconds는 양수여야 합니다.")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds는 backoff_base_seconds 이상이어야 합니다.")
        if self.backoff_jitter_seconds < 0:
            raise ValueError("backoff_jitter_seconds는 음수일 수 없습니다.")
        if self.max_retry_after_seconds <= 0:
            raise ValueError("max_retry_after_seconds는 양수여야 합니다.")


def backoff_seconds(
    attempt: int,
    *,
    base: float,
    maximum: float,
    jitter_span: float,
    jitter_fraction: float,
) -> float:
    """Delay before retry number `attempt` (1-based).

    Exponential from `base`, clamped at `maximum`, plus `jitter_fraction` of
    `jitter_span`. The jitter is a parameter rather than an internal draw so
    the schedule can be pinned in tests.
    """
    if attempt < 1:
        raise ValueError("attempt는 1 이상이어야 합니다.")
    if not 0.0 <= jitter_fraction < 1.0:
        raise ValueError("jitter_fraction은 [0, 1) 범위여야 합니다.")
    exponential = min(base * (2.0 ** (attempt - 1)), maximum)
    return exponential + jitter_span * jitter_fraction


def parse_retry_after(raw: str | None) -> float | None:
    """Return the `Retry-After` delay in seconds, or None to fall back.

    Only the delta-seconds form is honoured, which is what Alpaca sends. The
    HTTP-date form returns None on purpose: guessing at it risks turning a
    misread header into a multi-hour stall, and the backoff schedule is a safe
    substitute.
    """
    if not raw:
        return None
    try:
        delay = float(raw)
    except ValueError:
        return None
    return delay if delay > 0 else None


def _default_jitter() -> float:
    """A uniform fraction in [0, 1) drawn from the system CSPRNG."""
    return secrets.randbelow(1000) / 1000.0


class AlpacaBarsClient:
    """Fetch `/v2/stocks/bars` pages with rate limiting, retries and typing."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        config: AlpacaClientConfig | None = None,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
        clock: Clock = SYSTEM_CLOCK,
        rate_limiter: TokenBucketRateLimiter | None = None,
        jitter: Callable[[], float] = _default_jitter,
    ) -> None:
        if not api_key or not api_secret:
            raise AlpacaAuthError("Alpaca API 키와 시크릿이 모두 필요합니다.")
        self._config = config or AlpacaClientConfig()
        self._clock = clock
        self._jitter = jitter
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(
            self._config.requests_per_minute,
            burst=self._config.burst,
            clock=clock,
        )
        # Credentials live here and nowhere else. `_headers` is never logged.
        self._headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        }
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            base_url=self._config.base_url,
            transport=transport,
            timeout=httpx.Timeout(
                connect=self._config.connect_timeout_seconds,
                read=self._config.read_timeout_seconds,
                write=self._config.read_timeout_seconds,
                pool=self._config.connect_timeout_seconds,
            ),
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self._config.base_url!r})"

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> AlpacaBarsClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def iter_bar_pages(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        adjustment: str,
    ) -> Iterator[dict[str, Any]]:
        """Yield every bars page for `[start, end)`, following pagination."""
        base_params = self._base_params(symbols, start, end, adjustment)

        def fetch_page(token: str | None) -> dict[str, Any]:
            params = dict(base_params)
            if token:
                params["page_token"] = token
            return self._request(params)

        return iter_pages(fetch_page)

    def _base_params(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        adjustment: str,
    ) -> dict[str, Any]:
        if adjustment not in _ADJUSTMENTS:
            raise AlpacaRequestError(f"지원하지 않는 adjustment입니다: {adjustment}")
        if not symbols:
            raise AlpacaRequestError("symbols가 비었습니다.")
        if start.tzinfo is None or end.tzinfo is None:
            raise AlpacaRequestError("start와 end는 timezone-aware여야 합니다.")
        if start >= end:
            raise AlpacaRequestError("start는 end보다 앞서야 합니다.")
        return {
            "symbols": ",".join(symbols),
            "timeframe": self._config.timeframe,
            "start": _iso_z(start),
            "end": _iso_z(end),
            "adjustment": adjustment,
            "feed": self._config.feed,
            "sort": "asc",
            "limit": self._config.page_limit,
        }

    def _request(self, params: dict[str, Any]) -> dict[str, Any]:
        response = self._request_response(BARS_PATH, params)
        return self._payload(response)

    def _request_response(self, path: str, params: Mapping[str, Any]) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            # Proactive: the token is taken *before* the call leaves, so the
            # ceiling covers retries too, and a failed call still costs quota.
            self._rate_limiter.acquire()
            response: httpx.Response | None = None
            try:
                response = self._client.get(
                    path,
                    params=params,
                    headers=self._headers,
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = TransientAlpacaError(
                    f"Alpaca 전송 실패: {type(exc).__name__}"
                )
                last_error.__cause__ = exc
            else:
                status = response.status_code
                if status in AUTH_STATUS:
                    # Never retried and never degraded: this needs an operator.
                    raise AlpacaAuthError(f"Alpaca 인증에 실패했습니다: HTTP {status}")
                if status == 400:
                    raise AlpacaRequestError(f"Alpaca 요청이 거부되었습니다: HTTP {status}")
                if status in RETRYABLE_STATUS:
                    last_error = TransientAlpacaError(f"재시도 가능한 Alpaca HTTP {status}")
                elif status >= 400:
                    raise AlpacaResponseError(f"예상치 못한 Alpaca HTTP {status}")
                else:
                    return response

            if attempt == self._config.max_attempts:
                break
            delay = self._retry_delay(attempt, response)
            LOGGER.warning(
                "Alpaca 요청 재시도 attempt=%d/%d status=%s delay=%.3fs",
                attempt,
                self._config.max_attempts,
                "none" if response is None else response.status_code,
                delay,
            )
            self._clock.sleep(delay)
        raise AlpacaRetriesExhausted(
            f"Alpaca 재시도 예산({self._config.max_attempts}회)을 모두 소진했습니다."
        ) from last_error

    def _payload(self, response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise AlpacaResponseError("Alpaca 응답이 JSON이 아닙니다.") from exc
        if not isinstance(payload, dict):
            raise AlpacaResponseError("Alpaca 응답 최상위가 객체가 아닙니다.")
        bars = payload.get("bars")
        if bars is not None and not isinstance(bars, dict):
            raise AlpacaResponseError("Alpaca 응답의 bars가 객체가 아닙니다.")
        return payload

    def _retry_delay(self, attempt: int, response: httpx.Response | None) -> float:
        if response is not None:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                return min(retry_after, self._config.max_retry_after_seconds)
        return backoff_seconds(
            attempt,
            base=self._config.backoff_base_seconds,
            maximum=self._config.backoff_max_seconds,
            jitter_span=self._config.backoff_jitter_seconds,
            jitter_fraction=self._jitter(),
        )


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class AlpacaCorporateActionPage:
    """One byte-preserving corporate-actions response page."""

    corporate_actions: Mapping[str, tuple[Mapping[str, Any], ...]]
    raw_bytes: bytes
    retrieved_at: datetime
    source_uri: str


class AlpacaCorporateActionsClient(AlpacaBarsClient):
    """Fetch `/v1/corporate-actions` once per source window, following every page."""

    def __init__(self, *args: Any, now: Callable[[], datetime] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._now = now or (lambda: datetime.now(UTC))

    def iter_corporate_action_pages(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        *,
        event_types: Sequence[str] = ("cash_dividend", "forward_split", "reverse_split"),
    ) -> Iterator[AlpacaCorporateActionPage]:
        if not symbols:
            raise AlpacaRequestError("symbols must not be empty")
        if start > end:
            raise AlpacaRequestError("start must be on or before end")
        if not event_types:
            raise AlpacaRequestError("event_types must not be empty")
        normalized_symbols = tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
        if any(not symbol for symbol in normalized_symbols):
            raise AlpacaRequestError("symbols must be non-empty market symbols")
        base_params: dict[str, Any] = {
            "symbols": ",".join(normalized_symbols),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "types": ",".join(dict.fromkeys(event_types)),
            "limit": min(self._config.page_limit, 1000),
            "sort": "asc",
        }
        token: str | None = None
        seen: set[str] = set()
        while True:
            params = dict(base_params)
            if token:
                params["page_token"] = token
            response = self._request_response(CORPORATE_ACTIONS_PATH, params)
            try:
                payload = response.json()
            except ValueError as exc:
                raise AlpacaResponseError("Alpaca corporate-actions response is not JSON") from exc
            if not isinstance(payload, dict):
                raise AlpacaResponseError("Alpaca corporate-actions response must be an object")
            raw_actions = payload.get("corporate_actions", {})
            if not isinstance(raw_actions, dict):
                raise AlpacaResponseError("Alpaca corporate_actions must be an object")
            actions: dict[str, tuple[Mapping[str, Any], ...]] = {}
            for action_type, entries in raw_actions.items():
                if not isinstance(action_type, str) or not isinstance(entries, list):
                    raise AlpacaResponseError(
                        "Alpaca corporate_actions entries must be arrays keyed by type"
                    )
                if any(not isinstance(entry, dict) for entry in entries):
                    raise AlpacaResponseError(
                        f"Alpaca corporate_actions[{action_type!r}] contains a non-object"
                    )
                actions[action_type] = tuple(entries)
            retrieved_at = self._now()
            if retrieved_at.tzinfo is None or retrieved_at.utcoffset() != UTC.utcoffset(retrieved_at):
                raise AlpacaResponseError("corporate-action retrieval clock must return UTC")
            yield AlpacaCorporateActionPage(
                corporate_actions=actions,
                raw_bytes=response.content,
                retrieved_at=retrieved_at,
                source_uri=str(response.request.url),
            )
            next_token = payload.get("next_page_token")
            if next_token is None or next_token == "":
                return
            if not isinstance(next_token, str):
                raise AlpacaResponseError("Alpaca next_page_token must be a string")
            if next_token in seen:
                raise AlpacaResponseError("Alpaca next_page_token repeated")
            seen.add(next_token)
            token = next_token
