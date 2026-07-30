from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

import httpx

from market_loader.config import AlpacaConfig
from market_loader.errors import PermanentAlpacaError, TransientAlpacaError

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class AlpacaBarsClient:
    def __init__(
        self,
        config: AlpacaConfig,
        api_key: str,
        api_secret: str,
        *,
        client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key or not api_secret:
            raise PermanentAlpacaError("Alpaca credentials are required")
        self._config = config
        self._sleeper = sleeper
        self._headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        }
        self._owned_client = client is None
        self._client = client or httpx.Client(
            base_url=config.base_url,
            timeout=httpx.Timeout(
                connect=config.connect_timeout_seconds,
                read=config.read_timeout_seconds,
                write=config.read_timeout_seconds,
                pool=config.connect_timeout_seconds,
            ),
            headers=self._headers,
        )

    def close(self) -> None:
        if self._owned_client:
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
        if adjustment not in {"raw", "all"}:
            raise PermanentAlpacaError(f"unsupported adjustment: {adjustment}")
        if start.tzinfo is None or end.tzinfo is None or not start < end:
            raise PermanentAlpacaError("request must use an aware [start, end) range")
        base_params = {
            "symbols": ",".join(symbols),
            "timeframe": self._config.request_timeframe,
            "start": start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "end": end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "adjustment": adjustment,
            "feed": self._config.feed,
            "sort": "asc",
            "limit": self._config.page_limit,
        }
        token: str | None = None
        seen: set[str] = set()
        while True:
            params = dict(base_params)
            if token:
                params["page_token"] = token
            page = self._request(params)
            yield page
            next_token = page.get("next_page_token")
            if next_token in (None, ""):
                return
            if not isinstance(next_token, str) or next_token in seen:
                raise PermanentAlpacaError("invalid or repeated next_page_token")
            seen.add(next_token)
            token = next_token

    def _request(self, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            response: httpx.Response | None = None
            try:
                response = self._client.get("/v2/stocks/bars", params=params, headers=self._headers)
                if response.status_code in RETRYABLE_STATUS:
                    raise TransientAlpacaError(f"retryable Alpaca HTTP {response.status_code}")
                if response.status_code in {400, 401, 403}:
                    raise PermanentAlpacaError(f"Alpaca HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("bars", {}), dict):
                    raise PermanentAlpacaError("invalid Alpaca bars response schema")
                return payload
            except PermanentAlpacaError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError, TransientAlpacaError) as exc:
                last_error = exc
                if attempt == self._config.max_attempts:
                    break
                retry_after = response.headers.get("Retry-After") if response is not None else None
                try:
                    delay = float(retry_after) if retry_after else 0.0
                except ValueError:
                    delay = 0.0
                if delay <= 0:
                    delay = min(2 ** (attempt - 1), 16) + secrets.randbelow(251) / 1000
                self._sleeper(delay)
            except (httpx.HTTPError, ValueError) as exc:
                raise PermanentAlpacaError(f"Alpaca response failed: {type(exc).__name__}") from exc
        raise TransientAlpacaError("Alpaca request retries exhausted") from last_error
