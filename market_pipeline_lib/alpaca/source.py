"""`BarSource` adapter over the canonical Alpaca client.

This is the drop-in replacement for `market_pipeline_lib.engine.AlpacaBarSource`.
It keeps the `fetch(symbol, start, end, price_type)` signature the engine calls
but changes one thing deliberately: **it never returns `None`**. An empty range
returns an empty frame, and every failure raises a typed error, so the engine
can no longer record an expired API key as an anonymous `ALPACA_FETCH_FAILED`.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from ..rate_limit import SYSTEM_CLOCK, Clock, TokenBucketRateLimiter
from .client import AlpacaBarsClient, AlpacaClientConfig, _default_jitter
from .errors import AlpacaRequestError
from .mapper import BAR_ROW_COLUMNS, map_page

__all__ = ["ADJUSTMENT_BY_PRICE_TYPE", "AlpacaBarSource", "InactivityProbe"]

# The pipeline's `price_type` vocabulary mapped onto Alpaca's `adjustment`.
ADJUSTMENT_BY_PRICE_TYPE = {"raw": "raw", "adjusted": "all"}

InactivityProbe = Callable[[str, datetime, datetime], bool]


class AlpacaBarSource:
    """Provider adapter; credentials stay in the client's header map."""

    # Declared but intentionally unbound. The engine feature-detects with
    # `hasattr(source, "should_skip_inactive")`, so leaving the attribute
    # absent when no probe was injected keeps `--check-inactive` from
    # degrading into a silent "never skip" policy: the engine simply does not
    # offer the optimisation instead of pretending to apply it.
    should_skip_inactive: InactivityProbe

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
        inactivity_probe: InactivityProbe | None = None,
    ) -> None:
        self.client = AlpacaBarsClient(
            api_key,
            api_secret,
            config=config,
            transport=transport,
            http_client=http_client,
            clock=clock,
            rate_limiter=rate_limiter,
            jitter=jitter,
        )
        if inactivity_probe is not None:
            self.should_skip_inactive = inactivity_probe

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.client!r})"

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> AlpacaBarSource:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def fetch(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        price_type: str,
    ) -> pd.DataFrame:
        """Return every 30-minute bar for one symbol over `[start, end)`.

        Raises `AlpacaAuthError`, `AlpacaRequestError`, `AlpacaResponseError`
        or `AlpacaRetriesExhausted` on failure; returns an empty frame — never
        `None` — when the provider simply has no bars for the range.
        """
        adjustment = ADJUSTMENT_BY_PRICE_TYPE.get(price_type)
        if adjustment is None:
            raise AlpacaRequestError(
                f"지원하지 않는 price_type입니다: {price_type!r} "
                f"(가능: {sorted(ADJUSTMENT_BY_PRICE_TYPE)})"
            )
        rows: list[dict[str, Any]] = []
        for page in self.client.iter_bar_pages([symbol], start, end, adjustment):
            rows.extend(row for row in map_page(page) if row["symbol"] == symbol)
        return pd.DataFrame(rows, columns=list(BAR_ROW_COLUMNS))
