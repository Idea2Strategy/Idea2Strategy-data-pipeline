"""The single canonical Alpaca provider client for the D bundle.

`market_pipeline_lib.engine.AlpacaBarSource` and the orphaned
`idea2strategy-market-loader` client are both superseded by this package.
"""

from __future__ import annotations

from .client import (
    BARS_PATH,
    RETRYABLE_STATUS,
    AlpacaBarsClient,
    AlpacaClientConfig,
    backoff_seconds,
    parse_retry_after,
)
from .errors import (
    AlpacaAuthError,
    AlpacaError,
    AlpacaRequestError,
    AlpacaResponseError,
    AlpacaRetriesExhausted,
    PermanentAlpacaError,
    TransientAlpacaError,
)
from .mapper import BAR_ROW_COLUMNS, map_bar, map_page, parse_utc_timestamp
from .pagination import iter_pages
from .source import ADJUSTMENT_BY_PRICE_TYPE, AlpacaBarSource, InactivityProbe

__all__ = [
    "ADJUSTMENT_BY_PRICE_TYPE",
    "BAR_ROW_COLUMNS",
    "BARS_PATH",
    "RETRYABLE_STATUS",
    "AlpacaAuthError",
    "AlpacaBarSource",
    "AlpacaBarsClient",
    "AlpacaClientConfig",
    "AlpacaError",
    "AlpacaRequestError",
    "AlpacaResponseError",
    "AlpacaRetriesExhausted",
    "InactivityProbe",
    "PermanentAlpacaError",
    "TransientAlpacaError",
    "backoff_seconds",
    "iter_pages",
    "map_bar",
    "map_page",
    "parse_retry_after",
    "parse_utc_timestamp",
]
