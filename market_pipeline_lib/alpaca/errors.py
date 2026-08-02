"""Typed Alpaca failures, split by whether retrying can ever help.

The adapter this package replaces caught bare `Exception`, retried three
times, and then returned `None`. The caller recorded every one of those as an
indistinguishable `ALPACA_FETCH_FAILED`, so an expired API key looked exactly
like a transient 503 and a run could "complete" having silently fetched
nothing. Each failure mode now carries its own class and stable `code`.
"""

from __future__ import annotations

__all__ = [
    "AlpacaAuthError",
    "AlpacaError",
    "AlpacaRequestError",
    "AlpacaResponseError",
    "AlpacaRetriesExhausted",
    "PermanentAlpacaError",
    "TransientAlpacaError",
]


class AlpacaError(Exception):
    """Base class for every Alpaca provider failure."""

    code = "ALPACA_ERROR"
    retryable = False


class TransientAlpacaError(AlpacaError):
    """A failure that may succeed if the same request is issued again."""

    code = "ALPACA_TRANSIENT"
    retryable = True


class AlpacaRetriesExhausted(TransientAlpacaError):
    """The retry budget ran out while the failure was still transient."""

    code = "ALPACA_RETRIES_EXHAUSTED"


class PermanentAlpacaError(AlpacaError):
    """A failure that retrying cannot fix. Never retried, never swallowed."""

    code = "ALPACA_PERMANENT"


class AlpacaAuthError(PermanentAlpacaError):
    """HTTP 401/403: credentials are missing, wrong, or lack entitlement.

    Deliberately distinct from every other failure: an authentication problem
    is an operator action, not a data-availability problem, and must never be
    folded into a generic fetch failure or degraded into an empty result.
    """

    code = "ALPACA_AUTH_FAILED"


class AlpacaRequestError(PermanentAlpacaError):
    """The request itself is invalid (HTTP 400 or a client-side guard)."""

    code = "ALPACA_REQUEST_INVALID"


class AlpacaResponseError(PermanentAlpacaError):
    """The response could not be parsed or violated the expected schema."""

    code = "ALPACA_RESPONSE_INVALID"
