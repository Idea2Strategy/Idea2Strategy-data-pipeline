"""Split and dividend adjustment arithmetic.

Back-adjustment convention
--------------------------
A corporate action changes the meaning of every price quoted *before* it.  A
2-for-1 split does not make the stock cheaper, so to compare a pre-split price
with a post-split one the historical side is scaled down.  Prices at or after
the effective instant are already expressed in current terms and are left
alone.

The effective instant is exchange-local midnight on the effective date (see
:func:`market_pipeline_lib.corporate_action_research.effective_at_for`), and the
comparison is strict: a bar starting exactly at the effective instant belongs to
the first adjusted session and is not scaled.

Precision follows spec 2.3: every resulting price is quantized to eight decimal
places with ROUND_HALF_EVEN.  Ratios themselves are quantized once, when the
factor is built, and multiple factors compound by multiplication before the
single quantization of the result.

This is deliberately *not* algebraically idempotent -- feeding adjusted output
back in adjusts it twice.  Idempotency is a property of the regenerator, which
always rebuilds from the raw revision; see
:mod:`market_pipeline_lib.corporate_actions.regeneration`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal

from ..corporate_action_research import (
    CashDividendTerms,
    CorporateActionTerms,
    SplitTerms,
)

__all__ = [
    "PRICE_EXPONENT",
    "AdjustmentFactor",
    "ApprovedAction",
    "Bar",
    "adjusted_bars",
    "cash_dividend_factor",
    "split_factor",
]

#: Spec 2.3: monetary values carry eight decimal places, ROUND_HALF_EVEN.
PRICE_EXPONENT = Decimal("0.00000001")
_WHOLE = Decimal(1)
_ONE = Decimal(1)


def _quantize_price(value: Decimal) -> Decimal:
    return value.quantize(PRICE_EXPONENT, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Bar:
    """One OHLCV bar. Prices are Decimal so adjustment never sees binary float."""

    instrument_id: str
    bar_start_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def __post_init__(self) -> None:
        if self.bar_start_at.tzinfo is None or self.bar_start_at.utcoffset() != timedelta(0):
            raise ValueError("bar_start_at must be timezone-aware UTC")
        for name in ("open", "high", "low", "close"):
            value = getattr(self, name)
            if not isinstance(value, Decimal):
                raise ValueError(f"bar {name} must be a Decimal")
            if value <= 0:
                raise ValueError(f"bar {name} must be positive")
        if not isinstance(self.volume, int) or isinstance(self.volume, bool):
            raise ValueError("bar volume must be an integer")
        if self.volume < 0:
            raise ValueError("bar volume cannot be negative")


@dataclass(frozen=True)
class AdjustmentFactor:
    """What one corporate action does to historical prices and volumes."""

    price: Decimal
    volume: Decimal

    def __post_init__(self) -> None:
        for name in ("price", "volume"):
            value = getattr(self, name)
            if not isinstance(value, Decimal):
                raise ValueError(f"{name} factor must be a Decimal")
            if value <= 0:
                raise ValueError(f"{name} factor must be positive")


@dataclass(frozen=True)
class ApprovedAction:
    """A corporate action an administrator has approved for application."""

    action_type: str
    effective_at: datetime
    terms: CorporateActionTerms

    def __post_init__(self) -> None:
        if self.effective_at.tzinfo is None or self.effective_at.utcoffset() != timedelta(0):
            raise ValueError("effective_at must be timezone-aware UTC")
        if self.action_type != self.terms.event_type:
            raise ValueError(
                f"action_type {self.action_type!r} disagrees with the terms, which "
                f"describe {self.terms.event_type!r}"
            )


def split_factor(terms: SplitTerms) -> AdjustmentFactor:
    """`to_shares`-for-`from_shares`: prices scale down, volumes scale up.

    A 2-for-1 split turns one share into two, so a historical price is worth
    half as much per share and the historical share count doubles.
    """
    if not isinstance(terms, SplitTerms):
        raise TypeError("split_factor requires SplitTerms")
    from_shares = Decimal(terms.from_shares)
    to_shares = Decimal(terms.to_shares)
    return AdjustmentFactor(
        price=_quantize_price(from_shares / to_shares),
        volume=_quantize_price(to_shares / from_shares),
    )


def cash_dividend_factor(
    terms: CashDividendTerms,
    *,
    previous_close: Decimal,
) -> AdjustmentFactor:
    """The ex-dividend price ratio `(C - D) / C`.

    `C` is the raw close of the last bar before the ex-date -- the price that
    still contained the dividend.  Volume is untouched: a cash distribution does
    not change how many shares exist.
    """
    if not isinstance(terms, CashDividendTerms):
        raise TypeError("cash_dividend_factor requires CashDividendTerms")
    if not isinstance(previous_close, Decimal):
        raise ValueError("previous close must be a Decimal")
    if previous_close <= 0:
        raise ValueError("previous close must be positive to price a dividend")
    if terms.amount >= previous_close:
        raise ValueError(
            f"dividend {terms.amount} is not less than the previous close "
            f"{previous_close}; the implied adjusted price would be non-positive"
        )
    return AdjustmentFactor(
        price=_quantize_price((previous_close - terms.amount) / previous_close),
        volume=_quantize_price(_ONE),
    )


def _factor_for(action: ApprovedAction, bars: Sequence[Bar]) -> AdjustmentFactor:
    terms = action.terms
    if isinstance(terms, SplitTerms):
        return split_factor(terms)
    if not isinstance(terms, CashDividendTerms):
        raise TypeError(
            f"no adjustment rule is defined for {type(terms).__name__}; refusing rather "
            "than leaving the series silently unadjusted"
        )
    preceding = [bar for bar in bars if bar.bar_start_at < action.effective_at]
    if not preceding:
        raise ValueError(
            f"there is no bar before {action.effective_at.isoformat()}, so the "
            f"{action.action_type} cannot be priced against a previous close"
        )
    latest = max(preceding, key=lambda bar: bar.bar_start_at)
    return cash_dividend_factor(terms, previous_close=latest.close)


def adjusted_bars(
    bars: Sequence[Bar],
    actions: Sequence[ApprovedAction],
) -> tuple[Bar, ...]:
    """Back-adjust `bars` for every approved `actions` entry.

    The input must be the *raw* series.  Each action's factor is derived from
    raw closes, so the result does not depend on the order the actions are
    supplied in.
    """
    ordered = tuple(sorted(bars, key=lambda bar: bar.bar_start_at))
    priced = tuple((action, _factor_for(action, ordered)) for action in actions)

    adjusted: list[Bar] = []
    for bar in ordered:
        price_factor = _ONE
        volume_factor = _ONE
        for action, factor in priced:
            if action.effective_at > bar.bar_start_at:
                price_factor *= factor.price
                volume_factor *= factor.volume
        adjusted.append(
            Bar(
                instrument_id=bar.instrument_id,
                bar_start_at=bar.bar_start_at,
                open=_quantize_price(bar.open * price_factor),
                high=_quantize_price(bar.high * price_factor),
                low=_quantize_price(bar.low * price_factor),
                close=_quantize_price(bar.close * price_factor),
                volume=int(
                    (Decimal(bar.volume) * volume_factor).quantize(
                        _WHOLE, rounding=ROUND_HALF_EVEN
                    )
                ),
            )
        )
    return tuple(adjusted)
