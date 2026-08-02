"""Versioned feature calculators.

Everything here is `Decimal`.  Binary floating point is not reproducible enough for a
value that a `result_hash` is taken over: ``0.1 + 0.2`` is one number on the machine
that materialized the feature and the same number on the machine that recomputes it,
but the *order* a sum is accumulated in changes the last bits, and a feature recomputed
on a differently-shaped partition would then hash differently while being, in every
sense a user cares about, the same number.  Decimal with an explicit quantum removes
that whole class of difference.

Quantization follows the project's precision rule (`precision:1.0.0`): eight decimal
places, ``ROUND_HALF_EVEN``, applied at exactly one place -- `quantize` below -- so no
calculator can accidentally introduce a second convention.

Versioning
----------
A calculator is identified by ``(code, version)`` and is **frozen** once published: a
`feature_definitions` row records the version it was computed with, so changing the
arithmetic means registering a new version, never editing an existing one.  Nothing
here is removed either -- an old definition row has to stay readable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any, Protocol, runtime_checkable

from .errors import InsufficientHistory, InvalidFeatureParameters, UnknownCalculator

__all__ = [
    "BarPoint",
    "FeatureCalculator",
    "FeatureValue",
    "PRICE_FIELDS",
    "QUANTUM",
    "known_calculators",
    "get_calculator",
    "quantize",
]


#: `precision:1.0.0` -- eight decimal places, the same quantum the money utilities use.
QUANTUM = Decimal("0.00000001")

#: Working precision for intermediate arithmetic.  Wide enough that the only rounding
#: that ever affects an output is the explicit `quantize` call.
_WORKING_PRECISION = 50

PRICE_FIELDS: tuple[str, ...] = ("open", "high", "low", "close")


def quantize(value: Decimal) -> Decimal:
    """The single quantization point.  Eight places, banker's rounding."""

    return value.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class BarPoint:
    """One input bar.  Prices are `Decimal`; see the module docstring."""

    bar_start_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def price(self, field: str) -> Decimal:
        """One of the four OHLC prices, chosen by name.

        An explicit mapping rather than `getattr`, so `price("volume")` is a refusal
        rather than an `int` flowing into `Decimal` arithmetic.
        """

        match field:
            case "open":
                return self.open
            case "high":
                return self.high
            case "low":
                return self.low
            case "close":
                return self.close
            case _:
                raise InvalidFeatureParameters(
                    f"price_field must be one of {list(PRICE_FIELDS)}, got {field!r}"
                )


@dataclass(frozen=True)
class FeatureValue:
    """One materialized value, already quantized."""

    bar_start_at: datetime
    value: Decimal


@runtime_checkable
class FeatureCalculator(Protocol):
    """A frozen, versioned computation over a bar series."""

    code: str
    version: str
    output_value_type: str

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        """Validate and canonicalise parameters, or raise `InvalidFeatureParameters`."""

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        """Bars needed before the first value can be produced."""

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        """Every value the series supports, oldest first."""


# --------------------------------------------------------------------------------------
# Parameter helpers
# --------------------------------------------------------------------------------------


def _require_exact_keys(parameters: Mapping[str, Any], expected: tuple[str, ...], code: str) -> None:
    if not isinstance(parameters, Mapping):
        raise InvalidFeatureParameters(f"{code} parameters must be a mapping, got {type(parameters).__name__}")
    for key in parameters:
        if not isinstance(key, str):
            raise InvalidFeatureParameters(f"{code} parameter names must be strings, got {key!r}")
    missing = sorted(set(expected) - set(parameters))
    unknown = sorted(set(parameters) - set(expected))
    if missing:
        raise InvalidFeatureParameters(f"{code} is missing parameter(s) {missing}")
    if unknown:
        raise InvalidFeatureParameters(f"{code} does not take parameter(s) {unknown}; expected {list(expected)}")


def _positive_int(parameters: Mapping[str, Any], name: str, minimum: int, code: str) -> int:
    value = parameters[name]
    # `bool` is an `int` subclass, and `True` silently meaning a window of 1 is exactly
    # the sort of hidden coercion a definition hash is supposed to make impossible.
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidFeatureParameters(
            f"{code}.{name} must be an integer, got {type(value).__name__} ({value!r}). "
            "Floats are refused: a window is a count, and 3.0 and 3 would hash differently."
        )
    if value < minimum:
        raise InvalidFeatureParameters(f"{code}.{name} must be >= {minimum}, got {value}")
    return value


def _price_field(parameters: Mapping[str, Any], code: str) -> str:
    value = parameters["price_field"]
    if not isinstance(value, str) or value not in PRICE_FIELDS:
        raise InvalidFeatureParameters(f"{code}.price_field must be one of {list(PRICE_FIELDS)}, got {value!r}")
    return value


def _check_length(bars: Sequence[BarPoint], required: int, code: str) -> None:
    if len(bars) < required:
        raise InsufficientHistory(
            f"{code} needs {required} bars and was given {len(bars)}"
        )


# --------------------------------------------------------------------------------------
# Calculators
# --------------------------------------------------------------------------------------


class SimpleMovingAverage:
    """Arithmetic mean of the last ``window`` prices, emitted from bar ``window-1``."""

    code = "SMA"
    version = "1.0.0"
    output_value_type = "DECIMAL"

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        _require_exact_keys(parameters, ("price_field", "window"), self.code)
        return {
            "price_field": _price_field(parameters, self.code),
            "window": _positive_int(parameters, "window", 1, self.code),
        }

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        return int(self.normalize_parameters(parameters)["window"])

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        settings = self.normalize_parameters(parameters)
        window = int(settings["window"])
        field = str(settings["price_field"])
        _check_length(bars, window, self.code)
        prices = [bar.price(field) for bar in bars]
        values: list[FeatureValue] = []
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            for index in range(window - 1, len(prices)):
                total = sum(prices[index - window + 1 : index + 1], Decimal(0))
                values.append(
                    FeatureValue(
                        bar_start_at=bars[index].bar_start_at,
                        value=quantize(total / window),
                    )
                )
        return tuple(values)


class ExponentialMovingAverage:
    """EMA with ``alpha = 2 / (window + 1)``, seeded with the SMA of the first window.

    Seeding with the SMA rather than with the first price is stated here because it is a
    real modelling choice, not an implementation detail: two EMA implementations that
    disagree about the seed produce different numbers forever, and the version string is
    what a consumer uses to know which one it got.  Each step is quantized, so the
    recursion is over exact eight-place decimals and cannot drift with accumulation
    order.
    """

    code = "EMA"
    version = "1.0.0"
    output_value_type = "DECIMAL"

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        _require_exact_keys(parameters, ("price_field", "window"), self.code)
        return {
            "price_field": _price_field(parameters, self.code),
            "window": _positive_int(parameters, "window", 2, self.code),
        }

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        return int(self.normalize_parameters(parameters)["window"])

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        settings = self.normalize_parameters(parameters)
        window = int(settings["window"])
        field = str(settings["price_field"])
        _check_length(bars, window, self.code)
        prices = [bar.price(field) for bar in bars]
        values: list[FeatureValue] = []
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            alpha = Decimal(2) / Decimal(window + 1)
            current = quantize(sum(prices[:window], Decimal(0)) / window)
            values.append(FeatureValue(bar_start_at=bars[window - 1].bar_start_at, value=current))
            for index in range(window, len(prices)):
                current = quantize(current + alpha * (prices[index] - current))
                values.append(FeatureValue(bar_start_at=bars[index].bar_start_at, value=current))
        return tuple(values)


class ReturnPercent:
    """Percentage change against the price ``lag`` bars earlier."""

    code = "RETURN_PCT"
    version = "1.0.0"
    output_value_type = "DECIMAL"

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        _require_exact_keys(parameters, ("lag", "price_field"), self.code)
        return {
            "lag": _positive_int(parameters, "lag", 1, self.code),
            "price_field": _price_field(parameters, self.code),
        }

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        return int(self.normalize_parameters(parameters)["lag"]) + 1

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        settings = self.normalize_parameters(parameters)
        lag = int(settings["lag"])
        field = str(settings["price_field"])
        _check_length(bars, lag + 1, self.code)
        prices = [bar.price(field) for bar in bars]
        values: list[FeatureValue] = []
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            for index in range(lag, len(prices)):
                base = prices[index - lag]
                if base == 0:
                    raise InvalidFeatureParameters(
                        f"{self.code} cannot divide by a zero {field} at "
                        f"{bars[index - lag].bar_start_at.isoformat()}"
                    )
                values.append(
                    FeatureValue(
                        bar_start_at=bars[index].bar_start_at,
                        value=quantize((prices[index] - base) / base * Decimal(100)),
                    )
                )
        return tuple(values)


_REGISTRY: dict[tuple[str, str], FeatureCalculator] = {}


def _register(calculator: FeatureCalculator) -> None:
    key = (calculator.code, calculator.version)
    if key in _REGISTRY:  # pragma: no cover - a duplicate registration is a coding error
        raise RuntimeError(f"calculator {key} is already registered")
    _REGISTRY[key] = calculator


for _calculator in (SimpleMovingAverage(), ExponentialMovingAverage(), ReturnPercent()):
    _register(_calculator)
del _calculator


def known_calculators() -> tuple[tuple[str, str], ...]:
    return tuple(sorted(_REGISTRY))


def get_calculator(feature_code: str, calculator_version: str) -> FeatureCalculator:
    """Resolve a versioned calculator, or raise `UnknownCalculator`."""

    try:
        return _REGISTRY[(feature_code, calculator_version)]
    except KeyError as exc:
        raise UnknownCalculator(
            f"no calculator registered for feature_code={feature_code!r} "
            f"calculator_version={calculator_version!r}; known: {list(known_calculators())}"
        ) from exc
