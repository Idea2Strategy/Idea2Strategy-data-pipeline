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

Formula rules
-------------
Every calculator declares a `formula_rules` mapping naming the choices that make two
implementations of "the same" indicator disagree forever: what the recursion is seeded
with, how it is smoothed, what happens before warm-up, where rounding lands, and what
shape of input the formula assumes.  They are *declared by the calculator* rather than
passed by a caller, because they are facts about a frozen version -- but they are not
private, either: `market_pipeline_lib.features.catalog` hashes them into every official
catalog entry, so changing one is a new calculator version and a new catalog version,
never a quiet edit.  A rule that is not in `formula_rules` and covered by a test does
not exist.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from .errors import InsufficientHistory, InvalidFeatureParameters, UnknownCalculator

__all__ = [
    "BarPoint",
    "FeatureCalculator",
    "FeatureValue",
    "MACD_OUTPUT_LINES",
    "PRECISION_RULES_VERSION",
    "PRICE_FIELDS",
    "QUANTUM",
    "known_calculators",
    "get_calculator",
    "quantize",
    "render",
]


#: The project-wide precision rule these calculators implement (spec 2.3).  Named here
#: so a catalog entry can bind it into its hash instead of merely documenting it.
PRECISION_RULES_VERSION = "precision:1.0.0"

#: `precision:1.0.0` -- eight decimal places, the same quantum the money utilities use.
QUANTUM = Decimal("0.00000001")

#: Working precision for intermediate arithmetic.  Wide enough that the only rounding
#: that ever affects an output is the explicit `quantize` call.
_WORKING_PRECISION = 50

PRICE_FIELDS: tuple[str, ...] = ("open", "high", "low", "close")

#: MACD produces three series and a `FeatureValue` carries one number, so a definition
#: names the line it is for.  See `MovingAverageConvergenceDivergence`.
MACD_OUTPUT_LINES: tuple[str, ...] = ("MACD", "SIGNAL", "HISTOGRAM")


def quantize(value: Decimal) -> Decimal:
    """The single quantization point.  Eight places, banker's rounding.

    Negative zero is collapsed to zero.  ``Decimal`` keeps the sign of a value that
    rounded away from below, so a MACD histogram of ``-0.000000001`` quantizes to
    ``-0E-8`` while an identical-by-every-measure ``0.000000001`` quantizes to ``0E-8``:
    the same number, two renderings, therefore two `result_hash` values for one result.
    """

    quantized = value.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)
    return quantized.copy_abs() if quantized == 0 else quantized


def render(value: Decimal) -> str:
    """The single rendering point: fixed point, always eight places.

    ``str(Decimal("0E-8"))`` is ``"0E-8"``, not ``"0.00000000"`` -- `Decimal` switches to
    scientific notation for a zero coefficient.  Every other quantized value already
    renders as plain fixed point, so this only changes zeros, but a feature series that
    renders one of its values in a different notation than the rest is exactly the sort
    of thing a `result_hash` must not depend on noticing.
    """

    return format(quantize(value), "f")


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
    #: The named seed / smoothing / null / precision / input rules of this version.
    #: Read-only: a caller may inspect it, never edit it.
    formula_rules: Mapping[str, str]

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


def _ema_series(values: Sequence[Decimal], window: int) -> dict[int, Decimal]:
    """The one exponential-moving-average recursion in this package.

    Seeded with the simple mean of the first ``window`` values and smoothed with
    ``alpha = 2 / (window + 1)``, quantized at every step.  Keyed by the index of the
    input it belongs to, so a caller can line the result up with bars (`EMA`) or with
    another derived series (`MACD`'s signal line) without either of them re-implementing
    the recursion and drifting from the other.

    Must be called inside a `localcontext` with a wide precision; the callers below do.
    """

    alpha = Decimal(2) / Decimal(window + 1)
    current = quantize(sum(values[:window], Decimal(0)) / window)
    series = {window - 1: current}
    for index in range(window, len(values)):
        current = quantize(current + alpha * (values[index] - current))
        series[index] = current
    return series


# --------------------------------------------------------------------------------------
# Calculators
# --------------------------------------------------------------------------------------


class SimpleMovingAverage:
    """Arithmetic mean of the last ``window`` prices, emitted from bar ``window-1``."""

    code = "SMA"
    version = "1.0.0"
    output_value_type = "DECIMAL"
    formula_rules: Mapping[str, str] = MappingProxyType(
        {
            "input_rule": "SINGLE_PRICE_FIELD",
            "null_rule": "OMIT_UNTIL_WARM",
            "precision_rule": "QUANTIZE_OUTPUT_8DP_HALF_EVEN",
            "seed_rule": "NONE_FULL_WINDOW_MEAN",
            "smoothing_rule": "NONE_EQUAL_WEIGHT",
        }
    )

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
    formula_rules: Mapping[str, str] = MappingProxyType(
        {
            "input_rule": "SINGLE_PRICE_FIELD",
            "null_rule": "OMIT_UNTIL_WARM",
            "precision_rule": "QUANTIZE_EVERY_STEP_8DP_HALF_EVEN",
            "seed_rule": "SMA_OF_FIRST_WINDOW",
            "smoothing_rule": "ALPHA_TWO_OVER_WINDOW_PLUS_ONE",
        }
    )

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
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            series = _ema_series(prices, window)
        return tuple(
            FeatureValue(bar_start_at=bars[index].bar_start_at, value=series[index])
            for index in sorted(series)
        )


class ReturnPercent:
    """Percentage change against the price ``lag`` bars earlier."""

    code = "RETURN_PCT"
    version = "1.0.0"
    output_value_type = "DECIMAL"
    formula_rules: Mapping[str, str] = MappingProxyType(
        {
            "input_rule": "SINGLE_PRICE_FIELD",
            "null_rule": "OMIT_UNTIL_WARM",
            "precision_rule": "QUANTIZE_OUTPUT_8DP_HALF_EVEN",
            "seed_rule": "NONE",
            "smoothing_rule": "NONE",
            "zero_base_rule": "REFUSE",
        }
    )

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


class RelativeStrengthIndex:
    """Wilder's RSI over the changes in one price field.

    Every rule below is a real product decision, which is why each one is also a named
    value in `formula_rules` and hashed into the official catalog entry.

    ``seed_rule = WILDER_SIMPLE_MEAN_OF_FIRST_PERIOD_CHANGES``
        The first ``avg_gain``/``avg_loss`` pair is the plain arithmetic mean of the
        first ``period`` changes -- Wilder's own seed.  The alternative, starting the
        recursion from the first change alone, converges to nearly the same number but
        never to the same number, and this is the one the version string promises.

    ``smoothing_rule = WILDER_RMA_ONE_OVER_PERIOD``
        ``avg += (x - avg) / period``: Wilder's running average, effective smoothing
        ``1/period``.  Not the ``2/(period+1)`` of a conventional EMA, and not Cutler's
        plain moving average either; the three disagree from the first smoothed bar on.

    ``null_rule = OMIT_UNTIL_WARM``
        ``period`` changes need ``period + 1`` closes, so the first value lands on bar
        ``period`` (0-based) and the warm-up bars produce **no row at all** rather than a
        NULL one -- the same convention SMA and EMA already use here.  A period of 14
        therefore needs 15 closes, which is the warm-up the backtest engine reserves.

    ``zero_average_loss_rule = RSI_100`` / ``flat_series_rule = RSI_50``
        With no losses in the window ``RS`` is unbounded and RSI is 100.  With neither
        gains nor losses ``RS`` is not a number at all; a perfectly flat window is
        defined here as the neutral 50 rather than inheriting 100 from the zero-loss
        branch, because a market that did not move is not a market that only rose.

    ``precision_rule = QUANTIZE_EVERY_AVERAGE_AND_OUTPUT_8DP_HALF_EVEN``
        Both running averages are quantized at every step, exactly as `EMA` quantizes
        its recursion, so the state carried forward is an exact eight-place decimal and
        cannot drift with the shape of the partition it was computed over.  The RSI
        itself is then computed at full working precision from those two values and
        quantized once.
    """

    code = "RSI"
    version = "1.0.0"
    output_value_type = "DECIMAL"
    formula_rules: Mapping[str, str] = MappingProxyType(
        {
            "flat_series_rule": "RSI_50",
            "input_rule": "SINGLE_PRICE_FIELD_CHANGES",
            "null_rule": "OMIT_UNTIL_WARM",
            "precision_rule": "QUANTIZE_EVERY_AVERAGE_AND_OUTPUT_8DP_HALF_EVEN",
            "seed_rule": "WILDER_SIMPLE_MEAN_OF_FIRST_PERIOD_CHANGES",
            "smoothing_rule": "WILDER_RMA_ONE_OVER_PERIOD",
            "zero_average_loss_rule": "RSI_100",
        }
    )

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        _require_exact_keys(parameters, ("period", "price_field"), self.code)
        return {
            # Two, not one: a period of 1 makes every window a single change, so the
            # index is only ever 0 or 100 and the smoothing does nothing.
            "period": _positive_int(parameters, "period", 2, self.code),
            "price_field": _price_field(parameters, self.code),
        }

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        return int(self.normalize_parameters(parameters)["period"]) + 1

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        settings = self.normalize_parameters(parameters)
        period = int(settings["period"])
        field = str(settings["price_field"])
        _check_length(bars, period + 1, self.code)
        prices = [bar.price(field) for bar in bars]
        values: list[FeatureValue] = []
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            gains: list[Decimal] = []
            losses: list[Decimal] = []
            for index in range(1, len(prices)):
                change = prices[index] - prices[index - 1]
                gains.append(change if change > 0 else Decimal(0))
                losses.append(-change if change < 0 else Decimal(0))

            average_gain = quantize(sum(gains[:period], Decimal(0)) / period)
            average_loss = quantize(sum(losses[:period], Decimal(0)) / period)
            values.append(
                FeatureValue(
                    bar_start_at=bars[period].bar_start_at,
                    value=self._index(average_gain, average_loss),
                )
            )
            for index in range(period + 1, len(prices)):
                average_gain = quantize(average_gain + (gains[index - 1] - average_gain) / period)
                average_loss = quantize(average_loss + (losses[index - 1] - average_loss) / period)
                values.append(
                    FeatureValue(
                        bar_start_at=bars[index].bar_start_at,
                        value=self._index(average_gain, average_loss),
                    )
                )
        return tuple(values)

    @staticmethod
    def _index(average_gain: Decimal, average_loss: Decimal) -> Decimal:
        if average_gain == 0 and average_loss == 0:
            return quantize(Decimal(50))
        if average_loss == 0:
            return quantize(Decimal(100))
        relative_strength = average_gain / average_loss
        return quantize(Decimal(100) - Decimal(100) / (Decimal(1) + relative_strength))


class MovingAverageConvergenceDivergence:
    """MACD, one output line per definition.

    ``line_rule = ONE_DEFINITION_PER_OUTPUT_LINE``
        MACD is three series -- the line, its signal, and their difference -- and a
        `FeatureValue` carries one number.  So ``output_line`` is a *parameter*, which
        means each line is its own `definition_hash`, its own materialization and its
        own `result_hash`.  Packing three numbers into one row would have made
        "recompute the histogram" indistinguishable from "recompute MACD".

    ``seed_rule = SMA_OF_FIRST_WINDOW_FOR_EVERY_EMA`` and
    ``component_calculator = EMA:1.0.0``
        The fast and slow legs are the *registered* `EMA` recursion, not a private
        re-implementation, so a MACD leg and a standalone `EMA_12` over the same closes
        are the same number by construction rather than by coincidence.

    ``signal_seed_rule = SMA_OF_FIRST_SIGNAL_PERIOD_MACD_VALUES``
        The signal line is an EMA *of the MACD line*, seeded with the simple mean of the
        first ``signal_period`` MACD values -- the same seed rule as every other EMA
        here.  Seeding it from the first MACD value instead is the common alternative
        and produces a permanently different signal.

    ``null_rule = OMIT_UNTIL_WARM``
        The line needs ``slow_period`` closes; the signal and histogram need
        ``slow_period + signal_period - 1``, because the signal EMA needs
        ``signal_period`` MACD values before it has a seed.  For 12/26/9 that is 26 and
        34.  Warm-up bars produce no row.

    The periods are parameters, not constants: 12/26/9 is what the **official catalog**
    pins, and it pins it in a hash rather than in this file.
    """

    code = "MACD"
    version = "1.0.0"
    output_value_type = "DECIMAL"
    formula_rules: Mapping[str, str] = MappingProxyType(
        {
            "component_calculator": "EMA:1.0.0",
            "input_rule": "SINGLE_PRICE_FIELD",
            "line_rule": "ONE_DEFINITION_PER_OUTPUT_LINE",
            "null_rule": "OMIT_UNTIL_WARM",
            "precision_rule": "QUANTIZE_EVERY_STEP_8DP_HALF_EVEN",
            "seed_rule": "SMA_OF_FIRST_WINDOW_FOR_EVERY_EMA",
            "signal_seed_rule": "SMA_OF_FIRST_SIGNAL_PERIOD_MACD_VALUES",
            "smoothing_rule": "ALPHA_TWO_OVER_WINDOW_PLUS_ONE",
        }
    )

    _PARAMETERS = ("fast_period", "output_line", "price_field", "signal_period", "slow_period")

    def normalize_parameters(self, parameters: Mapping[str, Any]) -> dict[str, Any]:
        _require_exact_keys(parameters, self._PARAMETERS, self.code)
        fast = _positive_int(parameters, "fast_period", 2, self.code)
        slow = _positive_int(parameters, "slow_period", 2, self.code)
        signal = _positive_int(parameters, "signal_period", 2, self.code)
        if fast >= slow:
            raise InvalidFeatureParameters(
                f"{self.code}.fast_period must be shorter than slow_period, got "
                f"fast_period={fast} and slow_period={slow}. A MACD line is the fast leg "
                "minus the slow leg; equal or inverted periods do not describe a "
                "convergence/divergence at all."
            )
        line = parameters["output_line"]
        if not isinstance(line, str) or line not in MACD_OUTPUT_LINES:
            raise InvalidFeatureParameters(
                f"{self.code}.output_line must be one of {list(MACD_OUTPUT_LINES)}, got {line!r}"
            )
        return {
            "fast_period": fast,
            "output_line": line,
            "price_field": _price_field(parameters, self.code),
            "signal_period": signal,
            "slow_period": slow,
        }

    def required_history_points(self, parameters: Mapping[str, Any]) -> int:
        settings = self.normalize_parameters(parameters)
        slow = int(settings["slow_period"])
        if settings["output_line"] == "MACD":
            return slow
        return slow + int(settings["signal_period"]) - 1

    def compute(
        self, bars: Sequence[BarPoint], parameters: Mapping[str, Any]
    ) -> tuple[FeatureValue, ...]:
        settings = self.normalize_parameters(parameters)
        fast = int(settings["fast_period"])
        slow = int(settings["slow_period"])
        signal = int(settings["signal_period"])
        line = str(settings["output_line"])
        field = str(settings["price_field"])
        _check_length(bars, self.required_history_points(settings), self.code)

        prices = [bar.price(field) for bar in bars]
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            fast_ema = _ema_series(prices, fast)
            slow_ema = _ema_series(prices, slow)
            offset = slow - 1
            macd_line = [
                quantize(fast_ema[index] - slow_ema[index]) for index in range(offset, len(prices))
            ]
            if line == "MACD":
                return tuple(
                    FeatureValue(bar_start_at=bars[offset + position].bar_start_at, value=value)
                    for position, value in enumerate(macd_line)
                )
            signal_line = _ema_series(macd_line, signal)
            values: list[FeatureValue] = []
            for position in sorted(signal_line):
                signal_value = signal_line[position]
                values.append(
                    FeatureValue(
                        bar_start_at=bars[offset + position].bar_start_at,
                        value=(
                            signal_value
                            if line == "SIGNAL"
                            else quantize(macd_line[position] - signal_value)
                        ),
                    )
                )
        return tuple(values)


_REGISTRY: dict[tuple[str, str], FeatureCalculator] = {}


def _register(calculator: FeatureCalculator) -> None:
    key = (calculator.code, calculator.version)
    if key in _REGISTRY:  # pragma: no cover - a duplicate registration is a coding error
        raise RuntimeError(f"calculator {key} is already registered")
    _REGISTRY[key] = calculator


for _calculator in (
    SimpleMovingAverage(),
    ExponentialMovingAverage(),
    ReturnPercent(),
    RelativeStrengthIndex(),
    MovingAverageConvergenceDivergence(),
):
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
