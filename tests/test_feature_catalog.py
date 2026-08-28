"""D13 (issue #15) -- the official v1 feature catalog, and the RSI/MACD calculators.

What this suite is for
----------------------
`tests/test_features.py` already holds the three merged properties of D13: definitions
are immutable, materialization is byte-reproducible, a partial batch is not consumable.
This file adds the part issue #15 still asked for:

*the official feature set is a named, versioned, content-addressed artefact*
    `OfficialFeatureCatalog` is the v1 list.  Every entry binds the element catalog
    version, calculator version, normalized parameters, output type, required history,
    trading calendar, precision rule, input-adjustment rule **and** the calculator's
    formula rules into one `entry_hash`, and the catalog verifies its own hash the
    moment it is constructed.  Change any of those and the import fails; there is no
    path that edits an entry in place.

*the formulas are pinned, not implied*
    Every semantic choice an RSI or MACD implementation has to make -- the seed, the
    smoothing, what happens before warm-up, where rounding lands, whether the input is
    adjusted -- is a named value in `formula_rules`, hashed into the catalog entry, and
    asserted here as a literal.  A rule that is not in this file does not exist.

*the RSI the backtest engine consumes is the RSI this pipeline computes*
    The backtest engine pins `RSI_14` at calculator version `1.0.0` with a fifteen-bar
    warm-up.  `test_rsi_14_agrees_with_the_backtest_engine_warm_up_contract` states that
    agreement in literals on this side of the wire, with no import across repositories.

Oracles
-------
Nothing here asks the production code what the answer should be.

* Arithmetic expectations are hand-computed from the fixture closes (the derivations are
  in the docstrings) and written as literals.  The fixtures are chosen so that no
  quantization lands on an exact ``.5`` tie -- see `MACD_SMALL_CLOSES`, where the
  awkward sixth close exists precisely to keep the signal seed off a tie.
* Every hash expectation is the sha256 of a canonical JSON string written out by hand in
  this file and hashed **by this file**, so the shape of a hashed payload is pinned
  independently of the module that assembles it.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.contracts import DATASET_CONTRACTS
from market_pipeline_lib.features import (
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    FeatureSnapshotBatchBuilder,
    MarketInput,
    MaterializationRequest,
    SnapshotBatchPlan,
    SourceObject,
    get_calculator,
    known_calculators,
    quantize,
)
from market_pipeline_lib.features.calculators import PRECISION_RULES_VERSION, render
from market_pipeline_lib.features.catalog import (
    OFFICIAL_FEATURE_CATALOG_HASH,
    OFFICIAL_FEATURE_CATALOG_VERSION,
    OfficialFeatureCatalog,
)
from market_pipeline_lib.features.errors import (
    FeatureCatalogIntegrityError,
    InsufficientHistory,
    InvalidFeatureParameters,
    UnknownOfficialFeature,
)
from market_pipeline_lib.features.tables import (
    FEATURE_DEFINITIONS,
    FEATURE_MATERIALIZATIONS,
    FEATURE_SNAPSHOT_BATCHES,
)
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.realtime_warmup import (
    FEATURE_OBJECT_SCHEMA_VERSION,
    FeatureRequirement,
    WarmupPublicationSpec,
    WarmupReadiness,
    publish_realtime_warmup_bundle,
    verify_realtime_warmup_bundle,
)

# ======================================================================================
# Fixture constants
# ======================================================================================

CATALOG_VERSION_ID = "0e5a1c9e-1111-4a11-8a11-000000000001"
INSTRUMENT_A = "aaaaaaaa-0000-4000-8000-0000000000d1"

RUN_1 = "11111111-0000-4000-8000-0000000000d1"
RUN_2 = "11111111-0000-4000-8000-0000000000d2"
OUTPUT_MANIFESTS = {
    RUN_1: "dddddddd-0000-4000-8000-0000000000e1",
    RUN_2: "dddddddd-0000-4000-8000-0000000000e2",
}
SOURCE_OBJECT_ID = "cccccccc-0000-4000-8000-0000000000d1"
SOURCE_MANIFEST_ID = "dddddddd-0000-4000-8000-0000000000d1"
SNAPSHOT_OBJECT = "eeeeeeee-0000-4000-8000-0000000000d1"

#: One period, wide enough for both a 40-minute and a 40-day fixture; see `bars_at`.
PERIOD_START = datetime(2026, 1, 2, tzinfo=UTC)
PERIOD_END = datetime(2026, 3, 2, tzinfo=UTC)
SOURCE_WATERMARK = "ALPACA_SIP_ADJUSTED@2026-03-01T00:00:00Z"


# --------------------------------------------------------------------------------------
# RSI fixture.
#
# Seventeen closes.  The first fourteen changes are seven gains of +4 and seven losses
# of -2, alternating, so the Wilder seed is exact:
#
#     avg_gain = 28 / 14 = 2      avg_loss = 14 / 14 = 1
#     RS = 2, RSI = 100 - 100/3 = 66.666666...  ->  66.66666667
#
# The fifteenth change is +7 and the sixteenth is -3, which is where the Wilder
# smoothing (``avg += (x - avg) / 14``) actually gets exercised.  No value lands on a
# ``.5`` quantization tie.
# --------------------------------------------------------------------------------------
RSI_CLOSES: tuple[int, ...] = (
    100, 104, 102, 106, 104, 108, 106, 110, 108, 112, 110, 114, 112, 116, 114, 121, 118,
)

#: bar 14 -- the seed, hand-derived above.
RSI_VALUE_AT_SEED = "66.66666667"
#: bar 15 -- avg_gain = q(33/14) = 2.35714286, avg_loss = q(13/14) = 0.92857143,
#: RSI = 100 * 235714286 / 328571429 = 71.7391304282...  ->  71.73913043
RSI_VALUE_AFTER_A_GAIN = "71.73913043"
#: bar 16 -- avg_gain = q(2.35714286*13/14) = 2.18877551,
#: avg_loss = q((0.92857143*13 + 3)/14) = 1.07653061,
#: RSI = 100 * 218877551 / 326530612 = 67.0312500440...  ->  67.03125004
RSI_VALUE_AFTER_A_LOSS = "67.03125004"


# --------------------------------------------------------------------------------------
# MACD fixture, small periods (fast=3, slow=4, signal=3).
#
# Small periods are used for the arithmetic pin because 2/(3+1) = 0.5, 2/(4+1) = 0.4 and
# 2/(3+1) = 0.5 are exact decimals, so every intermediate below is exact and the whole
# chain is hand-checkable.  12/26/9 is pinned separately, further down.
#
#   closes           100     104     108     111     107    103.75    118      116
#   EMA(3) a=0.5       .       .    104.00  107.50  107.25  105.50  111.75  113.87500
#   EMA(4) a=0.4       .       .       .    105.75  106.25  105.25  110.35  112.61000
#   MACD               .       .       .      1.75    1.00    0.25    1.40    1.26500
#   SIGNAL(3) a=0.5    .       .       .       .       .      1.00    1.20    1.23250
#   HISTOGRAM          .       .       .       .       .     -0.75    0.20    0.03250
#
# EMA(3) seed = (100+104+108)/3 = 104.  EMA(4) seed = (100+104+108+111)/4 = 105.75.
# SIGNAL seed = (1.75 + 1.00 + 0.25)/3 = 1.00 exactly -- that is why the sixth close is
# 103.75 rather than a round number: with 113 there the seed would be 1.30833333... and
# the next signal step would land on an exact .5 tie.
# --------------------------------------------------------------------------------------
MACD_SMALL_CLOSES: tuple[str, ...] = ("100", "104", "108", "111", "107", "103.75", "118", "116")

MACD_SMALL_LINE = ("1.75000000", "1.00000000", "0.25000000", "1.40000000", "1.26500000")
MACD_SMALL_SIGNAL = ("1.00000000", "1.20000000", "1.23250000")
MACD_SMALL_HISTOGRAM = ("-0.75000000", "0.20000000", "0.03250000")


# --------------------------------------------------------------------------------------
# MACD fixture, the official 12/26/9 periods.
#
# `MACD_RAMP_CLOSES` rises by exactly 1 per bar.  On a constant-slope series an EMA
# seeded with the SMA of its first window sits at the steady-state lag of (n-1)/2 and
# stays there, so EMA(12) lags by 5.5, EMA(26) lags by 12.5, and
# MACD = 12.5 - 5.5 = 7 exactly, at every bar.  That is the hand-derivable case.
#
# `MACD_REVERSING_CLOSES` rises by 1 for 26 bars and then falls by 2, so nothing is
# constant and the pinned values below are all distinct.  They are quantized eight-place
# decimals derived from the rules above by exact rational arithmetic.
# --------------------------------------------------------------------------------------
MACD_RAMP_CLOSES: tuple[int, ...] = tuple(100 + index for index in range(36))
MACD_RAMP_LINE_VALUE = "7.00000000"

MACD_REVERSING_CLOSES: tuple[int, ...] = tuple(100 + index for index in range(26)) + tuple(
    125 - 2 * (index - 25) for index in range(26, 40)
)
#: bars 33, 34, 35 of `MACD_REVERSING_CLOSES` -- the first three bars on which all three
#: lines exist (SIGNAL needs 26 + 9 - 1 = 34 closes).
MACD_REVERSING_LINE = ("1.92418966", "1.09050330", "0.26535868")
MACD_REVERSING_SIGNAL = ("4.84404163", "4.09333396", "3.32773890")
MACD_REVERSING_HISTOGRAM = ("-2.91985197", "-3.00283066", "-3.06238022")


# ======================================================================================
# Hand-written canonical JSON.  This file hashes these strings itself.
# ======================================================================================

SMA_FORMULA_RULES_JSON = (
    '{"input_rule":"SINGLE_PRICE_FIELD",'
    '"null_rule":"OMIT_UNTIL_WARM",'
    '"precision_rule":"QUANTIZE_OUTPUT_8DP_HALF_EVEN",'
    '"seed_rule":"NONE_FULL_WINDOW_MEAN",'
    '"smoothing_rule":"NONE_EQUAL_WEIGHT"}'
)
EMA_FORMULA_RULES_JSON = (
    '{"input_rule":"SINGLE_PRICE_FIELD",'
    '"null_rule":"OMIT_UNTIL_WARM",'
    '"precision_rule":"QUANTIZE_EVERY_STEP_8DP_HALF_EVEN",'
    '"seed_rule":"SMA_OF_FIRST_WINDOW",'
    '"smoothing_rule":"ALPHA_TWO_OVER_WINDOW_PLUS_ONE"}'
)
RETURN_PCT_FORMULA_RULES_JSON = (
    '{"input_rule":"SINGLE_PRICE_FIELD",'
    '"null_rule":"OMIT_UNTIL_WARM",'
    '"precision_rule":"QUANTIZE_OUTPUT_8DP_HALF_EVEN",'
    '"seed_rule":"NONE",'
    '"smoothing_rule":"NONE",'
    '"zero_base_rule":"REFUSE"}'
)
RSI_FORMULA_RULES_JSON = (
    '{"flat_series_rule":"RSI_50",'
    '"input_rule":"SINGLE_PRICE_FIELD_CHANGES",'
    '"null_rule":"OMIT_UNTIL_WARM",'
    '"precision_rule":"QUANTIZE_EVERY_AVERAGE_AND_OUTPUT_8DP_HALF_EVEN",'
    '"seed_rule":"WILDER_SIMPLE_MEAN_OF_FIRST_PERIOD_CHANGES",'
    '"smoothing_rule":"WILDER_RMA_ONE_OVER_PERIOD",'
    '"zero_average_loss_rule":"RSI_100"}'
)
MACD_FORMULA_RULES_JSON = (
    '{"component_calculator":"EMA:1.0.0",'
    '"input_rule":"SINGLE_PRICE_FIELD",'
    '"line_rule":"ONE_DEFINITION_PER_OUTPUT_LINE",'
    '"null_rule":"OMIT_UNTIL_WARM",'
    '"precision_rule":"QUANTIZE_EVERY_STEP_8DP_HALF_EVEN",'
    '"seed_rule":"SMA_OF_FIRST_WINDOW_FOR_EVERY_EMA",'
    '"signal_seed_rule":"SMA_OF_FIRST_SIGNAL_PERIOD_MACD_VALUES",'
    '"smoothing_rule":"ALPHA_TWO_OVER_WINDOW_PLUS_ONE"}'
)

FORMULA_RULES_JSON: dict[tuple[str, str], str] = {
    ("EMA", "1.0.0"): EMA_FORMULA_RULES_JSON,
    ("MACD", "1.0.0"): MACD_FORMULA_RULES_JSON,
    ("RETURN_PCT", "1.0.0"): RETURN_PCT_FORMULA_RULES_JSON,
    ("RSI", "1.0.0"): RSI_FORMULA_RULES_JSON,
    ("SMA", "1.0.0"): SMA_FORMULA_RULES_JSON,
}


def macd_parameters_json(line: str) -> str:
    return (
        '{"fast_period":12,'
        f'"output_line":"{line}",'
        '"price_field":"close",'
        '"signal_period":9,'
        '"slow_period":26}'
    )


def entry_json(
    *,
    name: str,
    feature_code: str,
    rules: str,
    parameters: str,
    required_history_points: int,
    resolution: str,
) -> str:
    """One official catalog entry, written out in canonical form by hand."""

    return (
        '{"calculator_version":"1.0.0",'
        '"calendar_id":"XNYS",'
        '"catalog_version":"feature-catalog:1.0.0",'
        '"entry_schema_version":1,'
        f'"feature_code":"{feature_code}",'
        f'"formula_rules":{rules},'
        '"input_adjustment":"SPLIT_DIVIDEND_ADJUSTED",'
        f'"name":"{name}",'
        f'"normalized_parameters":{parameters},'
        '"output_value_type":"DECIMAL",'
        '"precision_rules_version":"precision:1.0.0",'
        f'"required_history_points":{required_history_points},'
        f'"resolution":"{resolution}"}}'
    )


#: The resolution each official entry is pinned at.  Not uniform, and deliberately so:
#: `RSI_14` is one minute because the backend's live `strategy-bot.v1` compiled plan
#: already decided it -- see
#: `test_rsi_14_conforms_to_the_backend_compiled_plan_contract`, which transcribes that
#: contract's own values.  Nothing pins the other six, so they stay at the conventional
#: daily, and the mix is asserted here rather than left to be noticed.
EXPECTED_RESOLUTIONS: dict[str, str] = {
    "EMA_12": "1d",
    "EMA_26": "1d",
    "MACD_12_26_9": "1d",
    "MACD_12_26_9_HISTOGRAM": "1d",
    "MACD_12_26_9_SIGNAL": "1d",
    "RSI_14": "1m",
    "SMA_20": "1d",
}


#: The whole official v1 catalog, by hand, in name order.
EXPECTED_ENTRY_JSON: dict[str, str] = {
    "EMA_12": entry_json(
        resolution=EXPECTED_RESOLUTIONS["EMA_12"],
        name="EMA_12",
        feature_code="EMA",
        rules=EMA_FORMULA_RULES_JSON,
        parameters='{"price_field":"close","window":12}',
        required_history_points=12,
    ),
    "EMA_26": entry_json(
        resolution=EXPECTED_RESOLUTIONS["EMA_26"],
        name="EMA_26",
        feature_code="EMA",
        rules=EMA_FORMULA_RULES_JSON,
        parameters='{"price_field":"close","window":26}',
        required_history_points=26,
    ),
    "MACD_12_26_9": entry_json(
        resolution=EXPECTED_RESOLUTIONS["MACD_12_26_9"],
        name="MACD_12_26_9",
        feature_code="MACD",
        rules=MACD_FORMULA_RULES_JSON,
        parameters=macd_parameters_json("MACD"),
        required_history_points=26,
    ),
    "MACD_12_26_9_HISTOGRAM": entry_json(
        resolution=EXPECTED_RESOLUTIONS["MACD_12_26_9_HISTOGRAM"],
        name="MACD_12_26_9_HISTOGRAM",
        feature_code="MACD",
        rules=MACD_FORMULA_RULES_JSON,
        parameters=macd_parameters_json("HISTOGRAM"),
        required_history_points=34,
    ),
    "MACD_12_26_9_SIGNAL": entry_json(
        resolution=EXPECTED_RESOLUTIONS["MACD_12_26_9_SIGNAL"],
        name="MACD_12_26_9_SIGNAL",
        feature_code="MACD",
        rules=MACD_FORMULA_RULES_JSON,
        parameters=macd_parameters_json("SIGNAL"),
        required_history_points=34,
    ),
    "RSI_14": entry_json(
        resolution=EXPECTED_RESOLUTIONS["RSI_14"],
        name="RSI_14",
        feature_code="RSI",
        rules=RSI_FORMULA_RULES_JSON,
        parameters='{"period":14,"price_field":"close"}',
        required_history_points=15,
    ),
    "SMA_20": entry_json(
        resolution=EXPECTED_RESOLUTIONS["SMA_20"],
        name="SMA_20",
        feature_code="SMA",
        rules=SMA_FORMULA_RULES_JSON,
        parameters='{"price_field":"close","window":20}',
        required_history_points=20,
    ),
}


def sha256_of(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def definition_json(
    *, feature_code: str, parameters: str, required_history_points: int, resolution: str
) -> str:
    """One `feature_definitions` hashed payload, written out by hand."""

    return (
        '{"calculator_version":"1.0.0",'
        f'"element_catalog_version_id":"{CATALOG_VERSION_ID}",'
        f'"feature_code":"{feature_code}",'
        f'"normalized_parameters":{parameters},'
        '"output_value_type":"DECIMAL",'
        f'"required_history_points":{required_history_points},'
        f'"resolution":"{resolution}",'
        '"schema_version":1}'
    )


# ======================================================================================
# Helpers
# ======================================================================================


def bars_at(closes: tuple[Any, ...], step: timedelta) -> tuple[BarPoint, ...]:
    """Evenly spaced bars from `PERIOD_START`, `step` apart, with the given closes.

    The cadence matches the resolution of the feature under test: minute bars for
    ``RSI_14`` (which the catalog pins at ``1m``) and daily bars for the six entries
    pinned at ``1d``.  Both fit inside the one period below, so the fixtures share a
    period without either of them pretending to a cadence it does not have.
    """

    return tuple(
        BarPoint(
            bar_start_at=PERIOD_START + step * index,
            open=Decimal(str(close)) - 1,
            high=Decimal(str(close)) + 1,
            low=Decimal(str(close)) - 2,
            close=Decimal(str(close)),
            volume=1000 + index,
        )
        for index, close in enumerate(closes)
    )


def minute_bars(closes: tuple[Any, ...]) -> tuple[BarPoint, ...]:
    return bars_at(closes, timedelta(minutes=1))


def daily_bars(closes: tuple[Any, ...]) -> tuple[BarPoint, ...]:
    return bars_at(closes, timedelta(days=1))


def catalog_object() -> OfficialFeatureCatalog:
    return OfficialFeatureCatalog()


def sources() -> tuple[SourceObject, ...]:
    return (
        SourceObject(
            dataset_object_id=SOURCE_OBJECT_ID,
            dataset_manifest_id=SOURCE_MANIFEST_ID,
            content_hash="3" * 64,
            partition_start="2026-01-02",
            partition_end="2026-03-02",
            row_count=40,
        ),
    )


#: The `input_bundle_fingerprint` of `sources()`, hashed here from hand-written bytes.
SOURCE_BUNDLE_JSON = (
    '{"bundle_schema_version":1,'
    '"objects":[{'
    f'"content_hash":"{"3" * 64}",'
    f'"dataset_manifest_id":"{SOURCE_MANIFEST_ID}",'
    f'"dataset_object_id":"{SOURCE_OBJECT_ID}",'
    '"partition_end":"2026-03-02",'
    '"partition_start":"2026-01-02",'
    '"row_count":40}]}'
)
SOURCE_BUNDLE_FINGERPRINT = sha256_of(SOURCE_BUNDLE_JSON)


def materialization_request(
    definition: FeatureDefinition,
    *,
    closes: tuple[Any, ...],
    pipeline_run_id: str = RUN_1,
    step: timedelta | None = None,
) -> MaterializationRequest:
    """A request whose bar cadence matches the definition's own resolution.

    Derived from `definition.resolution` rather than defaulted, so a fixture cannot
    quietly feed daily bars to a feature the catalog pins at one minute.
    """

    cadence = step or {"1m": timedelta(minutes=1), "1d": timedelta(days=1)}[definition.resolution]
    return MaterializationRequest(
        definition=definition,
        instrument_id=INSTRUMENT_A,
        pipeline_run_id=pipeline_run_id,
        sources=sources(),
        bars=bars_at(closes, cadence),
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        source_watermark=SOURCE_WATERMARK,
        output_dataset_manifest_id=OUTPUT_MANIFESTS[pipeline_run_id],
    )


@pytest.fixture
def local_features_catalog(tmp_path: Path) -> LocalCatalog:
    return LocalCatalog(tmp_path / "official-feature-catalog")


@pytest.fixture
def registry(local_features_catalog: LocalCatalog) -> FeatureDefinitionRegistry:
    return FeatureDefinitionRegistry(local_features_catalog)


@pytest.fixture
def materializer(
    local_features_catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> FeatureMaterializer:
    return FeatureMaterializer(local_features_catalog, registry)


# ======================================================================================
# 1. The calculator registry
# ======================================================================================


def test_the_registry_holds_exactly_the_five_official_calculators() -> None:
    """A literal list.  A calculator may be added, but never silently."""

    assert known_calculators() == (
        ("EMA", "1.0.0"),
        ("MACD", "1.0.0"),
        ("RETURN_PCT", "1.0.0"),
        ("RSI", "1.0.0"),
        ("SMA", "1.0.0"),
    )


@pytest.mark.parametrize(("code", "version"), sorted(FORMULA_RULES_JSON))
def test_every_calculator_declares_its_pinned_formula_rules(code: str, version: str) -> None:
    """The seed / smoothing / null / precision / input rules, as literals.

    These are the choices that make two implementations of "the same" indicator disagree
    forever.  They are declared by the calculator (a fact about the frozen version, not
    something a caller may pass) and they are hashed into the official catalog entry, so
    changing one is a new calculator version and a new catalog version.
    """

    calculator = get_calculator(code, version)
    rules = calculator.formula_rules
    assert json.dumps(dict(rules), sort_keys=True, ensure_ascii=False, separators=(",", ":")) == (
        FORMULA_RULES_JSON[(code, version)]
    )
    # Frozen: the mapping handed out cannot be used to edit the declaration.
    with pytest.raises(TypeError):
        rules["seed_rule"] = "SOMETHING_ELSE"  # type: ignore[index]


def test_the_precision_rule_version_is_the_project_wide_one() -> None:
    assert PRECISION_RULES_VERSION == "precision:1.0.0"


@pytest.mark.parametrize(
    "raw",
    ["0", "-0", "0.000000001", "-0.000000001", "-0.0000000049"],
)
def test_a_zero_feature_value_is_one_number_with_one_rendering(raw: str) -> None:
    """`Decimal` gives a quantized zero two spellings; a `result_hash` may not see both.

    ``Decimal("-0.000000001").quantize(1e-8)`` is ``-0E-8`` and
    ``Decimal("0.000000001").quantize(1e-8)`` is ``0E-8``: the same number, two strings,
    therefore two hashes for one result.  A MACD histogram crosses zero routinely, so
    this is not hypothetical.
    """

    quantized = quantize(Decimal(raw))
    assert quantized == 0
    assert not quantized.is_signed()
    assert render(Decimal(raw)) == "0.00000000"


def test_a_materialized_zero_is_serialised_as_fixed_point(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """The rendering rule reaches the bytes `result_hash` is taken over.

    A flat MACD histogram over the constant-slope ramp is exactly zero on every bar.
    """

    definition = FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="MACD",
        calculator_version="1.0.0",
        resolution="1d",
        parameters=macd_parameters("HISTOGRAM", fast=12, slow=26, signal=9),
    )
    registry.publish(definition)
    result = materializer.materialize(
        materialization_request(definition, closes=MACD_RAMP_CLOSES)
    )
    assert json.loads(result.serialized_rows()) == [
        {"at": "2026-02-04T00:00:00Z", "value": "0.00000000"},
        {"at": "2026-02-05T00:00:00Z", "value": "0.00000000"},
        {"at": "2026-02-06T00:00:00Z", "value": "0.00000000"},
    ]


# ======================================================================================
# 2. RSI
# ======================================================================================


def test_rsi_14_values_are_hand_computed_literals() -> None:
    """Wilder RSI over `RSI_CLOSES`; the derivation is in the fixture comment."""

    calculator = get_calculator("RSI", "1.0.0")
    values = calculator.compute(minute_bars(RSI_CLOSES), {"period": 14, "price_field": "close"})
    assert [str(item.value) for item in values] == [
        RSI_VALUE_AT_SEED,
        RSI_VALUE_AFTER_A_GAIN,
        RSI_VALUE_AFTER_A_LOSS,
    ]
    assert [item.bar_start_at.isoformat() for item in values] == [
        "2026-01-02T00:14:00+00:00",
        "2026-01-02T00:15:00+00:00",
        "2026-01-02T00:16:00+00:00",
    ]


def test_rsi_14_agrees_with_the_backtest_engine_warm_up_contract() -> None:
    """The cross-repository agreement, stated in literals on this side of the wire.

    The backtest engine consumes the feature named ``RSI_14`` at calculator version
    ``1.0.0`` and reserves a fifteen-bar warm-up: fifteen closes are needed to produce
    fourteen changes, and the first RSI value lands on the fifteenth bar.  Nothing is
    imported across repositories -- if either side moves, this fails here.
    """

    calculator = get_calculator("RSI", "1.0.0")
    parameters = {"period": 14, "price_field": "close"}
    assert calculator.required_history_points(parameters) == 15

    fourteen = minute_bars(RSI_CLOSES[:14])
    with pytest.raises(InsufficientHistory) as raised:
        calculator.compute(fourteen, parameters)
    assert "15" in str(raised.value)

    fifteen = calculator.compute(minute_bars(RSI_CLOSES[:15]), parameters)
    assert len(fifteen) == 1
    assert str(fifteen[0].value) == RSI_VALUE_AT_SEED
    assert fifteen[0].bar_start_at == PERIOD_START + timedelta(minutes=14)

    entry = catalog_object().entry("RSI_14")
    assert entry.feature_code == "RSI"
    assert entry.calculator_version == "1.0.0"
    assert entry.calculator_id == "rsi:1.0.0"
    assert entry.required_history_points == 15
    assert entry.normalized_parameters == {"period": 14, "price_field": "close"}


def test_rsi_14_conforms_to_the_backend_compiled_plan_contract() -> None:
    """The backend already decided this feature's name and resolution; we conform.

    Copied by hand from ``strategy-bot.v1`` `basic-compiled-plan.valid.json`, which the
    backend owns and the backtest engine vendors byte-for-byte.  Deliberately transcribed
    rather than imported: the spec forbids a consumer keeping a second copy of a
    producer's schema, and a cross-repository import would make this suite unrunnable on
    its own.  If the backend moves either value, this test still passes and the *contract
    cross-check* in CI is what catches the drift -- so what this pins is our side's
    conformance, in literals, at the point where it is decided.

    Two resolutions, and they are not in conflict:

    * ``requiredFeatures[0].resolution = "PT1M"`` is the ISO-8601 cadence of the *warm-up
      observation stream*, the same field D90's `FeatureRequirement.resolution` carries.
    * ``steps[0].arguments.resolution = "1m"`` is the *feature's own* resolution, in the
      short token form, and that is what a `feature_definitions` row stores.

    ``requiredObservations: 14`` is a floor, not an exact count -- the backtest engine
    resolves it as ``max(requiredObservations, definition_bars)`` -- and ``rsi:1.0.0``
    needs 15 closes to yield 14 changes.  15 and 14 therefore agree; 15 wins.
    """

    backend_required_feature = {
        "requirementId": "rsi-14-pt1m",
        "featureId": "00000000-0000-4000-8000-000000000401",
        "featureVersion": "1.0.0",
        "instruments": ["00000000-0000-4000-8000-000000000301"],
        "resolution": "PT1M",
        "requiredObservations": 14,
    }
    backend_load_feature_arguments = {"feature": "RSI_14", "resolution": "1m"}

    entry = catalog_object().entry("RSI_14")
    assert entry.name == backend_load_feature_arguments["feature"]
    assert entry.resolution == backend_load_feature_arguments["resolution"]
    assert entry.calculator_version == backend_required_feature["featureVersion"]
    # The floor is satisfied, with the one extra close a 14-change window needs.
    assert entry.required_history_points > int(backend_required_feature["requiredObservations"])
    assert entry.required_history_points == int(backend_required_feature["requiredObservations"]) + 1
    # `PT1M` and `1m` are the same minute in the two vocabularies the plan uses.
    assert backend_required_feature["resolution"] == "PT1M"
    assert entry.resolution == "1m"


def test_rsi_of_a_flat_series_is_fifty_and_of_a_rising_series_is_one_hundred() -> None:
    """Two pinned edge rules, both of them real product decisions.

    ``avg_loss == 0`` with a positive ``avg_gain`` makes ``RS`` unbounded, so RSI is 100.
    A perfectly flat series has ``avg_gain == avg_loss == 0``, where ``RS`` is not a
    number at all; this catalog defines it as the neutral 50 rather than inheriting 100
    from the zero-loss branch, because a market that did not move is not a market that
    only rose.
    """

    calculator = get_calculator("RSI", "1.0.0")
    parameters = {"period": 14, "price_field": "close"}

    flat = calculator.compute(minute_bars((100,) * 15), parameters)
    assert [str(item.value) for item in flat] == ["50.00000000"]

    rising = calculator.compute(minute_bars(tuple(100 + 3 * index for index in range(15))), parameters)
    assert [str(item.value) for item in rising] == ["100.00000000"]


def test_rsi_uses_the_named_price_field_rather_than_assuming_close() -> None:
    """`high` is `close + 1` throughout, so every change is identical and so is the RSI."""

    calculator = get_calculator("RSI", "1.0.0")
    on_high = calculator.compute(minute_bars(RSI_CLOSES), {"period": 14, "price_field": "high"})
    assert [str(item.value) for item in on_high] == [
        RSI_VALUE_AT_SEED,
        RSI_VALUE_AFTER_A_GAIN,
        RSI_VALUE_AFTER_A_LOSS,
    ]
    with pytest.raises(InvalidFeatureParameters):
        calculator.compute(minute_bars(RSI_CLOSES), {"period": 14, "price_field": "volume"})


@pytest.mark.parametrize(
    "parameters",
    [
        pytest.param({"period": 14.0, "price_field": "close"}, id="float-period"),
        pytest.param({"period": 1, "price_field": "close"}, id="degenerate-period"),
        pytest.param({"period": 0, "price_field": "close"}, id="zero-period"),
        pytest.param({"period": True, "price_field": "close"}, id="bool-period"),
        pytest.param({"period": 14}, id="missing-price-field"),
        pytest.param({"period": 14, "price_field": "close", "seed": "x"}, id="unknown-parameter"),
        pytest.param({"window": 14, "price_field": "close"}, id="wrong-parameter-name"),
    ],
)
def test_rsi_parameters_are_refused_when_they_do_not_match_the_contract(
    parameters: dict[str, Any],
) -> None:
    with pytest.raises(InvalidFeatureParameters):
        get_calculator("RSI", "1.0.0").normalize_parameters(parameters)


def test_a_changed_rsi_period_is_a_different_definition() -> None:
    fourteen = FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="RSI",
        calculator_version="1.0.0",
        resolution="1m",
        parameters={"period": 14, "price_field": "close"},
    )
    twenty_one = fourteen.with_parameters({"period": 21, "price_field": "close"})
    assert fourteen.definition_hash != twenty_one.definition_hash
    assert twenty_one.required_history_points == 22


# ======================================================================================
# 3. MACD
# ======================================================================================


def macd_parameters(line: str, *, fast: int = 3, slow: int = 4, signal: int = 3) -> dict[str, Any]:
    return {
        "fast_period": fast,
        "slow_period": slow,
        "signal_period": signal,
        "price_field": "close",
        "output_line": line,
    }


def test_macd_lines_are_hand_computed_literals() -> None:
    """All three lines over `MACD_SMALL_CLOSES`; the table is in the fixture comment."""

    calculator = get_calculator("MACD", "1.0.0")
    bars = daily_bars(MACD_SMALL_CLOSES)

    line = calculator.compute(bars, macd_parameters("MACD"))
    assert [str(item.value) for item in line] == list(MACD_SMALL_LINE)
    assert [item.bar_start_at.date().isoformat() for item in line] == [
        "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09",
    ]

    signal = calculator.compute(bars, macd_parameters("SIGNAL"))
    assert [str(item.value) for item in signal] == list(MACD_SMALL_SIGNAL)
    assert [item.bar_start_at.date().isoformat() for item in signal] == [
        "2026-01-07", "2026-01-08", "2026-01-09",
    ]

    histogram = calculator.compute(bars, macd_parameters("HISTOGRAM"))
    assert [str(item.value) for item in histogram] == list(MACD_SMALL_HISTOGRAM)


def test_macd_12_26_9_on_a_constant_slope_series_is_the_hand_derivable_lag_difference() -> None:
    """A series rising by exactly 1 per bar makes the answer derivable without arithmetic.

    An EMA seeded with the SMA of its first ``n`` bars starts at the steady-state lag of
    ``(n-1)/2`` behind a unit ramp and never leaves it.  So EMA(12) lags by 5.5, EMA(26)
    lags by 12.5, and the MACD line is 12.5 - 5.5 = 7 at every bar it exists.  The signal
    line is an EMA of a constant, so it is 7 too, and the histogram is 0.
    """

    calculator = get_calculator("MACD", "1.0.0")
    bars = daily_bars(MACD_RAMP_CLOSES)
    line = calculator.compute(bars, macd_parameters("MACD", fast=12, slow=26, signal=9))
    assert len(line) == 11
    assert {str(item.value) for item in line} == {MACD_RAMP_LINE_VALUE}
    assert line[0].bar_start_at == PERIOD_START + timedelta(days=25)

    signal = calculator.compute(bars, macd_parameters("SIGNAL", fast=12, slow=26, signal=9))
    assert [str(item.value) for item in signal] == [MACD_RAMP_LINE_VALUE] * 3
    assert signal[0].bar_start_at == PERIOD_START + timedelta(days=33)

    histogram = calculator.compute(bars, macd_parameters("HISTOGRAM", fast=12, slow=26, signal=9))
    assert [format(item.value, "f") for item in histogram] == ["0.00000000"] * 3


def test_macd_12_26_9_over_a_reversing_series_is_pinned_and_not_constant() -> None:
    """The official periods over a series that turns; every pinned value is distinct."""

    calculator = get_calculator("MACD", "1.0.0")
    bars = daily_bars(MACD_REVERSING_CLOSES)

    line = calculator.compute(bars, macd_parameters("MACD", fast=12, slow=26, signal=9))
    signal = calculator.compute(bars, macd_parameters("SIGNAL", fast=12, slow=26, signal=9))
    histogram = calculator.compute(bars, macd_parameters("HISTOGRAM", fast=12, slow=26, signal=9))

    # Bars 33, 34 and 35: the first three on which all three lines exist.  The MACD line
    # starts at bar 25 and the other two at bar 33, so they are indexed differently, and
    # the timestamps below are what proves the three are lined up on the same bars.
    assert [format(item.value, "f") for item in line[8:11]] == list(MACD_REVERSING_LINE)
    assert [format(item.value, "f") for item in signal[:3]] == list(MACD_REVERSING_SIGNAL)
    assert [format(item.value, "f") for item in histogram[:3]] == list(MACD_REVERSING_HISTOGRAM)
    assert line[8].bar_start_at == signal[0].bar_start_at == PERIOD_START + timedelta(days=33)
    assert histogram[0].bar_start_at == signal[0].bar_start_at
    assert len(set(MACD_REVERSING_LINE)) == 3


def test_macd_history_requirements_are_pinned_for_every_line() -> None:
    """26 for the line, 34 for signal and histogram -- ``slow + signal - 1``."""

    calculator = get_calculator("MACD", "1.0.0")
    official = {"fast": 12, "slow": 26, "signal": 9}
    assert calculator.required_history_points(macd_parameters("MACD", **official)) == 26
    assert calculator.required_history_points(macd_parameters("SIGNAL", **official)) == 34
    assert calculator.required_history_points(macd_parameters("HISTOGRAM", **official)) == 34

    short = daily_bars(MACD_REVERSING_CLOSES[:33])
    with pytest.raises(InsufficientHistory) as raised:
        calculator.compute(short, macd_parameters("SIGNAL", **official))
    assert "34" in str(raised.value)
    # 33 closes are still enough for the MACD line itself.
    assert len(calculator.compute(short, macd_parameters("MACD", **official))) == 8


@pytest.mark.parametrize(
    "parameters",
    [
        pytest.param(
            {"fast_period": 26, "slow_period": 12, "signal_period": 9,
             "price_field": "close", "output_line": "MACD"},
            id="fast-not-faster-than-slow",
        ),
        pytest.param(
            {"fast_period": 12, "slow_period": 12, "signal_period": 9,
             "price_field": "close", "output_line": "MACD"},
            id="equal-periods",
        ),
        pytest.param(
            {"fast_period": 1, "slow_period": 26, "signal_period": 9,
             "price_field": "close", "output_line": "MACD"},
            id="degenerate-fast",
        ),
        pytest.param(
            {"fast_period": 12, "slow_period": 26, "signal_period": 1,
             "price_field": "close", "output_line": "MACD"},
            id="degenerate-signal",
        ),
        pytest.param(
            {"fast_period": 12, "slow_period": 26, "signal_period": 9,
             "price_field": "close", "output_line": "TREND"},
            id="unknown-output-line",
        ),
        pytest.param(
            {"fast_period": 12, "slow_period": 26, "signal_period": 9, "price_field": "close"},
            id="missing-output-line",
        ),
        pytest.param(
            {"fast_period": 12.0, "slow_period": 26, "signal_period": 9,
             "price_field": "close", "output_line": "MACD"},
            id="float-period",
        ),
    ],
)
def test_macd_parameters_are_refused_when_they_do_not_match_the_contract(
    parameters: dict[str, Any],
) -> None:
    with pytest.raises(InvalidFeatureParameters):
        get_calculator("MACD", "1.0.0").normalize_parameters(parameters)


def test_each_macd_output_line_is_its_own_definition() -> None:
    """One value per bar means one line per definition; the hashes must differ."""

    hashes = {
        line: FeatureDefinition.create(
            element_catalog_version_id=CATALOG_VERSION_ID,
            feature_code="MACD",
            calculator_version="1.0.0",
            resolution="1d",
            parameters=macd_parameters(line, fast=12, slow=26, signal=9),
        ).definition_hash
        for line in ("MACD", "SIGNAL", "HISTOGRAM")
    }
    assert len(set(hashes.values())) == 3


# ======================================================================================
# 4. The official catalog
# ======================================================================================


def test_the_official_catalog_lists_exactly_the_v1_feature_set() -> None:
    assert OFFICIAL_FEATURE_CATALOG_VERSION == "feature-catalog:1.0.0"
    assert catalog_object().names() == (
        "EMA_12",
        "EMA_26",
        "MACD_12_26_9",
        "MACD_12_26_9_HISTOGRAM",
        "MACD_12_26_9_SIGNAL",
        "RSI_14",
        "SMA_20",
    )


@pytest.mark.parametrize("name", sorted(EXPECTED_ENTRY_JSON))
def test_every_catalog_entry_hashes_the_pinned_canonical_payload(name: str) -> None:
    """The exact bytes each `entry_hash` is taken over, written out by hand.

    This is what "bound together" means in issue #15: the element catalog version is
    supplied per deployment, and everything else -- calculator version, normalized
    parameters, output type, required history, trading calendar, precision rule,
    input-adjustment rule and the formula rules -- is inside these bytes.
    """

    entry = catalog_object().entry(name)
    assert entry.canonical_payload() == EXPECTED_ENTRY_JSON[name]
    assert entry.entry_hash == sha256_of(EXPECTED_ENTRY_JSON[name])
    assert entry.calendar_id == "XNYS"
    assert entry.input_adjustment == "SPLIT_DIVIDEND_ADJUSTED"
    assert entry.precision_rules_version == "precision:1.0.0"
    assert entry.resolution == EXPECTED_RESOLUTIONS[name]


def test_the_catalog_hash_covers_every_entry() -> None:
    """The catalog's own hash, hand-assembled here from the seven pinned entry hashes."""

    expected_document = (
        '{"catalog_schema_version":1,'
        '"catalog_version":"feature-catalog:1.0.0",'
        '"entries":['
        + ",".join(
            f'{{"entry_hash":"{sha256_of(EXPECTED_ENTRY_JSON[name])}","name":"{name}"}}'
            for name in sorted(EXPECTED_ENTRY_JSON)
        )
        + "]}"
    )
    assert catalog_object().canonical_payload() == expected_document
    assert OFFICIAL_FEATURE_CATALOG_HASH == sha256_of(expected_document)
    assert catalog_object().catalog_hash == OFFICIAL_FEATURE_CATALOG_HASH


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("calendar_id", "XLON", id="calendar"),
        pytest.param("input_adjustment", "RAW", id="input-adjustment"),
        pytest.param("resolution", "1d", id="resolution"),
        pytest.param("name", "RSI_FOURTEEN", id="name"),
        pytest.param("parameters", {"period": 21, "price_field": "close"}, id="parameters"),
        pytest.param("parameters", {"period": 14, "price_field": "open"}, id="price-field"),
    ],
)
def test_changing_a_bound_rule_changes_the_entry_hash(field: str, value: Any) -> None:
    """None of these is decoration: each one changes what the feature *means*.

    The calculator's `formula_rules` are bound too, but they are not a field a caller can
    replace -- they are inside the payload
    `test_every_catalog_entry_hashes_the_pinned_canonical_payload` writes out by hand, so
    a changed rule changes that string and therefore the hash.
    """

    from dataclasses import replace

    original = catalog_object().entry("RSI_14")
    changed = replace(original, **{field: value})
    assert changed.entry_hash != original.entry_hash
    assert changed.canonical_payload() != original.canonical_payload()
    assert RSI_FORMULA_RULES_JSON in original.canonical_payload()


def test_a_catalog_whose_content_does_not_match_its_declared_hash_is_refused() -> None:
    """The self-verification, exercised rather than asserted about.

    `OfficialFeatureCatalog` recomputes its hash at construction and refuses to exist if
    it disagrees with the declared constant, so an edited entry cannot reach a caller.
    """

    with pytest.raises(FeatureCatalogIntegrityError) as raised:
        OfficialFeatureCatalog(expected_hash="0" * 64)
    assert OFFICIAL_FEATURE_CATALOG_HASH in str(raised.value)


def test_an_unknown_official_feature_is_named_in_the_failure() -> None:
    with pytest.raises(UnknownOfficialFeature) as raised:
        catalog_object().entry("STOCHASTIC_14")
    assert "STOCHASTIC_14" in str(raised.value)
    assert "RSI_14" in str(raised.value)


@pytest.mark.parametrize(
    ("name", "feature_code", "parameters", "history"),
    [
        ("RSI_14", "RSI", '{"period":14,"price_field":"close"}', 15),
        ("MACD_12_26_9", "MACD", macd_parameters_json("MACD"), 26),
        ("MACD_12_26_9_SIGNAL", "MACD", macd_parameters_json("SIGNAL"), 34),
        ("MACD_12_26_9_HISTOGRAM", "MACD", macd_parameters_json("HISTOGRAM"), 34),
        ("SMA_20", "SMA", '{"price_field":"close","window":20}', 20),
        ("EMA_12", "EMA", '{"price_field":"close","window":12}', 12),
        ("EMA_26", "EMA", '{"price_field":"close","window":26}', 26),
    ],
)
def test_a_catalog_entry_resolves_to_a_definition_with_a_pinned_hash(
    name: str, feature_code: str, parameters: str, history: int
) -> None:
    """The `definition_hash` an entry produces, pinned against hand-written bytes.

    The resolution comes from `EXPECTED_RESOLUTIONS`, so ``RSI_14`` is hashed at ``1m``
    and the other six at ``1d`` -- the mix is inside these bytes, not beside them.
    """

    expected = definition_json(
        feature_code=feature_code,
        parameters=parameters,
        required_history_points=history,
        resolution=EXPECTED_RESOLUTIONS[name],
    )
    definition = catalog_object().definition(name, element_catalog_version_id=CATALOG_VERSION_ID)
    assert definition.canonical_payload() == expected
    assert definition.definition_hash == sha256_of(expected)
    assert definition.feature_definition_version == (
        f"fdv1:{feature_code}:1.0.0:{sha256_of(expected)[:16]}"
    )


def test_publishing_the_catalog_writes_one_immutable_row_per_entry(
    local_features_catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    published = catalog_object().publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    assert sorted(published) == list(catalog_object().names())
    rows = local_features_catalog.records(FEATURE_DEFINITIONS)
    assert len(rows) == 7
    assert {row["definition_hash"] for row in rows} == {
        item.definition_hash for item in published.values()
    }
    # Idempotent: publishing the same catalog again is the same seven rows.
    catalog_object().publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    assert len(local_features_catalog.records(FEATURE_DEFINITIONS)) == 7


def test_verification_refuses_a_catalog_version_that_was_never_published(
    registry: FeatureDefinitionRegistry,
) -> None:
    """`verify_published` is the "immutable definition_hash verification" of issue #15."""

    official = catalog_object()
    with pytest.raises(FeatureCatalogIntegrityError) as raised:
        official.verify_published(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    assert "EMA_12" in str(raised.value)

    official.publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    official.verify_published(registry, element_catalog_version_id=CATALOG_VERSION_ID)


def test_verification_refuses_a_definition_that_drifted_from_its_catalog_entry(
    local_features_catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    """A neighbouring definition is not the official one, however plausible it looks.

    Everything but ``RSI_14`` is published from the catalog; in its place goes an
    RSI(21) -- a perfectly valid definition that a hand-rolled "is there an RSI row?"
    check would happily accept.  Verification is against the hash, so it does not.
    """

    official = catalog_object()
    for name in official.names():
        if name != "RSI_14":
            registry.publish(official.definition(name, element_catalog_version_id=CATALOG_VERSION_ID))
    expected = official.definition("RSI_14", element_catalog_version_id=CATALOG_VERSION_ID)
    registry.publish(expected.with_parameters({"period": 21, "price_field": "close"}))
    assert len(local_features_catalog.records(FEATURE_DEFINITIONS)) == 7

    with pytest.raises(FeatureCatalogIntegrityError) as raised:
        official.verify_published(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    assert "RSI_14" in str(raised.value)
    assert expected.definition_hash in str(raised.value)


def test_the_catalog_export_document_pins_every_entry_and_definition(
    registry: FeatureDefinitionRegistry,
) -> None:
    """The persisted export: catalog, entries, and the definitions they resolved to."""

    official = catalog_object()
    official.publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    document = official.to_document(element_catalog_version_id=CATALOG_VERSION_ID)

    assert document["catalog_version"] == "feature-catalog:1.0.0"
    assert document["catalog_hash"] == OFFICIAL_FEATURE_CATALOG_HASH
    assert document["element_catalog_version_id"] == CATALOG_VERSION_ID
    assert document["precision_rules_version"] == "precision:1.0.0"
    assert [item["name"] for item in document["entries"]] == list(official.names())

    rsi = next(item for item in document["entries"] if item["name"] == "RSI_14")
    assert rsi["entry_hash"] == sha256_of(EXPECTED_ENTRY_JSON["RSI_14"])
    assert rsi["definition_hash"] == sha256_of(
        definition_json(
            feature_code="RSI",
            parameters='{"period":14,"price_field":"close"}',
            required_history_points=15,
            resolution="1m",
        )
    )
    assert rsi["calculator_id"] == "rsi:1.0.0"
    assert rsi["formula_rules"] == json.loads(RSI_FORMULA_RULES_JSON)
    # An export a consumer can actually store.
    json.dumps(document)


# ======================================================================================
# 5. Materialization of an official feature
# ======================================================================================


def test_an_official_rsi_materialization_has_a_pinned_result_hash(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """The whole chain -- entry, definition, arithmetic, rendering, result payload."""

    definition = catalog_object().definition("RSI_14", element_catalog_version_id=CATALOG_VERSION_ID)
    registry.publish(definition)
    result = materializer.materialize(
        materialization_request(definition, closes=RSI_CLOSES)
    )

    assert result.input_dataset_set_hash == SOURCE_BUNDLE_FINGERPRINT
    expected_payload = (
        "{"
        f'"definition_hash":"{definition.definition_hash.removeprefix("sha256:")}",'
        f'"input_dataset_set_hash":"{SOURCE_BUNDLE_FINGERPRINT}",'
        f'"instrument_id":"{INSTRUMENT_A}",'
        '"period_end":"2026-03-02T00:00:00Z",'
        '"period_start":"2026-01-02T00:00:00Z",'
        '"result_schema_version":1,'
        '"rows":['
        f'{{"at":"2026-01-02T00:14:00Z","value":"{RSI_VALUE_AT_SEED}"}},'
        f'{{"at":"2026-01-02T00:15:00Z","value":"{RSI_VALUE_AFTER_A_GAIN}"}},'
        f'{{"at":"2026-01-02T00:16:00Z","value":"{RSI_VALUE_AFTER_A_LOSS}"}}'
        "]}"
    )
    assert result.serialized_rows() == (
        '[{"at":"2026-01-02T00:14:00Z","value":"66.66666667"},'
        '{"at":"2026-01-02T00:15:00Z","value":"71.73913043"},'
        '{"at":"2026-01-02T00:16:00Z","value":"67.03125004"}]'
    )
    assert result.result_hash == sha256_of(expected_payload)
    assert result.feature_materialization_version == (
        f"fmv1:m:{definition.definition_hash[:16]}:{SOURCE_BUNDLE_FINGERPRINT[:16]}"
        f":{result.result_hash[:16]}"
    )


def test_recomputing_an_official_rsi_materialization_reproduces_the_same_hash(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    definition = catalog_object().definition("RSI_14", element_catalog_version_id=CATALOG_VERSION_ID)
    registry.publish(definition)
    first = materializer.materialize(materialization_request(definition, closes=RSI_CLOSES))
    second = materializer.materialize(materialization_request(definition, closes=RSI_CLOSES))
    # Pinned rather than `first == second`: a constant-returning implementation must not
    # be able to pass this.
    assert first.result_hash == second.result_hash
    assert first.result_hash == sha256_of(
        "{"
        f'"definition_hash":"{definition.definition_hash.removeprefix("sha256:")}",'
        f'"input_dataset_set_hash":"{SOURCE_BUNDLE_FINGERPRINT}",'
        f'"instrument_id":"{INSTRUMENT_A}",'
        '"period_end":"2026-03-02T00:00:00Z",'
        '"period_start":"2026-01-02T00:00:00Z",'
        '"result_schema_version":1,'
        '"rows":['
        '{"at":"2026-01-02T00:14:00Z","value":"66.66666667"},'
        '{"at":"2026-01-02T00:15:00Z","value":"71.73913043"},'
        '{"at":"2026-01-02T00:16:00Z","value":"67.03125004"}'
        "]}"
    )


def test_a_materialization_pins_the_input_dataset_manifest_it_read(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """Issue #15's "pinned immutable input Dataset Manifest ID/hash".

    The same bars read from a different manifest, or from an object whose content hash
    moved, are a different materialization -- different `input_dataset_set_hash`,
    different `result_hash`, different id -- even though the values are identical.
    """

    definition = catalog_object().definition("RSI_14", element_catalog_version_id=CATALOG_VERSION_ID)
    registry.publish(definition)
    original = materializer.materialize(materialization_request(definition, closes=RSI_CLOSES))

    for changed_field, changed_value in (
        ("dataset_manifest_id", "dddddddd-0000-4000-8000-0000000000d9"),
        ("content_hash", "4" * 64),
    ):
        source = sources()[0]
        replacement = SourceObject(
            dataset_object_id=source.dataset_object_id,
            dataset_manifest_id=(
                changed_value if changed_field == "dataset_manifest_id" else source.dataset_manifest_id
            ),
            content_hash=changed_value if changed_field == "content_hash" else source.content_hash,
            partition_start=source.partition_start,
            partition_end=source.partition_end,
            row_count=source.row_count,
        )
        moved = MaterializationRequest(
            definition=definition,
            instrument_id=INSTRUMENT_A,
            pipeline_run_id=RUN_2,
            sources=(replacement,),
            bars=minute_bars(RSI_CLOSES),
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            source_watermark=SOURCE_WATERMARK,
            output_dataset_manifest_id=OUTPUT_MANIFESTS[RUN_2],
        )
        assert moved.input_dataset_set_hash != original.input_dataset_set_hash
        assert moved.materialization_id != original.materialization_id


# ======================================================================================
# 6. Compatibility with what is already merged
# ======================================================================================


def test_this_card_does_not_move_the_already_published_sma_definition() -> None:
    """The merged SMA(3) definition over 30-minute bars must hash exactly as before.

    Adding `formula_rules`, RSI and MACD must not disturb a definition another card has
    already written rows for.  The expected bytes are the ones
    `tests/test_features.py::DEFINITION_CANONICAL_JSON` pins, restated here so this file
    fails on its own if the hashed payload shape ever moves.
    """

    merged = (
        '{"calculator_version":"1.0.0",'
        '"element_catalog_version_id":"0e5a1c9e-1111-4a11-8a11-000000000001",'
        '"feature_code":"SMA",'
        '"normalized_parameters":{"price_field":"close","window":3},'
        '"output_value_type":"DECIMAL",'
        '"required_history_points":3,'
        '"resolution":"30m",'
        '"schema_version":1}'
    )
    definition = FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="30m",
        parameters={"window": 3, "price_field": "close"},
    )
    assert definition.canonical_payload() == merged
    assert definition.definition_hash == sha256_of(merged)
    assert definition.id == "9578e4e5-e8c2-538b-9f73-581fd66782e3"


def test_the_merged_sma_and_ema_arithmetic_is_unchanged() -> None:
    """The literals `tests/test_features.py` already pins, recomputed here.

    Factoring the EMA recursion out so MACD can share it is the kind of refactor that
    silently moves a seed.  These are the merged answers.
    """

    closes = (100, 104, 108, 111, 107)
    bars = daily_bars(closes)
    sma = get_calculator("SMA", "1.0.0").compute(bars, {"price_field": "close", "window": 3})
    assert [str(item.value) for item in sma] == ["104.00000000", "107.66666667", "108.66666667"]
    ema = get_calculator("EMA", "1.0.0").compute(bars, {"price_field": "close", "window": 3})
    assert [str(item.value) for item in ema] == ["104.00000000", "107.50000000", "107.25000000"]


# --------------------------------------------------------------------------------------
# The D90 `feature-object-v1` consumer.
#
# Proven by publishing and verifying a real bundle whose requirements are named from the
# official catalog, not asserted about.  `publish_realtime_warmup_bundle` calls
# `verify_realtime_warmup_bundle` on the staged tree before it is moved into place, so a
# bundle that comes back is a bundle that passed the consumer's binding checks.
# --------------------------------------------------------------------------------------

D90_FIXTURE = Path(__file__).parent / "fixtures" / "d90" / "provider-neutral-market-events.json"
D90_INSTRUMENT = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"
D90_SESSION = "2026-07-31"
D90_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]


def test_the_d90_feature_object_v1_consumer_accepts_official_catalog_feature_names(
    tmp_path: Path,
) -> None:
    """D90 keeps working, and now carries a version string that pins a real definition.

    `FeatureRequirement.feature_version` used to be an unconstrained label.  Naming it
    from the official catalog's `feature_definition_version` is what makes the warm-up
    bundle and the feature catalog talk about the same artefact -- and the object schema
    version stays `feature-object-v1`, so C's consumer is untouched.

    `FeatureRequirement.resolution` is ``PT1M`` -- the ISO-8601 form of the catalog
    entry's ``1m``, and the same value the backend's own warm-up requirement carries
    (``requirementId: rsi-14-pt1m``).  The two spellings live in different fields on
    purpose: the requirement states the cadence of the *observation stream* C must have
    received, the catalog entry states the feature's own resolution, exactly as
    `value_field` states the provider's key rather than the feature's name.  Collapsing
    those vocabularies is how ``PT1M`` and ``close`` got hardcoded in the first place.

    This is proof rather than assertion: `publish_realtime_warmup_bundle` runs
    `verify_realtime_warmup_bundle` over the staged tree before moving it into place, and
    the published tree is verified again below, so a bundle that comes back is one the
    consumer's own binding checks accepted.
    """

    assert FEATURE_OBJECT_SCHEMA_VERSION == "feature-object-v1"

    definition = catalog_object().definition("RSI_14", element_catalog_version_id=CATALOG_VERSION_ID)
    requirement = FeatureRequirement(
        requirement_id="rsi-14-entry",
        feature_id="RSI_14",
        feature_version=definition.feature_definition_version,
        resolution="PT1M",
        value_field="close",
        instruments=(D90_INSTRUMENT,),
        required_observations=1,
    )
    document = json.loads(D90_FIXTURE.read_text(encoding="utf-8"))
    root = Path(tempfile.mkdtemp(dir=tmp_path))
    try:
        bundle = publish_realtime_warmup_bundle(
            document,
            root / "bundle",
            (requirement,),
            spec=WarmupPublicationSpec(contract=D90_CONTRACT, event_type="BAR_1M", granularity="DAY"),
            readiness=WarmupReadiness(
                state="READY",
                session_date_et=D90_SESSION,
                feed_id=D90_CONTRACT.feed_code,
                evaluated_at=datetime(2026, 7, 31, 21, 5, tzinfo=UTC),
            ),
        )
        assert bundle.manifest["status"] == "AVAILABLE"

        feature = json.loads(bundle.feature_object_path.read_text(encoding="utf-8"))
        assert feature["object_schema_version"] == "feature-object-v1"
        assert feature["series"][0]["feature_id"] == "RSI_14"
        assert feature["series"][0]["feature_version"] == definition.feature_definition_version
        assert feature["series"][0]["feature_version"].startswith("fdv1:RSI:1.0.0:")

        # The consumer's own binding check, run against the published tree.
        verified = verify_realtime_warmup_bundle(bundle.manifest_path.parent)
        assert verified["manifest_id"] == bundle.manifest["manifest_id"]
    finally:
        shutil.rmtree(long_path(root), ignore_errors=True)


# ======================================================================================
# 7. PostgreSQL
# ======================================================================================

PROVIDER_ID = "0aaaaaaa-0000-4000-8000-0000000000d1"
FEED_ID = "0bbbbbbb-0000-4000-8000-0000000000d1"


@pytest.fixture
def upstream_element_catalog_version(admin_engine: Any) -> None:
    """Seed the `strategy` row the definitions' foreign key cites.

    `strategy` is read-only for this repository, so the row is arranged through the
    harness's unguarded `admin_engine`.
    """

    from sqlalchemy import text

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO strategy.element_catalog_versions "
                "(id, language_version, schema_version, catalog_version, "
                " data_requirement_version, definition_hash, published_at) "
                "VALUES (:id, '1.0.0', '1', '1.0.0', '1.0.0', :digest, "
                " TIMESTAMPTZ '2026-01-01 00:00:00+00') "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": CATALOG_VERSION_ID, "digest": "e" * 64},
        )


def seed_foreign_keys(catalog: Any) -> None:
    """Every row the feature tables' foreign keys point at, through the catalog itself."""

    catalog.upsert(
        "market_data.providers",
        {
            "id": PROVIDER_ID,
            "code": "OFFICIAL_FEATURE_TEST",
            "display_name": "Official Feature Test",
            "rights_version": "1.0.0",
            "status": "ACTIVE",
            "created_at": "2026-01-02T00:00:00Z",
        },
    )
    catalog.upsert(
        "market_data.feeds",
        {
            "id": FEED_ID,
            "provider_id": PROVIDER_ID,
            "code": "OFFICIAL_FEATURE_TEST_1D",
            "data_kind": "BARS",
            "resolution": "1d",
            "timezone_name": "America/New_York",
            "feed_version": "official-feature-v1",
            "created_at": "2026-01-02T00:00:00Z",
            "retired_at": None,
        },
    )
    catalog.upsert(
        "market_data.instruments",
        {
            "id": INSTRUMENT_A,
            "asset_type": "STOCK",
            "primary_exchange_mic": "XNYS",
            "currency_code": "USD",
            "provider_reference": None,
            "listed_at": None,
            "delisted_at": None,
            "created_at": "2026-01-02T00:00:00Z",
        },
    )
    catalog.upsert(
        "market_data.dataset_manifests",
        {
            "id": SOURCE_MANIFEST_ID,
            "feed_id": FEED_ID,
            "instrument_id": None,
            "data_layer": "ADJUSTED",
            "resolution": "1d",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": "2026-01-02T00:00:00Z",
            "period_end": "2026-03-02T00:00:00Z",
            "schema_version": "market-bars-v2",
            "dataset_hash": "9" * 64,
            "supersedes_manifest_id": None,
            "created_at": "2026-03-02T00:00:00Z",
            "available_at": "2026-03-02T00:00:00Z",
        },
    )
    for index, run_id in enumerate((RUN_1, RUN_2)):
        catalog.begin_pipeline_run(
            {
                "id": run_id,
                "pipeline_code": "FEATURE_MATERIALIZATION",
                "pipeline_version": "features-v1",
                "idempotency_key": f"official-feature-test:{run_id}",
                "status": "RUNNING",
                "input_hash": "a" * 64,
                "output_hash": None,
                "started_at": "2026-03-02T01:00:00Z",
                "completed_at": None,
                "failure_code": None,
            }
        )
        catalog.upsert(
            "market_data.dataset_manifests",
            {
                "id": OUTPUT_MANIFESTS[run_id],
                "feed_id": FEED_ID,
                "instrument_id": None,
                "data_layer": "DERIVED",
                "resolution": "1d",
                "revision_number": index + 1,
                "status": "BUILDING",
                "period_start": "2026-01-02T00:00:00Z",
                "period_end": "2026-03-02T00:00:00Z",
                "schema_version": "market-features-v1",
                "dataset_hash": f"{index:064d}",
                "supersedes_manifest_id": None,
                "created_at": "2026-03-02T01:00:00Z",
                "available_at": None,
            },
        )
    catalog.upsert(
        "storage.objects",
        {
            "id": SNAPSHOT_OBJECT,
            "status": "AVAILABLE",
            "storage_provider": "LOCAL",
            "bucket_name": "official-feature-test",
            "object_key": f"market-data/features/{SNAPSHOT_OBJECT}.parquet",
            "provider_version_id": "v1",
            "content_hash": "f" * 64,
            "byte_size": 4096,
            "file_format": "PARQUET",
            "compression_codec": "UNCOMPRESSED",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": "market-features-v1",
            "row_count": 6,
            "period_start": "2026-01-02T00:00:00Z",
            "period_end": "2026-03-02T00:00:00Z",
            "encryption_key_ref": None,
            "retention_policy_version": "UNSPECIFIED",
            "retention_until": None,
            "legal_hold": False,
            "created_at": "2026-03-02T01:00:00Z",
            "verified_at": "2026-03-02T01:00:00Z",
            "quarantined_at": None,
            "superseded_at": None,
            "deleted_at": None,
        },
    )


@pytest.mark.integration
def test_the_official_catalog_persists_and_verifies_against_postgres(
    postgres_catalog: Any, upstream_element_catalog_version: None
) -> None:
    """The whole v1 catalog, its RSI and MACD materializations and a sealed batch.

    Against the canonical central schema in a real PostgreSQL 16 container, not JSONL:
    `feature_definitions` and `feature_materializations` are persisted, re-read, and the
    catalog re-verifies the definition hashes it finds there.
    """

    seed_foreign_keys(postgres_catalog)
    registry = FeatureDefinitionRegistry(postgres_catalog)
    materializer = FeatureMaterializer(postgres_catalog, registry)
    builder = FeatureSnapshotBatchBuilder(postgres_catalog, registry)
    official = catalog_object()

    published = official.publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)
    assert len(postgres_catalog.records(FEATURE_DEFINITIONS)) == 7
    official.verify_published(registry, element_catalog_version_id=CATALOG_VERSION_ID)

    rsi = published["RSI_14"]
    macd = published["MACD_12_26_9"]
    assert rsi.definition_hash == sha256_of(
        definition_json(
            feature_code="RSI",
            parameters='{"period":14,"price_field":"close"}',
            required_history_points=15,
            resolution="1m",
        )
    )

    rsi_result = materializer.materialize(
        materialization_request(rsi, closes=RSI_CLOSES, pipeline_run_id=RUN_1)
    )
    macd_result = materializer.materialize(
        materialization_request(macd, closes=MACD_REVERSING_CLOSES, pipeline_run_id=RUN_2)
    )
    assert rsi_result.row_count == 3
    assert macd_result.row_count == 15
    # Bars 33-35 again, so the values that crossed a real database are the pinned ones.
    assert [format(item.value, "f") for item in macd_result.values[8:11]] == list(
        MACD_REVERSING_LINE
    )

    plan = SnapshotBatchPlan(
        definition_hashes=(rsi.definition_hash, macd.definition_hash),
        market_inputs=(
            MarketInput(
                instrument_id=INSTRUMENT_A, input_dataset_set_hash=SOURCE_BUNDLE_FINGERPRINT
            ),
        ),
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        source_start_watermark="ALPACA_SIP_ADJUSTED_1D@2026-01-02T00:00:00Z",
        source_end_watermark=SOURCE_WATERMARK,
    )
    builder.open(plan)
    sealed = builder.seal(
        plan, results=(rsi_result, macd_result), snapshot_object_id=SNAPSHOT_OBJECT
    )
    assert sealed.row_count == 18
    assert len(postgres_catalog.records(FEATURE_MATERIALIZATIONS)) == 2
    assert len(postgres_catalog.records(FEATURE_SNAPSHOT_BATCHES)) == 1

    # Re-read through a fresh registry: the rows prove their own integrity.
    reread = FeatureDefinitionRegistry(postgres_catalog).get(rsi.definition_hash)
    assert reread.normalized_parameters == {"period": 14, "price_field": "close"}
    assert reread.required_history_points == 15
    official.verify_published(
        FeatureDefinitionRegistry(postgres_catalog),
        element_catalog_version_id=CATALOG_VERSION_ID,
    )


@pytest.mark.integration
def test_postgres_stores_the_macd_output_line_in_the_normalized_parameters(
    postgres_catalog: Any, upstream_element_catalog_version: None
) -> None:
    """Three MACD rows that differ only inside the JSONB column, and stay distinguishable."""

    seed_foreign_keys(postgres_catalog)
    registry = FeatureDefinitionRegistry(postgres_catalog)
    catalog_object().publish(registry, element_catalog_version_id=CATALOG_VERSION_ID)

    rows = [
        row
        for row in postgres_catalog.records(FEATURE_DEFINITIONS)
        if row["feature_code"] == "MACD"
    ]
    assert sorted(row["normalized_parameters"]["output_line"] for row in rows) == [
        "HISTOGRAM",
        "MACD",
        "SIGNAL",
    ]
    assert sorted(row["required_history_points"] for row in rows) == [26, 34, 34]
    assert len({row["definition_hash"] for row in rows}) == 3
