"""D13 -- feature definitions, materialization and snapshot batches.

Three properties this suite exists to hold:

*definitions are immutable*
    A published `market_data.feature_definitions` row is never edited.  A changed
    parameter is a different `definition_hash`, therefore a different row, therefore a
    new version.  `publish` refuses an in-place edit rather than upserting over it.

*materialization is byte-reproducible*
    Recomputing the same definition over the same inputs produces the same
    `result_hash` and therefore the same `feature_materialization_version`.  Every
    determinism assertion here pins a **literal**: ``first == second`` would pass
    against an implementation that returns a constant.

*a partial batch is not consumable*
    `feature_snapshot_batches` is the point-in-time grouping the backtest engine
    pins.  A batch missing one of its planned members must not acquire a
    `batch_hash`, must not reach ``SUCCEEDED``, and must refuse to hand out a version
    string.

Oracles
-------
The arithmetic expectations (SMA/EMA/RETURN_PCT) are hand-computed from the fixture
closes and written as literals -- the test never calls the production formula to decide
what the answer should be.  `DEFINITION_CANONICAL_JSON` pins the exact bytes the
definition hash is taken over, and the test hashes that literal itself, so the shape of
the hashed payload is checked independently of the module that builds it.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.features import (
    BarPoint,
    FeatureDefinition,
    FeatureDefinitionRegistry,
    FeatureMaterializer,
    FeatureSnapshotBatchBuilder,
    FeatureSnapshotValidator,
    MarketInput,
    MaterializationRequest,
    SnapshotBatchPlan,
    SourceObject,
    get_calculator,
    input_bundle_fingerprint,
    materialization_version,
    parse_feature_materialization_version,
)
from market_pipeline_lib.features.errors import (
    DefinitionIntegrityError,
    FeatureDefinitionImmutable,
    FeatureDefinitionNotPublished,
    InsufficientHistory,
    InvalidBarSeries,
    InvalidFeatureParameters,
    MaterializationConflict,
    PartialSnapshotBatch,
    SnapshotBatchNotConsumable,
    UnknownCalculator,
)
from market_pipeline_lib.features.tables import (
    FEATURE_DEFINITIONS,
    FEATURE_MATERIALIZATIONS,
    FEATURE_SNAPSHOT_BATCHES,
)

# --------------------------------------------------------------------------------------
# Fixture constants.  Deterministic literals only.
# --------------------------------------------------------------------------------------

CATALOG_VERSION_ID = "0e5a1c9e-1111-4a11-8a11-000000000001"
INSTRUMENT_A = "aaaaaaaa-0000-4000-8000-000000000001"
INSTRUMENT_B = "bbbbbbbb-0000-4000-8000-000000000002"

RUN_1 = "11111111-0000-4000-8000-000000000001"
RUN_2 = "11111111-0000-4000-8000-000000000002"
RUN_3 = "11111111-0000-4000-8000-000000000003"
RUN_4 = "11111111-0000-4000-8000-000000000004"

OBJECT_1 = "cccccccc-0000-4000-8000-000000000001"
OBJECT_2 = "cccccccc-0000-4000-8000-000000000002"
MANIFEST_IN = "dddddddd-0000-4000-8000-000000000001"
MANIFEST_OUT = "dddddddd-0000-4000-8000-000000000002"
SNAPSHOT_OBJECT = "eeeeeeee-0000-4000-8000-000000000001"

#: One output manifest per materialization: `uq_feature_materializations_output_manifest`
#: is UNIQUE, so two materializations cannot share a manifest.
OUTPUT_MANIFESTS = {
    RUN_1: "dddddddd-0000-4000-8000-000000000011",
    RUN_2: "dddddddd-0000-4000-8000-000000000012",
    RUN_3: "dddddddd-0000-4000-8000-000000000013",
    RUN_4: "dddddddd-0000-4000-8000-000000000014",
}

PERIOD_START = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
PERIOD_END = datetime(2026, 1, 5, 21, 0, tzinfo=UTC)
WATERMARK = "ALPACA_SIP_RAW_30M@2026-01-05T21:00:00Z"

#: Hand-written closes.  Chosen so no quantization lands on an exact .5 tie, which
#: would make the expected values depend on the rounding mode rather than pin it.
CLOSES = (100, 104, 108, 111, 107)

#: The exact bytes `definition_hash` is taken over, written out by hand.
DEFINITION_CANONICAL_JSON = (
    '{"calculator_version":"1.0.0",'
    '"element_catalog_version_id":"0e5a1c9e-1111-4a11-8a11-000000000001",'
    '"feature_code":"SMA",'
    '"normalized_parameters":{"price_field":"close","window":3},'
    '"output_value_type":"DECIMAL",'
    '"required_history_points":3,'
    '"resolution":"30m",'
    '"schema_version":1}'
)
SMA_DEFINITION_HASH = hashlib.sha256(DEFINITION_CANONICAL_JSON.encode("utf-8")).hexdigest()

# Pinned outputs.  Literals, not `first == second`: an implementation that returned a
# constant, or that quietly changed a hashed payload, has to fail here.

#: The `input_bundle_fingerprint` of `sources()`, pinned so the result payload in
#: `test_the_result_hash_is_taken_over_the_pinned_canonical_payload` can be written out
#: in full.
INPUT_BUNDLE_FINGERPRINT = "3966c424f160c212f4ed4444ff3baeaeacbac732a49a023ffed576a1143937ca"
SMA_RESULT_HASH = "b8eaa7fc0de55a162b6bdc86c224bc6a411cb72578f21433f5f9cf1e1e11a3c8"
SMA_FEATURE_SET_HASH = "41bc5718b77de675e135717fd03f5b885e869821a747b7f7d720d513ce0e6e48"
ONE_INSTRUMENT_MARKET_SET_HASH = "dd251bdd35146ee5b49661c6ca817927ff1456979158a8496008834ebc101064"
SMA_ONLY_BATCH_HASH = "50edd8597f1845718eeef0cc7f4dac1dd5fe1bf78888438858fecee96601adfb"
SMA_EMA_BATCH_HASH = "32d667dc3ad8c33c911f77d5adaedb81528a504edf3b3c205ea64eb5a6ac99aa"


def bars() -> tuple[BarPoint, ...]:
    return tuple(
        BarPoint(
            bar_start_at=datetime(2026, 1, 5, 14 + (30 + 30 * index) // 60, (30 + 30 * index) % 60, tzinfo=UTC),
            open=Decimal(close) - 1,
            high=Decimal(close) + 1,
            low=Decimal(close) - 2,
            close=Decimal(close),
            volume=1000 + index,
        )
        for index, close in enumerate(CLOSES)
    )


def sma_definition(window: int = 3) -> FeatureDefinition:
    return FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="30m",
        parameters={"window": window, "price_field": "close"},
    )


def ema_definition() -> FeatureDefinition:
    return FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="EMA",
        calculator_version="1.0.0",
        resolution="30m",
        parameters={"window": 3, "price_field": "close"},
    )


def sources() -> tuple[SourceObject, ...]:
    return (
        SourceObject(
            dataset_object_id=OBJECT_1,
            dataset_manifest_id=MANIFEST_IN,
            content_hash="1" * 64,
            partition_start="2026-01-05",
            partition_end="2026-01-06",
            row_count=13,
        ),
        SourceObject(
            dataset_object_id=OBJECT_2,
            dataset_manifest_id=MANIFEST_IN,
            content_hash="2" * 64,
            partition_start="2026-01-06",
            partition_end="2026-01-07",
            row_count=13,
        ),
    )


def request(
    definition: FeatureDefinition,
    *,
    pipeline_run_id: str = RUN_1,
    instrument_id: str = INSTRUMENT_A,
    series: tuple[BarPoint, ...] | None = None,
    output_manifest_id: str | None = None,
) -> MaterializationRequest:
    return MaterializationRequest(
        definition=definition,
        instrument_id=instrument_id,
        pipeline_run_id=pipeline_run_id,
        sources=sources(),
        bars=bars() if series is None else series,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        source_watermark=WATERMARK,
        output_dataset_manifest_id=output_manifest_id or OUTPUT_MANIFESTS[pipeline_run_id],
    )


@pytest.fixture
def catalog(tmp_path: Path) -> LocalCatalog:
    return LocalCatalog(tmp_path / "features-catalog")


@pytest.fixture
def registry(catalog: LocalCatalog) -> FeatureDefinitionRegistry:
    return FeatureDefinitionRegistry(catalog)


@pytest.fixture
def materializer(catalog: LocalCatalog, registry: FeatureDefinitionRegistry) -> FeatureMaterializer:
    return FeatureMaterializer(catalog, registry)


# --------------------------------------------------------------------------------------
# Definitions
# --------------------------------------------------------------------------------------


def test_definition_hash_is_taken_over_the_pinned_canonical_payload() -> None:
    """The hashed bytes are pinned by hand, so the payload shape cannot drift silently."""

    definition = sma_definition()
    assert definition.canonical_payload() == DEFINITION_CANONICAL_JSON
    assert definition.definition_hash == SMA_DEFINITION_HASH


def test_definition_identity_is_pinned() -> None:
    definition = sma_definition()
    assert definition.required_history_points == 3
    assert definition.output_value_type == "DECIMAL"
    assert definition.normalized_parameters == {"price_field": "close", "window": 3}
    assert definition.definition_hash == SMA_DEFINITION_HASH
    assert definition.id == "9578e4e5-e8c2-538b-9f73-581fd66782e3"
    assert definition.feature_definition_version == f"fdv1:SMA:1.0.0:{SMA_DEFINITION_HASH[:16]}"


def test_parameter_key_order_does_not_change_the_definition() -> None:
    first = FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="30m",
        parameters={"window": 3, "price_field": "close"},
    )
    second = FeatureDefinition.create(
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="30m",
        parameters={"price_field": "close", "window": 3},
    )
    assert first.definition_hash == SMA_DEFINITION_HASH
    assert second.definition_hash == SMA_DEFINITION_HASH


def test_a_changed_parameter_is_a_different_definition() -> None:
    three = sma_definition(window=3)
    four = sma_definition(window=4)
    assert three.definition_hash != four.definition_hash
    assert three.id != four.id
    assert four.required_history_points == 4


@pytest.mark.parametrize(
    "parameters",
    [
        pytest.param({"window": 3.0, "price_field": "close"}, id="float-window"),
        pytest.param({"window": 0, "price_field": "close"}, id="zero-window"),
        pytest.param({"window": 3}, id="missing-price-field"),
        pytest.param({"window": 3, "price_field": "close", "extra": 1}, id="unknown-parameter"),
        pytest.param({"window": 3, "price_field": "midpoint"}, id="unknown-price-field"),
        pytest.param({"window": True, "price_field": "close"}, id="bool-window"),
    ],
)
def test_invalid_parameters_are_refused(parameters: dict[str, Any]) -> None:
    with pytest.raises(InvalidFeatureParameters):
        FeatureDefinition.create(
            element_catalog_version_id=CATALOG_VERSION_ID,
            feature_code="SMA",
            calculator_version="1.0.0",
            resolution="30m",
            parameters=parameters,
        )


def test_a_definition_for_an_unknown_calculator_cannot_be_created() -> None:
    with pytest.raises(UnknownCalculator):
        FeatureDefinition.create(
            element_catalog_version_id=CATALOG_VERSION_ID,
            feature_code="SMA",
            calculator_version="99.0.0",
            resolution="30m",
            parameters={"window": 3, "price_field": "close"},
        )


def test_publish_writes_one_canonical_row(catalog: LocalCatalog, registry: FeatureDefinitionRegistry) -> None:
    definition = registry.publish(sma_definition())
    rows = catalog.records(FEATURE_DEFINITIONS)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == definition.id
    assert row["definition_hash"] == SMA_DEFINITION_HASH
    assert row["feature_code"] == "SMA"
    assert row["calculator_version"] == "1.0.0"
    assert row["resolution"] == "30m"
    assert row["normalized_parameters"] == {"price_field": "close", "window": 3}
    assert row["output_value_type"] == "DECIMAL"
    assert row["required_history_points"] == 3
    assert row["element_catalog_version_id"] == CATALOG_VERSION_ID


def test_publishing_the_same_definition_twice_is_one_row(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    registry.publish(sma_definition())
    registry.publish(sma_definition())
    assert len(catalog.records(FEATURE_DEFINITIONS)) == 1


def test_a_published_definition_cannot_be_edited_in_place(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    registry.publish(sma_definition())
    tampered = FeatureDefinition(
        id=sma_definition().id,
        element_catalog_version_id=CATALOG_VERSION_ID,
        feature_code="SMA",
        calculator_version="1.0.0",
        resolution="30m",
        normalized_parameters={"price_field": "close", "window": 9},
        output_value_type="DECIMAL",
        required_history_points=9,
        definition_hash=SMA_DEFINITION_HASH,
        verify=False,
    )
    with pytest.raises(FeatureDefinitionImmutable):
        registry.publish(tampered)
    assert catalog.records(FEATURE_DEFINITIONS)[0]["required_history_points"] == 3


def test_a_changed_definition_is_published_as_a_new_version(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    # Explicit publication timestamps: `versions` orders by them, and two `now()` calls
    # on Windows can land in the same tick.
    first = registry.publish(sma_definition(window=3), created_at=datetime(2026, 1, 2, tzinfo=UTC))
    second = registry.publish(sma_definition(window=4), created_at=datetime(2026, 3, 4, tzinfo=UTC))
    rows = catalog.records(FEATURE_DEFINITIONS)
    assert len(rows) == 2
    versions = registry.versions(feature_code="SMA", element_catalog_version_id=CATALOG_VERSION_ID)
    assert [item.definition_hash for item in versions] == [
        first.definition_hash,
        second.definition_hash,
    ]
    assert first.definition_hash != second.definition_hash


def test_a_row_whose_hash_does_not_match_its_content_is_refused(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry
) -> None:
    published = registry.publish(sma_definition())
    row = dict(catalog.records(FEATURE_DEFINITIONS)[0])
    row["normalized_parameters"] = {"price_field": "close", "window": 50}
    catalog.upsert(FEATURE_DEFINITIONS, row)
    with pytest.raises(DefinitionIntegrityError):
        registry.get(published.definition_hash)


def test_get_of_an_unpublished_definition_is_refused(registry: FeatureDefinitionRegistry) -> None:
    with pytest.raises(FeatureDefinitionNotPublished):
        registry.get("f" * 64)


# --------------------------------------------------------------------------------------
# Calculators.  Expected values are hand-computed from CLOSES = (100, 104, 108, 111, 107).
# --------------------------------------------------------------------------------------


def test_sma_values_are_hand_computed_literals() -> None:
    calculator = get_calculator("SMA", "1.0.0")
    values = calculator.compute(bars(), {"price_field": "close", "window": 3})
    # (100+104+108)/3 = 104 ; (104+108+111)/3 = 323/3 ; (108+111+107)/3 = 326/3
    assert [str(item.value) for item in values] == [
        "104.00000000",
        "107.66666667",
        "108.66666667",
    ]
    assert [item.bar_start_at.isoformat() for item in values] == [
        "2026-01-05T15:30:00+00:00",
        "2026-01-05T16:00:00+00:00",
        "2026-01-05T16:30:00+00:00",
    ]


def test_ema_values_are_hand_computed_literals() -> None:
    calculator = get_calculator("EMA", "1.0.0")
    values = calculator.compute(bars(), {"price_field": "close", "window": 3})
    # alpha = 2/(3+1) = 0.5, seeded with the SMA of the first three closes (=104).
    # 104 + 0.5*(111-104) = 107.5 ; 107.5 + 0.5*(107-107.5) = 107.25
    assert [str(item.value) for item in values] == [
        "104.00000000",
        "107.50000000",
        "107.25000000",
    ]


def test_return_pct_values_are_hand_computed_literals() -> None:
    calculator = get_calculator("RETURN_PCT", "1.0.0")
    values = calculator.compute(bars(), {"price_field": "close", "lag": 2})
    # (108-100)/100 ; (111-104)/104 = 700/104 ; (107-108)/108 = -100/108
    assert [str(item.value) for item in values] == [
        "8.00000000",
        "6.73076923",
        "-0.92592593",
    ]


def test_high_price_field_is_honoured() -> None:
    calculator = get_calculator("SMA", "1.0.0")
    values = calculator.compute(bars(), {"price_field": "high", "window": 3})
    # highs are close+1: (101+105+109)/3 = 105
    assert str(values[0].value) == "105.00000000"


def test_an_unknown_calculator_is_named_in_the_failure() -> None:
    with pytest.raises(UnknownCalculator) as raised:
        get_calculator("SORCERY", "1.0.0")
    assert "SORCERY" in str(raised.value)


# --------------------------------------------------------------------------------------
# Input bundle fingerprint (= COM06 `input_bundle_fingerprint`)
# --------------------------------------------------------------------------------------


def test_input_bundle_fingerprint_is_pinned_and_order_independent() -> None:
    forward = input_bundle_fingerprint(sources())
    backward = input_bundle_fingerprint(tuple(reversed(sources())))
    assert forward == INPUT_BUNDLE_FINGERPRINT
    assert backward == forward
    assert len(forward) == 64


def test_input_bundle_fingerprint_changes_with_the_source_content() -> None:
    changed = list(sources())
    changed[0] = SourceObject(
        dataset_object_id=OBJECT_1,
        dataset_manifest_id=MANIFEST_IN,
        content_hash="9" * 64,
        partition_start="2026-01-05",
        partition_end="2026-01-06",
        row_count=13,
    )
    assert input_bundle_fingerprint(changed) != input_bundle_fingerprint(sources())


def test_an_empty_input_bundle_is_refused() -> None:
    with pytest.raises(ValueError):
        input_bundle_fingerprint(())


# --------------------------------------------------------------------------------------
# Materialization
# --------------------------------------------------------------------------------------


def test_materialization_writes_a_succeeded_row_with_a_pinned_result_hash(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    definition = registry.publish(sma_definition())
    result = materializer.materialize(request(definition))

    assert result.status == "SUCCEEDED"
    assert result.row_count == 3
    assert result.result_hash == SMA_RESULT_HASH
    assert result.input_dataset_set_hash == input_bundle_fingerprint(sources())
    assert result.feature_materialization_version == (
        f"fmv1:m:{SMA_DEFINITION_HASH[:16]}:{result.input_dataset_set_hash[:16]}:{result.result_hash[:16]}"
    )

    rows = catalog.records(FEATURE_MATERIALIZATIONS)
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "SUCCEEDED"
    assert row["result_hash"] == result.result_hash
    assert row["feature_definition_id"] == definition.id
    assert row["instrument_id"] == INSTRUMENT_A
    assert row["pipeline_run_id"] == RUN_1
    assert row["input_dataset_set_hash"] == result.input_dataset_set_hash
    assert row["source_watermark"] == WATERMARK
    assert row["period_start"] == "2026-01-05T14:30:00Z"
    assert row["period_end"] == "2026-01-05T21:00:00Z"
    assert row["available_at"] is not None


def test_the_result_hash_is_taken_over_the_pinned_canonical_payload() -> None:
    """`SMA_RESULT_HASH` is not merely a recorded output: it is the sha256 of these bytes.

    The row values inside are the same hand-computed literals as
    `test_sma_values_are_hand_computed_literals`, so this pins the whole chain --
    arithmetic, rendering and payload shape -- without calling the production
    canonicaliser.
    """

    payload = (
        "{"
        f'"definition_hash":"{SMA_DEFINITION_HASH}",'
        f'"input_dataset_set_hash":"{INPUT_BUNDLE_FINGERPRINT}",'
        f'"instrument_id":"{INSTRUMENT_A}",'
        '"period_end":"2026-01-05T21:00:00Z",'
        '"period_start":"2026-01-05T14:30:00Z",'
        '"result_schema_version":1,'
        '"rows":['
        '{"at":"2026-01-05T15:30:00Z","value":"104.00000000"},'
        '{"at":"2026-01-05T16:00:00Z","value":"107.66666667"},'
        '{"at":"2026-01-05T16:30:00Z","value":"108.66666667"}'
        "]}"
    )
    assert hashlib.sha256(payload.encode("utf-8")).hexdigest() == SMA_RESULT_HASH


def test_recomputation_over_the_same_inputs_is_byte_identical(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    definition = registry.publish(sma_definition())
    first = materializer.materialize(request(definition, pipeline_run_id=RUN_1))
    second = materializer.materialize(request(definition, pipeline_run_id=RUN_1))
    # Pinned, not merely `first == second`: a constant-returning implementation must
    # not be able to pass this.
    assert first.result_hash == SMA_RESULT_HASH
    assert second.result_hash == SMA_RESULT_HASH
    assert first.feature_materialization_version == second.feature_materialization_version
    assert first.serialized_rows() == second.serialized_rows()


def test_a_different_definition_over_the_same_bars_has_a_different_result_hash(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    sma_result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    ema_result = materializer.materialize(request(ema, pipeline_run_id=RUN_2))
    assert sma_result.result_hash != ema_result.result_hash
    assert sma_result.feature_materialization_version != ema_result.feature_materialization_version


def test_materializing_an_unpublished_definition_is_refused(
    catalog: LocalCatalog, materializer: FeatureMaterializer
) -> None:
    with pytest.raises(FeatureDefinitionNotPublished):
        materializer.materialize(request(sma_definition()))
    assert catalog.records(FEATURE_MATERIALIZATIONS) == []


def test_insufficient_history_is_recorded_as_a_failed_materialization(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    definition = registry.publish(sma_definition(window=4))
    short = bars()[:2]
    with pytest.raises(InsufficientHistory):
        materializer.materialize(request(definition, series=short))
    rows = catalog.records(FEATURE_MATERIALIZATIONS)
    assert len(rows) == 1
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["result_hash"] is None
    assert rows[0]["available_at"] is None


@pytest.mark.parametrize(
    "series_name",
    ["unsorted", "duplicated", "naive", "outside-period"],
)
def test_a_bad_bar_series_is_refused(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer, series_name: str
) -> None:
    definition = registry.publish(sma_definition())
    original = list(bars())
    if series_name == "unsorted":
        original[1], original[2] = original[2], original[1]
    elif series_name == "duplicated":
        original[2] = original[1]
    elif series_name == "naive":
        original[0] = BarPoint(
            bar_start_at=datetime(2026, 1, 5, 14, 30),
            open=Decimal(99),
            high=Decimal(101),
            low=Decimal(98),
            close=Decimal(100),
            volume=1000,
        )
    else:
        original[4] = BarPoint(
            bar_start_at=datetime(2026, 1, 6, 14, 30, tzinfo=UTC),
            open=Decimal(106),
            high=Decimal(108),
            low=Decimal(105),
            close=Decimal(107),
            volume=1004,
        )
    with pytest.raises(InvalidBarSeries):
        materializer.materialize(request(definition, series=tuple(original)))


def test_one_pipeline_run_cannot_carry_two_materializations(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(MaterializationConflict):
        materializer.materialize(request(ema, pipeline_run_id=RUN_1))


def test_materialization_records_lineage_from_the_output_manifest_to_its_sources(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    definition = registry.publish(sma_definition())
    materializer.materialize(request(definition))
    lineage = catalog.records("market_data.dataset_lineage")
    assert lineage == [
        {
            "derived_manifest_id": OUTPUT_MANIFESTS[RUN_1],
            "source_manifest_id": MANIFEST_IN,
            "relation_type": "FEATURE_MATERIALIZED_FROM",
        }
    ]
    row = catalog.records(FEATURE_MATERIALIZATIONS)[0]
    assert row["output_dataset_manifest_id"] == OUTPUT_MANIFESTS[RUN_1]


# --------------------------------------------------------------------------------------
# Snapshot batches
# --------------------------------------------------------------------------------------


def plan(*definitions: FeatureDefinition, instruments: tuple[str, ...] = (INSTRUMENT_A,)) -> SnapshotBatchPlan:
    fingerprint = input_bundle_fingerprint(sources())
    return SnapshotBatchPlan(
        definition_hashes=tuple(item.definition_hash for item in definitions),
        market_inputs=tuple(
            MarketInput(instrument_id=instrument, input_dataset_set_hash=fingerprint) for instrument in instruments
        ),
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        source_start_watermark="ALPACA_SIP_RAW_30M@2026-01-05T14:30:00Z",
        source_end_watermark=WATERMARK,
    )


def test_batch_identity_is_pinned() -> None:
    batch_plan = plan(sma_definition())
    assert batch_plan.feature_set_hash == SMA_FEATURE_SET_HASH
    assert batch_plan.input_market_set_hash == ONE_INSTRUMENT_MARKET_SET_HASH
    assert batch_plan.expected_member_count == 1
    assert batch_plan.idempotency_key == (
        f"fsb1:{batch_plan.feature_set_hash[:16]}:{batch_plan.input_market_set_hash[:16]}"
        ":20260105T143000Z:20260105T210000Z"
    )
    assert len(batch_plan.idempotency_key) <= 160
    assert batch_plan.id == "6bab1c6d-4510-5c9d-adea-a2da1d50b70a"


def test_batch_identity_ignores_the_order_definitions_are_listed_in() -> None:
    sma = sma_definition()
    ema = ema_definition()
    assert plan(sma, ema).feature_set_hash == plan(ema, sma).feature_set_hash
    assert plan(sma, ema).id == plan(ema, sma).id


def test_opening_a_batch_writes_a_pending_row(catalog: LocalCatalog, registry: FeatureDefinitionRegistry) -> None:
    definition = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(definition)
    builder.open(batch_plan)
    rows = catalog.records(FEATURE_SNAPSHOT_BATCHES)
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "PENDING"
    assert row["batch_hash"] is None
    assert row["row_count"] is None
    assert row["available_at"] is None
    assert row["idempotency_key"] == batch_plan.idempotency_key
    assert row["source_start_watermark"] == "ALPACA_SIP_RAW_30M@2026-01-05T14:30:00Z"
    assert row["source_end_watermark"] == WATERMARK


def test_a_partially_materialized_batch_is_not_consumable(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma, ema)
    builder.open(batch_plan)
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))  # EMA deliberately absent

    with pytest.raises(PartialSnapshotBatch) as raised:
        builder.seal(batch_plan, results=(), snapshot_object_id=SNAPSHOT_OBJECT)
    assert ema.definition_hash in str(raised.value)

    row = catalog.records(FEATURE_SNAPSHOT_BATCHES)[0]
    assert row["status"] == "PENDING"
    assert row["batch_hash"] is None
    assert row["available_at"] is None

    with pytest.raises(SnapshotBatchNotConsumable):
        builder.consume(batch_plan)


def test_a_batch_with_a_failed_member_is_marked_failed_not_partial(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    wide = registry.publish(sma_definition(window=4))
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma, wide)
    builder.open(batch_plan)
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(InsufficientHistory):
        materializer.materialize(request(wide, pipeline_run_id=RUN_2, series=bars()[:2]))

    with pytest.raises(PartialSnapshotBatch):
        builder.seal(batch_plan, results=(), snapshot_object_id=SNAPSHOT_OBJECT)
    assert catalog.records(FEATURE_SNAPSHOT_BATCHES)[0]["status"] == "FAILED"
    with pytest.raises(SnapshotBatchNotConsumable):
        builder.consume(batch_plan)


def test_a_complete_batch_seals_with_a_pinned_batch_hash(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma, ema)
    builder.open(batch_plan)
    sma_result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    ema_result = materializer.materialize(request(ema, pipeline_run_id=RUN_2))

    sealed = builder.seal(batch_plan, results=(sma_result, ema_result), snapshot_object_id=SNAPSHOT_OBJECT)
    assert sealed.batch_hash == SMA_EMA_BATCH_HASH
    assert sealed.row_count == 6
    assert sealed.status == "SUCCEEDED"
    assert sealed.feature_materialization_version == (
        f"fmv1:b:{batch_plan.feature_set_hash[:16]}:{batch_plan.input_market_set_hash[:16]}:{sealed.batch_hash[:16]}"
    )

    row = catalog.records(FEATURE_SNAPSHOT_BATCHES)[0]
    assert row["status"] == "SUCCEEDED"
    assert row["batch_hash"] == sealed.batch_hash
    assert row["row_count"] == 6
    assert row["available_at"] is not None

    consumable = builder.consume(batch_plan)
    assert consumable.feature_materialization_version == sealed.feature_materialization_version
    assert consumable.input_bundle_fingerprint == batch_plan.input_market_set_hash


def test_sealing_a_sealed_batch_is_idempotent(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    first = builder.seal(batch_plan, results=(result,), snapshot_object_id=SNAPSHOT_OBJECT)
    second = builder.seal(batch_plan, results=(result,), snapshot_object_id=SNAPSHOT_OBJECT)
    assert first.batch_hash == second.batch_hash
    assert len(catalog.records(FEATURE_SNAPSHOT_BATCHES)) == 1


def test_a_second_instrument_changes_the_batch_identity_and_membership(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    one = plan(sma)
    two = plan(sma, instruments=(INSTRUMENT_A, INSTRUMENT_B))
    assert one.input_market_set_hash != two.input_market_set_hash
    assert two.expected_member_count == 2

    builder.open(two)
    first = materializer.materialize(request(sma, pipeline_run_id=RUN_1, instrument_id=INSTRUMENT_A))
    with pytest.raises(PartialSnapshotBatch):
        builder.seal(two, results=(), snapshot_object_id=SNAPSHOT_OBJECT)
    second = materializer.materialize(request(sma, pipeline_run_id=RUN_2, instrument_id=INSTRUMENT_B))
    sealed = builder.seal(two, results=(first, second), snapshot_object_id=SNAPSHOT_OBJECT)
    assert sealed.member_count == 2
    assert sealed.row_count == 6


def test_sealing_a_complete_batch_still_needs_a_snapshot_object(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """`feature_snapshot_batch_success_complete` -- SUCCEEDED needs the object.

    Application-side, so `LocalCatalog` refuses it too rather than writing a row that
    PostgreSQL would have rejected.
    """

    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(ValueError, match="snapshot_object_id"):
        builder.seal(batch_plan, results=(result,), snapshot_object_id="")
    assert catalog.records(FEATURE_SNAPSHOT_BATCHES)[0]["status"] == "PENDING"


def test_sealing_a_complete_batch_still_needs_every_result_for_the_row_count(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """`row_count` is NOT NULL for a SUCCEEDED batch and cannot be read back."""

    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(PartialSnapshotBatch):
        builder.seal(batch_plan, results=(), snapshot_object_id=SNAPSHOT_OBJECT)
    assert catalog.records(FEATURE_SNAPSHOT_BATCHES)[0]["status"] == "PENDING"


def test_a_single_definition_batch_seals_with_its_own_pinned_hash(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    sealed = builder.seal(batch_plan, results=(result,), snapshot_object_id=SNAPSHOT_OBJECT)
    # Different membership from the two-definition batch, therefore a different hash.
    assert sealed.batch_hash == SMA_ONLY_BATCH_HASH
    assert sealed.batch_hash != SMA_EMA_BATCH_HASH
    assert sealed.row_count == 3
    assert sealed.snapshot_object_id == SNAPSHOT_OBJECT


def test_materializing_without_an_output_manifest_is_refused(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    """`feature_materialization_success_complete` -- values must go somewhere."""

    definition = registry.publish(sma_definition())
    with pytest.raises(ValueError, match="output_dataset_manifest_id"):
        materializer.materialize(
            MaterializationRequest(
                definition=definition,
                instrument_id=INSTRUMENT_A,
                pipeline_run_id=RUN_1,
                sources=sources(),
                bars=bars(),
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                source_watermark=WATERMARK,
                output_dataset_manifest_id=None,  # type: ignore[arg-type]
            )
        )
    assert catalog.records(FEATURE_MATERIALIZATIONS) == []


def test_two_materializations_cannot_share_an_output_manifest(
    registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(MaterializationConflict, match="output manifest"):
        materializer.materialize(request(ema, pipeline_run_id=RUN_2, output_manifest_id=OUTPUT_MANIFESTS[RUN_1]))


def test_row_count_refuses_a_result_the_catalog_did_not_record(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    from dataclasses import replace

    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    genuine = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    fabricated = replace(genuine, result_hash="e" * 64, values=genuine.values * 100)
    with pytest.raises(PartialSnapshotBatch):
        builder.seal(batch_plan, results=(fabricated,), snapshot_object_id=SNAPSHOT_OBJECT)


def test_the_version_string_round_trips_through_its_parser() -> None:
    parsed = parse_feature_materialization_version("fmv1:b:0123456789abcdef:fedcba9876543210:00112233445566ff")
    assert parsed.scope == "b"
    assert parsed.identity_prefix == "0123456789abcdef"
    assert parsed.inputs_prefix == "fedcba9876543210"
    assert parsed.result_prefix == "00112233445566ff"


def test_materialization_version_normalizes_the_canonical_sha256_prefix() -> None:
    digest = "a" * 64

    bare = materialization_version(
        definition_hash=digest,
        input_dataset_set_hash="b" * 64,
        result_hash="c" * 64,
    )
    prefixed = materialization_version(
        definition_hash=f"sha256:{digest}",
        input_dataset_set_hash="b" * 64,
        result_hash="c" * 64,
    )

    assert prefixed == bare


def test_materialization_version_rejects_a_non_sha256_prefix() -> None:
    with pytest.raises(ValueError, match="identity component"):
        materialization_version(
            definition_hash=f"md5:{'a' * 64}",
            input_dataset_set_hash="b" * 64,
            result_hash="c" * 64,
        )


@pytest.mark.parametrize(
    "value",
    [
        "",
        "feature-materialization-v1",
        "fmv1:x:0123456789abcdef:fedcba9876543210:00112233445566ff",
        "fmv1:b:0123456789abcdef:fedcba9876543210",
        "fmv2:b:0123456789abcdef:fedcba9876543210:00112233445566ff",
        "fmv1:b:0123456789ABCDEF:fedcba9876543210:00112233445566ff",
    ],
)
def test_a_malformed_version_string_is_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_feature_materialization_version(value)


# --------------------------------------------------------------------------------------
# The feature-snapshot validator the lightweight-validation Lambda uses
# --------------------------------------------------------------------------------------


def sealed_document(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> dict[str, Any]:
    sma = registry.publish(sma_definition())
    builder = FeatureSnapshotBatchBuilder(catalog, registry)
    batch_plan = plan(sma)
    builder.open(batch_plan)
    result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    sealed = builder.seal(batch_plan, results=(result,), snapshot_object_id=SNAPSHOT_OBJECT)
    return sealed.to_document()


def test_the_validator_accepts_a_sealed_batch_document(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    document = sealed_document(catalog, registry, materializer)
    decision = FeatureSnapshotValidator().validate(document)
    assert decision["decision"] == "ACCEPTED"
    assert decision["documentType"] == "feature-snapshot"
    assert decision["featureMaterializationVersion"] == document["feature_materialization_version"]
    json.dumps(decision)


def test_the_validator_rejects_a_version_string_that_does_not_match_the_hashes(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    document = sealed_document(catalog, registry, materializer)
    document["batch_hash"] = "d" * 64
    decision = FeatureSnapshotValidator().validate(document)
    assert decision["decision"] == "REJECTED"
    assert "batch_hash" in decision["violation"]


def test_the_validator_rejects_a_batch_that_is_not_sealed(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    document = sealed_document(catalog, registry, materializer)
    document["status"] = "PENDING"
    decision = FeatureSnapshotValidator().validate(document)
    assert decision["decision"] == "REJECTED"
    assert "SUCCEEDED" in decision["violation"]


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param({"feature_set_hash": "nope"}, id="short-hash"),
        pytest.param({"feature_set_hash": "A" * 64}, id="uppercase-hash"),
        pytest.param({"row_count": -1}, id="negative-row-count"),
        pytest.param({"period_end": "2026-01-05T14:30:00Z"}, id="empty-period"),
        pytest.param({"feature_materialization_version": "feature-materialization-v1"}, id="opaque-version"),
    ],
)
def test_the_validator_rejects_malformed_documents(
    catalog: LocalCatalog,
    registry: FeatureDefinitionRegistry,
    materializer: FeatureMaterializer,
    mutation: dict[str, Any],
) -> None:
    document = sealed_document(catalog, registry, materializer)
    document.update(mutation)
    decision = FeatureSnapshotValidator().validate(document)
    assert decision["decision"] == "REJECTED"
    assert decision["violation"]


def test_the_validator_rejects_a_document_with_a_missing_field(
    catalog: LocalCatalog, registry: FeatureDefinitionRegistry, materializer: FeatureMaterializer
) -> None:
    document = sealed_document(catalog, registry, materializer)
    del document["input_market_set_hash"]
    decision = FeatureSnapshotValidator().validate(document)
    assert decision["decision"] == "REJECTED"
    assert "input_market_set_hash" in decision["violation"]


# --------------------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------------------


@pytest.fixture
def upstream_element_catalog_version(admin_engine: Any) -> None:
    """Seed the `strategy` row that `feature_definitions.element_catalog_version_id` cites.

    `market_pipeline_lib/db/tables.py` documents this foreign key as "deliberately not
    declared as a ForeignKey so `strategy` stays out of this metadata" -- but the
    *applied* central DDL does declare and enforce it
    (`V1__initial_schema.sql:3328`).  The metadata's omission is a description choice;
    the database's constraint is real, and a feature definition therefore cannot be
    published against an element catalog version that does not exist.

    `strategy` is read-only for this repository, so the row is arranged through the
    harness's unguarded `admin_engine` rather than through the catalog, which would
    refuse the write.
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


PROVIDER_ID = "0aaaaaaa-0000-4000-8000-00000000000f"
FEED_ID = "0bbbbbbb-0000-4000-8000-00000000000f"


def seed_feature_foreign_keys(catalog: Any, *, run_ids: tuple[str, ...]) -> None:
    """Every row the feature tables' foreign keys point at.

    All of these are enforced by the applied DDL, so a materialization cannot be written
    against instruments, runs, manifests or objects that do not exist.  Arranged through
    the catalog (not raw SQL) so the seeding itself goes through the contract under test.
    """

    catalog.upsert(
        "market_data.providers",
        {
            "id": PROVIDER_ID,
            "code": "FEATURE_TEST",
            "display_name": "Feature Test",
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
            "code": "FEATURE_TEST_30M",
            "data_kind": "BARS",
            "resolution": "30m",
            "timezone_name": "America/New_York",
            "feed_version": "feature-v1",
            "created_at": "2026-01-02T00:00:00Z",
            "retired_at": None,
        },
    )
    for instrument_id in (INSTRUMENT_A, INSTRUMENT_B):
        catalog.upsert(
            "market_data.instruments",
            {
                "id": instrument_id,
                "asset_type": "STOCK",
                "primary_exchange_mic": "XNYS",
                "currency_code": "USD",
                "provider_reference": None,
                "listed_at": None,
                "delisted_at": None,
                "created_at": "2026-01-02T00:00:00Z",
            },
        )
    # The source manifest the input bundle belongs to; `dataset_lineage` points at it.
    catalog.upsert(
        "market_data.dataset_manifests",
        {
            "id": MANIFEST_IN,
            "feed_id": FEED_ID,
            "instrument_id": None,
            "data_layer": "RAW",
            "resolution": "30m",
            "revision_number": 1,
            "status": "AVAILABLE",
            "period_start": "2026-01-05T14:30:00Z",
            "period_end": "2026-01-05T21:00:00Z",
            "schema_version": "market-bars-v2",
            "dataset_hash": "9" * 64,
            "supersedes_manifest_id": None,
            "created_at": "2026-01-05T21:00:00Z",
            "available_at": "2026-01-05T21:00:00Z",
        },
    )
    for index, run_id in enumerate(run_ids):
        catalog.begin_pipeline_run(
            {
                "id": run_id,
                "pipeline_code": "FEATURE_MATERIALIZATION",
                "pipeline_version": "features-v1",
                "idempotency_key": f"feature-test:{run_id}",
                "status": "RUNNING",
                "input_hash": "a" * 64,
                "output_hash": None,
                "started_at": "2026-01-05T21:05:00Z",
                "completed_at": None,
                "failure_code": None,
            }
        )
        # One output dataset manifest per materialization; see OUTPUT_MANIFESTS.
        catalog.upsert(
            "market_data.dataset_manifests",
            {
                "id": OUTPUT_MANIFESTS[run_id],
                "feed_id": FEED_ID,
                "instrument_id": None,
                "data_layer": "DERIVED",
                "resolution": "30m",
                "revision_number": index + 1,
                "status": "BUILDING",
                "period_start": "2026-01-05T14:30:00Z",
                "period_end": "2026-01-05T21:00:00Z",
                "schema_version": "market-features-v1",
                "dataset_hash": f"{index:064d}",
                "supersedes_manifest_id": None,
                "created_at": "2026-01-05T21:05:00Z",
                "available_at": None,
            },
        )
    catalog.upsert(
        "storage.objects",
        {
            "id": SNAPSHOT_OBJECT,
            "status": "AVAILABLE",
            "storage_provider": "LOCAL",
            "bucket_name": "feature-test",
            "object_key": f"market-data/features/{SNAPSHOT_OBJECT}.parquet",
            "provider_version_id": "v1",
            "content_hash": "f" * 64,
            "byte_size": 2048,
            "file_format": "PARQUET",
            "compression_codec": "UNCOMPRESSED",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": "market-features-v1",
            "row_count": 6,
            "period_start": "2026-01-05T14:30:00Z",
            "period_end": "2026-01-05T21:00:00Z",
            "encryption_key_ref": None,
            "retention_policy_version": "UNSPECIFIED",
            "retention_until": None,
            "legal_hold": False,
            "created_at": "2026-01-05T21:05:00Z",
            "verified_at": "2026-01-05T21:05:00Z",
            "quarantined_at": None,
            "superseded_at": None,
            "deleted_at": None,
        },
    )


@pytest.mark.integration
def test_the_three_feature_tables_round_trip_through_postgres(
    postgres_catalog: Any, upstream_element_catalog_version: None
) -> None:
    """The same hashes, against the canonical schema rather than JSONL."""

    seed_feature_foreign_keys(postgres_catalog, run_ids=(RUN_1, RUN_2))

    registry = FeatureDefinitionRegistry(postgres_catalog)
    materializer = FeatureMaterializer(postgres_catalog, registry)
    builder = FeatureSnapshotBatchBuilder(postgres_catalog, registry)

    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    batch_plan = plan(sma, ema)
    builder.open(batch_plan)
    sma_result = materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    ema_result = materializer.materialize(request(ema, pipeline_run_id=RUN_2))
    sealed = builder.seal(batch_plan, results=(sma_result, ema_result), snapshot_object_id=SNAPSHOT_OBJECT)

    assert sma.definition_hash == SMA_DEFINITION_HASH
    assert sma_result.result_hash == SMA_RESULT_HASH
    assert sealed.batch_hash == SMA_EMA_BATCH_HASH
    assert len(postgres_catalog.records(FEATURE_DEFINITIONS)) == 2
    assert len(postgres_catalog.records(FEATURE_MATERIALIZATIONS)) == 2
    assert len(postgres_catalog.records(FEATURE_SNAPSHOT_BATCHES)) == 1

    reread = FeatureDefinitionRegistry(postgres_catalog).get(SMA_DEFINITION_HASH)
    assert reread.definition_hash == SMA_DEFINITION_HASH
    assert reread.normalized_parameters == {"price_field": "close", "window": 3}


@pytest.mark.integration
def test_postgres_refuses_a_second_materialization_on_one_pipeline_run(
    postgres_catalog: Any, upstream_element_catalog_version: None
) -> None:
    """The canonical unique index on `pipeline_run_id`, against the real database."""

    seed_feature_foreign_keys(postgres_catalog, run_ids=(RUN_1, RUN_2))
    registry = FeatureDefinitionRegistry(postgres_catalog)
    materializer = FeatureMaterializer(postgres_catalog, registry)
    sma = registry.publish(sma_definition())
    ema = registry.publish(ema_definition())
    materializer.materialize(request(sma, pipeline_run_id=RUN_1))
    with pytest.raises(MaterializationConflict):
        materializer.materialize(request(ema, pipeline_run_id=RUN_1, output_manifest_id=OUTPUT_MANIFESTS[RUN_2]))
    assert len(postgres_catalog.records(FEATURE_MATERIALIZATIONS)) == 1


@pytest.mark.integration
def test_postgres_refuses_a_succeeded_batch_without_its_snapshot_object(
    postgres_catalog: Any, upstream_element_catalog_version: None
) -> None:
    """`feature_snapshot_batch_success_complete`, enforced by the database itself.

    The application refuses this first, so the check is bypassed deliberately here: the
    point is that the canonical CHECK is real and the application rule matches it rather
    than being a private opinion.
    """

    from sqlalchemy.exc import IntegrityError

    seed_feature_foreign_keys(postgres_catalog, run_ids=(RUN_1,))
    registry = FeatureDefinitionRegistry(postgres_catalog)
    sma = registry.publish(sma_definition())
    batch_plan = plan(sma)
    FeatureSnapshotBatchBuilder(postgres_catalog, registry).open(batch_plan)

    record = dict(postgres_catalog.records(FEATURE_SNAPSHOT_BATCHES)[0])
    record.update({"status": "SUCCEEDED", "batch_hash": SMA_ONLY_BATCH_HASH})
    with pytest.raises(IntegrityError, match="feature_snapshot_batch_success_complete"):
        postgres_catalog.upsert(FEATURE_SNAPSHOT_BATCHES, record)
