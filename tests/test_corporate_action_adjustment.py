"""D15 -- adjustment application and adjusted-dataset regeneration.

Every expected price in this module is hand-computed and written as a literal.
Nothing here calls the production factor helpers to build an expectation, so a
regression in the adjustment arithmetic cannot be absorbed by the test.

The hand computations these tests pin:

*Split*, 2-for-1 effective 2026-08-15 (price x 1/2, volume x 2)::

    98.00 x 0.5 = 49.00      101.00 x 0.5 = 50.50
    97.00 x 0.5 = 48.50      100.00 x 0.5 = 50.00     1000 x 2 = 2000
    190.00 x 0.5 = 95.00     205.00 x 0.5 = 102.50
    188.00 x 0.5 = 94.00     200.00 x 0.5 = 100.00    2000 x 2 = 4000

*Cash dividend*, 2.00 USD ex-date 2026-09-01, raw close the bar before is
100.00, so the factor is (100.00 - 2.00) / 100.00 = 0.98 exactly::

    90.00 x 0.98 = 88.20     95.00 x 0.98 = 93.10
    89.00 x 0.98 = 87.22     94.00 x 0.98 = 92.12     volume unchanged
    96.00 x 0.98 = 94.08     101.00 x 0.98 = 98.98
    95.00 x 0.98 = 93.10     100.00 x 0.98 = 98.00

*Both*, for a bar preceding both events the factors compound to
0.5 x 0.98 = 0.49::

    98.00 x 0.49 = 48.02     101.00 x 0.49 = 49.49
    97.00 x 0.49 = 47.53     100.00 x 0.49 = 49.00     1000 x 2 = 2000
"""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.contracts import deterministic_uuid
from market_pipeline_lib.corporate_action_research import (
    CashDividendTerms,
    Claim,
    Evidence,
    ResearchCandidate,
    SplitTerms,
)
from market_pipeline_lib.corporate_actions import (
    AdjustedDatasetRegenerator,
    AdjustmentFactor,
    AdminDecision,
    ApprovedAction,
    Bar,
    ConflictingDecisionError,
    CorporateActionReviewService,
    DecisionType,
    ReviewState,
    UnknownCandidateError,
    WrittenDataset,
    adjusted_bars,
    cash_dividend_factor,
    split_factor,
)
from market_pipeline_lib.corporate_actions.cli import main as decision_cli

SPLIT_EFFECTIVE_AT = datetime(2026, 8, 15, 4, 0, tzinfo=UTC)  # ET midnight, EDT = UTC-4
DIVIDEND_EFFECTIVE_AT = datetime(2026, 9, 1, 4, 0, tzinfo=UTC)

INSTRUMENT_ID = "11111111-1111-4111-8111-111111111111"
FEED_RAW_ID = "22222222-2222-4222-8222-222222222222"
FEED_ADJUSTED_ID = "33333333-3333-4333-8333-333333333333"
RAW_MANIFEST_ID = "44444444-4444-4444-8444-444444444444"
SOURCE_MANIFEST_ID = RAW_MANIFEST_ID


def _bar(day: int, opening: str, high: str, low: str, close: str, volume: int, *, month: int = 8) -> Bar:
    return Bar(
        instrument_id=INSTRUMENT_ID,
        bar_start_at=datetime(2026, month, day, 13, 30, tzinfo=UTC),
        open=Decimal(opening),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=volume,
    )


def _split_action() -> ApprovedAction:
    return ApprovedAction(
        action_type="STOCK_SPLIT",
        effective_at=SPLIT_EFFECTIVE_AT,
        terms=SplitTerms(from_shares=1, to_shares=2),
    )


def _dividend_action() -> ApprovedAction:
    return ApprovedAction(
        action_type="CASH_DIVIDEND",
        effective_at=DIVIDEND_EFFECTIVE_AT,
        terms=CashDividendTerms(amount=Decimal("2.00"), currency="USD"),
    )


def _evidence() -> Evidence:
    return Evidence(
        source_uri="https://issuer.example/investors/split-notice",
        source_title="Issuer split notice",
        content_sha256="a" * 64,
        retrieved_at=datetime(2026, 8, 2, 0, 1, tzinfo=UTC),
    )


def _split_candidate() -> ResearchCandidate:
    source = "https://issuer.example/investors/split-notice"
    return ResearchCandidate.create(
        ticker="AAPL",
        event_type="STOCK_SPLIT",
        proposed_date=date(2026, 8, 15),
        terms=SplitTerms(from_shares=1, to_shares=2),
        evidence=(_evidence(),),
        claims=(
            Claim("event_type", "STOCK_SPLIT", source, Decimal("0.95")),
            Claim("effective_date", "2026-08-15", source, Decimal("0.90")),
            Claim("from_shares", "1", source, Decimal("0.99")),
            Claim("to_shares", "2", source, Decimal("0.99")),
        ),
        researched_at=datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
    )


# --------------------------------------------------------------------------------------
# Factors
# --------------------------------------------------------------------------------------
class SplitFactorTests(unittest.TestCase):
    def test_two_for_one_halves_price_and_doubles_volume(self) -> None:
        factor = split_factor(SplitTerms(from_shares=1, to_shares=2))

        self.assertEqual(factor.price, Decimal("0.50000000"))
        self.assertEqual(factor.volume, Decimal("2.00000000"))

    def test_three_for_two_uses_the_share_ratio(self) -> None:
        # 3-for-2: one old share becomes 1.5 new shares, so price x 2/3.
        factor = split_factor(SplitTerms(from_shares=2, to_shares=3))

        self.assertEqual(factor.price, Decimal("0.66666667"))
        self.assertEqual(factor.volume, Decimal("1.50000000"))

    def test_reverse_split_raises_price_and_cuts_volume(self) -> None:
        factor = split_factor(SplitTerms(from_shares=10, to_shares=1))

        self.assertEqual(factor.price, Decimal("10.00000000"))
        self.assertEqual(factor.volume, Decimal("0.10000000"))

    def test_non_positive_share_counts_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive"):
            SplitTerms(from_shares=0, to_shares=2)
        with self.assertRaisesRegex(ValueError, "positive"):
            SplitTerms(from_shares=1, to_shares=-2)

    def test_a_one_for_one_split_is_not_a_corporate_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "no-op"):
            SplitTerms(from_shares=1, to_shares=1)


class CashDividendFactorTests(unittest.TestCase):
    def test_factor_is_the_ex_dividend_price_ratio(self) -> None:
        factor = cash_dividend_factor(
            CashDividendTerms(amount=Decimal("2.00"), currency="USD"),
            previous_close=Decimal("100.00"),
        )

        self.assertEqual(factor.price, Decimal("0.98000000"))
        self.assertEqual(factor.volume, Decimal("1.00000000"))

    def test_factor_is_quantized_half_even_to_eight_places(self) -> None:
        # (7.00 - 0.13) / 7.00 = 0.981428571428... -> 0.98142857
        factor = cash_dividend_factor(
            CashDividendTerms(amount=Decimal("0.13"), currency="USD"),
            previous_close=Decimal("7.00"),
        )

        self.assertEqual(factor.price, Decimal("0.98142857"))

    def test_dividend_at_or_above_the_previous_close_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "previous close"):
            cash_dividend_factor(
                CashDividendTerms(amount=Decimal("100.00"), currency="USD"),
                previous_close=Decimal("100.00"),
            )

    def test_non_positive_previous_close_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "previous close"):
            cash_dividend_factor(
                CashDividendTerms(amount=Decimal("1.00"), currency="USD"),
                previous_close=Decimal("0"),
            )

    def test_non_positive_dividend_amount_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive"):
            CashDividendTerms(amount=Decimal("0"), currency="USD")

    def test_currency_must_be_an_iso_alpha_three_code(self) -> None:
        with self.assertRaisesRegex(ValueError, "currency"):
            CashDividendTerms(amount=Decimal("1.00"), currency="dollars")


# --------------------------------------------------------------------------------------
# Bar adjustment
# --------------------------------------------------------------------------------------
class SplitAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bars = (
            _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
            _bar(14, "190.00", "205.00", "188.00", "200.00", 2000),
            _bar(17, "104.00", "106.00", "103.00", "105.00", 5000),
        )

    def test_bars_before_the_split_are_back_adjusted_to_pinned_values(self) -> None:
        result = adjusted_bars(self.bars, (_split_action(),))

        self.assertEqual(
            [(bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in result],
            [
                (
                    Decimal("49.00000000"),
                    Decimal("50.50000000"),
                    Decimal("48.50000000"),
                    Decimal("50.00000000"),
                    2000,
                ),
                (
                    Decimal("95.00000000"),
                    Decimal("102.50000000"),
                    Decimal("94.00000000"),
                    Decimal("100.00000000"),
                    4000,
                ),
                (
                    Decimal("104.00000000"),
                    Decimal("106.00000000"),
                    Decimal("103.00000000"),
                    Decimal("105.00000000"),
                    5000,
                ),
            ],
        )

    def test_a_bar_exactly_at_the_effective_instant_is_not_adjusted(self) -> None:
        at_boundary = Bar(
            instrument_id=INSTRUMENT_ID,
            bar_start_at=SPLIT_EFFECTIVE_AT,
            open=Decimal("60.00"),
            high=Decimal("61.00"),
            low=Decimal("59.00"),
            close=Decimal("60.50"),
            volume=700,
        )

        (result,) = adjusted_bars((at_boundary,), (_split_action(),))

        self.assertEqual(result.close, Decimal("60.50000000"))
        self.assertEqual(result.volume, 700)

    def test_timestamps_and_instrument_are_carried_through_unchanged(self) -> None:
        result = adjusted_bars(self.bars, (_split_action(),))

        self.assertEqual(
            [bar.bar_start_at for bar in result],
            [bar.bar_start_at for bar in self.bars],
        )
        self.assertEqual({bar.instrument_id for bar in result}, {INSTRUMENT_ID})

    def test_no_approved_actions_only_quantizes(self) -> None:
        result = adjusted_bars(self.bars, ())

        self.assertEqual(result[0].close, Decimal("100.00000000"))
        self.assertEqual(result[0].volume, 1000)


class CashDividendAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bars = (
            _bar(28, "90.00", "95.00", "89.00", "94.00", 1000),
            _bar(31, "96.00", "101.00", "95.00", "100.00", 1500),
            _bar(2, "99.00", "100.00", "98.00", "99.50", 1200, month=9),
        )

    def test_bars_before_the_ex_date_are_scaled_by_the_pinned_factor(self) -> None:
        result = adjusted_bars(self.bars, (_dividend_action(),))

        self.assertEqual(
            [(bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in result],
            [
                (
                    Decimal("88.20000000"),
                    Decimal("93.10000000"),
                    Decimal("87.22000000"),
                    Decimal("92.12000000"),
                    1000,
                ),
                (
                    Decimal("94.08000000"),
                    Decimal("98.98000000"),
                    Decimal("93.10000000"),
                    Decimal("98.00000000"),
                    1500,
                ),
                (
                    Decimal("99.00000000"),
                    Decimal("100.00000000"),
                    Decimal("98.00000000"),
                    Decimal("99.50000000"),
                    1200,
                ),
            ],
        )

    def test_volume_is_never_changed_by_a_cash_dividend(self) -> None:
        result = adjusted_bars(self.bars, (_dividend_action(),))

        self.assertEqual([bar.volume for bar in result], [1000, 1500, 1200])

    def test_a_dividend_with_no_preceding_bar_cannot_be_priced(self) -> None:
        only_after = (_bar(2, "99.00", "100.00", "98.00", "99.50", 1200, month=9),)

        with self.assertRaisesRegex(ValueError, "no bar before"):
            adjusted_bars(only_after, (_dividend_action(),))


class CompoundAdjustmentTests(unittest.TestCase):
    def test_factors_before_both_events_compound_to_pinned_values(self) -> None:
        bars = (
            _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
            _bar(31, "96.00", "101.00", "95.00", "100.00", 1500),
            _bar(2, "99.00", "100.00", "98.00", "99.50", 1200, month=9),
        )

        result = adjusted_bars(bars, (_dividend_action(), _split_action()))

        self.assertEqual(
            [(bar.open, bar.high, bar.low, bar.close, bar.volume) for bar in result],
            [
                # before split and dividend: 0.5 * 0.98 = 0.49, volume x2
                (
                    Decimal("48.02000000"),
                    Decimal("49.49000000"),
                    Decimal("47.53000000"),
                    Decimal("49.00000000"),
                    2000,
                ),
                # after split, before dividend: 0.98 only, volume unchanged
                (
                    Decimal("94.08000000"),
                    Decimal("98.98000000"),
                    Decimal("93.10000000"),
                    Decimal("98.00000000"),
                    1500,
                ),
                # after both
                (
                    Decimal("99.00000000"),
                    Decimal("100.00000000"),
                    Decimal("98.00000000"),
                    Decimal("99.50000000"),
                    1200,
                ),
            ],
        )

    def test_action_order_in_the_input_does_not_change_the_result(self) -> None:
        bars = (_bar(13, "98.00", "101.00", "97.00", "100.00", 1000),)

        forward = adjusted_bars(bars, (_split_action(), _dividend_action()))
        reverse = adjusted_bars(bars, (_dividend_action(), _split_action()))

        self.assertEqual(forward[0].close, Decimal("49.00000000"))
        self.assertEqual(reverse[0].close, Decimal("49.00000000"))

    def test_dividend_factor_uses_the_raw_close_not_a_split_adjusted_one(self) -> None:
        # The Aug-31 raw close of 100.00 is already post-split in provider terms,
        # so the dividend factor must be 0.98 and not (50 - 2) / 50 = 0.96.
        bars = (
            _bar(31, "96.00", "101.00", "95.00", "100.00", 1500),
            _bar(2, "99.00", "100.00", "98.00", "99.50", 1200, month=9),
        )

        result = adjusted_bars(bars, (_split_action(), _dividend_action()))

        self.assertEqual(result[0].close, Decimal("98.00000000"))


class AdjustmentIdempotenceTests(unittest.TestCase):
    def test_adjusting_the_same_raw_input_twice_is_byte_identical(self) -> None:
        bars = (
            _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
            _bar(17, "104.00", "106.00", "103.00", "105.00", 5000),
        )

        first = adjusted_bars(bars, (_split_action(),))
        second = adjusted_bars(bars, (_split_action(),))

        self.assertEqual(first, second)
        self.assertEqual(first[0].close, Decimal("50.00000000"))

    def test_re_adjusting_an_already_adjusted_series_would_double_apply(self) -> None:
        """Guards the design, not the arithmetic.

        Adjustment is *not* algebraically idempotent -- feeding output back in
        halves the price a second time.  The regenerator is idempotent because
        it always rebuilds from the raw revision, and this test pins the reason
        that rule exists so nobody 'optimises' it into incremental adjustment.
        """
        bars = (_bar(13, "98.00", "101.00", "97.00", "100.00", 1000),)

        once = adjusted_bars(bars, (_split_action(),))
        twice = adjusted_bars(once, (_split_action(),))

        self.assertEqual(once[0].close, Decimal("50.00000000"))
        self.assertEqual(twice[0].close, Decimal("25.00000000"))


class AdjustmentFactorTests(unittest.TestCase):
    def test_a_zero_or_negative_price_factor_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive"):
            AdjustmentFactor(price=Decimal("0"), volume=Decimal("1"))
        with self.assertRaisesRegex(ValueError, "positive"):
            AdjustmentFactor(price=Decimal("1"), volume=Decimal("-1"))


# --------------------------------------------------------------------------------------
# Fakes for the regeneration boundary
# --------------------------------------------------------------------------------------
class FakeBarReader:
    """Serves raw bars per manifest id. Records every read so tests can prove
    that regeneration reads the *raw* revision and never the adjusted one."""

    def __init__(self, bars_by_manifest: dict[str, Sequence[Bar]]) -> None:
        self._bars = bars_by_manifest
        self.reads: list[str] = []

    def read_bars(self, manifest_id: str) -> Sequence[Bar]:
        self.reads.append(manifest_id)
        return tuple(self._bars[manifest_id])


class RecordingBarWriter:
    """Captures written datasets instead of touching a filesystem or S3."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, tuple[Bar, ...]]] = []

    def write_bars(self, bars: Sequence[Bar], *, dataset_key: str) -> WrittenDataset:
        frozen = tuple(bars)
        self.writes.append((dataset_key, frozen))
        payload = "|".join(
            f"{bar.bar_start_at.isoformat()},{bar.open},{bar.high},{bar.low},{bar.close},{bar.volume}"
            for bar in frozen
        )
        import hashlib

        return WrittenDataset(
            object_key=dataset_key,
            content_hash=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            row_count=len(frozen),
            byte_size=len(payload.encode("utf-8")),
        )


def _manifest_record(
    *,
    manifest_id: str,
    feed_id: str,
    data_layer: str,
    revision_number: int,
    status: str,
    dataset_hash: str,
    supersedes: str | None = None,
) -> dict[str, Any]:
    return {
        "id": manifest_id,
        "feed_id": feed_id,
        "instrument_id": INSTRUMENT_ID,
        "data_layer": data_layer,
        "resolution": "30m",
        "revision_number": revision_number,
        "status": status,
        "period_start": "2026-01-01T00:00:00Z",
        "period_end": "2027-01-01T00:00:00Z",
        "schema_version": "market-bars-v2",
        "dataset_hash": dataset_hash,
        "supersedes_manifest_id": supersedes,
        "created_at": "2026-08-01T00:00:00Z",
        "available_at": "2026-08-01T00:00:00Z",
    }


class RegenerationHarness:
    """A LocalCatalog seeded with a raw revision and one adjusted revision."""

    def __init__(self, root: Path, raw_bars: Sequence[Bar]) -> None:
        self.catalog = LocalCatalog(root)
        self.reader = FakeBarReader({RAW_MANIFEST_ID: raw_bars})
        self.writer = RecordingBarWriter()
        self.adjusted_v1_id = deterministic_uuid("adjusted", "v1")
        self.catalog.publish_manifest(
            _manifest_record(
                manifest_id=RAW_MANIFEST_ID,
                feed_id=FEED_RAW_ID,
                data_layer="RAW",
                revision_number=1,
                status="AVAILABLE",
                dataset_hash="raw-hash-1",
            )
        )
        self.catalog.publish_manifest(
            _manifest_record(
                manifest_id=self.adjusted_v1_id,
                feed_id=FEED_ADJUSTED_ID,
                data_layer="ADJUSTED",
                revision_number=1,
                status="AVAILABLE",
                dataset_hash="adjusted-hash-1",
            )
        )

    def regenerator(self) -> AdjustedDatasetRegenerator:
        return AdjustedDatasetRegenerator(
            catalog=self.catalog,
            reader=self.reader,
            writer=self.writer,
        )

    def manifests(self) -> dict[str, dict[str, Any]]:
        return {
            str(row["id"]): row
            for row in self.catalog.records("market_data.dataset_manifests")
        }

    def lineage(self) -> list[dict[str, Any]]:
        return self.catalog.records("market_data.dataset_lineage")


# --------------------------------------------------------------------------------------
# Regeneration
# --------------------------------------------------------------------------------------
class RegenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.harness = RegenerationHarness(
            Path(self._temporary.name) / "catalog",
            (
                _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
                _bar(17, "104.00", "106.00", "103.00", "105.00", 5000),
            ),
        )

    def _regenerate(self, actions: Sequence[ApprovedAction]) -> Any:
        return self.harness.regenerator().regenerate(
            raw_manifest_id=RAW_MANIFEST_ID,
            adjusted_feed_id=FEED_ADJUSTED_ID,
            approved_actions=actions,
            now=datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
        )

    def test_approval_publishes_a_new_revision_that_supersedes_the_prior(self) -> None:
        result = self._regenerate((_split_action(),))

        self.assertTrue(result.created)
        self.assertEqual(result.revision_number, 2)
        self.assertEqual(result.supersedes_manifest_id, self.harness.adjusted_v1_id)
        self.assertNotEqual(result.manifest_id, self.harness.adjusted_v1_id)

    def test_the_prior_revision_is_retained_and_only_marked_superseded(self) -> None:
        before = self.harness.manifests()[self.harness.adjusted_v1_id]

        self._regenerate((_split_action(),))

        after = self.harness.manifests()[self.harness.adjusted_v1_id]
        self.assertEqual(after["status"], "SUPERSEDED")
        # Everything that identifies the old dataset's *content* is untouched.
        for field in ("id", "revision_number", "dataset_hash", "period_start", "period_end"):
            self.assertEqual(after[field], before[field], field)

    def test_lineage_links_the_new_revision_to_raw_and_to_the_prior_revision(self) -> None:
        result = self._regenerate((_split_action(),))

        edges = {
            (row["derived_manifest_id"], row["source_manifest_id"], row["relation_type"])
            for row in self.harness.lineage()
        }
        self.assertIn((result.manifest_id, RAW_MANIFEST_ID, "ADJUSTMENT_SOURCE"), edges)
        self.assertIn(
            (result.manifest_id, self.harness.adjusted_v1_id, "SUPERSEDES"), edges
        )

    def test_the_written_bars_are_the_pinned_adjusted_values(self) -> None:
        self._regenerate((_split_action(),))

        _, written = self.harness.writer.writes[-1]
        self.assertEqual(
            [(bar.close, bar.volume) for bar in written],
            [(Decimal("50.00000000"), 2000), (Decimal("105.00000000"), 5000)],
        )

    def test_regeneration_always_reads_the_raw_revision(self) -> None:
        self._regenerate((_split_action(),))

        self.assertEqual(self.harness.reader.reads, [RAW_MANIFEST_ID])

    def test_re_running_with_the_same_actions_creates_no_further_revision(self) -> None:
        first = self._regenerate((_split_action(),))
        second = self._regenerate((_split_action(),))

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(second.manifest_id, first.manifest_id)
        self.assertEqual(second.revision_number, 2)
        available = [
            row
            for row in self.harness.manifests().values()
            if row["data_layer"] == "ADJUSTED" and row["status"] == "AVAILABLE"
        ]
        self.assertEqual(len(available), 1)

    def test_re_running_does_not_double_adjust_the_prices(self) -> None:
        self._regenerate((_split_action(),))
        self._regenerate((_split_action(),))

        _, written = self.harness.writer.writes[-1]
        self.assertEqual(written[0].close, Decimal("50.00000000"))

    def test_a_further_approved_action_creates_the_next_revision(self) -> None:
        first = self._regenerate((_split_action(),))
        second = self._regenerate((_split_action(), _dividend_action()))

        self.assertTrue(second.created)
        self.assertEqual(second.revision_number, 3)
        self.assertEqual(second.supersedes_manifest_id, first.manifest_id)

    def test_exactly_one_adjusted_revision_is_available_after_each_step(self) -> None:
        self._regenerate((_split_action(),))
        self._regenerate((_split_action(), _dividend_action()))

        available = [
            row
            for row in self.harness.manifests().values()
            if row["data_layer"] == "ADJUSTED" and row["status"] == "AVAILABLE"
        ]
        self.assertEqual(len(available), 1)
        self.assertEqual(available[0]["revision_number"], 3)

    def test_an_unknown_raw_manifest_is_refused(self) -> None:
        with self.assertRaisesRegex(LookupError, "manifest"):
            self.harness.regenerator().regenerate(
                raw_manifest_id=deterministic_uuid("missing"),
                adjusted_feed_id=FEED_ADJUSTED_ID,
                approved_actions=(_split_action(),),
                now=datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
            )


# --------------------------------------------------------------------------------------
# Admin decisions
# --------------------------------------------------------------------------------------
class DecisionHarness(RegenerationHarness):
    def __init__(self, root: Path) -> None:
        super().__init__(
            root,
            (
                _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
                _bar(17, "104.00", "106.00", "103.00", "105.00", 5000),
            ),
        )
        self.candidate = _split_candidate()
        self.service = CorporateActionReviewService(
            catalog=self.catalog,
            regenerator=self.regenerator(),
            raw_manifest_id=RAW_MANIFEST_ID,
            adjusted_feed_id=FEED_ADJUSTED_ID,
        )
        self.service.record_candidate(
            self.candidate,
            instrument_id=INSTRUMENT_ID,
            source_manifest_id=SOURCE_MANIFEST_ID,
        )

    def action_rows(self) -> list[dict[str, Any]]:
        return self.catalog.records("market_data.corporate_actions")

    def adjusted_revisions(self) -> list[dict[str, Any]]:
        return [
            row
            for row in self.catalog.records("market_data.dataset_manifests")
            if row["data_layer"] == "ADJUSTED"
        ]


class DecisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.harness = DecisionHarness(Path(self._temporary.name) / "catalog")

    def _decision(self, decision: DecisionType) -> AdminDecision:
        return AdminDecision(
            candidate_id=self.harness.candidate.candidate_id,
            decision=decision,
            decided_by="admin@example.com",
            decided_at=datetime(2026, 8, 3, 9, 0, tzinfo=UTC),
            rationale="Confirmed against the issuer notice.",
        )

    def test_a_recorded_candidate_starts_in_review_required(self) -> None:
        (row,) = self.harness.action_rows()

        self.assertEqual(row["terms_document"]["review"]["state"], "REVIEW_REQUIRED")
        self.assertEqual(len(self.harness.adjusted_revisions()), 1)

    def test_a_pending_candidate_has_no_dataset_effect(self) -> None:
        self.assertEqual(len(self.harness.adjusted_revisions()), 1)
        self.assertEqual(self.harness.writer.writes, [])

    def test_approval_records_the_state_and_regenerates(self) -> None:
        outcome = self.harness.service.apply_decision(self._decision(DecisionType.APPROVE))

        self.assertEqual(outcome.state, ReviewState.APPROVED)
        self.assertIsNotNone(outcome.regeneration)
        assert outcome.regeneration is not None
        self.assertTrue(outcome.regeneration.created)
        self.assertEqual(outcome.regeneration.revision_number, 2)

        (row,) = self.harness.action_rows()
        review = row["terms_document"]["review"]
        self.assertEqual(review["state"], "APPROVED")
        self.assertEqual(review["decided_by"], "admin@example.com")
        self.assertEqual(review["decided_at"], "2026-08-03T09:00:00Z")

    def test_rejection_records_the_state_and_touches_no_dataset(self) -> None:
        outcome = self.harness.service.apply_decision(self._decision(DecisionType.REJECT))

        self.assertEqual(outcome.state, ReviewState.REJECTED)
        self.assertIsNone(outcome.regeneration)
        self.assertEqual(self.harness.writer.writes, [])
        self.assertEqual(len(self.harness.adjusted_revisions()), 1)
        self.assertEqual(
            self.harness.adjusted_revisions()[0]["id"], self.harness.adjusted_v1_id
        )
        self.assertEqual(self.harness.adjusted_revisions()[0]["status"], "AVAILABLE")

    def test_a_rejected_action_is_excluded_from_a_later_regeneration(self) -> None:
        self.harness.service.apply_decision(self._decision(DecisionType.REJECT))

        actions = self.harness.service.approved_actions()

        self.assertEqual(actions, ())

    def test_an_approved_action_is_included_in_approved_actions(self) -> None:
        self.harness.service.apply_decision(self._decision(DecisionType.APPROVE))

        (action,) = self.harness.service.approved_actions()

        self.assertEqual(action.action_type, "STOCK_SPLIT")
        self.assertEqual(action.effective_at, SPLIT_EFFECTIVE_AT)
        self.assertEqual(action.terms, SplitTerms(from_shares=1, to_shares=2))

    def test_replaying_the_same_decision_is_idempotent(self) -> None:
        first = self.harness.service.apply_decision(self._decision(DecisionType.APPROVE))
        second = self.harness.service.apply_decision(self._decision(DecisionType.APPROVE))

        self.assertTrue(first.regeneration is not None and first.regeneration.created)
        self.assertIsNotNone(second.regeneration)
        assert second.regeneration is not None
        self.assertFalse(second.regeneration.created)
        (row,) = self.harness.action_rows()
        self.assertEqual(len(row["terms_document"]["review_history"]), 1)

    def test_reversing_a_decision_is_refused_rather_than_silently_applied(self) -> None:
        self.harness.service.apply_decision(self._decision(DecisionType.APPROVE))

        with self.assertRaises(ConflictingDecisionError):
            self.harness.service.apply_decision(self._decision(DecisionType.REJECT))

    def test_a_decision_for_an_unknown_candidate_is_refused(self) -> None:
        stray = AdminDecision(
            candidate_id="f" * 64,
            decision=DecisionType.APPROVE,
            decided_by="admin@example.com",
            decided_at=datetime(2026, 8, 3, 9, 0, tzinfo=UTC),
            rationale="No such candidate.",
        )

        with self.assertRaises(UnknownCandidateError):
            self.harness.service.apply_decision(stray)

    def test_a_decision_requires_an_identified_admin_and_a_rationale(self) -> None:
        with self.assertRaisesRegex(ValueError, "decided_by"):
            AdminDecision(
                candidate_id=self.harness.candidate.candidate_id,
                decision=DecisionType.APPROVE,
                decided_by="   ",
                decided_at=datetime(2026, 8, 3, 9, 0, tzinfo=UTC),
                rationale="ok",
            )
        with self.assertRaisesRegex(ValueError, "rationale"):
            AdminDecision(
                candidate_id=self.harness.candidate.candidate_id,
                decision=DecisionType.APPROVE,
                decided_by="admin@example.com",
                decided_at=datetime(2026, 8, 3, 9, 0, tzinfo=UTC),
                rationale="",
            )

    def test_a_naive_decision_timestamp_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "UTC"):
            AdminDecision(
                candidate_id=self.harness.candidate.candidate_id,
                decision=DecisionType.APPROVE,
                decided_by="admin@example.com",
                decided_at=datetime(2026, 8, 3, 9, 0),
                rationale="ok",
            )

    def test_the_persisted_row_keeps_the_full_research_provenance(self) -> None:
        (row,) = self.harness.action_rows()
        document = row["terms_document"]

        self.assertEqual(row["action_type"], "STOCK_SPLIT")
        self.assertEqual(row["instrument_id"], INSTRUMENT_ID)
        self.assertEqual(row["source_manifest_id"], SOURCE_MANIFEST_ID)
        self.assertEqual(document["candidate_id"], self.harness.candidate.candidate_id)
        self.assertEqual(document["confidence"], "0.9000")
        self.assertEqual(len(document["evidence"]), 1)
        claimed = {claim["field"]: claim["source_uri"] for claim in document["claims"]}
        self.assertEqual(
            set(claimed),
            {"event_type", "effective_date", "from_shares", "to_shares"},
        )
        self.assertEqual(
            set(claimed.values()),
            {"https://issuer.example/investors/split-notice"},
        )


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
class DecisionCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name) / "catalog"
        self.harness = DecisionHarness(self.root)

    def _argv(self, **overrides: str) -> list[str]:
        arguments: dict[str, str] = {
            "--catalog-root": str(self.root),
            "--candidate-id": self.harness.candidate.candidate_id,
            "--decision": "REJECT",
            "--decided-by": "admin@example.com",
            "--decided-at": "2026-08-03T09:00:00Z",
            "--rationale": "Not corroborated by a second source.",
            "--raw-manifest-id": RAW_MANIFEST_ID,
            "--adjusted-feed-id": FEED_ADJUSTED_ID,
        }
        arguments.update(overrides)
        return [item for pair in arguments.items() for item in pair]

    def test_a_rejection_succeeds_and_exits_zero(self) -> None:
        code = decision_cli(self._argv())

        self.assertEqual(code, 0)
        (row,) = LocalCatalog(self.root).records("market_data.corporate_actions")
        self.assertEqual(row["terms_document"]["review"]["state"], "REJECTED")

    def test_unknown_candidate_exits_non_zero(self) -> None:
        code = decision_cli(self._argv(**{"--candidate-id": "f" * 64}))

        self.assertNotEqual(code, 0)

    def test_a_malformed_timestamp_exits_non_zero(self) -> None:
        code = decision_cli(self._argv(**{"--decided-at": "not-a-timestamp"}))

        self.assertNotEqual(code, 0)

    def test_a_blank_rationale_exits_non_zero(self) -> None:
        code = decision_cli(self._argv(**{"--rationale": "   "}))

        self.assertNotEqual(code, 0)

    def test_approval_without_object_store_wiring_exits_non_zero(self) -> None:
        """An approval needs to rebuild data.  Without a reader/writer the CLI
        must refuse rather than record the approval and quietly skip the
        regeneration it promised."""
        code = decision_cli(self._argv(**{"--decision": "APPROVE"}))

        self.assertNotEqual(code, 0)
        (row,) = LocalCatalog(self.root).records("market_data.corporate_actions")
        self.assertEqual(row["terms_document"]["review"]["state"], "REVIEW_REQUIRED")

    def test_approval_with_injected_wiring_regenerates_and_exits_zero(self) -> None:
        reader = FakeBarReader(
            {
                RAW_MANIFEST_ID: (
                    _bar(13, "98.00", "101.00", "97.00", "100.00", 1000),
                    _bar(17, "104.00", "106.00", "103.00", "105.00", 5000),
                )
            }
        )
        writer = RecordingBarWriter()

        code = decision_cli(
            self._argv(**{"--decision": "APPROVE"}), reader=reader, writer=writer
        )

        self.assertEqual(code, 0)
        _, written = writer.writes[-1]
        self.assertEqual(written[0].close, Decimal("50.00000000"))


if __name__ == "__main__":
    unittest.main()
