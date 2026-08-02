"""Card D10: six quality check families, each recorded with its impact scope.

Every expected value in this module is a literal.  Nothing here re-derives a
production formula: partition bounds, shard keys, incident identifiers and
policy thresholds are all pinned, so an implementation that returns a constant,
or that silently changes a threshold, fails.
"""

import json
import unittest
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from market_pipeline_lib.contracts import DATASET_CONTRACTS, ET, bar_schema
from market_pipeline_lib.processing import (
    quality_findings,
    quality_issues,
    sort_bar_table,
)
from market_pipeline_lib.quality import (
    MISSING_BAR_POLICY,
    ORDERING_POLICY,
    PRICE_OUTLIER_POLICY,
    QUALITY_INCIDENT_COLUMNS,
    VOLUME_POLICY,
    ImpactScope,
    QualityIncident,
    QualityIncidentRecorder,
    ScopeBreadth,
    content_hash_mismatch_incident,
    incident_report_from_issue,
    incident_row_from_issue,
    record_issue_incidents,
    record_quality_incidents,
    scoped_incidents,
)

AAPL = "11111111-1111-4111-8111-111111111111"
MSFT = "22222222-2222-4222-8222-222222222222"
OBJECT_ID = "44444444-4444-4444-8444-444444444444"
MANIFEST_ID = "55555555-5555-4555-8555-555555555555"
SHARD = "s00-of-2"

RAW_30M = DATASET_CONTRACTS[("raw", "RAW", "30m")]
DERIVED_1H = DATASET_CONTRACTS[("raw", "DERIVED", "1h")]

DETECTED_AT = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)


def utc(text: str) -> datetime:
    """Parse an explicit UTC instant. No calendar or partition maths involved."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def bar(
    instrument_id: str,
    timestamp: str,
    *,
    symbol: str = "AAPL",
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: int = 100,
    trade_count: int | None = 10,
    vwap: float | None = 100.25,
    source_minutes: int | None = None,
) -> dict[str, object]:
    started = utc(timestamp)
    row: dict[str, object] = {
        "instrument_id": instrument_id,
        "provider_symbol": symbol,
        "bar_start_at": started,
        "session_date_et": started.astimezone(ET).date(),
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "trade_count": trade_count,
        "vwap": vwap,
    }
    if source_minutes is not None:
        row["source_minutes"] = source_minutes
    return row


def table_of(rows: list[dict[str, object]], *, derived: bool = False) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=bar_schema(derived))


def session_bars(
    instrument_id: str,
    session: str,
    starts: list[str],
    **overrides: object,
) -> list[dict[str, object]]:
    """Bars of one session, given as explicit UTC start times."""
    del session
    return [bar(instrument_id, start, **overrides) for start in starts]  # type: ignore[arg-type]


# The thirteen regular-session 30m bar starts of a full XNYS day, in UTC.
# 09:30-16:00 America/New_York on a winter date is 14:30-21:00Z.
FULL_WINTER_DAY = [
    "14:30", "15:00", "15:30", "16:00", "16:30", "17:00", "17:30",
    "18:00", "18:30", "19:00", "19:30", "20:00", "20:30",
]


def full_session(instrument_id: str, day: str, **overrides: object) -> list[dict[str, object]]:
    return [
        bar(instrument_id, f"{day}T{clock}:00", **overrides)  # type: ignore[arg-type]
        for clock in FULL_WINTER_DAY
    ]


def codes(findings: list[object]) -> list[str]:
    return [getattr(item, "code") for item in findings]


def only(findings: list, code: str) -> list:
    return [item for item in findings if item.code == code]


class ImpactScopeTests(unittest.TestCase):
    def test_bar_range_scope_rejects_a_manifest_wide_period(self):
        """A single bad bar must never be recorded as 'somewhere in 2025'."""
        with self.assertRaises(ValueError) as caught:
            ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=utc("2025-01-01T05:00:00"),
                period_end=utc("2026-01-01T05:00:00"),
                instrument_id=AAPL,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=1,
            )
        self.assertIn("partition", str(caught.exception))

    def test_bar_range_scope_accepts_a_period_inside_the_partition(self):
        scope = ImpactScope(
            breadth=ScopeBreadth.BAR_RANGE,
            period_start=utc("2025-02-03T15:30:00"),
            period_end=utc("2025-02-03T16:00:00"),
            instrument_id=AAPL,
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            affected_bar_count=1,
        )
        self.assertEqual(scope.affected_bar_count, 1)
        self.assertEqual(scope.instrument_id, AAPL)

    def test_bar_range_scope_requires_an_instrument(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=utc("2025-02-03T15:30:00"),
                period_end=utc("2025-02-03T16:00:00"),
                instrument_id=None,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=1,
            )

    def test_bar_range_scope_requires_at_least_one_affected_bar(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=utc("2025-02-03T15:30:00"),
                period_end=utc("2025-02-03T16:00:00"),
                instrument_id=AAPL,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=0,
            )

    def test_partition_scope_rejects_a_single_instrument(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.PARTITION,
                period_start=utc("2025-02-03T05:00:00"),
                period_end=utc("2025-02-04T05:00:00"),
                instrument_id=AAPL,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
            )

    def test_object_scope_requires_evidence_object_id(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.OBJECT,
                period_start=utc("2025-02-03T14:30:00"),
                period_end=utc("2025-02-03T21:00:00"),
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
            )

    def test_manifest_wide_scope_must_declare_why_it_is_manifest_wide(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.MANIFEST,
                period_start=utc("2025-01-01T05:00:00"),
                period_end=utc("2026-01-01T05:00:00"),
            )
        scope = ImpactScope.manifest_wide(
            period_start=utc("2025-01-01T05:00:00"),
            period_end=utc("2026-01-01T05:00:00"),
            reason="카탈로그 전체 재검증 중 발견되어 파티션을 특정할 수 없습니다.",
        )
        self.assertEqual(scope.breadth, ScopeBreadth.MANIFEST)
        self.assertIsNone(scope.shard_key)

    def test_manifest_wide_scope_rejects_partition_fields(self):
        with self.assertRaises(ValueError):
            ImpactScope(
                breadth=ScopeBreadth.MANIFEST,
                period_start=utc("2025-01-01T05:00:00"),
                period_end=utc("2026-01-01T05:00:00"),
                shard_key=SHARD,
                manifest_wide_reason="설명",
            )

    def test_period_must_be_timezone_aware_and_ordered(self):
        with self.assertRaises(ValueError):
            ImpactScope.manifest_wide(
                period_start=datetime(2025, 1, 1),  # noqa: DTZ001 - deliberate
                period_end=utc("2026-01-01T05:00:00"),
                reason="설명",
            )
        with self.assertRaises(ValueError):
            ImpactScope.manifest_wide(
                period_start=utc("2026-01-01T05:00:00"),
                period_end=utc("2025-01-01T05:00:00"),
                reason="설명",
            )


class QualityIncidentRowTests(unittest.TestCase):
    @staticmethod
    def incident() -> QualityIncident:
        return QualityIncident(
            incident_code="MISSING_BARS",
            severity="WARNING",
            scope=ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=utc("2025-02-03T15:30:00"),
                period_end=utc("2025-02-03T16:00:00"),
                instrument_id=AAPL,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=1,
            ),
            detected_at=DETECTED_AT,
            message="AAPL 15:30 누락",
        )

    def test_row_columns_match_the_canonical_table(self):
        self.assertEqual(
            QUALITY_INCIDENT_COLUMNS,
            (
                "id",
                "dataset_manifest_id",
                "instrument_id",
                "severity",
                "incident_code",
                "period_start",
                "period_end",
                "status",
                "evidence_object_id",
                "detected_at",
                "resolved_at",
            ),
        )
        row = self.incident().to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(tuple(row), QUALITY_INCIDENT_COLUMNS)

    def test_row_carries_the_narrow_scope_not_the_manifest_period(self):
        row = self.incident().to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["dataset_manifest_id"], MANIFEST_ID)
        self.assertEqual(row["instrument_id"], AAPL)
        self.assertEqual(row["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(row["period_end"], "2025-02-03T16:00:00Z")
        self.assertEqual(row["severity"], "WARNING")
        self.assertEqual(row["incident_code"], "MISSING_BARS")
        self.assertEqual(row["status"], "ACTIVE")
        self.assertIsNone(row["evidence_object_id"])
        self.assertEqual(row["detected_at"], "2026-03-01T12:00:00Z")
        self.assertIsNone(row["resolved_at"])
        self.assertEqual(
            json.loads(json.dumps(row, ensure_ascii=False)),
            row,
        )

    def test_row_id_is_pinned_and_scope_specific(self):
        base = self.incident()
        self.assertEqual(
            base.to_db_row(dataset_manifest_id=MANIFEST_ID)["id"],
            "06e42ce9-d579-5c4e-9cf9-5034ef3ac030",
        )
        other_instrument = QualityIncident(
            incident_code=base.incident_code,
            severity=base.severity,
            scope=ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=base.scope.period_start,
                period_end=base.scope.period_end,
                instrument_id=MSFT,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=1,
            ),
            detected_at=base.detected_at,
        )
        other_period = QualityIncident(
            incident_code=base.incident_code,
            severity=base.severity,
            scope=ImpactScope(
                breadth=ScopeBreadth.BAR_RANGE,
                period_start=utc("2025-02-03T16:30:00"),
                period_end=utc("2025-02-03T17:00:00"),
                instrument_id=AAPL,
                shard_key=SHARD,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
                affected_bar_count=1,
            ),
            detected_at=base.detected_at,
        )
        identifiers = {
            base.to_db_row(dataset_manifest_id=MANIFEST_ID)["id"],
            other_instrument.to_db_row(dataset_manifest_id=MANIFEST_ID)["id"],
            other_period.to_db_row(dataset_manifest_id=MANIFEST_ID)["id"],
        }
        self.assertEqual(len(identifiers), 3)

    def test_detected_at_must_be_timezone_aware(self):
        with self.assertRaises(ValueError):
            QualityIncident(
                incident_code="MISSING_BARS",
                severity="WARNING",
                scope=self.incident().scope,
                detected_at=datetime(2026, 3, 1, 12, 0),  # noqa: DTZ001 - deliberate
            )

    def test_unknown_severity_is_rejected(self):
        with self.assertRaises(ValueError):
            QualityIncident(
                incident_code="MISSING_BARS",
                severity="CATASTROPHE",  # type: ignore[arg-type]
                scope=self.incident().scope,
                detected_at=DETECTED_AT,
            )


class ContentHashMismatchIncidentTests(unittest.TestCase):
    def test_checksum_failure_is_a_first_class_incident_with_evidence(self):
        incident = content_hash_mismatch_incident(
            object_id=OBJECT_ID,
            object_key="market-data/provider=ALPACA/part-00001.parquet",
            expected_content_hash="a" * 64,
            actual_content_hash="b" * 64,
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            period_start=utc("2025-02-03T14:30:00"),
            period_end=utc("2025-02-03T21:00:00"),
            detected_at=DETECTED_AT,
        )
        row = incident.to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["incident_code"], "CONTENT_HASH_MISMATCH")
        self.assertEqual(row["severity"], "ERROR")
        self.assertEqual(row["evidence_object_id"], OBJECT_ID)
        self.assertEqual(row["period_start"], "2025-02-03T14:30:00Z")
        self.assertEqual(row["period_end"], "2025-02-03T21:00:00Z")
        self.assertEqual(incident.scope.breadth, ScopeBreadth.OBJECT)
        self.assertIn("a" * 64, incident.message)
        self.assertIn("b" * 64, incident.message)


class MissingBarDetectionTests(unittest.TestCase):
    def findings(self, rows: list[dict[str, object]]) -> list:
        return only(quality_findings(table_of(rows), RAW_30M), "MISSING_BARS")

    def test_two_gaps_in_one_session_are_reported_separately(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
            bar(AAPL, "2025-02-03T20:30:00"),
        ]
        found = self.findings(rows)
        self.assertEqual(len(found), 2)
        self.assertEqual(
            [
                (
                    item.instrument_id,
                    item.period_start.isoformat(),
                    item.period_end.isoformat(),
                    item.affected_bar_count,
                )
                for item in found
            ],
            [
                (AAPL, "2025-02-03T15:30:00+00:00", "2025-02-03T16:00:00+00:00", 1),
                (AAPL, "2025-02-03T16:30:00+00:00", "2025-02-03T20:30:00+00:00", 8),
            ],
        )
        self.assertEqual(
            {item.severity for item in found},
            {MISSING_BAR_POLICY.severity},
        )
        self.assertEqual(
            {item.policy_version for item in found},
            {MISSING_BAR_POLICY.version},
        )

    def test_a_whole_missing_session_is_one_interval_scoped_to_that_session(self):
        rows = full_session(AAPL, "2025-02-03") + full_session(AAPL, "2025-02-05")
        found = self.findings(rows)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-04T14:30:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-04T21:00:00+00:00")
        self.assertEqual(found[0].affected_bar_count, 13)
        self.assertEqual(found[0].instrument_id, AAPL)

    def test_gaps_are_reported_per_instrument(self):
        rows = (
            full_session(AAPL, "2025-02-03")
            + [
                bar(MSFT, "2025-02-03T14:30:00", symbol="MSFT"),
                bar(MSFT, "2025-02-03T15:30:00", symbol="MSFT"),
            ]
        )
        found = self.findings(rows)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].instrument_id, MSFT)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T15:00:00+00:00")
        self.assertEqual(found[0].affected_bar_count, 1)

    def test_nothing_is_expected_before_the_first_or_after_the_last_bar(self):
        rows = [
            bar(AAPL, "2025-02-03T16:00:00"),
            bar(AAPL, "2025-02-03T16:30:00"),
        ]
        self.assertEqual(self.findings(rows), [])

    def test_early_close_session_expects_only_seven_bars(self):
        """2024-11-29 closes 13:00 ET, so 18:00Z is the last bar start."""
        rows = [
            bar(AAPL, "2024-11-29T14:30:00"),
            bar(AAPL, "2024-11-29T18:00:00"),
        ]
        found = self.findings(rows)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].affected_bar_count, 6)
        self.assertEqual(found[0].period_start.isoformat(), "2024-11-29T15:00:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2024-11-29T18:00:00+00:00")

    def test_derived_layer_keeps_the_source_minutes_proxy_and_adds_no_gap_rows(self):
        rows = [
            # A 1h bar that aggregated only one 30m source bar out of the two
            # the session still had room for.
            bar(AAPL, "2024-11-29T14:30:00", source_minutes=30),
            bar(AAPL, "2024-11-29T17:30:00", source_minutes=30),
        ]
        found = quality_findings(table_of(rows, derived=True), DERIVED_1H)
        self.assertNotIn("MISSING_BARS", codes(found))
        proxy = only(found, "UNEXPECTED_MISSING_SOURCE_BARS")
        self.assertEqual(len(proxy), 1)
        self.assertEqual(proxy[0].instrument_id, AAPL)
        self.assertEqual(proxy[0].period_start.isoformat(), "2024-11-29T14:30:00+00:00")
        self.assertEqual(proxy[0].period_end.isoformat(), "2024-11-29T15:30:00+00:00")


class OutOfOrderDetectionTests(unittest.TestCase):
    UNSORTED = [
        bar(AAPL, "2025-02-03T14:30:00"),
        bar(AAPL, "2025-02-03T15:30:00"),
        bar(AAPL, "2025-02-03T15:00:00"),
    ]

    def test_reverse_ordered_bars_are_detected_on_unsorted_input(self):
        found = only(quality_findings(table_of(self.UNSORTED), RAW_30M), "OUT_OF_ORDER_BARS")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].instrument_id, AAPL)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T15:00:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-03T16:00:00+00:00")
        self.assertEqual(found[0].affected_bar_count, 2)
        self.assertEqual(found[0].severity, ORDERING_POLICY.severity)
        self.assertEqual(found[0].policy_version, ORDERING_POLICY.version)

    def test_sorting_before_validating_destroys_the_evidence(self):
        """Documents why engine.py must validate before it calls sort_bar_table."""
        sorted_table = sort_bar_table(table_of(self.UNSORTED))
        self.assertNotIn(
            "OUT_OF_ORDER_BARS",
            codes(quality_findings(sorted_table, RAW_30M)),
        )

    def test_ascending_input_reports_no_ordering_violation(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T15:30:00"),
        ]
        self.assertNotIn("OUT_OF_ORDER_BARS", codes(quality_findings(table_of(rows), RAW_30M)))

    def test_interleaved_instruments_are_not_an_ordering_violation(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(MSFT, "2025-02-03T14:30:00", symbol="MSFT"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(MSFT, "2025-02-03T15:00:00", symbol="MSFT"),
        ]
        self.assertNotIn("OUT_OF_ORDER_BARS", codes(quality_findings(table_of(rows), RAW_30M)))


class PriceOutlierPolicyTests(unittest.TestCase):
    def test_threshold_is_a_named_versioned_policy_value(self):
        self.assertEqual(PRICE_OUTLIER_POLICY.version, "price-outlier:1.0.0")
        self.assertEqual(PRICE_OUTLIER_POLICY.max_abs_log_return, 0.30)
        self.assertEqual(PRICE_OUTLIER_POLICY.max_intrabar_range_ratio, 0.20)

    def test_intraday_move_above_the_threshold_is_an_abnormality_warning(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T15:30:00", open_price=136.0, high=137.0, low=135.0, close=136.0),
        ]
        found = only(quality_findings(table_of(rows), RAW_30M), "PRICE_OUTLIER_RETURN")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "WARNING")
        self.assertEqual(found[0].instrument_id, AAPL)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T15:30:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-03T16:00:00+00:00")
        self.assertEqual(found[0].affected_bar_count, 1)
        self.assertEqual(found[0].policy_version, "price-outlier:1.0.0")

    def test_intraday_move_below_the_threshold_is_not_reported(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T15:30:00", open_price=135.0, high=136.0, low=134.0, close=135.0),
        ]
        self.assertEqual(
            only(quality_findings(table_of(rows), RAW_30M), "PRICE_OUTLIER_RETURN"),
            [],
        )

    def test_overnight_gap_is_not_an_intraday_outlier(self):
        rows = [
            bar(AAPL, "2025-02-03T20:30:00"),
            bar(AAPL, "2025-02-04T14:30:00", open_price=300.0, high=301.0, low=299.0, close=300.0),
        ]
        self.assertEqual(
            only(quality_findings(table_of(rows), RAW_30M), "PRICE_OUTLIER_RETURN"),
            [],
        )

    def test_intrabar_range_above_the_threshold_is_reported(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00", open_price=100.0, high=130.0, low=100.0, close=100.0),
        ]
        found = only(quality_findings(table_of(rows), RAW_30M), "PRICE_OUTLIER_RANGE")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].affected_bar_count, 1)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T14:30:00+00:00")

    def test_intrabar_range_below_the_threshold_is_not_reported(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00", open_price=100.0, high=119.0, low=100.0, close=100.0),
        ]
        self.assertEqual(
            only(quality_findings(table_of(rows), RAW_30M), "PRICE_OUTLIER_RANGE"),
            [],
        )

    def test_invalid_prices_stay_errors_and_are_not_downgraded_to_outliers(self):
        rows = [bar(AAPL, "2025-02-03T14:30:00", open_price=-1.0, low=-1.0)]
        found = quality_findings(table_of(rows), RAW_30M)
        invalid = only(found, "INVALID_PRICE")
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0].severity, "ERROR")
        self.assertEqual(invalid[0].instrument_id, AAPL)


class VolumePolicyTests(unittest.TestCase):
    def test_threshold_is_a_named_versioned_policy_value(self):
        self.assertEqual(VOLUME_POLICY.version, "volume-anomaly:1.0.0")
        self.assertEqual(VOLUME_POLICY.spike_multiple, 50.0)
        self.assertEqual(VOLUME_POLICY.spike_reference_min_bars, 20)

    @staticmethod
    def two_full_sessions(**overrides: object) -> list[dict[str, object]]:
        return full_session(AAPL, "2025-02-03", **overrides) + full_session(
            AAPL, "2025-02-04", **overrides
        )

    def test_zero_volume_run_is_reported_with_its_exact_interval(self):
        rows = self.two_full_sessions()
        rows[3]["volume"] = 0
        rows[4]["volume"] = 0
        found = only(quality_findings(table_of(rows), RAW_30M), "ZERO_VOLUME_BAR")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "WARNING")
        self.assertEqual(found[0].affected_bar_count, 2)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T16:00:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-03T17:00:00+00:00")
        self.assertEqual(found[0].policy_version, "volume-anomaly:1.0.0")

    def test_volume_above_fifty_times_the_median_is_a_spike(self):
        rows = self.two_full_sessions()
        rows[7]["volume"] = 5001
        found = only(quality_findings(table_of(rows), RAW_30M), "VOLUME_SPIKE")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].affected_bar_count, 1)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T18:00:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-03T18:30:00+00:00")

    def test_volume_at_exactly_the_threshold_is_not_a_spike(self):
        rows = self.two_full_sessions()
        rows[7]["volume"] = 5000
        self.assertEqual(only(quality_findings(table_of(rows), RAW_30M), "VOLUME_SPIKE"), [])

    def test_short_history_has_no_spike_reference(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00", volume=1_000_000),
        ]
        self.assertEqual(only(quality_findings(table_of(rows), RAW_30M), "VOLUME_SPIKE"), [])

    def test_negative_volume_remains_an_error(self):
        rows = [bar(AAPL, "2025-02-03T14:30:00", volume=-5)]
        found = only(quality_findings(table_of(rows), RAW_30M), "NEGATIVE_ACTIVITY")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "ERROR")
        self.assertEqual(found[0].instrument_id, AAPL)


class DuplicateAndSessionScopeTests(unittest.TestCase):
    def test_duplicate_bar_is_scoped_to_the_repeated_timestamp(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
        ]
        found = only(quality_findings(table_of(rows), RAW_30M), "DUPLICATE_BAR")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].severity, "ERROR")
        self.assertEqual(found[0].instrument_id, AAPL)
        self.assertEqual(found[0].affected_bar_count, 2)
        self.assertEqual(found[0].period_start.isoformat(), "2025-02-03T14:30:00+00:00")
        self.assertEqual(found[0].period_end.isoformat(), "2025-02-03T15:00:00+00:00")

    def test_session_date_mismatch_is_scoped_to_the_offending_bar(self):
        rows = [bar(AAPL, "2025-02-03T14:30:00")]
        rows[0]["session_date_et"] = date(2025, 2, 4)
        found = only(quality_findings(table_of(rows), RAW_30M), "SESSION_DATE_ET_MISMATCH")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].instrument_id, AAPL)
        self.assertEqual(found[0].affected_bar_count, 1)

    def test_partition_boundary_violation_is_partition_scoped(self):
        rows = [bar(AAPL, "2025-02-05T14:30:00")]
        found = only(
            quality_findings(
                table_of(rows),
                RAW_30M,
                partition_start=date(2025, 2, 3),
                partition_end=date(2025, 2, 4),
            ),
            "PARTITION_BOUNDARY_VIOLATION",
        )
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].breadth, ScopeBreadth.PARTITION)
        self.assertIsNone(found[0].instrument_id)


class QualityIssueAdapterTests(unittest.TestCase):
    def test_issue_dicts_stay_json_serialisable_and_carry_scope(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
        ]
        issues = quality_issues(table_of(rows), RAW_30M)
        self.assertEqual([issue["code"] for issue in issues], ["MISSING_BARS"])
        issue = issues[0]
        self.assertEqual(issue["severity"], "WARNING")
        self.assertEqual(issue["instrument_id"], AAPL)
        self.assertEqual(issue["scope_breadth"], "BAR_RANGE")
        self.assertEqual(issue["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(issue["period_end"], "2025-02-03T16:00:00Z")
        self.assertEqual(issue["affected_bar_count"], 1)
        self.assertEqual(issue["policy_version"], "missing-bar:1.0.0")
        self.assertEqual(json.loads(json.dumps(issue, ensure_ascii=False)), issue)

    def test_schema_mismatch_short_circuits_with_partition_scope(self):
        table = pa.table({"nope": pa.array([1])})
        findings = quality_findings(table, RAW_30M)
        self.assertEqual(codes(findings), ["SCHEMA_MISMATCH"])
        self.assertEqual(findings[0].breadth, ScopeBreadth.PARTITION)
        self.assertIsNone(findings[0].period_start)

    def test_scoped_incidents_attach_shard_and_partition(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
        ]
        findings = quality_findings(
            table_of(rows),
            RAW_30M,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
        )
        incidents = scoped_incidents(
            findings,
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            detected_at=DETECTED_AT,
        )
        self.assertEqual(len(incidents), 1)
        scope = incidents[0].scope
        self.assertEqual(scope.shard_key, SHARD)
        self.assertEqual(scope.partition_start, date(2025, 2, 3))
        self.assertEqual(scope.partition_end, date(2025, 2, 4))
        self.assertEqual(scope.instrument_id, AAPL)
        row = incidents[0].to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(row["period_end"], "2025-02-03T16:00:00Z")

    def test_schema_mismatch_incident_falls_back_to_the_partition_window(self):
        incidents = scoped_incidents(
            quality_findings(pa.table({"nope": pa.array([1])}), RAW_30M),
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            detected_at=DETECTED_AT,
        )
        self.assertEqual(len(incidents), 1)
        row = incidents[0].to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["incident_code"], "SCHEMA_MISMATCH")
        self.assertEqual(row["period_start"], "2025-02-03T05:00:00Z")
        self.assertEqual(row["period_end"], "2025-02-04T05:00:00Z")
        self.assertIsNone(row["instrument_id"])

    def test_an_out_of_partition_finding_is_widened_to_the_partition_not_the_year(self):
        rows = [bar(AAPL, "2025-02-05T14:30:00")]
        rows[0]["session_date_et"] = date(2025, 2, 6)
        findings = quality_findings(table_of(rows), RAW_30M)
        incidents = scoped_incidents(
            only(findings, "SESSION_DATE_ET_MISMATCH"),
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            detected_at=DETECTED_AT,
        )
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents[0].scope.breadth, ScopeBreadth.PARTITION)
        row = incidents[0].to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["period_start"], "2025-02-03T05:00:00Z")
        self.assertEqual(row["period_end"], "2025-02-04T05:00:00Z")

    def test_empty_table_produces_no_findings(self):
        self.assertEqual(quality_findings(table_of([]), RAW_30M), [])


class BarSpanTests(unittest.TestCase):
    def test_a_daily_derived_bar_period_stays_inside_its_day_partition(self):
        rows = [
            bar(
                AAPL,
                "2024-11-29T14:30:00",
                open_price=100.0,
                high=130.0,
                low=100.0,
                close=100.0,
                source_minutes=210,
            )
        ]
        contract = DATASET_CONTRACTS[("raw", "DERIVED", "1d")]
        findings = only(quality_findings(table_of(rows, derived=True), contract), "PRICE_OUTLIER_RANGE")
        self.assertEqual(len(findings), 1)
        incidents = scoped_incidents(
            findings,
            shard_key=SHARD,
            partition_start=date(2024, 11, 29),
            partition_end=date(2024, 11, 30),
            detected_at=DETECTED_AT,
        )
        row = incidents[0].to_db_row(dataset_manifest_id=MANIFEST_ID)
        self.assertEqual(row["period_start"], "2024-11-29T14:30:00Z")
        self.assertEqual(row["period_end"], "2024-11-29T21:00:00Z")
        self.assertLess(
            datetime.fromisoformat(row["period_end"].replace("Z", "+00:00")),
            datetime(2024, 11, 30, 5, tzinfo=UTC) + timedelta(seconds=1),
        )


class RecordingCatalog:
    """The narrowest thing that can persist an incident.

    Structurally identical to what ``LocalCatalog``/``PostgresCatalog`` already
    expose, so a passing test here means the real catalogs satisfy the protocol.
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def record_quality_incident(self, record) -> None:
        self.rows.append(dict(record))


#: The engine attaches these to every issue before recording it.  Kept as
#: literals so the adapter cannot be "proved" by reusing engine code.
ENGINE_SHARD = "s00-of-2"
ENGINE_PARTITION_START = "2025-02-03"
ENGINE_PARTITION_END = "2025-02-04"

#: The manifest period the *old* code wrote onto every incident.  Nothing below
#: may reproduce these values; that is the whole point of the card.
MANIFEST_PERIOD_START = "2025-01-01T05:00:00Z"
MANIFEST_PERIOD_END = "2026-01-01T05:00:00Z"


class IncidentPersistenceTests(unittest.TestCase):
    """D10: a checksum failure and a bad bar must reach `quality_incidents`.

    Before this, `engine.py` recorded every incident with `instrument_id=None`
    and the manifest's whole period, and `operations.validate_catalog` wrote
    `CONTENT_HASH_MISMATCH` to a local `validation-report.json` only.
    """

    @staticmethod
    def engine_issue(issue: dict[str, object]) -> dict[str, object]:
        return {
            **issue,
            "shard_key": ENGINE_SHARD,
            "partition_start": ENGINE_PARTITION_START,
            "partition_end": ENGINE_PARTITION_END,
        }

    def test_issue_round_trips_into_a_row_scoped_to_the_bar_not_the_manifest(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
        ]
        issue = self.engine_issue(quality_issues(table_of(rows), RAW_30M)[0])

        row = incident_row_from_issue(
            issue,
            dataset_manifest_id=MANIFEST_ID,
            detected_at=DETECTED_AT,
        )

        self.assertEqual(row["incident_code"], "MISSING_BARS")
        self.assertEqual(row["severity"], "WARNING")
        self.assertEqual(row["dataset_manifest_id"], MANIFEST_ID)
        # The four impact-scope facts the card demands.
        self.assertEqual(row["instrument_id"], AAPL)
        self.assertEqual(row["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(row["period_end"], "2025-02-03T16:00:00Z")
        # ...and emphatically not the manifest-wide period.
        self.assertNotEqual(row["period_start"], MANIFEST_PERIOD_START)
        self.assertNotEqual(row["period_end"], MANIFEST_PERIOD_END)

    def test_row_keeps_shard_and_partition_alongside_the_table_columns(self):
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
        ]
        issue = self.engine_issue(quality_issues(table_of(rows), RAW_30M)[0])

        report = incident_report_from_issue(
            issue,
            dataset_manifest_id=MANIFEST_ID,
            detected_at=DETECTED_AT,
        )

        self.assertEqual(report["shard_key"], "s00-of-2")
        self.assertEqual(report["partition_start"], "2025-02-03")
        self.assertEqual(report["partition_end"], "2025-02-04")
        self.assertEqual(report["affected_bar_count"], 1)
        self.assertEqual(report["scope_breadth"], "BAR_RANGE")

    def test_issue_without_row_scope_falls_back_to_the_partition_not_the_year(self):
        """The engine's `OBJECT_PUBLISH_FAILED` shape carries no bar extent."""
        row = incident_row_from_issue(
            {
                "severity": "ERROR",
                "code": "OBJECT_PUBLISH_FAILED",
                "message": "disk full",
                "shard_key": ENGINE_SHARD,
                "partition_start": ENGINE_PARTITION_START,
                "partition_end": ENGINE_PARTITION_END,
            },
            dataset_manifest_id=MANIFEST_ID,
            detected_at=DETECTED_AT,
        )

        self.assertEqual(row["incident_code"], "OBJECT_PUBLISH_FAILED")
        self.assertIsNone(row["instrument_id"])
        # One ET day, not one year.
        self.assertEqual(row["period_start"], "2025-02-03T05:00:00Z")
        self.assertEqual(row["period_end"], "2025-02-04T05:00:00Z")

    def test_two_instruments_in_one_partition_are_two_distinct_rows(self):
        """The old id salt collapsed these into one row."""
        rows = [
            bar(AAPL, "2025-02-03T14:30:00"),
            bar(AAPL, "2025-02-03T15:00:00"),
            bar(AAPL, "2025-02-03T16:00:00"),
            bar(MSFT, "2025-02-03T14:30:00", symbol="MSFT"),
            bar(MSFT, "2025-02-03T15:00:00", symbol="MSFT"),
            bar(MSFT, "2025-02-03T17:00:00", symbol="MSFT"),
        ]
        issues = quality_issues(table_of(rows), RAW_30M)
        catalog = RecordingCatalog()

        written = record_issue_incidents(
            catalog,
            [self.engine_issue(issue) for issue in issues],
            dataset_manifest_id=MANIFEST_ID,
            detected_at=DETECTED_AT,
        )

        self.assertEqual(written, 2)
        self.assertEqual(len(catalog.rows), 2)
        self.assertEqual(
            sorted(row["instrument_id"] for row in catalog.rows), sorted([AAPL, MSFT])
        )
        self.assertEqual(len({row["id"] for row in catalog.rows}), 2)
        by_instrument = {row["instrument_id"]: row for row in catalog.rows}
        self.assertEqual(by_instrument[AAPL]["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(by_instrument[MSFT]["period_start"], "2025-02-03T15:30:00Z")
        self.assertEqual(by_instrument[MSFT]["period_end"], "2025-02-03T17:00:00Z")

    def test_recorded_row_has_exactly_the_table_columns(self):
        catalog = RecordingCatalog()

        record_issue_incidents(
            catalog,
            [
                self.engine_issue(
                    {
                        "severity": "ERROR",
                        "code": "OBJECT_PUBLISH_FAILED",
                        "message": "disk full",
                    }
                )
            ],
            dataset_manifest_id=MANIFEST_ID,
            detected_at=DETECTED_AT,
        )

        self.assertEqual(len(catalog.rows), 1)
        self.assertEqual(tuple(catalog.rows[0]), QUALITY_INCIDENT_COLUMNS)
        self.assertEqual(
            json.loads(json.dumps(catalog.rows[0], ensure_ascii=False)),
            catalog.rows[0],
        )

    def test_checksum_failure_reaches_the_catalog_with_its_evidence_object(self):
        """Previously this ended its life in a local JSON report."""
        catalog = RecordingCatalog()
        incident = content_hash_mismatch_incident(
            object_id=OBJECT_ID,
            object_key="market-data/.../part-0001.parquet",
            expected_content_hash="a" * 64,
            actual_content_hash="b" * 64,
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            period_start=utc("2025-02-03T14:30:00"),
            period_end=utc("2025-02-03T21:00:00"),
            detected_at=DETECTED_AT,
            instrument_id=AAPL,
        )

        written = record_quality_incidents(
            catalog, [incident], dataset_manifest_id=MANIFEST_ID
        )

        self.assertEqual(written, 1)
        row = catalog.rows[0]
        self.assertEqual(row["incident_code"], "CONTENT_HASH_MISMATCH")
        self.assertEqual(row["severity"], "ERROR")
        self.assertEqual(row["status"], "ACTIVE")
        self.assertEqual(row["evidence_object_id"], OBJECT_ID)
        self.assertEqual(row["instrument_id"], AAPL)
        self.assertEqual(row["dataset_manifest_id"], MANIFEST_ID)
        self.assertEqual(row["period_start"], "2025-02-03T14:30:00Z")
        self.assertEqual(row["period_end"], "2025-02-03T21:00:00Z")
        self.assertEqual(row["detected_at"], "2026-03-01T12:00:00Z")
        self.assertIsNone(row["resolved_at"])

    def test_recording_is_idempotent_for_the_same_finding(self):
        """Re-running validation must not double-count the same defect."""
        incident = content_hash_mismatch_incident(
            object_id=OBJECT_ID,
            object_key="market-data/.../part-0001.parquet",
            expected_content_hash="a" * 64,
            actual_content_hash="b" * 64,
            shard_key=SHARD,
            partition_start=date(2025, 2, 3),
            partition_end=date(2025, 2, 4),
            period_start=utc("2025-02-03T14:30:00"),
            period_end=utc("2025-02-03T21:00:00"),
            detected_at=DETECTED_AT,
        )
        first = RecordingCatalog()
        second = RecordingCatalog()
        record_quality_incidents(first, [incident], dataset_manifest_id=MANIFEST_ID)
        record_quality_incidents(second, [incident], dataset_manifest_id=MANIFEST_ID)

        # Pinned literal, not `first == second`: a constant-returning id would
        # satisfy equality but not this value.
        self.assertEqual(
            first.rows[0]["id"], "19f4d123-9ef5-5688-931f-a4a496723a54"
        )
        self.assertEqual(first.rows[0]["id"], second.rows[0]["id"])

    def test_both_real_catalogs_can_receive_incidents_not_just_the_local_one(self):
        """D07/D08: the quality gate must reach the DB catalog, not only local.

        The recorder interface is deliberately one method wide so that the
        Postgres catalog satisfies it without an `isinstance(LocalCatalog)`
        gate.  If either catalog loses `record_quality_incident`, or the
        protocol grows a method only the local catalog implements, this fails.
        """
        from market_pipeline_lib.catalog import LocalCatalog, PostgresCatalog

        self.assertTrue(issubclass(LocalCatalog, QualityIncidentRecorder))
        self.assertTrue(issubclass(PostgresCatalog, QualityIncidentRecorder))

    def test_an_unscoped_incident_is_refused_rather_than_widened_silently(self):
        catalog = RecordingCatalog()
        with self.assertRaises(ValueError):
            record_issue_incidents(
                catalog,
                [{"severity": "ERROR", "code": "OBJECT_PUBLISH_FAILED"}],
                dataset_manifest_id=MANIFEST_ID,
                detected_at=DETECTED_AT,
            )
        self.assertEqual(catalog.rows, [])


if __name__ == "__main__":
    unittest.main()
