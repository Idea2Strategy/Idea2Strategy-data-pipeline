import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

import data_validation.audit_regular_session as audit_module
from data_validation.audit_regular_session import audit_dataframe, audit_source
from market_pipeline_lib.contracts import DATASET_CONTRACTS, ET, bar_schema
from market_pipeline_lib.processing import quality_findings
from market_pipeline_lib.quality import (
    expected_session_bar_starts,
    missing_bar_intervals,
)

#: The pipeline addresses instruments by id; the audit addresses them by ticker.
#: Both paths below are asked about the same three bars of the same session.
AAPL_INSTRUMENT_ID = "11111111-1111-4111-8111-111111111111"
RAW_30M = DATASET_CONTRACTS[("raw", "RAW", "30m")]


class AuditRegularSessionTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAPL"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame({"close": range(len(timestamps))}, index=index)

    @staticmethod
    def make_bar_table(timestamps: list[str]) -> pa.Table:
        """The same bars as :meth:`make_frame`, in the pipeline's own schema."""
        rows = []
        for text in timestamps:
            started = datetime.fromisoformat(text).astimezone(UTC)
            rows.append(
                {
                    "instrument_id": AAPL_INSTRUMENT_ID,
                    "provider_symbol": "AAPL",
                    "bar_start_at": started,
                    "session_date_et": started.astimezone(ET).date(),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 100,
                    "trade_count": 10,
                    "vwap": 100.25,
                }
            )
        return pa.Table.from_pylist(rows, schema=bar_schema(False))

    def test_reports_missing_bar_between_observed_boundaries(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:35:00+00:00",
                "2025-02-03 14:45:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(dataframe, "AAPL")

        self.assertEqual(summary["expected_bars"], 4)
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(summary["missing_intervals"], 1)
        self.assertEqual(summary["coverage_pct"], 75.0)
        self.assertEqual(intervals[0]["missing_start_utc"], "2025-02-03T14:40:00+00:00")
        self.assertEqual(intervals[0]["previous_bar_utc"], "2025-02-03T14:35:00+00:00")
        self.assertEqual(intervals[0]["next_bar_utc"], "2025-02-03T14:45:00+00:00")

    def test_does_not_expect_bars_outside_observed_date_range(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 15:00:00+00:00",
                "2025-02-03 15:05:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(dataframe, "AAPL")

        self.assertEqual(summary["expected_bars"], 2)
        self.assertEqual(summary["missing_bars"], 0)
        self.assertEqual(intervals, [])

    def test_one_minute_sip_audit_uses_one_minute_frequency(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:31:00+00:00",
                "2025-02-03 14:33:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(
            dataframe,
            "AAPL",
            bar_frequency=pd.Timedelta(minutes=1),
        )

        self.assertEqual(summary["expected_bars"], 4)
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(intervals[0]["missing_minutes"], 1)
        self.assertEqual(
            intervals[0]["missing_start_utc"],
            "2025-02-03T14:32:00+00:00",
        )

    def test_summary_only_counts_gaps_without_materializing_rows(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:40:00+00:00",
                "2025-02-03 14:50:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(
            dataframe,
            "AAPL",
            include_intervals=False,
        )

        self.assertEqual(summary["missing_bars"], 2)
        self.assertEqual(summary["missing_intervals"], 2)
        self.assertEqual(intervals, [])

    def test_multi_interval_sip_filename_reports_only_ticker(self):
        """Both filename shapes reduce to the ticker, and the audit still runs.

        The old version asserted only `symbol == "AAPL"`, which a stub that
        never opened the file would pass.  Every audited quantity is pinned
        here, and the legacy `_5min_historical` shape is exercised alongside
        the `_<interval>min_sip_historical` one.
        """
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_dir = Path(temporary_directory)
            self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:45:00+00:00",
                ]
            ).to_csv(
                source_dir / "AAPL_15min_sip_historical.csv",
                index=True,
            )
            self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 15:00:00+00:00",
                ]
            ).to_csv(
                source_dir / "MSFT_5min_historical.csv",
                index=True,
            )

            summary, intervals = audit_source(
                source_dir,
                "csv",
                "XNYS",
                pd.Timedelta(minutes=15),
            )

            by_symbol = summary.set_index("symbol")
            self.assertEqual(sorted(by_symbol.index), ["AAPL", "MSFT"])
            self.assertEqual(
                by_symbol.loc["AAPL", "file"], "AAPL_15min_sip_historical.csv"
            )
            self.assertEqual(by_symbol.loc["MSFT", "file"], "MSFT_5min_historical.csv")
            self.assertEqual(by_symbol.loc["AAPL", "status"], "ok")
            self.assertEqual(by_symbol.loc["AAPL", "expected_bars"], 2)
            self.assertEqual(by_symbol.loc["AAPL", "missing_bars"], 0)
            self.assertEqual(by_symbol.loc["AAPL", "coverage_pct"], 100.0)
            # MSFT spans 14:30-15:00 at a 15 minute cadence, so 14:45 is absent.
            self.assertEqual(by_symbol.loc["MSFT", "expected_bars"], 3)
            self.assertEqual(by_symbol.loc["MSFT", "missing_bars"], 1)
            self.assertEqual(by_symbol.loc["MSFT", "missing_intervals"], 1)
            self.assertEqual(
                intervals.loc[intervals["symbol"] == "MSFT", "missing_start_utc"].tolist(),
                ["2025-02-03T14:45:00+00:00"],
            )

    def test_pipeline_detector_and_audit_report_the_same_missing_bar(self):
        """The two *entry points* — not just the shared helper — must agree.

        ``quality_findings`` is what the pipeline actually calls when it writes
        a partition, and ``audit_dataframe`` is what the CSV audit CLI calls.
        They are driven here from the same three 30-minute bars of the same
        session, and every boundary on both sides is a pinned literal: no value
        produced by one path is fed into the other, and neither expectation is
        recomputed from a production formula.
        """
        observed_bars = [
            "2025-02-03 14:30:00+00:00",
            "2025-02-03 15:00:00+00:00",
            # 15:30 is absent — the gap both paths must find.
            "2025-02-03 16:00:00+00:00",
        ]

        # --- pipeline path -------------------------------------------------
        gaps = [
            finding
            for finding in quality_findings(self.make_bar_table(observed_bars), RAW_30M)
            if finding.code == "MISSING_BARS"
        ]
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].instrument_id, AAPL_INSTRUMENT_ID)
        self.assertEqual(gaps[0].severity, "WARNING")
        self.assertEqual(gaps[0].affected_bar_count, 1)
        self.assertEqual(
            gaps[0].period_start.isoformat(), "2025-02-03T15:30:00+00:00"
        )
        # Exclusive end: the missing bar start plus one 30-minute span.
        self.assertEqual(gaps[0].period_end.isoformat(), "2025-02-03T16:00:00+00:00")

        # --- audit path ----------------------------------------------------
        summary, intervals = audit_dataframe(
            self.make_frame(observed_bars),
            "AAPL",
            bar_frequency=pd.Timedelta(minutes=30),
        )
        self.assertEqual(summary["expected_bars"], 4)
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(summary["missing_intervals"], 1)
        self.assertEqual(summary["coverage_pct"], 75.0)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0]["missing_start_utc"], "2025-02-03T15:30:00+00:00")
        # The audit reports the inclusive last missing bar start, so its
        # `missing_end_utc` is one span earlier than the detector's exclusive
        # `period_end`. Both describe the identical single 15:30 bar.
        self.assertEqual(intervals[0]["missing_end_utc"], "2025-02-03T15:30:00+00:00")
        self.assertEqual(intervals[0]["missing_bars"], 1)
        self.assertEqual(intervals[0]["missing_minutes"], 30)
        self.assertEqual(intervals[0]["previous_bar_utc"], "2025-02-03T15:00:00+00:00")
        self.assertEqual(intervals[0]["next_bar_utc"], "2025-02-03T16:00:00+00:00")

    def test_audit_and_pipeline_agree_on_the_same_gap(self):
        """The audit is a thin adapter over the pipeline's gap logic.

        The two used to be separate implementations; if they diverge again this
        fails, because both are asked about the same session and the interval
        boundaries are pinned literals rather than one being fed to the other.
        """
        observed = pd.DatetimeIndex(
            pd.to_datetime(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:45:00+00:00",
                    "2025-02-03 15:15:00+00:00",
                ],
                utc=True,
            )
        )
        expected = expected_session_bar_starts(
            observed.min(),
            observed.max(),
            bar_frequency=pd.Timedelta(minutes=15),
            calendar_name="XNYS",
        )
        gaps = missing_bar_intervals(
            expected.difference(observed),
            expected,
            observed,
            bar_frequency=pd.Timedelta(minutes=15),
            calendar_name="XNYS",
        )
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].start.isoformat(), "2025-02-03T15:00:00+00:00")
        self.assertEqual(gaps[0].last_start.isoformat(), "2025-02-03T15:00:00+00:00")
        self.assertEqual(gaps[0].end.isoformat(), "2025-02-03T15:15:00+00:00")
        self.assertEqual(gaps[0].bar_count, 1)

        summary, intervals = audit_dataframe(
            self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:45:00+00:00",
                    "2025-02-03 15:15:00+00:00",
                ]
            ),
            "AAPL",
            bar_frequency=pd.Timedelta(minutes=15),
        )
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(intervals[0]["missing_start_utc"], "2025-02-03T15:00:00+00:00")
        self.assertEqual(intervals[0]["missing_end_utc"], "2025-02-03T15:00:00+00:00")
        self.assertEqual(intervals[0]["missing_minutes"], 15)


class AuditCliExitCodeTests(unittest.TestCase):
    """The CLI must report failure through its exit code, not only on stdout.

    Spec §1: several pipeline CLIs `return 0` regardless of outcome, so a
    scheduled run that audited nothing — or that failed to read every file it
    found — was indistinguishable from a clean one.
    """

    def run_cli(self, root: Path, argv: list[str]) -> int:
        with patch.object(audit_module, "PROJECT_ROOT", root):
            return audit_module.main(argv)

    @staticmethod
    def write_csv(root: Path, name: str, text: str) -> None:
        source_dir = root / "regular_market_data" / "raw" / "csv"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / name).write_text(text, encoding="utf-8")

    def test_unsupported_calendar_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = self.run_cli(
                root,
                [
                    "--data-type", "raw",
                    "--format", "csv",
                    "--calendar", "NOT_A_REAL_CALENDAR",
                    "--report-dir", str(root / "report"),
                ],
            )
        self.assertEqual(code, 2)

    def test_no_input_files_exits_non_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = self.run_cli(
                root,
                [
                    "--data-type", "raw",
                    "--format", "csv",
                    "--report-dir", str(root / "report"),
                ],
            )
        self.assertEqual(code, 1)

    def test_a_file_that_cannot_be_audited_exits_non_zero(self):
        """A run that read nothing usable must not look like a clean run."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # No `symbol`/`timestamp` columns: load_market_data raises.
            self.write_csv(root, "AAPL_5min_historical.csv", "nope\n1\n")
            code = self.run_cli(
                root,
                [
                    "--data-type", "raw",
                    "--format", "csv",
                    "--report-dir", str(root / "report"),
                ],
            )
        self.assertEqual(code, 3)

    def test_a_fully_readable_run_exits_zero(self):
        """The failure codes above must come from real failures, not from
        the CLI having become unable to succeed."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_csv(
                root,
                "AAPL_5min_historical.csv",
                "symbol,timestamp,close\n"
                "AAPL,2025-02-03 14:30:00+00:00,1\n"
                "AAPL,2025-02-03 14:35:00+00:00,2\n",
            )
            code = self.run_cli(
                root,
                [
                    "--data-type", "raw",
                    "--format", "csv",
                    "--report-dir", str(root / "report"),
                ],
            )
            self.assertEqual(code, 0)
            self.assertTrue((root / "report" / "raw_csv_summary.csv").is_file())


if __name__ == "__main__":
    unittest.main()
