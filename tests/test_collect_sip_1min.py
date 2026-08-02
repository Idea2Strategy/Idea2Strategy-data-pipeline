import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from alpaca.trading.enums import AssetStatus

from data_collection.collect_sip_1min import (
    alpaca_symbol,
    collection_window,
    CollectionResult,
    earliest_changed_timestamp,
    fetch_chunk,
    InactiveSymbolCache,
    merge_frames,
    MIN_EXPECTED_TICKERS,
    process_symbol,
    resolve_symbol_universe,
    save_local_data,
    should_skip_inactive_symbol,
    storage_path,
    update_symbol_data,
)
from data_collection.get_ticker import (
    get_historical_sp500_tickers,
    SP500UniverseError,
)
from daily_pipeline import run_collection


class CollectSipOneMinuteTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str], values: list[int]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAPL"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame({"close": values}, index=index)

    def test_collection_window_uses_exact_three_years_and_fifteen_minute_delay(self):
        now = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
        start, end = collection_window(now)

        self.assertEqual(end, datetime(2026, 7, 19, 11, 45, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2023, 7, 19, 12, 0, tzinfo=timezone.utc))

    def test_storage_path_separates_feed_type_and_format(self):
        path = storage_path("BRK/B", "adjusted", "parquet", Path("sip_market_data"))
        self.assertEqual(
            path,
            Path("sip_market_data/adjusted/parquet/BRK-B_1min_sip_historical.parquet"),
        )

    def test_alpaca_symbol_uses_dot_for_class_shares(self):
        self.assertEqual(alpaca_symbol("BRK/B"), "BRK.B")
        self.assertEqual(alpaca_symbol("BF/B"), "BF.B")
        self.assertEqual(alpaca_symbol("AAPL"), "AAPL")

    def test_fetch_chunk_sends_alpaca_class_share_symbol(self):
        client = Mock()
        client.get_stock_bars.return_value = Mock(df=pd.DataFrame())

        result = fetch_chunk(
            client,
            "BRK/B",
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
            "adjusted",
        )

        request = client.get_stock_bars.call_args.args[0]
        self.assertEqual(request.symbol_or_symbols, ["BRK.B"])
        self.assertTrue(result.empty)

    def test_merge_deduplicates_and_keeps_latest_value_inside_window(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        new = self.make_frame(
            ["2025-01-02 14:31:00Z", "2025-01-02 14:32:00Z"],
            [201, 202],
        )

        combined = merge_frames(
            existing,
            [new],
            datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 14, 33, tzinfo=timezone.utc),
        )

        self.assertEqual(len(combined), 3)
        self.assertEqual(combined.loc[("AAPL", pd.Timestamp("2025-01-02 14:31:00Z")), "close"], 201)

    def test_detects_first_adjusted_overlap_revision(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [50, 50],
        )

        changed = earliest_changed_timestamp(existing, refreshed)

        self.assertEqual(changed, pd.Timestamp("2025-01-02 14:30:00Z"))

    def test_comparison_deduplicates_overlapping_chunk_boundaries(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            [
                "2025-01-02 14:30:00Z",
                "2025-01-02 14:31:00Z",
                "2025-01-02 14:31:00Z",
            ],
            [100, 101, 101],
        )

        changed = earliest_changed_timestamp(existing, refreshed)

        self.assertIsNone(changed)

    def test_update_saves_when_fetched_chunks_share_a_boundary_bar(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        first_chunk = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        second_chunk = self.make_frame(
            ["2025-01-02 14:31:00Z", "2025-01-02 14:32:00Z"],
            [101, 102],
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("AAPL", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                return_value=[first_chunk, second_chunk],
            ):
                result = update_symbol_data(
                    Mock(),
                    "AAPL",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 14, 33, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                )

            saved = pd.read_csv(path)

        self.assertTrue(result.success)
        self.assertFalse(result.adjustment_revision)
        self.assertEqual(result.added_rows, 1)
        self.assertEqual(len(saved), 3)

    def test_adjusted_revision_refreshes_full_rolling_window(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [50, 50],
        )
        historical = self.make_frame(["2025-01-01 14:30:00Z"], [49])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("AAPL", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                side_effect=[[refreshed], [historical]],
            ) as mock_fetch:
                result = update_symbol_data(
                    Mock(),
                    "AAPL",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 14, 32, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                )

        self.assertTrue(result.success)
        self.assertTrue(result.adjustment_revision)
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(
            result.changed_from_utc,
            pd.Timestamp("2025-01-01 14:30:00+00:00"),
        )

    def test_confirmed_inactive_symbol_is_cached_and_skipped(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache_path = Path(temporary_directory) / "inactive_symbols.json"
            cache = InactiveSymbolCache(cache_path)
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.INACTIVE)
            last_bar = datetime(2023, 10, 12, 23, 48, tzinfo=timezone.utc)
            end_time = datetime(2026, 7, 20, tzinfo=timezone.utc)

            first = should_skip_inactive_symbol(
                asset_client, cache, "ATVI", last_bar, end_time
            )
            reloaded_cache = InactiveSymbolCache(cache_path)
            cached_client = Mock()
            second = should_skip_inactive_symbol(
                cached_client, reloaded_cache, "ATVI", last_bar, end_time
            )

        self.assertTrue(first)
        self.assertTrue(second)
        asset_client.get_asset.assert_called_once_with("ATVI")
        cached_client.get_asset.assert_not_called()

    def test_active_or_recent_symbol_is_not_skipped(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache = InactiveSymbolCache(
                Path(temporary_directory) / "inactive_symbols.json"
            )
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.ACTIVE)
            end_time = datetime(2026, 7, 20, tzinfo=timezone.utc)

            active = should_skip_inactive_symbol(
                asset_client,
                cache,
                "AAPL",
                datetime(2026, 5, 1, tzinfo=timezone.utc),
                end_time,
            )
            active_again = should_skip_inactive_symbol(
                asset_client,
                cache,
                "AAPL",
                datetime(2026, 5, 1, tzinfo=timezone.utc),
                end_time,
            )
            recent = should_skip_inactive_symbol(
                asset_client,
                cache,
                "MSFT",
                datetime(2026, 7, 19, tzinfo=timezone.utc),
                end_time,
            )

        self.assertFalse(active)
        self.assertFalse(active_again)
        self.assertFalse(recent)
        asset_client.get_asset.assert_called_once_with("AAPL")

    def test_update_does_not_fetch_bars_for_confirmed_inactive_symbol(self):
        existing = self.make_frame(["2023-10-12 23:48:00Z"], [95])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("ATVI", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            cache = InactiveSymbolCache(root / "inactive_symbols.json")
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.INACTIVE)
            with patch("data_collection.collect_sip_1min.fetch_range") as mock_fetch:
                result = update_symbol_data(
                    Mock(),
                    "ATVI",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2023, 7, 20, tzinfo=timezone.utc),
                    datetime(2026, 7, 20, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2026, 7, 1, tzinfo=timezone.utc),
                    asset_client,
                    cache,
                )

        self.assertTrue(result.success)
        self.assertTrue(result.inactive)
        self.assertEqual(result.added_rows, 0)
        self.assertEqual(result.outcome, "INACTIVE")
        mock_fetch.assert_not_called()


class EmptyCollectionOutcomeTests(unittest.TestCase):
    """A symbol that yields zero bars must not be reported as a success."""

    def test_update_with_zero_rows_is_not_success_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("ZZZZ", "adjusted", "csv", root)
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                return_value=[],
            ):
                result = update_symbol_data(
                    Mock(),
                    "ZZZZ",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, tzinfo=timezone.utc),
                    7,
                    0,
                )

            self.assertFalse(path.exists())

        self.assertFalse(result.success)
        self.assertFalse(bool(result))
        self.assertTrue(result.empty)
        self.assertEqual(result.outcome, "EMPTY")
        self.assertEqual(result.added_rows, 0)

    def test_empty_outcome_is_distinct_from_a_fetch_failure(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                return_value=None,
            ):
                failure = update_symbol_data(
                    Mock(),
                    "ZZZZ",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, tzinfo=timezone.utc),
                    7,
                    0,
                )

        self.assertFalse(failure.success)
        self.assertFalse(failure.empty)
        self.assertEqual(failure.outcome, "FAILED")

    def test_collection_result_cannot_be_both_empty_and_successful(self):
        with self.assertRaises(ValueError):
            CollectionResult(True, empty=True)

    def test_process_symbol_with_zero_rows_is_not_success(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("ZZZZ", "adjusted", "csv", root)
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                return_value=[],
            ):
                succeeded = process_symbol(
                    Mock(),
                    "ZZZZ",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, tzinfo=timezone.utc),
                    7,
                    0,
                )

            self.assertFalse(path.exists())

        self.assertFalse(succeeded)

    def test_empty_result_is_not_checkpointed_as_complete(self):
        state = Mock()
        with patch(
            "daily_pipeline.update_symbol_data",
            return_value=CollectionResult(False, empty=True),
        ), patch("daily_pipeline._checkpoint_complete", return_value=False):
            failures, added_rows = run_collection(
                Mock(),
                ["ZZZZ"],
                "csv",
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 2, tzinfo=timezone.utc),
                state,
                "2025-01-02T21:00:00+00:00",
                None,
                "adjusted",
                Mock(),
                Mock(),
                retry_delay=0,
            )

        self.assertEqual(added_rows, 0)
        self.assertEqual(len(failures), 1)
        recorded_statuses = {call.args[3] for call in state.mark_stage.call_args_list}
        self.assertEqual(recorded_statuses, {"failed"})
        self.assertNotIn("success", recorded_statuses)


class TickerUniverseFailureTests(unittest.TestCase):
    """A failed universe fetch must never silently shrink the ticker file."""

    def test_fetch_failure_raises_typed_error(self):
        with patch(
            "data_collection.get_ticker.urllib.request.urlopen",
            side_effect=OSError("wikipedia unreachable"),
        ):
            with self.assertRaises(SP500UniverseError):
                get_historical_sp500_tickers(years=10)

    def test_fetch_failure_does_not_write_the_ticker_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "sp500_tickers_10years.txt"
            with patch(
                "data_collection.collect_sip_1min.get_historical_sp500_tickers",
                side_effect=SP500UniverseError("wikipedia unreachable"),
            ), patch(
                "data_collection.collect_sip_1min.TICKER_FILE",
                ticker_file,
            ):
                with self.assertRaises(SP500UniverseError):
                    resolve_symbol_universe(None, ticker_file=ticker_file)

            self.assertFalse(ticker_file.exists())

    def test_suspiciously_small_universe_is_rejected_and_not_written(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "sp500_tickers_10years.txt"
            ticker_file.write_text("AAA\nBBB\nCCC\n", encoding="utf-8")
            with patch(
                "data_collection.collect_sip_1min.get_historical_sp500_tickers",
                return_value=["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA"],
            ):
                with self.assertRaises(SP500UniverseError):
                    resolve_symbol_universe(None, ticker_file=ticker_file)

            self.assertEqual(
                ticker_file.read_text(encoding="utf-8"), "AAA\nBBB\nCCC\n"
            )

    def test_full_universe_is_written(self):
        universe = [f"SYM{index:03d}" for index in range(MIN_EXPECTED_TICKERS)]
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "sp500_tickers_10years.txt"
            with patch(
                "data_collection.collect_sip_1min.get_historical_sp500_tickers",
                return_value=universe,
            ):
                symbols = resolve_symbol_universe(None, ticker_file=ticker_file)

            self.assertEqual(symbols, universe)
            self.assertEqual(
                ticker_file.read_text(encoding="utf-8").split(),
                universe,
            )


if __name__ == "__main__":
    unittest.main()
