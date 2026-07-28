import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
import pyarrow.parquet as pq

from market_pipeline_lib.catalog import LocalCatalog
from market_pipeline_lib.cli import build_parser, execute
from market_pipeline_lib.contracts import (
    ADJUSTED_FEED,
    DATASET_CONTRACTS,
    RAW_FEED,
    bar_schema,
    canonical_dataset_hash,
    logical_dataset_id,
    object_key,
    partition_bounds,
    stable_shard_key,
)
from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.operations import export_db_plan, validate_catalog
from market_pipeline_lib.processing import (
    derive_regular_bars,
    normalize_provider_frame,
)
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore
from pipeline_state import PipelineStateStore


IDS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
    "BRK.B": "33333333-3333-4333-8333-333333333333",
}


def provider_frame(symbol, sessions, *, adjusted=False):
    timestamps = []
    for session, count in sessions:
        start = pd.Timestamp(session, tz="America/New_York") + pd.Timedelta(
            hours=9, minutes=30
        )
        timestamps.extend(
            (start + pd.Timedelta(minutes=30 * index)).tz_convert("UTC")
            for index in range(count)
        )
    index = pd.MultiIndex.from_arrays(
        [[symbol] * len(timestamps), timestamps],
        names=["symbol", "timestamp"],
    )
    offset = 10.0 if adjusted else 0.0
    values = list(range(len(timestamps)))
    return pd.DataFrame(
        {
            "open": [100 + offset + value / 10 for value in values],
            "high": [101 + offset + value / 10 for value in values],
            "low": [99 + offset + value / 10 for value in values],
            "close": [100.5 + offset + value / 10 for value in values],
            "volume": [100] * len(timestamps),
            "trade_count": [10] * len(timestamps),
            "vwap": [100.25 + offset + value / 10 for value in values],
        },
        index=index,
    )


class FakeSource:
    def __init__(self, sessions):
        self.sessions = sessions
        self.calls = []

    def fetch(self, symbol, start, end, price_type):
        self.calls.append((symbol, start, end, price_type))
        frame = provider_frame(
            symbol,
            self.sessions,
            adjusted=price_type == "adjusted",
        )
        timestamps = frame.index.get_level_values("timestamp")
        return frame[(timestamps >= start) & (timestamps < end)]


class PartialSource(FakeSource):
    def fetch(self, symbol, start, end, price_type):
        if symbol == "MSFT":
            self.calls.append((symbol, start, end, price_type))
            return None
        return super().fetch(symbol, start, end, price_type)


class RevisedAdjustedSource(FakeSource):
    def fetch(self, symbol, start, end, price_type):
        frame = super().fetch(symbol, start, end, price_type)
        if frame is not None and price_type == "adjusted":
            for column in ("open", "high", "low", "close", "vwap"):
                frame[column] = frame[column] + 5.0
        return frame


class FakeS3Client:
    def __init__(self):
        self.objects = {}

    def upload_file(self, source, bucket, key, ExtraArgs):
        content = Path(source).read_bytes()
        self.objects[(bucket, key)] = {
            "Body": content,
            "Metadata": ExtraArgs["Metadata"],
        }

    def head_object(self, Bucket, Key):
        value = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(value["Body"]),
            "Metadata": value["Metadata"],
            "VersionId": "v1",
            "ETag": '"etag"',
        }


class MarketPipelineContractTests(unittest.TestCase):
    def test_all_eight_required_dataset_contracts_are_separate(self):
        self.assertEqual(len(DATASET_CONTRACTS), 8)
        self.assertIn(("raw", "RAW", "30m"), DATASET_CONTRACTS)
        self.assertIn(("adjusted", "ADJUSTED", "30m"), DATASET_CONTRACTS)
        for price_type in ("raw", "adjusted"):
            for resolution in ("1h", "4h", "1d"):
                self.assertIn(
                    (price_type, "DERIVED", resolution),
                    DATASET_CONTRACTS,
                )

    def test_derived_schema_uses_source_minutes(self):
        self.assertNotIn("source_bars", bar_schema(True).names)
        self.assertEqual(
            bar_schema(True).field("source_minutes").type,
            bar_schema(True).field("source_minutes").type,
        )

    def test_partition_boundaries_are_calendar_aligned(self):
        self.assertEqual(
            partition_bounds(date(2025, 1, 1), "WEEK"),
            (date(2024, 12, 30), date(2025, 1, 6)),
        )
        self.assertEqual(
            partition_bounds(date(2024, 2, 20), "MONTH"),
            (date(2024, 2, 1), date(2024, 3, 1)),
        )
        self.assertEqual(
            partition_bounds(date(2024, 12, 31), "YEAR"),
            (date(2024, 1, 1), date(2025, 1, 1)),
        )

    def test_symbol_change_does_not_change_shard(self):
        identifier = IDS["BRK.B"]
        self.assertEqual(
            stable_shard_key(identifier, 16),
            stable_shard_key(str(uuid.UUID(identifier)), 16),
        )

    def test_object_key_includes_provider_feed_and_partition_contract(self):
        contract = DATASET_CONTRACTS[("adjusted", "ADJUSTED", "30m")]
        key = object_key(
            contract,
            logical_dataset_id(contract, 2024),
            1,
            "YEAR",
            date(2024, 1, 1),
            date(2025, 1, 1),
            "s03-of-16",
            1,
        )
        self.assertIn("provider=ALPACA", key)
        self.assertIn(f"feed={ADJUSTED_FEED}", key)
        self.assertIn("granularity=YEAR", key)
        self.assertTrue(key.endswith("part-00001.parquet"))

    def test_dataset_hash_includes_object_kind(self):
        base = {
            "content_hash": "abc",
            "object_kind": "MARKET_BARS",
            "partition_granularity": "DAY",
            "partition_start": "2025-01-02",
            "partition_end": "2025-01-03",
            "period_start": "2025-01-02T14:30:00Z",
            "period_end": "2025-01-02T21:00:00Z",
            "shard_key": "s00-of-01",
            "part_number": 1,
            "row_count": 13,
            "schema_version": "market-bars-v2",
        }
        changed = {**base, "object_kind": "OTHER"}
        self.assertNotEqual(
            canonical_dataset_hash([base]),
            canonical_dataset_hash([changed]),
        )

    def test_early_close_source_minutes_and_missing_bars_are_observed(self):
        mapping = type(
            "Mapping",
            (),
            {
                "instrument_id": IDS["AAPL"],
                "provider_symbol": "AAPL",
            },
        )()
        source = normalize_provider_frame(
            provider_frame("AAPL", [("2024-11-29", 7)]),
            mapping,
        )
        one_hour = derive_regular_bars(source, "1h").to_pandas()
        four_hour = derive_regular_bars(source, "4h").to_pandas()
        one_day = derive_regular_bars(source, "1d").to_pandas()
        self.assertEqual(one_hour["source_minutes"].tolist(), [60, 60, 60, 30])
        self.assertEqual(four_hour["source_minutes"].tolist(), [210])
        self.assertEqual(one_day["source_minutes"].tolist(), [210])
        missing = source.take(list(range(0, source.num_rows, 2)))
        self.assertLess(
            derive_regular_bars(missing, "1h").to_pandas()[
                "source_minutes"
            ].sum(),
            210,
        )

    def test_local_and_s3_stores_keep_the_same_object_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.bin"
            source.write_bytes(b"immutable")
            key = "market-data/provider=ALPACA/feed=TEST/object.parquet"
            local = LocalObjectStore(root / "local")
            local_receipt = local.put(source, key)
            remote = S3ObjectStore(
                "bucket",
                client=FakeS3Client(),
            )
            remote_receipt = remote.put(source, key)
            self.assertEqual(local_receipt.object_key, key)
            self.assertEqual(remote_receipt.object_key, key)
            self.assertEqual(
                local_receipt.content_hash,
                remote_receipt.content_hash,
            )


class MarketPipelineEndToEndTests(unittest.TestCase):
    def make_engine(self, root, sessions, *, dry_run=False, revision=None):
        instrument_map = root / "instrument_map.csv"
        instrument_map.write_text(
            "provider_symbol,instrument_id\n"
            + "".join(f"{symbol},{identifier}\n" for symbol, identifier in IDS.items()),
            encoding="utf-8",
        )
        config = PipelineConfig(
            local_root=root / "objects",
            staging_root=root / "staging",
            instrument_map_path=instrument_map,
            shard_count=2,
            target_size_mib=1,
            max_size_mib=2,
            dry_run=dry_run,
            revision=revision,
        )
        return MarketPipelineEngine(config, source=FakeSource(sessions))

    def test_backfill_builds_raw_adjusted_and_six_derived_manifests(self):
        sessions = [("2024-11-29", 7), ("2024-12-02", 13)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            result = engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 12, 3, tzinfo=timezone.utc),
                price_types=("raw", "adjusted"),
            )
            self.assertEqual(result["status"], "SUCCEEDED")
            self.assertEqual(result["manifest_count"], 8)
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            manifests = catalog.records("market_data.dataset_manifests")
            self.assertEqual(
                len([row for row in manifests if row["status"] == "AVAILABLE"]),
                8,
            )
            feeds = {row["code"] for row in catalog.records("market_data.feeds")}
            self.assertEqual(feeds, {RAW_FEED, ADJUSTED_FEED})
            lineages = catalog.records("market_data.dataset_object_lineage")
            self.assertTrue(lineages)
            self.assertTrue(
                all(row["source_dataset_object_id"] for row in lineages)
            )
            self.assertTrue(
                (
                    root
                    / "objects"
                    / "catalog-export"
                    / "pipeline-run-outputs.local.jsonl"
                ).is_file()
            )
            report = validate_catalog(
                catalog,
                LocalObjectStore(root / "objects"),
            )
            self.assertEqual(report["status"], "PASSED", report)

    def test_incremental_day_then_week_compaction_has_no_overlap(self):
        sessions = [
            ("2025-01-06", 13),
            ("2025-01-07", 13),
            ("2025-01-08", 13),
            ("2025-01-10", 13),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            result = engine.incremental(
                sessions=[
                    date(2025, 1, 6),
                    date(2025, 1, 7),
                    date(2025, 1, 8),
                    date(2025, 1, 10),
                ],
                price_types=("raw",),
                symbols=["AAPL"],
            )
            self.assertEqual(result["status"], "SUCCEEDED")
            compacted = engine.compact(
                DATASET_CONTRACTS[("raw", "RAW", "30m")],
                granularity="WEEK",
                period=date(2025, 1, 8),
            )
            self.assertEqual(compacted["status"], "SUCCEEDED")
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            latest = catalog.latest_available_manifest(
                feed_id=engine.feed_ids[RAW_FEED],
                data_layer="RAW",
                resolution="30m",
                year=2025,
            )
            objects = catalog.objects_for_manifest(latest["id"])
            self.assertEqual(
                {item["partition_granularity"] for item in objects},
                {"WEEK"},
            )
            lineage = catalog.records("market_data.dataset_object_lineage")
            compacted_from = [
                row for row in lineage if row["relation_type"] == "COMPACTED_FROM"
            ]
            self.assertEqual(len(compacted_from), 4)
            self.assertEqual(
                validate_catalog(
                    catalog,
                    LocalObjectStore(root / "objects"),
                )["status"],
                "PASSED",
            )

    def test_month_crossing_week_is_kept_as_day_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, [], dry_run=True)
            result = engine.compact(
                DATASET_CONTRACTS[("raw", "RAW", "30m")],
                granularity="WEEK",
                period=date(2025, 1, 30),
            )
            self.assertEqual(result["status"], "SKIPPED")

    def test_db_export_has_only_dbml_columns_and_local_bucket_is_null(self):
        sessions = [("2024-11-29", 7)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL"],
            )
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            result = export_db_plan(
                catalog,
                root / "db-plan",
                dbml_path=Path("tests/fixtures/market-data-schema.dbml").resolve(),
            )
            self.assertEqual(result["status"], "PASSED", result)
            storage = catalog.records("storage.objects")
            self.assertTrue(storage)
            self.assertTrue(all(row["bucket_name"] is None for row in storage))

    def test_partition_checkpoint_uses_manifest_dimensions(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = PipelineStateStore(Path(temporary) / "state.json")
            state.mark_partition(
                "incremental",
                "RAW",
                "30m",
                "2025-01-02",
                "2025-01-03",
                "s00-of-16",
                1,
                status="success",
                content_hash="abc",
            )
            self.assertTrue(
                state.is_partition_complete(
                    "incremental",
                    "RAW",
                    "30m",
                    "2025-01-02",
                    "2025-01-03",
                    "s00-of-16",
                    1,
                    expected_content_hash="abc",
                )
            )

    def test_partial_source_failure_preserves_objects_but_quarantines_manifests(self):
        sessions = [("2024-11-29", 7)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            engine.source = PartialSource(sessions)
            result = engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL", "MSFT"],
            )
            self.assertEqual(result["status"], "FAILED")
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            manifests = catalog.records("market_data.dataset_manifests")
            self.assertTrue(manifests)
            self.assertTrue(
                all(row["status"] == "QUARANTINED" for row in manifests)
            )
            self.assertTrue(catalog.records("storage.objects"))
            incidents = catalog.records("market_data.quality_incidents")
            self.assertTrue(
                any(
                    row["incident_code"] == "ALPACA_FETCH_FAILED"
                    for row in incidents
                )
            )

    def test_resume_reuses_completed_staging_fragments(self):
        sessions = [("2024-11-29", 7)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.make_engine(root, sessions)
            first.collect_staging(
                run_code="resume-test",
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL"],
            )
            instrument_map = root / "instrument_map.csv"
            resumed_source = FakeSource(sessions)
            resumed = MarketPipelineEngine(
                PipelineConfig(
                    local_root=root / "objects",
                    staging_root=root / "staging",
                    instrument_map_path=instrument_map,
                    shard_count=2,
                    target_size_mib=1,
                    max_size_mib=2,
                    resume=True,
                ),
                source=resumed_source,
            )
            result = resumed.collect_staging(
                run_code="resume-test",
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL"],
            )
            self.assertEqual(result["fragment_count"], 1)
            self.assertEqual(resumed_source.calls, [])

    def test_month_compaction_consumes_weeks_and_boundary_days(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, [])
            run = engine._run_record("TEST_DAY_LOAD", "january-2025")
            engine._active_run_id = run["id"]
            contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
            schedule = mcal.get_calendar("XNYS").schedule(
                start_date="2025-01-01",
                end_date="2025-01-31",
                tz="UTC",
            )
            groups = []
            mapping = engine.mappings["AAPL"]
            for session in (pd.Timestamp(value).date() for value in schedule.index):
                table = normalize_provider_frame(
                    provider_frame("AAPL", [(session.isoformat(), 13)]),
                    mapping,
                )
                start, end = partition_bounds(session, "DAY")
                groups.append(
                    (
                        "DAY",
                        start,
                        end,
                        stable_shard_key(mapping.instrument_id, 2),
                        table,
                        [],
                    )
                )
            engine.publish_dataset(contract, 2025, groups)
            for week in (date(2025, 1, 6), date(2025, 1, 13), date(2025, 1, 20)):
                result = engine.compact(
                    contract,
                    granularity="WEEK",
                    period=week,
                )
                self.assertEqual(result["status"], "SUCCEEDED")
            monthly = engine.compact(
                contract,
                granularity="MONTH",
                period=date(2025, 1, 15),
            )
            self.assertEqual(monthly["status"], "SUCCEEDED")
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            latest = catalog.latest_available_manifest(
                feed_id=engine.feed_ids[RAW_FEED],
                data_layer="RAW",
                resolution="30m",
                year=2025,
            )
            objects = catalog.objects_for_manifest(latest["id"])
            self.assertEqual(
                {item["partition_granularity"] for item in objects},
                {"MONTH"},
            )
            sources = [
                row
                for row in catalog.records(
                    "market_data.dataset_object_lineage"
                )
                if row["pipeline_run_id"] == monthly["pipeline_run_id"]
            ]
            self.assertGreater(len(sources), 3)

    def test_year_compaction_consumes_all_month_objects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, [])
            run = engine._run_record("TEST_MONTH_LOAD", "year-2024")
            engine._active_run_id = run["id"]
            contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
            mapping = engine.mappings["AAPL"]
            groups = []
            calendar = mcal.get_calendar("XNYS")
            for month in range(1, 13):
                start = date(2024, month, 1)
                end = (
                    date(2025, 1, 1)
                    if month == 12
                    else date(2024, month + 1, 1)
                )
                schedule = calendar.schedule(
                    start_date=start,
                    end_date=end - timedelta(days=1),
                    tz="UTC",
                )
                session = pd.Timestamp(schedule.index[0]).date()
                table = normalize_provider_frame(
                    provider_frame("AAPL", [(session.isoformat(), 1)]),
                    mapping,
                )
                groups.append(
                    (
                        "MONTH",
                        start,
                        end,
                        stable_shard_key(mapping.instrument_id, 2),
                        table,
                        [],
                    )
                )
            engine.publish_dataset(contract, 2024, groups)
            yearly = engine.compact(
                contract,
                granularity="YEAR",
                period=date(2024, 6, 1),
            )
            self.assertEqual(yearly["status"], "SUCCEEDED")
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            latest = catalog.latest_available_manifest(
                feed_id=engine.feed_ids[RAW_FEED],
                data_layer="RAW",
                resolution="30m",
                year=2024,
            )
            objects = catalog.objects_for_manifest(latest["id"])
            self.assertEqual(
                {item["partition_granularity"] for item in objects},
                {"YEAR"},
            )
            lineage = [
                row
                for row in catalog.records(
                    "market_data.dataset_object_lineage"
                )
                if row["pipeline_run_id"] == yearly["pipeline_run_id"]
            ]
            self.assertEqual(len(lineage), 12)

    def test_successful_rerun_is_idempotently_reused(self):
        sessions = [("2024-11-29", 7)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            first = engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL"],
            )
            calls = len(engine.source.calls)
            second = engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("raw",),
                symbols=["AAPL"],
            )
            self.assertEqual(first["status"], "SUCCEEDED")
            self.assertTrue(second["reused"])
            self.assertEqual(len(engine.source.calls), calls)

    def test_adjusted_revision_two_preserves_revision_one_objects(self):
        sessions = [("2024-11-29", 7)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.make_engine(root, sessions, revision=1)
            first.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("adjusted",),
                symbols=["AAPL"],
            )
            second = self.make_engine(root, sessions, revision=2)
            second.source = RevisedAdjustedSource(sessions)
            result = second.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 11, 30, tzinfo=timezone.utc),
                price_types=("adjusted",),
                symbols=["AAPL"],
            )
            self.assertEqual(result["status"], "SUCCEEDED")
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            manifests = catalog.records("market_data.dataset_manifests")
            revisions = {
                (row["data_layer"], row["resolution"], row["revision_number"])
                for row in manifests
            }
            self.assertIn(("ADJUSTED", "30m", 1), revisions)
            self.assertIn(("ADJUSTED", "30m", 2), revisions)
            keys = [
                row["object_key"] for row in catalog.records("storage.objects")
            ]
            self.assertTrue(any("revision=1" in key for key in keys))
            self.assertTrue(any("revision=2" in key for key in keys))

    def test_incremental_detects_adjusted_change_and_rebuilds_retained_year(self):
        baseline_sessions = [
            ("2024-11-29", 7),
            ("2024-12-02", 13),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, baseline_sessions)
            engine.backfill(
                start=datetime(2024, 11, 29, tzinfo=timezone.utc),
                end=datetime(2024, 12, 3, tzinfo=timezone.utc),
                price_types=("adjusted",),
                resolutions=("30m",),
                symbols=["AAPL"],
            )
            engine.source = RevisedAdjustedSource(
                [
                    *baseline_sessions,
                    ("2024-12-03", 13),
                ]
            )
            result = engine.incremental(
                sessions=[date(2024, 12, 3)],
                price_types=("adjusted",),
                resolutions=("30m",),
                symbols=["AAPL"],
            )
            self.assertEqual(result["status"], "SUCCEEDED", result)
            self.assertTrue(result["adjustment_revision_detected"])
            self.assertEqual(
                result["adjustment_revision_backfill"]["status"],
                "SUCCEEDED",
            )
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            adjusted = [
                row
                for row in catalog.records("market_data.dataset_manifests")
                if row["data_layer"] == "ADJUSTED"
                and row["resolution"] == "30m"
            ]
            self.assertGreaterEqual(
                max(row["revision_number"] for row in adjusted),
                2,
            )
            self.assertEqual(
                validate_catalog(
                    catalog,
                    LocalObjectStore(root / "objects"),
                )["status"],
                "PASSED",
            )

    def test_plan_does_not_create_local_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            instrument_map = root / "map.csv"
            instrument_map.write_text(
                "provider_symbol,instrument_id\n"
                f"AAPL,{IDS['AAPL']}\n",
                encoding="utf-8",
            )
            local_root = root / "must-not-exist"
            args = build_parser().parse_args(
                [
                    "plan",
                    "--local-root",
                    str(local_root),
                    "--staging-root",
                    str(root / "staging"),
                    "--instrument-map",
                    str(instrument_map),
                    "--start-year",
                    "2024",
                    "--end-year",
                    "2024",
                ]
            )
            result = execute(args)
            self.assertEqual(result["mode"], "dry-run")
            self.assertFalse(local_root.exists())

    def test_legacy_migration_keeps_source_and_publishes_year_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, [])
            legacy_root = (
                root
                / "legacy"
                / "sip_market_data"
                / "raw"
                / "parquet"
            )
            legacy_root.mkdir(parents=True)
            source_path = legacy_root / "AAPL_30min_sip_historical.parquet"
            provider_frame(
                "AAPL",
                [("2024-11-29", 7)],
            ).to_parquet(source_path)
            result = engine.migrate_legacy(
                input_root=root / "legacy",
                start_year=2024,
                end_year=2024,
                price_types=("raw",),
                resolutions=("30m",),
            )
            self.assertEqual(result["status"], "SUCCEEDED", result)
            self.assertTrue(source_path.is_file())
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            objects = catalog.records("market_data.dataset_objects")
            self.assertTrue(objects)
            self.assertEqual(
                {row["partition_granularity"] for row in objects},
                {"YEAR"},
            )

    def test_small_ten_year_backfill_creates_one_manifest_per_year(self):
        sessions = [(f"{year}-06-15", 1) for year in range(2016, 2026)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, sessions)
            result = engine.backfill(
                start=datetime(2016, 1, 1, 5, tzinfo=timezone.utc),
                end=datetime(2026, 1, 1, 5, tzinfo=timezone.utc),
                price_types=("raw",),
                resolutions=("30m",),
                symbols=["AAPL"],
            )
            self.assertEqual(result["status"], "SUCCEEDED", result)
            self.assertEqual(result["manifest_count"], 10)
            catalog = LocalCatalog(root / "objects" / "catalog-export")
            partitions = {
                (
                    row["partition_start"],
                    row["partition_end"],
                )
                for row in catalog.records("market_data.dataset_objects")
            }
            self.assertEqual(len(partitions), 10)


if __name__ == "__main__":
    unittest.main()
