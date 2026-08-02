import contextlib
import shutil
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from market_pipeline_lib.catalog import CatalogCapability, LocalCatalog
from market_pipeline_lib.cli import build_parser, execute
from market_pipeline_lib.contracts import (
    ADJUSTED_FEED,
    DATASET_CONTRACTS,
    RAW_FEED,
    InstrumentMapping,
    bar_schema,
    canonical_dataset_hash,
    logical_dataset_id,
    object_key,
    partition_bounds,
    stable_shard_key,
)
from market_pipeline_lib.db.errors import RuntimeDdlForbidden, SchemaWriteForbidden
from market_pipeline_lib.db.tables import METADATA, instruments as instruments_table
from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.operations import (
    RightsAttestation,
    apply_catalog_to_postgres,
    export_db_plan,
    validate_catalog,
)
from market_pipeline_lib.processing import (
    derive_regular_bars,
    normalize_provider_frame,
)
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore
from pipeline_state import PipelineStateStore
from d_storage_testkit import FakeS3Client


IDS = {
    "AAPL": "11111111-1111-4111-8111-111111111111",
    "MSFT": "22222222-2222-4222-8222-222222222222",
    "BRK.B": "33333333-3333-4333-8333-333333333333",
}


@contextlib.contextmanager
def temporary_root():
    """A temp directory that can also be removed when it holds deep object keys.

    Canonical object keys nest ten `key=value` directories, so a published
    tree under the system temp directory exceeds the Windows MAX_PATH limit.
    `shutil.rmtree` cannot walk such a tree, and `tempfile.TemporaryDirectory`
    therefore raises `[WinError 145]` on exit even when the test itself passed.
    Cleanup goes through the extended-length form instead.
    """
    path = tempfile.mkdtemp()
    try:
        yield path
    finally:
        shutil.rmtree(long_path(path), ignore_errors=True)


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
        """Pinned against the canonical schema, not against itself.

        The previous body compared ``bar_schema(True).field("source_minutes").type``
        with itself, which no implementation can fail.  Spec section 4 lists it as
        an assertion that cannot fail; these are the facts it was meant to hold.
        """
        native = bar_schema(False)
        derived = bar_schema(True)
        self.assertNotIn("source_bars", derived.names)
        self.assertNotIn("source_minutes", native.names)
        self.assertEqual(
            derived.names,
            [*native.names, "source_minutes"],
        )
        field = derived.field("source_minutes")
        self.assertEqual(field.type, pa.int16())
        self.assertFalse(field.nullable)
        self.assertEqual(
            derived.metadata[b"schema_version"],
            b"market-bars-v2",
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
        """The shard follows the instrument, and the value is pinned.

        The previous body compared ``stable_shard_key(x)`` with
        ``stable_shard_key(str(uuid.UUID(x)))`` where ``x`` was already the
        canonical UUID text, so both sides were the same call on the same input
        and no implementation could fail it.  Spec section 4 lists it as an
        assertion that cannot fail.

        What the shard contract actually promises: a ticker rename must not move
        an instrument between shards, because the shard is derived from the
        immutable ``instrument_id`` and never from the provider symbol.
        """
        identifier = IDS["BRK.B"]
        # A renamed ticker maps to the same instrument, hence the same shard.
        before = InstrumentMapping(provider_symbol="BRK.B", instrument_id=identifier)
        after = InstrumentMapping(provider_symbol="BRKB", instrument_id=identifier)
        self.assertNotEqual(before.provider_symbol, after.provider_symbol)
        self.assertEqual(
            stable_shard_key(before.instrument_id, 16),
            stable_shard_key(after.instrument_id, 16),
        )
        # Pinned literals: a change to the hashing rule must fail here, and the
        # shard must not silently collapse to a single bucket.
        self.assertEqual(stable_shard_key(IDS["BRK.B"], 16), "s15-of-16")
        self.assertEqual(stable_shard_key(IDS["AAPL"], 16), "s04-of-16")
        self.assertEqual(stable_shard_key(IDS["MSFT"], 16), "s11-of-16")
        # Accepted in any casing/format PostgreSQL may hand back.
        self.assertEqual(
            stable_shard_key(identifier.upper(), 16),
            "s15-of-16",
        )
        self.assertEqual(
            stable_shard_key(identifier.replace("-", ""), 16),
            "s15-of-16",
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
            root = Path(temporary)
            engine = self.make_engine(root, [], dry_run=True)
            result = engine.compact(
                DATASET_CONTRACTS[("raw", "RAW", "30m")],
                granularity="WEEK",
                period=date(2025, 1, 30),
            )
            self.assertEqual(result["status"], "SKIPPED")

    def test_db_export_has_only_dbml_columns_and_local_bucket_is_named(self):
        """`storage.objects.bucket_name` is NOT NULL in the applied baseline.

        This test previously asserted ``bucket_name is None`` for locally
        published objects, which pinned a value the canonical schema rejects:
        every local publish would fail on insert the moment the pipeline was
        pointed at PostgreSQL.  `LocalObjectStore` now names its container, so
        the DBML column contract and the local path agree.
        """
        sessions = [("2024-11-29", 7)]
        with temporary_root() as temporary:
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
            self.assertTrue(all(row["bucket_name"] == "local" for row in storage))
            self.assertTrue(all(row["storage_provider"] == "LOCAL" for row in storage))

    def test_partition_checkpoint_uses_manifest_dimensions(self):
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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
        with temporary_root() as temporary:
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


# =======================================================================================
# The engine against a real database
#
# Spec section 1: "the canonical path does not work once a database is attached".  Every
# test below drives `MarketPipelineEngine` with a `PostgresCatalog` on a Testcontainers
# PostgreSQL 16 instance migrated with the central Flyway bundle.  They are the reason
# the `isinstance(self.catalog, LocalCatalog)` gates can be deleted: what the gates used
# to hide is now asserted here.
# =======================================================================================


ENGINE_SOURCE = Path(MarketPipelineEngine.__module__.replace(".", "/") + ".py")
ENGINE_PATH = Path(__file__).resolve().parents[1] / ENGINE_SOURCE


def write_instrument_map(root: Path) -> Path:
    path = root / "instrument_map.csv"
    path.write_text(
        "provider_symbol,instrument_id\n"
        + "".join(f"{symbol},{identifier}\n" for symbol, identifier in IDS.items()),
        encoding="utf-8",
    )
    return path


def build_engine(root: Path, catalog=None, *, sessions=(), revision=None, dry_run=False):
    """A `MarketPipelineEngine` bound to whichever catalog the caller supplies."""

    config = PipelineConfig(
        local_root=root / "objects",
        staging_root=root / "staging",
        instrument_map_path=write_instrument_map(root),
        shard_count=2,
        target_size_mib=1,
        max_size_mib=2,
        revision=revision,
        dry_run=dry_run,
    )
    return MarketPipelineEngine(config, catalog=catalog, source=FakeSource(list(sessions)))


def day_group(engine, session: str, *, symbol: str = "AAPL", bars: int = 13):
    """One `publish_dataset` group covering a single ET session."""

    mapping = engine.mappings[symbol]
    table = normalize_provider_frame(provider_frame(symbol, [(session, bars)]), mapping)
    start, end = partition_bounds(date.fromisoformat(session), "DAY")
    return (
        "DAY",
        start,
        end,
        stable_shard_key(mapping.instrument_id, 2),
        table,
        [],
    )


def test_engine_has_no_local_catalog_type_gates():
    """No behaviour may depend on which `MarketDataCatalog` implementation is in use.

    Spec section 1 names nine surviving gates; the file actually carried fourteen.
    The catalog boundary is a protocol, so a type test in the orchestrator is always a
    behaviour that `PostgresCatalog` silently does not get -- which is how the
    carry-forward data-loss bug survived.
    """

    source = ENGINE_PATH.read_text(encoding="utf-8")
    offenders = [
        f"{number}: {line.strip()}"
        for number, line in enumerate(source.splitlines(), 1)
        if "isinstance(self.catalog" in line
    ]

    assert offenders == []


# --------------------------------------------------------------------------------------
# The public run API
# --------------------------------------------------------------------------------------


def test_start_run_registers_a_run_and_makes_it_active():
    """Callers outside the engine must not have to reach for `_run_record`.

    `realtime_ingest` publishes datasets too, and used the private pair
    `_run_record` + `_active_run_id` because there was no public way in.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)

        run = engine.start_run("REALTIME_WARMUP", "sessions:2025-01-06")

        assert run["status"] == "RUNNING"
        assert run["pipeline_code"] == "REALTIME_WARMUP"
        assert run.get("_reused") is None
        assert engine.active_run_id == run["id"]
        stored = engine.catalog.pipeline_run(run["id"])
        assert stored is not None
        assert stored["idempotency_key"] == "sessions:2025-01-06"


def test_start_run_is_idempotent_for_a_completed_run():
    """A second call with the same key reuses the finished run instead of redoing it."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)
        first = engine.start_run("REALTIME_WARMUP", "sessions:2025-01-06")
        engine.catalog.finish_pipeline_run(first["id"], status="SUCCEEDED", output_hash="d" * 64)

        second = engine.start_run("REALTIME_WARMUP", "sessions:2025-01-06")

        assert second["id"] == first["id"]
        assert second["_reused"] is True
        assert second["status"] == "SUCCEEDED"
        assert engine.active_run_id == first["id"]


def test_active_run_id_is_refused_before_a_run_starts():
    with temporary_root() as temporary:
        engine = build_engine(Path(temporary))
        with pytest.raises(RuntimeError):
            engine.active_run_id  # noqa: B018 - the property access is the assertion


def test_start_run_ids_differ_per_code_and_key():
    """Deterministic, but not collapsed: two different jobs are two different runs."""

    with temporary_root() as temporary:
        engine = build_engine(Path(temporary))
        ids = {
            engine.start_run("REALTIME_WARMUP", "sessions:2025-01-06")["id"],
            engine.start_run("REALTIME_WARMUP", "sessions:2025-01-07")["id"],
            engine.start_run("MARKET_DATA_COMPACTION", "sessions:2025-01-06")["id"],
        }
        repeated = engine.start_run("REALTIME_WARMUP", "sessions:2025-01-06")["id"]

    assert len(ids) == 3
    assert repeated in ids


# --------------------------------------------------------------------------------------
# Carry-forward: the latent data-loss bug at the old engine.py:645
# --------------------------------------------------------------------------------------


def _publish_two_days(engine, contract):
    run = engine._run_record("TEST_DAY_LOAD", f"carry-forward:{contract.logical_code}")
    engine._active_run_id = run["id"]
    return engine.publish_dataset(
        contract,
        2025,
        [day_group(engine, "2025-01-06"), day_group(engine, "2025-01-07")],
    )


@pytest.mark.integration
def test_carry_forward_keeps_untouched_partitions_on_a_postgres_catalog(postgres_catalog):
    """Republishing one partition must not delete the partitions it did not touch.

    With the old `isinstance(self.catalog, LocalCatalog)` gate the carry-forward loop
    was skipped for every non-local catalog, so revision 2 was published with only the
    replaced partition and the 2025-01-06 object was silently dropped from the dataset
    while its bytes stayed in the object store, unreferenced.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]

        first = build_engine(root, postgres_catalog)
        first_result = _publish_two_days(first, contract)
        assert first_result["manifest"]["status"] == "AVAILABLE"
        assert first_result["new_object_count"] == 2

        second = build_engine(root, postgres_catalog)
        run = second._run_record("TEST_DAY_RELOAD", "carry-forward:second")
        second._active_run_id = run["id"]
        second_result = second.publish_dataset(
            contract,
            2025,
            [day_group(second, "2025-01-07", bars=7)],
            replace_periods=[partition_bounds(date(2025, 1, 7), "DAY")],
        )

        assert second_result["manifest"]["revision_number"] == 2
        assert second_result["new_object_count"] == 1
        assert second_result["retained_object_count"] == 1

        carried = postgres_catalog.objects_for_manifest(second_result["manifest"]["id"])
        assert sorted(item["partition_start"] for item in carried) == [
            "2025-01-06",
            "2025-01-07",
        ]
        # The carried-forward row points at the *original* storage object, not a copy.
        january_six = next(item for item in carried if item["partition_start"] == "2025-01-06")
        original = next(
            item
            for item in postgres_catalog.objects_for_manifest(first_result["manifest"]["id"])
            if item["partition_start"] == "2025-01-06"
        )
        assert january_six["object_id"] == original["object_id"]
        assert january_six["storage"]["content_hash"] == original["storage"]["content_hash"]


@pytest.mark.integration
def test_carry_forward_produces_the_same_dataset_hash_on_both_catalogs(postgres_catalog):
    """A dataset published identically through either catalog gets one identical hash.

    `publish_dataset` recomputes `dataset_hash` from the carried-forward
    `dataset_objects` rows, so any rendering difference between the two catalogs would
    fork the manifest identity of otherwise identical data.
    """

    contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
    digests = []
    for catalog_factory in (lambda root: None, lambda root: postgres_catalog):
        with temporary_root() as temporary:
            root = Path(temporary)
            engine = build_engine(root, catalog_factory(root))
            first = _publish_two_days(engine, contract)
            follow_up = build_engine(root, catalog_factory(root))
            run = follow_up._run_record("TEST_DAY_RELOAD", "hash-parity:second")
            follow_up._active_run_id = run["id"]
            second = follow_up.publish_dataset(
                contract,
                2025,
                [day_group(follow_up, "2025-01-07", bars=7)],
                replace_periods=[partition_bounds(date(2025, 1, 7), "DAY")],
            )
            digests.append((first["manifest"]["dataset_hash"], second["manifest"]["dataset_hash"]))

    assert digests[0] == digests[1]
    assert digests[0][0] != digests[0][1]


# --------------------------------------------------------------------------------------
# Atomicity
# --------------------------------------------------------------------------------------


@pytest.mark.integration
def test_publish_dataset_persists_nothing_when_it_fails_midway(postgres_catalog):
    """A publish that dies after staging objects must leave the catalog untouched.

    Without a single transaction around the catalog writes, the BUILDING manifest, the
    `storage.objects` rows and the `dataset_objects` rows are each committed by their
    own implicit unit of work and survive the failure, leaving a dataset that is
    permanently BUILDING and objects attached to it.
    """

    class Boom(RuntimeError):
        pass

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "atomicity")
        engine._active_run_id = run["id"]

        original = postgres_catalog.publish_manifest
        seen: list[str] = []

        def exploding(record):
            seen.append(record["status"])
            if record["status"] != "BUILDING":
                raise Boom("publish crashed after the objects were staged")
            return original(record)

        postgres_catalog.publish_manifest = exploding  # type: ignore[method-assign]
        try:
            with pytest.raises(Boom):
                engine.publish_dataset(contract, 2025, [day_group(engine, "2025-01-06")])
        finally:
            postgres_catalog.publish_manifest = original  # type: ignore[method-assign]

        # The BUILDING manifest was written inside the failed unit of work...
        assert seen[0] == "BUILDING"
        # ...and nothing survived it.
        assert postgres_catalog.records("market_data.dataset_manifests") == []
        assert postgres_catalog.records("market_data.dataset_objects") == []
        assert postgres_catalog.records("storage.objects") == []
        assert postgres_catalog.records("market_data.quality_incidents") == []
        # The pipeline run itself is not part of the publish unit of work.
        assert [row["id"] for row in postgres_catalog.records("market_data.pipeline_runs")] == [run["id"]]


@pytest.mark.integration
def test_publish_dataset_commits_every_row_on_success(postgres_catalog):
    """The committing half of the same guarantee, read back on a fresh connection."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        result = _publish_two_days(engine, contract)

    with postgres_catalog.engine.connect() as connection:
        counts = {
            name: connection.exec_driver_sql(f"SELECT count(*) FROM {name}").scalar_one()
            for name in (
                "market_data.providers",
                "market_data.feeds",
                "market_data.dataset_manifests",
                "market_data.dataset_objects",
                "storage.objects",
            )
        }

    assert counts == {
        "market_data.providers": 1,
        "market_data.feeds": 2,
        "market_data.dataset_manifests": 1,
        "market_data.dataset_objects": 2,
        "storage.objects": 2,
    }
    assert result["manifest"]["status"] == "AVAILABLE"


# --------------------------------------------------------------------------------------
# The whole canonical path, on a database
# --------------------------------------------------------------------------------------


@pytest.mark.integration
def test_backfill_runs_end_to_end_against_a_postgres_catalog(postgres_catalog):
    """Eight manifests, two feeds, real lineage -- all committed to PostgreSQL.

    This is the claim spec section 1 says is false today: the canonical pipeline path
    works with a database attached.
    """

    sessions = [("2024-11-29", 7), ("2024-12-02", 13)]
    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog, sessions=sessions)
        result = engine.backfill(
            start=datetime(2024, 11, 29, tzinfo=timezone.utc),
            end=datetime(2024, 12, 3, tzinfo=timezone.utc),
            price_types=("raw", "adjusted"),
            symbols=["AAPL"],
        )

        assert result["status"] == "SUCCEEDED", result
        assert result["manifest_count"] == 8
        assert result["available_manifests"] == 8

        manifests = postgres_catalog.records("market_data.dataset_manifests")
        assert len([row for row in manifests if row["status"] == "AVAILABLE"]) == 8
        assert {row["code"] for row in postgres_catalog.records("market_data.feeds")} == {
            RAW_FEED,
            ADJUSTED_FEED,
        }
        lineage = postgres_catalog.records("market_data.dataset_object_lineage")
        assert lineage
        assert all(row["source_dataset_object_id"] for row in lineage)
        runs = postgres_catalog.records("market_data.pipeline_runs")
        assert [row["status"] for row in runs] == ["SUCCEEDED"]
        assert runs[0]["output_hash"]
        # `market_data.pipeline_run_outputs` is not in the canonical DBML, so the
        # PostgreSQL catalog declares the gap instead of writing a sidecar.
        assert postgres_catalog.supports(CatalogCapability.PIPELINE_RUN_OUTPUTS) is False
        # `write_summary` is an operator artifact both catalogs produce.
        assert (postgres_catalog._artifact_root / "summary.json").is_file()


@pytest.mark.integration
def test_compaction_runs_from_a_postgres_catalog(postgres_catalog):
    """DP4's partition/compaction path depends on this: `compact` needs no LocalCatalog.

    The old gate raised ``Compaction 조회에는 조회 가능한 Catalog가 필요합니다`` for every
    non-local catalog, so compaction could never run against the real catalog.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "compaction-week")
        engine._active_run_id = run["id"]
        engine.publish_dataset(
            contract,
            2025,
            [day_group(engine, session) for session in ("2025-01-06", "2025-01-07", "2025-01-08", "2025-01-10")],
        )

        compacted = engine.compact(contract, granularity="WEEK", period=date(2025, 1, 8))

        assert compacted["status"] == "SUCCEEDED", compacted
        latest = postgres_catalog.latest_available_manifest(
            feed_id=engine.feed_ids[RAW_FEED],
            data_layer="RAW",
            resolution="30m",
            year=2025,
        )
        objects = postgres_catalog.objects_for_manifest(latest["id"])
        assert {item["partition_granularity"] for item in objects} == {"WEEK"}
        compacted_from = [
            row
            for row in postgres_catalog.records("market_data.dataset_object_lineage")
            if row["relation_type"] == "COMPACTED_FROM"
        ]
        assert len(compacted_from) == 4


@pytest.mark.integration
def test_incomplete_compaction_input_is_recorded_as_a_quality_incident(postgres_catalog):
    """The quarantine path also has to reach the database, not just a local JSON file."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "compaction-incomplete")
        engine._active_run_id = run["id"]
        engine.publish_dataset(
            contract,
            2025,
            [day_group(engine, "2025-01-06"), day_group(engine, "2025-01-07")],
        )

        result = engine.compact(contract, granularity="WEEK", period=date(2025, 1, 8))

    assert result["status"] == "QUARANTINED"
    # XNYS closed 2025-01-09 (national day of mourning), so it is not an expected
    # session and must not be reported as a gap.
    assert result["missing_sessions"] == ["2025-01-08", "2025-01-10"]
    incidents = postgres_catalog.records("market_data.quality_incidents")
    assert [row["incident_code"] for row in incidents] == ["COMPACTION_INPUT_INCOMPLETE"]
    assert incidents[0]["severity"] == "ERROR"
    assert incidents[0]["status"] == "ACTIVE"


# --------------------------------------------------------------------------------------
# D10 impact scope: validate before sorting, and record what was actually affected
# --------------------------------------------------------------------------------------


def reversed_day_group(engine, session: str, *, symbol: str = "AAPL", bars: int = 13):
    """A group whose rows are in descending time order, as a bad feed delivers them."""

    granularity, start, end, shard, table, sources = day_group(engine, session, symbol=symbol, bars=bars)
    descending = table.take(list(reversed(range(table.num_rows))))
    return granularity, start, end, shard, descending, sources


def test_out_of_order_bars_are_detected_before_the_table_is_sorted():
    """Sorting the input before validating it destroys the evidence of the defect.

    `_write_new_objects` used to call `sort_bar_table` on the line above
    `quality_issues`, so `OUT_OF_ORDER_BARS` could never fire in production no matter
    what a provider sent.  Spec section 1 records this as the D10 ordering defect.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "ordering")
        engine._active_run_id = run["id"]

        result = engine.publish_dataset(contract, 2025, [reversed_day_group(engine, "2025-01-06")])

        catalog = LocalCatalog(root / "objects" / "catalog-export")
        incidents = catalog.records("market_data.quality_incidents")
        out_of_order = [row for row in incidents if row["incident_code"] == "OUT_OF_ORDER_BARS"]

    assert out_of_order, [row["incident_code"] for row in incidents]
    # Out-of-order is a WARNING, so the objects are still published, sorted.
    assert result["manifest"]["status"] == "AVAILABLE"


def test_recorded_incidents_carry_the_impact_scope_not_the_whole_manifest():
    """`instrument_id`, and a period no wider than the bars that were actually wrong.

    The engine used to build the incident row by hand with ``instrument_id: None`` and
    the manifest's whole period, which is the useless scope the audit flagged: an
    operator could not tell which instrument or which bars to re-fetch.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "scope")
        engine._active_run_id = run["id"]

        result = engine.publish_dataset(contract, 2025, [reversed_day_group(engine, "2025-01-06")])

        catalog = LocalCatalog(root / "objects" / "catalog-export")
        incident = next(
            row
            for row in catalog.records("market_data.quality_incidents")
            if row["incident_code"] == "OUT_OF_ORDER_BARS"
        )

    assert incident["instrument_id"] == IDS["AAPL"]
    assert incident["dataset_manifest_id"] == result["manifest"]["id"]
    # The ET session 2025-01-06 opens 14:30Z; the affected pair is inside that session,
    # and strictly inside the manifest's whole-year period.
    assert incident["period_start"] >= "2025-01-06T14:30:00Z"
    assert incident["period_end"] <= "2025-01-07T05:00:00Z"
    assert incident["period_start"] > result["manifest"]["period_start"]
    assert incident["severity"] == "WARNING"
    assert incident["status"] == "ACTIVE"


def test_two_instruments_out_of_order_are_two_incidents_not_one():
    """The row id is salted with the scope, so one upsert cannot swallow the other."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "scope-two")
        engine._active_run_id = run["id"]
        groups = [
            reversed_day_group(engine, "2025-01-06", symbol="AAPL"),
            reversed_day_group(engine, "2025-01-06", symbol="MSFT"),
        ]

        engine.publish_dataset(contract, 2025, groups)

        catalog = LocalCatalog(root / "objects" / "catalog-export")
        rows = [
            row
            for row in catalog.records("market_data.quality_incidents")
            if row["incident_code"] == "OUT_OF_ORDER_BARS"
        ]

    assert {row["instrument_id"] for row in rows} == {IDS["AAPL"], IDS["MSFT"]}
    # Each inverted adjacent pair is its own row: the id is salted with the scope, so
    # thirteen descending bars per instrument are twelve incidents, not one.
    per_instrument = {
        identifier: sum(1 for row in rows if row["instrument_id"] == identifier)
        for identifier in (IDS["AAPL"], IDS["MSFT"])
    }
    assert per_instrument == {IDS["AAPL"]: 12, IDS["MSFT"]: 12}
    assert len({row["id"] for row in rows}) == 24
    # Every row names a distinct bar range; none was widened to the manifest period.
    assert len({(row["instrument_id"], row["period_start"]) for row in rows}) == 24


def test_compaction_incomplete_incident_is_scoped_to_the_compacted_partition():
    """Not the manifest year: the missing sessions are known and bounded."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root)
        contract = DATASET_CONTRACTS[("raw", "RAW", "30m")]
        run = engine._run_record("TEST_DAY_LOAD", "compaction-scope")
        engine._active_run_id = run["id"]
        engine.publish_dataset(
            contract,
            2025,
            [day_group(engine, "2025-01-06"), day_group(engine, "2025-01-07")],
        )

        result = engine.compact(contract, granularity="WEEK", period=date(2025, 1, 8))

        catalog = LocalCatalog(root / "objects" / "catalog-export")
        incident = next(
            row
            for row in catalog.records("market_data.quality_incidents")
            if row["incident_code"] == "COMPACTION_INPUT_INCOMPLETE"
        )

    assert result["status"] == "QUARANTINED"

    # ET week 2025-01-06 .. 2025-01-13, rendered in UTC.
    assert incident["period_start"] == "2025-01-06T05:00:00Z"
    assert incident["period_end"] == "2025-01-13T05:00:00Z"
    assert incident["instrument_id"] is None
    assert incident["severity"] == "ERROR"


def test_checksum_failure_reaches_the_quality_incidents_table():
    """D10: a `CONTENT_HASH_MISMATCH` must be a catalog row, not only a local JSON file.

    `validate_catalog` used to append the finding to `validation-report.json` and stop,
    and the candidate builder then dropped it because the finding carried no
    `manifest_id`.  The object is the evidence, so it belongs in `evidence_object_id`.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, sessions=[("2024-11-29", 7)])
        engine.backfill(
            start=datetime(2024, 11, 29, tzinfo=timezone.utc),
            end=datetime(2024, 11, 30, tzinfo=timezone.utc),
            price_types=("raw",),
            resolutions=("30m",),
            symbols=["AAPL"],
        )
        catalog = LocalCatalog(root / "objects" / "catalog-export")
        store = LocalObjectStore(root / "objects")
        corrupted = catalog.records("storage.objects")[0]
        store.path_for(corrupted["object_key"]).write_bytes(b"not a parquet file")

        report = validate_catalog(catalog, store)

        assert report["status"] == "FAILED"
        incidents = catalog.records("market_data.quality_incidents")

    mismatches = [row for row in incidents if row["incident_code"] == "CONTENT_HASH_MISMATCH"]
    assert len(mismatches) == 1
    assert mismatches[0]["evidence_object_id"] == corrupted["id"]
    assert mismatches[0]["severity"] == "ERROR"
    assert mismatches[0]["dataset_manifest_id"] is not None
    assert mismatches[0]["status"] == "ACTIVE"


@pytest.mark.integration
def test_validate_catalog_runs_against_a_postgres_catalog(postgres_catalog, admin_engine):
    """D07/D08 need validation against the real catalog, not only the JSONL one.

    The `catalog: LocalCatalog` annotation was a type gate in disguise: it named the
    one implementation the function was allowed to see.
    """

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, postgres_catalog, sessions=[("2024-11-29", 7)])
        result = engine.backfill(
            start=datetime(2024, 11, 29, tzinfo=timezone.utc),
            end=datetime(2024, 11, 30, tzinfo=timezone.utc),
            price_types=("raw",),
            resolutions=("30m",),
            symbols=["AAPL"],
        )
        assert result["status"] == "SUCCEEDED", result
        store = LocalObjectStore(root / "objects")

        clean = validate_catalog(postgres_catalog, store, write_report=False)
        assert clean["status"] == "PASSED", clean

        # `quality_incidents.instrument_id` is a foreign key, so a per-instrument
        # incident needs its instrument row; DP3/D04 owns populating that table.
        with admin_engine.begin() as connection:
            connection.execute(
                instruments_table.insert(),
                [
                    {
                        "id": uuid.UUID(IDS["AAPL"]),
                        "asset_type": "STOCK",
                        "primary_exchange_mic": "XNAS",
                        "currency_code": "USD",
                        "provider_reference": None,
                        "listed_at": None,
                        "delisted_at": None,
                        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    }
                ],
            )
        corrupted = postgres_catalog.records("storage.objects")[0]
        store.path_for(corrupted["object_key"]).write_bytes(b"not a parquet file")

        failed = validate_catalog(postgres_catalog, store, write_report=False)

    assert failed["status"] == "FAILED"
    incidents = postgres_catalog.records("market_data.quality_incidents")
    assert [row["incident_code"] for row in incidents] == ["CONTENT_HASH_MISMATCH"]
    assert incidents[0]["evidence_object_id"] == corrupted["id"]


# --------------------------------------------------------------------------------------
# The catalog-to-PostgreSQL apply path (D09)
# --------------------------------------------------------------------------------------


ATTESTATION = RightsAttestation(
    provider_code="ALPACA",
    rights_version="alpaca-sip-2026-01",
    status="ACTIVE",
    approved_by="integration-test",
    approved_at="2026-01-02T00:00:00Z",
    evidence_uri="https://example.invalid/evidence/alpaca-sip-2026-01",
)


@pytest.mark.integration
def test_apply_catalog_to_postgres_writes_the_local_catalog_into_the_database(
    postgres_catalog,
    postgres_url,
    admin_engine,
):
    """A local run, then `apply-db --execute`, then the rows are really in PostgreSQL."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, sessions=[("2024-11-29", 7)])
        engine.backfill(
            start=datetime(2024, 11, 29, tzinfo=timezone.utc),
            end=datetime(2024, 11, 30, tzinfo=timezone.utc),
            price_types=("raw",),
            resolutions=("30m",),
            symbols=["AAPL"],
        )
        catalog = LocalCatalog(root / "objects" / "catalog-export")
        store = LocalObjectStore(root / "objects")

        # `dataset_objects` rows reference instruments that must already exist.
        with admin_engine.begin() as connection:
            connection.execute(
                instruments_table.insert(),
                [
                    {
                        "id": uuid.UUID(IDS["AAPL"]),
                        "asset_type": "STOCK",
                        "primary_exchange_mic": "XNAS",
                        "currency_code": "USD",
                        "provider_reference": None,
                        "listed_at": None,
                        "delisted_at": None,
                        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    }
                ],
            )

        dbml = Path("tests/fixtures/market-data-schema.dbml").resolve()
        blocked = apply_catalog_to_postgres(catalog, store, dbml_path=dbml, execute=False)
        assert blocked["status"] == "PASSED"
        assert blocked["rights_review_required_providers"] == ["ALPACA"]

        applied = apply_catalog_to_postgres(
            catalog,
            store,
            dbml_path=dbml,
            execute=True,
            database_url=postgres_url,
            rights_attestations={"ALPACA": ATTESTATION},
        )

    assert applied["status"] == "APPLIED"
    assert applied["rights_attested_providers"] == ["ALPACA"]
    providers = postgres_catalog.records("market_data.providers")
    assert [row["rights_version"] for row in providers] == ["alpaca-sip-2026-01"]
    assert [row["status"] for row in providers] == ["ACTIVE"]
    assert postgres_catalog.records("market_data.dataset_manifests")
    assert postgres_catalog.records("storage.objects")
    assert postgres_catalog.records("market_data.dataset_objects")


@pytest.mark.integration
def test_apply_refuses_objects_whose_instruments_are_absent(postgres_catalog, postgres_url):
    """The apply must not create dangling references it cannot check later."""

    with temporary_root() as temporary:
        root = Path(temporary)
        engine = build_engine(root, sessions=[("2024-11-29", 7)])
        engine.backfill(
            start=datetime(2024, 11, 29, tzinfo=timezone.utc),
            end=datetime(2024, 11, 30, tzinfo=timezone.utc),
            price_types=("raw",),
            resolutions=("30m",),
            symbols=["AAPL"],
        )
        catalog = LocalCatalog(root / "objects" / "catalog-export")
        store = LocalObjectStore(root / "objects")

        with pytest.raises(RuntimeError, match="instrument_id"):
            apply_catalog_to_postgres(
                catalog,
                store,
                dbml_path=Path("tests/fixtures/market-data-schema.dbml").resolve(),
                execute=True,
                database_url=postgres_url,
                rights_attestations={"ALPACA": ATTESTATION},
            )

    assert postgres_catalog.records("market_data.providers") == []
    assert postgres_catalog.records("storage.objects") == []


# --------------------------------------------------------------------------------------
# The runtime never issues DDL
# --------------------------------------------------------------------------------------


@pytest.mark.integration
def test_the_pipeline_runtime_cannot_create_its_own_schema(postgres_catalog, admin_engine):
    """DDL through a pipeline engine is refused, not silently obeyed.

    Migration execution belongs to the central Flyway bundle; a runtime that can create
    tables can invent a schema that then diverges from the canonical one.  `checkfirst`
    is off deliberately: with it on SQLAlchemy reflects first, finds the central bundle
    already applied, and emits nothing, so the guard would never be reached.
    """

    with pytest.raises(RuntimeDdlForbidden):
        METADATA.create_all(postgres_catalog.engine, checkfirst=False)

    with pytest.raises(RuntimeDdlForbidden):
        with postgres_catalog.engine.begin() as connection:
            connection.execute(text("CREATE TABLE market_data.invented_by_runtime (id uuid)"))

    with admin_engine.connect() as connection:
        exists = connection.execute(
            text("SELECT to_regclass('market_data.invented_by_runtime') IS NOT NULL")
        ).scalar_one()
    assert exists is False


@pytest.mark.integration
def test_the_pipeline_runtime_cannot_write_a_schema_it_does_not_own(postgres_catalog):
    """`identity` is read-only to this repository; the guard enforces it on the wire."""

    with pytest.raises(SchemaWriteForbidden):
        with postgres_catalog.engine.begin() as connection:
            connection.execute(text("UPDATE identity.accounts SET status = 'ACTIVE'"))


if __name__ == "__main__":
    unittest.main()
