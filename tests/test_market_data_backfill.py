from __future__ import annotations

import csv
import json
import tempfile
import unittest
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_data_backfill.core import (
    DATASET_SPECS,
    InstrumentMapping,
    aggregate_regular_30m,
    bar_schema,
    canonical_dataset_hash,
    et_year_bounds_utc,
    normalize_legacy_frame,
    quality_issues,
    stable_shard_number,
)
from market_data_backfill.pipeline import (
    BackfillConfig,
    _write_shard_parts,
    transform,
)
from market_data_backfill.remote import apply_database_plan
from market_data_backfill.validation import read_jsonl, validate_output


INSTRUMENT_ID = "11111111-1111-4111-8111-111111111111"


def source_frame(
    timestamps: list[pd.Timestamp],
    *,
    source_minutes: list[int] | None = None,
) -> pd.DataFrame:
    count = len(timestamps)
    frame = pd.DataFrame(
        {
            "symbol": ["AAPL"] * count,
            "timestamp": timestamps,
            "open": [100.0 + index for index in range(count)],
            "high": [101.0 + index for index in range(count)],
            "low": [99.0 + index for index in range(count)],
            "close": [100.5 + index for index in range(count)],
            "volume": [10 + index for index in range(count)],
            "trade_count": pd.Series([1] * count, dtype="Int64"),
            "vwap": [100.25 + index for index in range(count)],
        }
    ).set_index(["symbol", "timestamp"])
    if source_minutes is not None:
        frame["source_minutes"] = source_minutes
    return frame


def write_test_inputs(root: Path, *, include_unmapped: bool = False) -> Path:
    regular_starts = list(
        pd.date_range(
            "2024-11-29T14:30:00Z",
            "2024-11-29T18:00:00Z",
            freq="30min",
            inclusive="left",
        )
    )
    frames = {
        ("ADJUSTED", "30m"): source_frame(regular_starts),
        ("DERIVED", "30m"): source_frame(regular_starts),
        ("DERIVED", "1h"): source_frame(
            regular_starts[::2],
            source_minutes=[60, 60, 60, 30],
        ),
        ("DERIVED", "4h"): source_frame(
            [regular_starts[0]],
            source_minutes=[210],
        ),
        ("DERIVED", "1d"): source_frame(
            [regular_starts[0]],
            source_minutes=[210],
        ),
    }
    for key, frame in frames.items():
        spec = DATASET_SPECS[key]
        directory = root / spec.source_relative_path
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / f"AAPL{spec.filename_marker}.parquet")
        if include_unmapped and key == ("ADJUSTED", "30m"):
            frame.to_parquet(directory / f"ZZZZ{spec.filename_marker}.parquet")
    mapping_path = root / "instrument_map.csv"
    with mapping_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "provider_symbol",
                "instrument_id",
                "provider_reference",
                "asset_type",
                "primary_exchange_mic",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "provider_symbol": "AAPL",
                "instrument_id": INSTRUMENT_ID,
                "provider_reference": "test-only",
                "asset_type": "STOCK",
                "primary_exchange_mic": "XNAS",
            }
        )
    return mapping_path


def config(
    input_root: Path,
    output_root: Path,
    mapping_path: Path,
    *,
    specs: tuple = tuple(DATASET_SPECS.values()),
    revision: int = 1,
    resume: bool = False,
    target_size_mib: int = 1,
    max_size_mib: int = 2,
) -> BackfillConfig:
    return BackfillConfig(
        input_root=input_root,
        output_root=output_root,
        instrument_map_path=mapping_path,
        start_year=2024,
        end_year=2024,
        specs=specs,
        shard_count=2,
        target_size_mib=target_size_mib,
        max_size_mib=max_size_mib,
        revision=revision,
        resume=resume,
    )


class MarketDataBackfillCoreTests(unittest.TestCase):
    def test_shard_depends_on_instrument_id_not_symbol(self) -> None:
        original = InstrumentMapping("OLD", INSTRUMENT_ID)
        renamed = InstrumentMapping("NEW", INSTRUMENT_ID)
        self.assertNotEqual(original.provider_symbol, renamed.provider_symbol)
        self.assertEqual(
            stable_shard_number(original.instrument_id, 16),
            stable_shard_number(renamed.instrument_id, 16),
        )

    def test_et_year_boundary_and_dst_are_timezone_aware(self) -> None:
        start, end = et_year_bounds_utc(2024)
        self.assertEqual(start.isoformat(), "2024-01-01T05:00:00+00:00")
        self.assertEqual(end.isoformat(), "2025-01-01T05:00:00+00:00")
        mapping = InstrumentMapping("AAPL", INSTRUMENT_ID)
        frame = source_frame(
            [
                pd.Timestamp("2024-03-08T14:30:00Z"),
                pd.Timestamp("2024-03-11T13:30:00Z"),
            ]
        )
        table = normalize_legacy_frame(
            frame,
            mapping,
            DATASET_SPECS[("DERIVED", "30m")],
            2024,
        )
        self.assertEqual(
            table.column("session_date_et").to_pylist(),
            [pd.Timestamp("2024-03-08").date(), pd.Timestamp("2024-03-11").date()],
        )

    def test_year_filter_prevents_cross_year_files(self) -> None:
        mapping = InstrumentMapping("AAPL", INSTRUMENT_ID)
        frame = source_frame(
            [
                pd.Timestamp("2023-12-29T14:30:00Z"),
                pd.Timestamp("2024-01-02T14:30:00Z"),
            ]
        )
        table = normalize_legacy_frame(
            frame,
            mapping,
            DATASET_SPECS[("ADJUSTED", "30m")],
            2024,
        )
        self.assertEqual(table.num_rows, 1)
        self.assertEqual(
            table.column("session_date_et")[0].as_py().year,
            2024,
        )

    def test_early_close_and_duplicate_detection(self) -> None:
        mapping = InstrumentMapping("AAPL", INSTRUMENT_ID)
        starts = list(
            pd.date_range(
                "2024-11-29T14:30:00Z",
                "2024-11-29T18:00:00Z",
                freq="30min",
                inclusive="left",
            )
        )
        table = normalize_legacy_frame(
            source_frame(starts),
            mapping,
            DATASET_SPECS[("DERIVED", "30m")],
            2024,
        )
        self.assertEqual(quality_issues(
            table, DATASET_SPECS[("DERIVED", "30m")], 2024
        ), [])
        duplicate = pa.concat_tables([table, table.slice(0, 1)])
        codes = {
            issue["code"]
            for issue in quality_issues(
                duplicate,
                DATASET_SPECS[("DERIVED", "30m")],
                2024,
            )
        }
        self.assertIn("DUPLICATE_BAR", codes)

    def test_null_optional_values_are_preserved(self) -> None:
        mapping = InstrumentMapping("AAPL", INSTRUMENT_ID)
        frame = source_frame([pd.Timestamp("2024-01-02T14:30:00Z")])
        frame["trade_count"] = pd.Series(
            [pd.NA],
            index=frame.index,
            dtype="Int64",
        )
        frame["vwap"] = float("nan")
        table = normalize_legacy_frame(
            frame,
            mapping,
            DATASET_SPECS[("ADJUSTED", "30m")],
            2024,
        )
        self.assertIsNone(table.column("trade_count")[0].as_py())
        self.assertIsNone(table.column("vwap")[0].as_py())

    def test_aggregation_uses_observed_bars_and_weighted_vwap(self) -> None:
        rows = [
            {
                "instrument_id": INSTRUMENT_ID,
                "provider_symbol": "AAPL",
                "bar_start_at": timestamp,
                "session_date_et": pd.Timestamp("2024-01-02").date(),
                "open": 10.0 + index,
                "high": 11.0 + index,
                "low": 9.0 + index,
                "close": 10.5 + index,
                "volume": 10 * (index + 1),
                "trade_count": None if index == 1 else index + 1,
                "vwap": None if index == 2 else 10.25 + index,
            }
            for index, timestamp in enumerate(
                [
                    pd.Timestamp("2024-01-02T14:30:00Z"),
                    pd.Timestamp("2024-01-02T15:00:00Z"),
                    # 15:30 is intentionally missing.
                    pd.Timestamp("2024-01-02T16:00:00Z"),
                ]
            )
        ]
        table = pa.Table.from_pylist(rows, schema=bar_schema(False))
        one_hour = aggregate_regular_30m(table, "1h").to_pandas()
        self.assertEqual(one_hour["source_bars"].tolist(), [2, 1])
        self.assertEqual(len(one_hour), 2)
        self.assertAlmostEqual(
            one_hour.iloc[0]["vwap"],
            (10.25 * 10 + 11.25 * 20) / 30,
        )
        self.assertEqual(one_hour.iloc[0]["trade_count"], 1)
        four_hour = aggregate_regular_30m(table, "4h").to_pandas()
        daily = aggregate_regular_30m(table, "1d").to_pandas()
        for aggregated in (four_hour, daily):
            self.assertEqual(len(aggregated), 1)
            self.assertEqual(aggregated.iloc[0]["source_bars"], 3)
            self.assertEqual(aggregated.iloc[0]["open"], 10.0)
            self.assertEqual(aggregated.iloc[0]["close"], 12.5)
            self.assertEqual(aggregated.iloc[0]["volume"], 60)

    def test_dataset_hash_ignores_paths_and_ids(self) -> None:
        canonical = {
            "content_hash": "a" * 64,
            "partition_granularity": "YEAR",
            "partition_start": "2024-01-01",
            "partition_end": "2025-01-01",
            "period_start": "2024-01-02T14:30:00+00:00",
            "period_end": "2024-01-02T15:00:00+00:00",
            "shard_key": "s00-of-02",
            "part_number": 1,
            "row_count": 1,
            "schema_version": "market-bars-v1",
        }
        left = {**canonical, "object_id": str(uuid.uuid4()), "path": "left"}
        right = {**canonical, "object_id": str(uuid.uuid4()), "path": "right"}
        self.assertEqual(
            canonical_dataset_hash([left]),
            canonical_dataset_hash([right]),
        )


class MarketDataBackfillEndToEndTests(unittest.TestCase):
    def test_max_size_creates_non_overlapping_parts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mapping_path = root / "instrument_map.csv"
            mapping_path.write_text(
                "provider_symbol,instrument_id\nAAPL," + INSTRUMENT_ID + "\n",
                encoding="utf-8",
            )
            timestamps = pd.date_range(
                "2024-01-02T14:30:00Z",
                periods=20_000,
                freq="30min",
            )
            frame = pd.DataFrame(
                {
                    "instrument_id": [INSTRUMENT_ID] * len(timestamps),
                    "provider_symbol": ["AAPL"] * len(timestamps),
                    "bar_start_at": timestamps,
                    "session_date_et": timestamps.tz_convert(
                        "America/New_York"
                    ).date,
                    "open": [100.0] * len(timestamps),
                    "high": [101.0] * len(timestamps),
                    "low": [99.0] * len(timestamps),
                    "close": [100.5] * len(timestamps),
                    "volume": [10] * len(timestamps),
                    "trade_count": pd.Series(
                        [1] * len(timestamps),
                        dtype="Int64",
                    ),
                    "vwap": [100.25] * len(timestamps),
                }
            )
            table = pa.Table.from_pandas(
                frame,
                schema=bar_schema(False),
                preserve_index=False,
            ).replace_schema_metadata(bar_schema(False).metadata)
            backfill_config = config(
                root,
                root / "output",
                mapping_path,
                specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                target_size_mib=1,
                max_size_mib=1,
            )
            artifacts = _write_shard_parts(
                table,
                backfill_config,
                DATASET_SPECS[("ADJUSTED", "30m")],
                2024,
                "22222222-2222-4222-8222-222222222222",
                root / "output" / "parts",
                0,
            )
            self.assertGreater(len(artifacts), 1)
            self.assertTrue(
                all(artifact.byte_size <= 1024 * 1024 for artifact in artifacts)
            )
            ordered = sorted(artifacts, key=lambda item: item.period_start)
            self.assertTrue(
                all(
                    left.period_end <= right.period_start
                    for left, right in zip(ordered, ordered[1:])
                )
            )

    def test_end_to_end_is_idempotent_and_dbml_aligned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_root = root / "input"
            output_root = root / "output"
            mapping = write_test_inputs(input_root)
            first, _ = transform(config(input_root, output_root, mapping))
            second, _ = transform(config(input_root, output_root, mapping))
            self.assertEqual(
                [result.manifest["dataset_hash"] for result in first],
                [result.manifest["dataset_hash"] for result in second],
            )
            self.assertTrue(all(result.available for result in first))
            report = validate_output(output_root)
            self.assertEqual(report["status"], "PASSED")
            manifests = read_jsonl(
                output_root / "load-plan" / "dataset-manifests.jsonl"
            )
            self.assertEqual(len(manifests), 5)
            self.assertTrue(all(row["instrument_id"] is None for row in manifests))
            self.assertEqual(
                {row["resolution"] for row in manifests},
                {"30m", "1h", "4h", "1d"},
            )
            contract = Path(__file__).parent / "fixtures" / "market-data-schema.dbml"
            db_plan = apply_database_plan(output_root, contract)
            self.assertEqual(db_plan["mode"], "dry-run")

    def test_partial_failure_preserves_successes_and_quarantines_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_root = root / "input"
            output_root = root / "output"
            mapping = write_test_inputs(input_root, include_unmapped=True)
            results, _ = transform(
                config(
                    input_root,
                    output_root,
                    mapping,
                    specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                )
            )
            self.assertEqual(results[0].manifest["status"], "QUARANTINED")
            self.assertGreater(results[0].manifest["row_count"], 0)
            self.assertTrue(results[0].objects)
            self.assertFalse(results[0].available)

    def test_revision_two_does_not_overwrite_revision_one(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_root = root / "input"
            output_root = root / "output"
            mapping = write_test_inputs(input_root)
            revision_one, _ = transform(
                config(
                    input_root,
                    output_root,
                    mapping,
                    specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                )
            )
            revision_two, _ = transform(
                config(
                    input_root,
                    output_root,
                    mapping,
                    specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                    revision=2,
                )
            )
            self.assertNotEqual(
                revision_one[0].manifest["dataset_manifest_id"],
                revision_two[0].manifest["dataset_manifest_id"],
            )
            for result in (revision_one[0], revision_two[0]):
                self.assertTrue(
                    all(Path(artifact.local_path).is_file() for artifact in result.objects)
                )
            manifests = read_jsonl(
                output_root / "load-plan" / "dataset-manifests.jsonl"
            )
            self.assertIsNotNone(manifests[0]["supersedes_manifest_id"])

    def test_resume_reuses_completed_shard_after_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_root = root / "input"
            output_root = root / "output"
            mapping = write_test_inputs(input_root)
            results, _ = transform(
                config(
                    input_root,
                    output_root,
                    mapping,
                    specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                )
            )
            artifact = results[0].objects[0]
            original_hash = artifact.content_hash
            original_mtime = Path(artifact.local_path).stat().st_mtime_ns
            manifest_path = Path(artifact.local_path).parents[1] / "manifest.json"
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload["status"] = "BUILDING"
            manifest_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            resumed, _ = transform(
                config(
                    input_root,
                    output_root,
                    mapping,
                    specs=(DATASET_SPECS[("ADJUSTED", "30m")],),
                    resume=True,
                )
            )
            self.assertTrue(resumed[0].available)
            self.assertEqual(resumed[0].objects[0].content_hash, original_hash)
            self.assertEqual(
                Path(resumed[0].objects[0].local_path).stat().st_mtime_ns,
                original_mtime,
            )


if __name__ == "__main__":
    unittest.main()
