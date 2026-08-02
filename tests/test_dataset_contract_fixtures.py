from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from d_storage_testkit import write_small_parquet
from market_pipeline_lib.dataset_fixtures import (
    DatasetFixtureError,
    validate_dataset_fixture,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "datasets" / "com-d1-v1"


class DatasetContractFixtureTests(unittest.TestCase):
    def test_raw_adjusted_manifests_bind_objects_ranges_and_lineage(self) -> None:
        fixture = validate_dataset_fixture(FIXTURE_ROOT / "fixture.json")

        manifests = {item["name"]: item for item in fixture["manifests"]}
        self.assertEqual(
            {item["data_layer"] for item in manifests.values()},
            {"RAW", "ADJUSTED"},
        )
        self.assertEqual(
            manifests["adjusted-v2"]["supersedes_manifest_id"],
            manifests["adjusted-v1"]["manifest_id"],
        )
        self.assertEqual(
            fixture["lineage"],
            [
                {
                    "derived_manifest_id": manifests["adjusted-v2"]["manifest_id"],
                    "source_manifest_id": manifests["adjusted-v1"]["manifest_id"],
                    "relation_type": "SUPERSEDES",
                }
            ],
        )

    def test_committed_parquet_objects_are_deterministic(self) -> None:
        cases = (
            ("raw.parquet", 0.0),
            ("adjusted-v1.parquet", 10.0),
            ("adjusted-v2.parquet", 20.0),
        )
        with tempfile.TemporaryDirectory() as temporary:
            for filename, price_offset in cases:
                generated = write_small_parquet(
                    Path(temporary) / filename,
                    price_offset=price_offset,
                )
                committed = FIXTURE_ROOT / "objects" / filename
                self.assertEqual(generated.read_bytes(), committed.read_bytes())

    def test_missing_object_failure_fixture_is_rejected(self) -> None:
        with self.assertRaisesRegex(DatasetFixtureError, "object missing"):
            validate_dataset_fixture(FIXTURE_ROOT / "failures" / "missing-object.json")

    def test_one_byte_checksum_failure_fixture_is_rejected(self) -> None:
        with self.assertRaisesRegex(DatasetFixtureError, "sha256 mismatch"):
            validate_dataset_fixture(
                FIXTURE_ROOT / "failures" / "checksum-mismatch.json"
            )


if __name__ == "__main__":
    unittest.main()
