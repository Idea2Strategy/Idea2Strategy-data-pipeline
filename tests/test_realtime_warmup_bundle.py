from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import pyarrow.parquet as pq

from market_pipeline_lib.realtime_warmup import (
    FeatureRequirement,
    RealtimeWarmupError,
    publish_realtime_warmup_bundle,
    verify_realtime_warmup_bundle,
)


FIXTURE = Path(__file__).parent / "fixtures" / "d90" / "provider-neutral-market-events.json"
INSTRUMENT_ID = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"


class RealtimeWarmupBundleTest(unittest.TestCase):
    def test_publishes_deterministic_daily_parquet_manifest_and_feature(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        requirement = FeatureRequirement(
            requirement_id="close-entry",
            feature_id="close",
            feature_version="1.0.0",
            resolution="PT1M",
            instruments=(INSTRUMENT_ID,),
            required_observations=1,
        )

        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            left = publish_realtime_warmup_bundle(document, Path(first), (requirement,))
            right = publish_realtime_warmup_bundle(document, Path(second), (requirement,))

            self.assertEqual(left.manifest, right.manifest)
            self.assertEqual(left.manifest["status"], "AVAILABLE")
            self.assertEqual(left.manifest["session_date_et"], "2026-07-31")
            self.assertEqual(left.manifest["dataset_hash_scope"], "MARKET_EVENTS")
            self.assertEqual(len(left.manifest["objects"]), 2)
            self.assertEqual(
                left.daily_object_path.read_bytes(), right.daily_object_path.read_bytes()
            )
            self.assertEqual(
                left.feature_object_path.read_bytes(), right.feature_object_path.read_bytes()
            )

            table = pq.read_table(left.daily_object_path)
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.column("event_id").to_pylist(), [
                "evt_96e3d1d3e45d92ab162eee652010595bae90825203db15590d2ee3afcac3c834"
            ])
            self.assertEqual(table.column("close").to_pylist(), [210.2])

            feature = json.loads(left.feature_object_path.read_text(encoding="utf-8"))
            self.assertEqual(feature["manifest_id"], left.manifest["manifest_id"])
            self.assertEqual(feature["dataset_hash"], left.manifest["dataset_hash"])
            self.assertEqual(feature["series"][0]["observations"], [
                {"instrument": INSTRUMENT_ID, "observed_at": "2026-07-31T14:30:00Z", "value": "210.2"}
            ])

    def test_blocks_publication_when_required_feature_coverage_is_missing(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        requirement = FeatureRequirement(
            requirement_id="close-entry",
            feature_id="close",
            feature_version="1.0.0",
            resolution="PT1M",
            instruments=(INSTRUMENT_ID,),
            required_observations=2,
        )

        with tempfile.TemporaryDirectory() as output:
            with self.assertRaisesRegex(RealtimeWarmupError, "coverage"):
                publish_realtime_warmup_bundle(document, Path(output), (requirement,))
            self.assertEqual(list(Path(output).iterdir()), [])

    def test_rejects_incompatible_c_event_envelopes(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        document["events"][2]["schemaVersion"] = 2
        requirement = FeatureRequirement(
            requirement_id="close-entry",
            feature_id="close",
            feature_version="1.0.0",
            resolution="PT1M",
            instruments=(INSTRUMENT_ID,),
            required_observations=1,
        )

        with tempfile.TemporaryDirectory() as output:
            with self.assertRaisesRegex(RealtimeWarmupError, "schema"):
                publish_realtime_warmup_bundle(document, Path(output), (requirement,))

    def test_applies_the_latest_revision_deterministically(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        corrected = deepcopy(document["events"][2])
        corrected.update({
            "eventId": "evt_corrected_bar",
            "receivedAt": "2026-07-31T14:31:01.050Z",
            "sequence": 46,
            "revision": 1,
            "correctionOfEventId": document["events"][2]["eventId"],
        })
        corrected["values"]["close"] = 211.0
        document["events"].append(corrected)
        requirement = FeatureRequirement(
            "close-entry", "close", "1.0.0", "PT1M", (INSTRUMENT_ID,), 1
        )

        with tempfile.TemporaryDirectory() as output:
            bundle = publish_realtime_warmup_bundle(document, Path(output), (requirement,))
            table = pq.read_table(bundle.daily_object_path)
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.column("revision").to_pylist(), [1])
            self.assertEqual(table.column("close").to_pylist(), [211.0])

    def test_verifier_rejects_a_corrupted_published_object(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        requirement = FeatureRequirement(
            "close-entry", "close", "1.0.0", "PT1M", (INSTRUMENT_ID,), 1
        )

        with tempfile.TemporaryDirectory() as output:
            bundle = publish_realtime_warmup_bundle(document, Path(output), (requirement,))
            bundle.feature_object_path.write_bytes(
                bundle.feature_object_path.read_bytes() + b"corrupt"
            )
            with self.assertRaisesRegex(RealtimeWarmupError, "hash mismatch"):
                verify_realtime_warmup_bundle(Path(output))


if __name__ == "__main__":
    unittest.main()
