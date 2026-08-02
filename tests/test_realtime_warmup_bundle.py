"""D90 warm-up bundle: no `PT1M`/`close` assumptions, canonical object keys.

Two defects are pinned here.

**Hardcoding.**  The old `FeatureRequirement` rejected any resolution but `PT1M`
and any feature but `close`, and `_validated_bars` only recognised `BAR_1M`.  The
requirement now names its own resolution, its own event type and the exact
``values`` key it reads, and the tests drive a 30-minute `BAR_30M` stream through
a `vwap` feature to prove none of the three is still assumed.

**Object keys.**  The bars object used to land under
``warmup/session_date_et=…/market-events-….parquet``, a key of the module's own
invention that `MarketPipelineEngine.compact` could never find.  It now lands
under the canonical key of spec 2.5, pinned character for character below.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import pyarrow.parquet as pq

from market_pipeline_lib.contracts import DATASET_CONTRACTS
from market_pipeline_lib.fs_paths import long_path
from market_pipeline_lib.realtime_warmup import (
    FeatureRequirement,
    RealtimeWarmupError,
    WarmupPublicationSpec,
    publish_realtime_warmup_bundle,
    verify_realtime_warmup_bundle,
)


FIXTURE = Path(__file__).parent / "fixtures" / "d90" / "provider-neutral-market-events.json"
INSTRUMENT_ID = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"

RAW_CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]
SPEC = WarmupPublicationSpec(
    contract=RAW_CONTRACT,
    event_type="BAR_1M",
    granularity="DAY",
)
#: `logical_dataset_id(RAW_CONTRACT, 2026)` -- pinned so a change to the id rule fails here.
DATASET_ID = "9e960b11-a181-59cb-b4dd-6d8c32c61228"
CANONICAL_EVENTS_KEY = (
    "market-data/provider=ALPACA/feed=ALPACA_SIP_RAW_30M"
    f"/dataset={DATASET_ID}/revision=1/layer=RAW/resolution=30m"
    "/granularity=DAY/partition_start=2026-07-31/partition_end=2026-08-01"
    "/shard=s00-of-1/part-00001.parquet"
)


def requirement(**overrides: object) -> FeatureRequirement:
    values: dict[str, object] = {
        "requirement_id": "close-entry",
        "feature_id": "close",
        "feature_version": "1.0.0",
        "resolution": "PT1M",
        "value_field": "close",
        "instruments": (INSTRUMENT_ID,),
        "required_observations": 1,
    }
    values.update(overrides)
    return FeatureRequirement(**values)  # type: ignore[arg-type]


class TemporaryBundleRoot:
    """`TemporaryDirectory` that can also remove a canonical (deep) key tree."""

    def __enter__(self) -> Path:
        self._path = tempfile.mkdtemp()
        return Path(self._path)

    def __exit__(self, *_: object) -> None:
        shutil.rmtree(long_path(self._path), ignore_errors=True)


class RealtimeWarmupBundleTest(unittest.TestCase):
    def test_publishes_deterministic_parquet_manifest_and_feature(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))

        with TemporaryBundleRoot() as first, TemporaryBundleRoot() as second:
            left = publish_realtime_warmup_bundle(document, first / "bundle", (requirement(),), spec=SPEC)
            right = publish_realtime_warmup_bundle(document, second / "bundle", (requirement(),), spec=SPEC)

            self.assertEqual(left.manifest, right.manifest)
            self.assertEqual(left.manifest["status"], "AVAILABLE")
            self.assertEqual(left.manifest["session_date_et"], "2026-07-31")
            self.assertEqual(left.manifest["dataset_hash_scope"], "MARKET_EVENTS")
            self.assertEqual(len(left.manifest["objects"]), 2)
            self.assertEqual(
                Path(long_path(left.daily_object_path)).read_bytes(),
                Path(long_path(right.daily_object_path)).read_bytes(),
            )
            self.assertEqual(
                left.feature_object_path.read_bytes(), right.feature_object_path.read_bytes()
            )

            table = pq.read_table(long_path(left.daily_object_path))
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

    def test_the_market_events_object_uses_the_canonical_key(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        with TemporaryBundleRoot() as root:
            bundle = publish_realtime_warmup_bundle(
                document, root / "bundle", (requirement(),), spec=SPEC
            )
            keys = {item["object_role"]: item["object_key"] for item in bundle.manifest["objects"]}
            self.assertEqual(keys["MARKET_EVENTS"], CANONICAL_EVENTS_KEY)
            # The sidecar is namespaced away from `market-data/` on purpose: it is
            # not a market-data object and must never be mistaken for one.
            self.assertTrue(keys["WARMUP_FEATURES"].startswith("warmup/"))
            self.assertTrue(Path(long_path(root / "bundle" / CANONICAL_EVENTS_KEY)).is_file())

    def test_a_thirty_minute_stream_and_a_vwap_feature_are_supported(self) -> None:
        """Neither `PT1M` nor `close` may be assumed anywhere in the path."""

        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        bar = deepcopy(document["events"][2])
        bar.update({"eventId": "evt_bar_30m", "eventType": "BAR_30M", "providerEventId": "bar-30m"})
        bar["values"]["vwap"] = 210.18
        document["events"].append(bar)

        spec = WarmupPublicationSpec(contract=RAW_CONTRACT, event_type="BAR_30M", granularity="DAY")
        with TemporaryBundleRoot() as root:
            bundle = publish_realtime_warmup_bundle(
                document,
                root / "bundle",
                (
                    requirement(
                        requirement_id="vwap-entry",
                        feature_id="volume_weighted_average_price",
                        resolution="PT30M",
                        value_field="vwap",
                    ),
                ),
                spec=spec,
            )
            table = pq.read_table(long_path(bundle.daily_object_path))
            self.assertEqual(table.column("event_id").to_pylist(), ["evt_bar_30m"])
            feature = json.loads(bundle.feature_object_path.read_text(encoding="utf-8"))
            self.assertEqual(feature["series"][0]["resolution"], "PT30M")
            self.assertEqual(feature["series"][0]["observations"][0]["value"], "210.18")

    def test_a_requirement_naming_an_absent_value_field_is_refused(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        with TemporaryBundleRoot() as root:
            with self.assertRaisesRegex(RealtimeWarmupError, "openInterest"):
                publish_realtime_warmup_bundle(
                    document,
                    root / "bundle",
                    (requirement(feature_id="open_interest", value_field="openInterest"),),
                    spec=SPEC,
                )

    def test_an_event_type_with_no_coverage_is_refused(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        spec = WarmupPublicationSpec(contract=RAW_CONTRACT, event_type="BAR_5M", granularity="DAY")
        with TemporaryBundleRoot() as root:
            with self.assertRaisesRegex(RealtimeWarmupError, "BAR_5M"):
                publish_realtime_warmup_bundle(document, root / "bundle", (requirement(),), spec=spec)

    def test_blocks_publication_when_required_feature_coverage_is_missing(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        with TemporaryBundleRoot() as root:
            output = root / "bundle"
            with self.assertRaisesRegex(RealtimeWarmupError, "coverage"):
                publish_realtime_warmup_bundle(
                    document, output, (requirement(required_observations=2),), spec=SPEC
                )
            self.assertFalse(output.exists())

    def test_rejects_incompatible_c_event_envelopes(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        document["events"][2]["schemaVersion"] = 2
        with TemporaryBundleRoot() as root:
            with self.assertRaisesRegex(RealtimeWarmupError, "schema"):
                publish_realtime_warmup_bundle(document, root / "bundle", (requirement(),), spec=SPEC)

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

        with TemporaryBundleRoot() as root:
            bundle = publish_realtime_warmup_bundle(
                document, root / "bundle", (requirement(),), spec=SPEC
            )
            table = pq.read_table(long_path(bundle.daily_object_path))
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.column("revision").to_pylist(), [1])
            self.assertEqual(table.column("close").to_pylist(), [211.0])

    def test_verifier_rejects_a_corrupted_published_object(self) -> None:
        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        with TemporaryBundleRoot() as root:
            output = root / "bundle"
            bundle = publish_realtime_warmup_bundle(document, output, (requirement(),), spec=SPEC)
            bundle.feature_object_path.write_bytes(
                bundle.feature_object_path.read_bytes() + b"corrupt"
            )
            with self.assertRaisesRegex(RealtimeWarmupError, "hash mismatch"):
                verify_realtime_warmup_bundle(output)


class WarmupConsumerTest(unittest.TestCase):
    """The bundle now has a consumer: it is built from a drained event source."""

    def test_a_bundle_is_built_from_events_drained_off_a_queue(self) -> None:
        from market_pipeline_lib.realtime_ingest import RealtimeDelivery
        from market_pipeline_lib.realtime_warmup import warmup_bundle_from_source

        document = json.loads(FIXTURE.read_text(encoding="utf-8"))
        source = _ReplayingSource(
            [
                RealtimeDelivery(
                    message_id="m1",
                    body={"events": document["events"]},
                    receipt_handle="rh1",
                    receive_count=1,
                )
            ]
        )
        with TemporaryBundleRoot() as root:
            bundle = warmup_bundle_from_source(
                source, root / "bundle", (requirement(),), spec=SPEC, wait_seconds=0.0
            )
            self.assertEqual(bundle.manifest["session_date_et"], "2026-07-31")
            self.assertEqual(source.acknowledged, ["m1"])
            self.assertTrue(Path(long_path(root / "bundle" / CANONICAL_EVENTS_KEY)).is_file())

    def test_a_drain_that_yields_no_events_refuses_to_publish_an_empty_bundle(self) -> None:
        from market_pipeline_lib.realtime_warmup import warmup_bundle_from_source

        source = _ReplayingSource([])
        with TemporaryBundleRoot() as root:
            with self.assertRaises(RealtimeWarmupError):
                warmup_bundle_from_source(
                    source, root / "bundle", (requirement(),), spec=SPEC, wait_seconds=0.0
                )


class _ReplayingSource:
    def __init__(self, deliveries: list[object]) -> None:
        self._deliveries = list(deliveries)
        self.acknowledged: list[str] = []
        self.closed = False

    def poll(self, max_messages: int, wait_seconds: float) -> list[object]:
        batch, self._deliveries = self._deliveries[:max_messages], self._deliveries[max_messages:]
        return batch

    def acknowledge(self, delivery: object) -> None:
        self.acknowledged.append(delivery.message_id)  # type: ignore[attr-defined]

    def retry_later(self, delivery: object, *, delay_seconds: float) -> None:  # pragma: no cover
        raise AssertionError("a warm-up drain must not retry")

    def dead_letter(self, delivery: object, *, reason: str) -> None:  # pragma: no cover
        raise AssertionError("a warm-up drain must not park")

    def close(self) -> None:
        self.closed = True


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
