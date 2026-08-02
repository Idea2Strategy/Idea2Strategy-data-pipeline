"""Windows MAX_PATH regression tests for transient staging paths.

Local staging and local object publishing both build very deep partition
directories. Appending a long transient file name on top of them pushes the
absolute path past the 260-character Windows limit, `[WinError 3]` /
`[WinError 206]` is raised inside `publish`, and the manifest comes back
`QUARANTINED` instead of `AVAILABLE`.

These tests pin the two invariants that keep that from happening:

* a transient staging name adds at most a short, bounded suffix, and
* the local object store can publish to a canonical object key even when the
  resulting absolute path is longer than 260 characters.
"""

import os
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

from market_pipeline_lib.contracts import (
    DATASET_CONTRACTS,
    logical_dataset_id,
    object_key,
)
from market_pipeline_lib.engine import legacy_staging_filename
from market_pipeline_lib.fs_paths import (
    MAX_TEMP_SUFFIX_LENGTH,
    long_path,
    short_temp_path,
)
from market_pipeline_lib.storage import LocalObjectStore

WINDOWS_MAX_PATH = 260


class ShortTempSuffixTests(unittest.TestCase):
    def test_temp_name_stays_within_the_suffix_budget(self):
        destination = Path("shard=s00-of-2") / "source=abc-batch=000001.parquet"

        temporary = short_temp_path(destination)

        self.assertLessEqual(len(temporary.name), MAX_TEMP_SUFFIX_LENGTH)
        self.assertLessEqual(MAX_TEMP_SUFFIX_LENGTH, 24)

    def test_temp_name_does_not_grow_with_the_destination_name(self):
        short = short_temp_path(Path("a.parquet"))
        long = short_temp_path(
            Path("source=1e4d0e28-7f2f-5a2c-9d1a-2b3c4d5e6f70-batch=000001.parquet")
        )

        self.assertEqual(len(short.name), len(long.name))

    def test_temp_name_sits_beside_the_destination_and_is_hidden(self):
        destination = Path("x") / "y" / "object.parquet"

        temporary = short_temp_path(destination)

        self.assertEqual(temporary.parent, destination.parent)
        self.assertTrue(temporary.name.startswith("."))
        self.assertTrue(temporary.name.endswith(".tmp"))

    def test_temp_names_are_unique_between_calls(self):
        destination = Path("object.parquet")

        names = {short_temp_path(destination).name for _ in range(64)}

        self.assertEqual(len(names), 64)

    def test_legacy_staging_fragment_name_is_short_and_deterministic(self):
        source = Path("legacy") / "AAPL_30min_adjusted.parquet"

        first = legacy_staging_filename(source, 1)
        second = legacy_staging_filename(source, 1)
        other_batch = legacy_staging_filename(source, 2)

        self.assertEqual(first, second)
        self.assertNotEqual(first, other_batch)
        self.assertTrue(first.endswith(".parquet"))
        self.assertLessEqual(len(first), 32)


class LongObjectPathPublishTests(unittest.TestCase):
    """A canonical object key must publish even past the Windows MAX_PATH."""

    def make_root(self) -> tuple[Path, Path]:
        """Return (base, store root) padded past MAX_PATH, cleaned up safely."""
        base = Path(tempfile.mkdtemp())
        # shutil.rmtree cannot walk a >MAX_PATH tree either, so remove it
        # through the extended-length form.
        self.addCleanup(shutil.rmtree, long_path(base), True)
        root = base / ("d" * 40) / "objects"
        os.makedirs(long_path(root), exist_ok=True)
        return base, root

    @staticmethod
    def canonical_key() -> str:
        contract = next(
            value
            for value in DATASET_CONTRACTS.values()
            if value.feed_code.endswith("RAW_30M")
        )
        return object_key(
            contract,
            logical_dataset_id(contract, 2024),
            1,
            "YEAR",
            date(2024, 1, 1),
            date(2025, 1, 1),
            "s00-of-2",
            1,
        )

    def test_publish_and_verify_survive_a_path_longer_than_max_path(self):
        key = self.canonical_key()
        # Pad the store root so that root + canonical key exceeds MAX_PATH,
        # exactly as a real output root under a user profile does.
        base, root = self.make_root()
        source = base / "src.parquet"
        source.write_bytes(b"PAR1" + os.urandom(512))

        store = LocalObjectStore(root)
        plain_length = len(str((root / key.replace("/", os.sep)).resolve()))
        self.assertGreater(
            plain_length,
            WINDOWS_MAX_PATH,
            "test setup must produce a path past the Windows limit",
        )

        receipt = store.put(source, key)

        self.assertEqual(receipt.object_key, key)
        self.assertTrue(store.exists(key))
        self.assertTrue(store.verify(key, receipt.content_hash).ok)
        with store.open(key) as handle:
            self.assertEqual(handle.read(), source.read_bytes())

    def test_no_staging_leftovers_remain_after_a_long_path_publish(self):
        key = self.canonical_key()
        base, root = self.make_root()
        source = base / "src.parquet"
        source.write_bytes(b"PAR1" + os.urandom(512))

        store = LocalObjectStore(root)
        store.put(source, key)

        directory = store.path_for(key).parent
        leftovers = [
            entry
            for entry in os.listdir(directory)
            if entry.startswith(".") or entry.endswith((".tmp", ".staged"))
        ]
        self.assertEqual(leftovers, [])

    def test_a_second_publish_of_different_bytes_is_refused(self):
        key = self.canonical_key()
        base, root = self.make_root()
        first = base / "first.parquet"
        first.write_bytes(b"PAR1" + b"a" * 512)
        second = base / "second.parquet"
        second.write_bytes(b"PAR1" + b"b" * 512)

        store = LocalObjectStore(root)
        store.put(first, key)

        with self.assertRaises(FileExistsError):
            store.put(second, key)

        self.assertTrue(store.verify(key, LocalObjectStore(root).put(first, key).content_hash).ok)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
