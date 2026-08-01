import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from d_storage_testkit import FakeS3Client, write_small_parquet
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore


class StorageAdapterContractTests(unittest.TestCase):
    def test_small_parquet_is_deterministic_and_portable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = write_small_parquet(root / "first.parquet")
            second = write_small_parquet(root / "second.parquet")
            self.assertEqual(first.read_bytes(), second.read_bytes())

            key = "market-data/provider=ALPACA/feed=TEST/part-00001.parquet"
            local = LocalObjectStore(root / "local")
            remote = S3ObjectStore("bucket", client=FakeS3Client())
            local_receipt = local.put(first, key)
            remote_receipt = remote.put(first, key)

            self.assertEqual(local_receipt.object_key, remote_receipt.object_key)
            self.assertEqual(local_receipt.content_hash, remote_receipt.content_hash)
            with remote.open(key) as stream:
                self.assertEqual(pq.read_table(stream).num_rows, 2)

    def test_s3_reuses_identical_object_and_rejects_different_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = write_small_parquet(root / "original.parquet")
            changed = root / "changed.parquet"
            changed.write_bytes(original.read_bytes() + b"different")
            key = "immutable/part-00001.parquet"
            client = FakeS3Client()
            store = S3ObjectStore("bucket", client=client)

            first = store.put(original, key)
            repeated = store.put(original, key)
            self.assertEqual(first.content_hash, repeated.content_hash)
            self.assertEqual(client.put_calls, 1)

            with self.assertRaisesRegex(FileExistsError, "immutable"):
                store.put(changed, key)
            self.assertEqual(client.put_calls, 1)
            self.assertEqual(client.objects[("bucket", key)]["Body"], original.read_bytes())

    def test_s3_missing_is_distinct_from_service_failure(self) -> None:
        client = FakeS3Client()
        store = S3ObjectStore("bucket", client=client)
        self.assertFalse(store.exists("missing.parquet"))
        result = store.verify("missing.parquet", "0" * 64)
        self.assertFalse(result.ok)
        self.assertEqual(result.message, "object missing")

        client.head_error = PermissionError("access denied")
        with self.assertRaisesRegex(PermissionError, "access denied"):
            store.exists("hidden.parquet")
        with self.assertRaisesRegex(PermissionError, "access denied"):
            store.verify("hidden.parquet", "0" * 64)


if __name__ == "__main__":
    unittest.main()
