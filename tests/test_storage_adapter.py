import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from d_storage_testkit import FakeS3Client, FakeS3Error, write_small_parquet
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

    def test_s3_retries_transient_head_failure(self) -> None:
        class FlakyHeadClient(FakeS3Client):
            def __init__(self) -> None:
                super().__init__()
                self.remaining_failures = 0
                self.head_calls = 0

            def head_object(self, *, Bucket: str, Key: str):  # type: ignore[no-untyped-def]
                self.head_calls += 1
                if self.remaining_failures:
                    self.remaining_failures -= 1
                    raise FakeS3Error("ServiceUnavailable", 503, "try again")
                return super().head_object(Bucket=Bucket, Key=Key)

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = FlakyHeadClient()
            store = S3ObjectStore(
                "bucket",
                client=client,
                max_attempts=3,
                retry_delay_seconds=0,
            )
            store.put(source, "retry/source.parquet")
            client.remaining_failures = 2
            client.head_calls = 0

            self.assertTrue(store.exists("retry/source.parquet"))
            self.assertEqual(client.head_calls, 3)

    def test_s3_recovers_when_upload_succeeds_but_response_fails(self) -> None:
        class StoreThenFailClient(FakeS3Client):
            def __init__(self) -> None:
                super().__init__()
                self.fail_after_first_put = True

            def put_object(self, **kwargs):  # type: ignore[no-untyped-def]
                result = super().put_object(**kwargs)
                if self.fail_after_first_put:
                    self.fail_after_first_put = False
                    raise FakeS3Error("RequestTimeout", 503, "response lost")
                return result

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = StoreThenFailClient()
            store = S3ObjectStore(
                "bucket",
                client=client,
                max_attempts=3,
                retry_delay_seconds=0,
            )

            receipt = store.put(source, "partial/source.parquet")

            self.assertEqual(client.put_calls, 1)
            verification = store.verify(
                "partial/source.parquet",
                receipt.content_hash,
            )
            self.assertTrue(verification.ok)
            self.assertEqual(receipt.content_hash, verification.content_hash)

    def test_s3_stops_after_retry_budget_without_publishing(self) -> None:
        class AlwaysFailClient(FakeS3Client):
            def __init__(self) -> None:
                super().__init__()
                self.attempts = 0

            def put_object(self, **kwargs):  # type: ignore[no-untyped-def]
                self.attempts += 1
                raise FakeS3Error("SlowDown", 503, "still unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = AlwaysFailClient()
            store = S3ObjectStore(
                "bucket",
                client=client,
                max_attempts=3,
                retry_delay_seconds=0,
            )

            with self.assertRaisesRegex(FakeS3Error, "still unavailable"):
                store.put(source, "failed/source.parquet")

            self.assertEqual(client.attempts, 3)
            self.assertEqual(client.objects, {})


if __name__ == "__main__":
    unittest.main()
