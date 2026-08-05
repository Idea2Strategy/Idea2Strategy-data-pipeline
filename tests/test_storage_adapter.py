"""Contract coverage for the immutable object-store adapters (card D03).

Everything that used to be untested here — the prefix rule, the constructor
guards, the boto3 import guard, the retry backoff schedule, the local
path-traversal guard, the staging-copy integrity check and the metadata
mismatch path — is exercised below. No test sleeps on a real clock: the S3
store's retry delay is driven through an injected `ManualClock`.

The conditional-write (412) reconciliation is asserted here against
`FakeS3Client` *and* against a real S3 implementation in
`tests/test_storage_adapter_localstack.py`; the fake alone cannot confirm that
`If-None-Match` behaves this way on the wire.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from d_storage_testkit import (
    FakeS3Client,
    FakeS3Error,
    base64_sha256,
    required_bar_fields,
    small_bar_table,
    write_small_parquet,
)
from market_pipeline_lib.contracts import bar_schema, sha256_file
from market_pipeline_lib.rate_limit import ManualClock
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore


class FixtureSchemaTests(unittest.TestCase):
    def test_fixture_columns_are_derived_from_the_canonical_bar_schema(self) -> None:
        table = small_bar_table()

        self.assertEqual(
            table.schema.names,
            [field.name for field in bar_schema(False) if not field.nullable],
        )
        self.assertEqual(
            [field.type for field in table.schema],
            [field.type for field in required_bar_fields()],
        )

    def test_written_object_round_trips_with_the_derived_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = write_small_parquet(Path(temporary) / "bars.parquet")

            table = pq.read_table(path)

            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table.schema.names, small_bar_table().schema.names)


class LocalObjectStoreTests(unittest.TestCase):
    def test_verify_version_pins_hash_and_size_to_the_requested_local_version(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = write_small_parquet(root / "source.parquet")
            store = LocalObjectStore(root / "objects")
            receipt = store.put(source, "market/source.parquet")

            verified = store.verify_version(
                receipt.object_key,
                receipt.provider_version_id,
                receipt.content_hash,
                receipt.byte_size,
            )
            wrong_size = store.verify_version(
                receipt.object_key,
                receipt.provider_version_id,
                receipt.content_hash,
                receipt.byte_size + 1,
            )

            self.assertTrue(verified.ok)
            self.assertFalse(wrong_size.ok)
            self.assertEqual(wrong_size.message, "byte size mismatch")

    def test_path_for_rejects_keys_that_escape_the_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalObjectStore(Path(temporary) / "objects")

            for key in (
                "../escape.parquet",
                "a/b/../../../escape.parquet",
                "..\\escape.parquet",
                "a/../../escape.parquet",
            ):
                with self.subTest(key=key), self.assertRaisesRegex(ValueError, "저장소 루트"):
                    store.path_for(key)

    def test_an_absolute_key_cannot_redirect_the_write_outside_the_root(self) -> None:
        """An absolute or drive-qualified key must never resolve outside the root.

        The two platforms uphold that guarantee by different mechanisms. On
        Windows a drive-qualified key makes ``root / key`` discard the root, so
        containment fails and the key is rejected. On POSIX ``lstrip("/")``
        strips the leading separator first, so the key is neutralised into a
        relative one and is contained rather than refused.

        Asserting ValueError pinned only the Windows mechanism, so this test
        passed on a Windows developer machine and failed on the Linux runner
        that actually deploys this code. Assert the invariant instead: either
        the key is refused, or it lands inside the root - never elsewhere.
        """
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "objects"
            store = LocalObjectStore(root)
            other = Path(temporary) / "elsewhere"
            other.mkdir()
            escaping = f"{other.drive}{other.as_posix()[len(other.drive):]}/pwned.parquet"

            try:
                resolved = store.path_for(escaping)
            except ValueError as exc:
                self.assertIn("저장소 루트", str(exc))
            else:
                # `long_path` may return the Windows extended-length form.
                plain = Path(str(resolved).replace("\\\\?\\", "")).resolve()
                self.assertTrue(
                    plain.is_relative_to(root.resolve()),
                    f"absolute key escaped the store root: {plain}",
                )

            self.assertFalse((other / "pwned.parquet").exists())

    def test_every_entry_point_enforces_the_traversal_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = LocalObjectStore(root / "objects")
            source = write_small_parquet(root / "source.parquet")
            key = "../escaped.parquet"

            with self.assertRaises(ValueError):
                store.put(source, key)
            with self.assertRaises(ValueError):
                store.exists(key)
            with self.assertRaises(ValueError):
                store.open(key)
            with self.assertRaises(ValueError):
                store.verify(key, "0" * 64)
            self.assertFalse((root / "escaped.parquet").exists())

    def test_leading_slash_is_anchored_to_the_root_rather_than_the_filesystem(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = (Path(temporary) / "objects").resolve()
            store = LocalObjectStore(root)

            resolved = store.path_for("/market-data/part-00001.parquet")

            self.assertTrue(str(resolved).endswith(str(Path("objects/market-data/part-00001.parquet"))))

    def test_staging_copy_corruption_never_reaches_the_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = write_small_parquet(root / "source.parquet")
            store = LocalObjectStore(root / "objects")
            key = "market-data/part-00001.parquet"

            def corrupting_copy(src: Any, dst: Any, **_: Any) -> Any:
                Path(dst).write_bytes(Path(src).read_bytes() + b"bit-rot")
                return dst

            with mock.patch.object(shutil, "copyfile", corrupting_copy):
                with self.assertRaisesRegex(OSError, "SHA-256"):
                    store.put(source, key)

            self.assertFalse(store.exists(key))
            self.assertEqual(list((root / "objects" / "market-data").glob("*")), [])

    def test_verify_reports_a_sha256_mismatch_on_an_existing_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = write_small_parquet(root / "source.parquet")
            store = LocalObjectStore(root / "objects")
            key = "market-data/part-00001.parquet"
            receipt = store.put(source, key)

            good = store.verify(key, receipt.content_hash)
            bad = store.verify(key, "0" * 64)

            self.assertTrue(good.ok)
            self.assertFalse(bad.ok)
            self.assertEqual(bad.message, "sha256 mismatch")
            self.assertEqual(bad.content_hash, receipt.content_hash)

    def test_republishing_different_bytes_under_one_key_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = write_small_parquet(root / "original.parquet")
            changed = write_small_parquet(root / "changed.parquet", price_offset=1.0)
            store = LocalObjectStore(root / "objects")
            key = "market-data/part-00001.parquet"

            first = store.put(original, key)
            repeated = store.put(original, key)
            self.assertEqual(first.content_hash, repeated.content_hash)

            with self.assertRaises(FileExistsError):
                store.put(changed, key)
            self.assertEqual(sha256_file(Path(first.local_path or "")), first.content_hash)


class S3ObjectStoreConstructionTests(unittest.TestCase):
    def test_constructor_guards_reject_unusable_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "bucket"):
            S3ObjectStore("", client=FakeS3Client())
        with self.assertRaisesRegex(ValueError, "max_attempts"):
            S3ObjectStore("bucket", client=FakeS3Client(), max_attempts=0)
        with self.assertRaisesRegex(ValueError, "retry_delay_seconds"):
            S3ObjectStore("bucket", client=FakeS3Client(), retry_delay_seconds=-0.001)

    def test_missing_boto3_is_reported_as_a_missing_optional_dependency(self) -> None:
        # Setting the entry to None makes `import boto3` raise ImportError,
        # which is the branch a default install without the extra takes.
        with mock.patch.dict(sys.modules, {"boto3": None}):
            with self.assertRaisesRegex(RuntimeError, "boto3"):
                S3ObjectStore("bucket")

    def test_an_injected_client_never_imports_boto3(self) -> None:
        with mock.patch.dict(sys.modules, {"boto3": None}):
            store = S3ObjectStore("bucket", client=FakeS3Client())

        self.assertEqual(store.bucket, "bucket")


class S3ObjectStorePrefixTests(unittest.TestCase):
    def test_prefix_is_applied_once_and_is_idempotent(self) -> None:
        store = S3ObjectStore("bucket", client=FakeS3Client(), prefix="/tenant-a/")

        self.assertEqual(store.prefix, "tenant-a")
        self.assertEqual(store._key("market/part.parquet"), "tenant-a/market/part.parquet")
        self.assertEqual(store._key("/market/part.parquet"), "tenant-a/market/part.parquet")
        self.assertEqual(
            store._key("tenant-a/market/part.parquet"),
            "tenant-a/market/part.parquet",
        )
        self.assertEqual(
            store._key(store._key("market/part.parquet")),
            "tenant-a/market/part.parquet",
        )

    def test_a_key_that_merely_starts_with_the_prefix_text_is_still_prefixed(self) -> None:
        store = S3ObjectStore("bucket", client=FakeS3Client(), prefix="tenant")

        self.assertEqual(store._key("tenantX/part.parquet"), "tenant/tenantX/part.parquet")

    def test_empty_prefix_leaves_the_key_untouched(self) -> None:
        store = S3ObjectStore("bucket", client=FakeS3Client())

        self.assertEqual(store._key("/market/part.parquet"), "market/part.parquet")

    def test_prefixed_store_round_trips_through_every_operation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = FakeS3Client()
            store = S3ObjectStore("bucket", client=client, prefix="tenant-a")

            receipt = store.put(source, "market/part-00001.parquet")

            self.assertEqual(receipt.object_key, "tenant-a/market/part-00001.parquet")
            self.assertIn(("bucket", "tenant-a/market/part-00001.parquet"), client.objects)
            self.assertTrue(store.exists("market/part-00001.parquet"))
            self.assertTrue(store.exists("tenant-a/market/part-00001.parquet"))
            self.assertTrue(store.verify("market/part-00001.parquet", receipt.content_hash).ok)
            with store.open("market/part-00001.parquet") as stream:
                self.assertEqual(stream.read(), source.read_bytes())


class S3ObjectStoreIntegrityTests(unittest.TestCase):
    def test_upload_requests_sse_s3_and_a_sha256_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = FakeS3Client()
            store = S3ObjectStore("bucket", client=client)

            receipt = store.put(source, "market/part-00001.parquet")

            sent = client.put_kwargs[0]
            self.assertEqual(sent["ServerSideEncryption"], "AES256")
            self.assertEqual(sent["ChecksumAlgorithm"], "SHA256")
            self.assertEqual(sent["ChecksumSHA256"], base64_sha256(source.read_bytes()))
            self.assertEqual(sent["IfNoneMatch"], "*")
            self.assertEqual(sent["Metadata"]["sha256"], receipt.content_hash)
            self.assertEqual(sent["ContentType"], "application/vnd.apache.parquet")

    def test_version_id_is_captured_into_the_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            store = S3ObjectStore("bucket", client=FakeS3Client())

            receipt = store.put(source, "market/part-00001.parquet")

            self.assertEqual(receipt.provider_version_id, "v1")
            self.assertEqual(receipt.storage_provider, "S3_COMPATIBLE")
            self.assertEqual(receipt.bucket_name, "bucket")
            self.assertEqual(receipt.byte_size, source.stat().st_size)

    def test_an_unencrypted_stored_object_fails_the_post_write_head_check(self) -> None:
        class SseDroppingClient(FakeS3Client):
            def put_object(self, **kwargs: Any) -> dict[str, str]:
                kwargs.pop("ServerSideEncryption", None)
                return super().put_object(**kwargs)

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            store = S3ObjectStore("bucket", client=SseDroppingClient())

            with self.assertRaisesRegex(RuntimeError, "SSE-S3"):
                store.put(source, "market/part-00001.parquet")

    def test_a_checksum_that_disagrees_with_the_body_fails_the_head_check(self) -> None:
        class ChecksumRewritingClient(FakeS3Client):
            def put_object(self, **kwargs: Any) -> dict[str, str]:
                kwargs["ChecksumSHA256"] = base64_sha256(b"a different object")
                return super().put_object(**kwargs)

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            store = S3ObjectStore("bucket", client=ChecksumRewritingClient())

            with self.assertRaisesRegex(RuntimeError, "ChecksumSHA256"):
                store.put(source, "market/part-00001.parquet")

    def test_verify_reports_a_sha256_metadata_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = FakeS3Client()
            store = S3ObjectStore("bucket", client=client)
            key = "market/part-00001.parquet"
            receipt = store.put(source, key)

            client.objects[("bucket", key)]["Metadata"]["sha256"] = "f" * 64
            result = store.verify(key, receipt.content_hash)

            self.assertFalse(result.ok)
            self.assertEqual(result.message, "sha256 metadata mismatch")
            self.assertEqual(result.content_hash, "f" * 64)
            self.assertEqual(result.byte_size, source.stat().st_size)


class S3ObjectStoreContractTests(unittest.TestCase):
    def test_verify_version_uses_the_exact_s3_version_and_all_attestations(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            client = FakeS3Client()
            store = S3ObjectStore("bucket", client=client)
            receipt = store.put(source, "market/source.parquet")

            verified = store.verify_version(
                receipt.object_key,
                receipt.provider_version_id,
                receipt.content_hash,
                receipt.byte_size,
            )
            client.put_object(
                Bucket="bucket",
                Key=receipt.object_key,
                Body=b"newer bytes",
                ContentLength=len(b"newer bytes"),
                ContentType="application/vnd.apache.parquet",
                ServerSideEncryption="AES256",
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=base64_sha256(b"newer bytes"),
                Metadata={"sha256": "f" * 64},
            )
            latest_is_tampered = store.verify(receipt.object_key, receipt.content_hash)
            exact_version_stays_valid = store.verify_version(
                receipt.object_key,
                receipt.provider_version_id,
                receipt.content_hash,
                receipt.byte_size,
            )

            self.assertTrue(verified.ok)
            self.assertFalse(latest_is_tampered.ok)
            self.assertTrue(exact_version_stays_valid.ok)

    def test_local_and_s3_publish_the_same_key_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = write_small_parquet(root / "source.parquet")

            key = "market-data/provider=ALPACA/feed=TEST/part-00001.parquet"
            local = LocalObjectStore(root / "local")
            remote = S3ObjectStore("bucket", client=FakeS3Client())
            local_receipt = local.put(source, key)
            remote_receipt = remote.put(source, key)

            self.assertEqual(local_receipt.object_key, remote_receipt.object_key)
            self.assertEqual(local_receipt.content_hash, remote_receipt.content_hash)
            self.assertEqual(local_receipt.byte_size, remote_receipt.byte_size)
            with remote.open(key) as stream:
                self.assertEqual(pq.read_table(stream).num_rows, 2)
            self.assertEqual(pa.parquet.read_table(local_receipt.local_path).num_rows, 2)

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

            def head_object(self, **kwargs: Any) -> dict[str, Any]:
                self.head_calls += 1
                if self.remaining_failures:
                    self.remaining_failures -= 1
                    raise FakeS3Error("ServiceUnavailable", 503, "try again")
                return super().head_object(**kwargs)

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

            def put_object(self, **kwargs: Any) -> dict[str, str]:
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

            # The retry hits 412 (the lost-response write did land) and the
            # adapter reconciles against the stored object instead of failing.
            self.assertEqual(client.put_calls, 1)
            verification = store.verify(
                "partial/source.parquet",
                receipt.content_hash,
            )
            self.assertTrue(verification.ok)
            self.assertEqual(receipt.content_hash, verification.content_hash)
            self.assertEqual(receipt.provider_version_id, "v1")

    def test_s3_stops_after_retry_budget_without_publishing(self) -> None:
        class AlwaysFailClient(FakeS3Client):
            def __init__(self) -> None:
                super().__init__()
                self.attempts = 0

            def put_object(self, **kwargs: Any) -> dict[str, str]:
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


class S3ObjectStoreBackoffTests(unittest.TestCase):
    def test_retry_delays_follow_the_pinned_exponential_schedule(self) -> None:
        class AlwaysFailClient(FakeS3Client):
            def put_object(self, **kwargs: Any) -> dict[str, str]:
                raise FakeS3Error("SlowDown", 503, "still unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            clock = ManualClock()
            store = S3ObjectStore(
                "bucket",
                client=AlwaysFailClient(),
                max_attempts=4,
                retry_delay_seconds=0.5,
                clock=clock,
            )

            with self.assertRaises(FakeS3Error):
                store.put(source, "failed/source.parquet")

            # 0.5 * 2**(attempt-1) for attempts 1..3; the 4th attempt raises.
            self.assertEqual(clock.sleeps, [0.5, 1.0, 2.0])
            self.assertEqual(clock.monotonic(), 3.5)

    def test_a_zero_delay_never_calls_the_clock(self) -> None:
        class AlwaysFailClient(FakeS3Client):
            def put_object(self, **kwargs: Any) -> dict[str, str]:
                raise FakeS3Error("SlowDown", 503, "still unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            source = write_small_parquet(Path(temporary) / "source.parquet")
            clock = ManualClock()
            store = S3ObjectStore(
                "bucket",
                client=AlwaysFailClient(),
                max_attempts=3,
                retry_delay_seconds=0,
                clock=clock,
            )

            with self.assertRaises(FakeS3Error):
                store.put(source, "failed/source.parquet")

            self.assertEqual(clock.sleeps, [])

    def test_head_retries_use_the_same_schedule(self) -> None:
        class FlakyHeadClient(FakeS3Client):
            def __init__(self) -> None:
                super().__init__()
                self.remaining_failures = 2

            def head_object(self, **kwargs: Any) -> dict[str, Any]:
                if self.remaining_failures:
                    self.remaining_failures -= 1
                    raise FakeS3Error("ServiceUnavailable", 503, "try again")
                return super().head_object(**kwargs)

        clock = ManualClock()
        store = S3ObjectStore(
            "bucket",
            client=FlakyHeadClient(),
            max_attempts=4,
            retry_delay_seconds=0.25,
            clock=clock,
        )

        self.assertFalse(store.exists("missing.parquet"))
        self.assertEqual(clock.sleeps, [0.25, 0.5])


if __name__ == "__main__":
    unittest.main()
