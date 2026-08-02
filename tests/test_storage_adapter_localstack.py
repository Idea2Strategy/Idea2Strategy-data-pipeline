"""LocalStack-backed integration coverage for the immutable S3 adapter.

This file exists to answer one question the unit suite cannot: does a real S3
implementation actually behave the way `tests/d_storage_testkit.FakeS3Client`
claims? `If-None-Match` on `PutObject` is a recent S3 feature, and the whole
lost-response reconciliation path hangs on the service returning HTTP 412 with
code `PreconditionFailed`. If it returned anything else, both
`_is_precondition_failed` and `_is_retryable` would answer False, production
would raise instead of reconciling, and no fake-based test would notice.

So the conflict is provoked here against the real endpoint, and the raw
`ClientError` it produces is fed straight into the classifier.
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any

import pytest

from market_pipeline_lib.contracts import sha256_file
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore, sha256_hex_and_base64

LOCALSTACK_ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT_URL")

pytestmark = pytest.mark.integration


class _HeadSuppressingClient:
    """Delegate to the real S3 client, but report the first HEAD as missing.

    `S3ObjectStore.put` probes with HEAD before writing, so a plain second
    `put` never reaches the conditional write. Suppressing that one probe
    reproduces exactly the two situations the reconciliation path exists for:
    a concurrent writer that won the race, and our own earlier write whose
    response was lost. Everything after the probe — including the `PutObject`
    that gets rejected — goes to the real service.
    """

    def __init__(self, delegate: Any, suppress_heads: int = 1) -> None:
        self._delegate = delegate
        self._remaining = suppress_heads
        self.put_errors: list[Exception] = []
        self.put_calls = 0

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if self._remaining:
            self._remaining -= 1
            raise self._delegate.exceptions.ClientError(
                {
                    "Error": {"Code": "404", "Message": "Not Found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )
        return self._delegate.head_object(**kwargs)

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls += 1
        try:
            return self._delegate.put_object(**kwargs)
        except Exception as exc:
            self.put_errors.append(exc)
            raise

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return self._delegate.get_object(**kwargs)


@unittest.skipUnless(
    LOCALSTACK_ENDPOINT_URL,
    "set LOCALSTACK_ENDPOINT_URL to run the LocalStack integration test",
)
class LocalStackStorageAdapterIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - environment guard
            raise unittest.SkipTest("boto3 is required for LocalStack integration") from exc

        cls.bucket = "idea2strategy-d03-integration"
        cls.client = boto3.client(
            "s3",
            endpoint_url=LOCALSTACK_ENDPOINT_URL,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        try:
            cls.client.create_bucket(Bucket=cls.bucket)
        except cls.client.exceptions.BucketAlreadyOwnedByYou:
            pass
        # Versioning is what makes `VersionId` capture observable at all.
        cls.client.put_bucket_versioning(
            Bucket=cls.bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    def setUp(self) -> None:
        # Each test owns a fresh key space so a reused container cannot make
        # one run's objects satisfy the next run's assertions.
        self.key_prefix = f"market-data/d03/{uuid.uuid4()}"

    def make_store(self, client: Any = None) -> S3ObjectStore:
        return S3ObjectStore(
            self.bucket,
            client=client or self.client,
            max_attempts=3,
            retry_delay_seconds=0.01,
        )

    def test_local_and_localstack_publish_same_key_checksum_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.parquet"
            source.write_bytes(b"deterministic-d03-localstack-object\n")
            object_key = f"{self.key_prefix}/localstack-object.parquet"
            local = LocalObjectStore(root / "objects")
            remote = self.make_store()

            local_receipt = local.put(source, object_key)
            remote_receipt = remote.put(source, object_key)

            self.assertEqual(local_receipt.object_key, remote_receipt.object_key)
            self.assertEqual(local_receipt.content_hash, remote_receipt.content_hash)
            self.assertEqual(remote_receipt.content_hash, sha256_file(source))
            self.assertEqual(remote_receipt.byte_size, source.stat().st_size)
            with remote.open(object_key) as body:
                self.assertEqual(body.read(), source.read_bytes())

    def test_real_s3_stores_sse_checksum_and_version_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.parquet"
            source.write_bytes(b"d03-sse-and-checksum\n")
            object_key = f"{self.key_prefix}/sse-object.parquet"
            content_hash, checksum = sha256_hex_and_base64(source)

            receipt = self.make_store().put(source, object_key)

            head = self.client.head_object(
                Bucket=self.bucket,
                Key=object_key,
                ChecksumMode="ENABLED",
            )
            self.assertEqual(head["ServerSideEncryption"], "AES256")
            self.assertEqual(head["ChecksumSHA256"], checksum)
            self.assertEqual(head["Metadata"]["sha256"], content_hash)
            # A real VersionId, not the ETag fallback.
            self.assertEqual(receipt.provider_version_id, head["VersionId"])
            self.assertNotEqual(receipt.provider_version_id, receipt.etag)

    def test_real_conditional_write_conflict_is_classified_as_precondition_failed(
        self,
    ) -> None:
        """The single most important assertion in this file.

        `_is_precondition_failed` was previously proven only against the fake.
        Here the exception object is the one botocore raised from a genuine
        `PutObject` with `If-None-Match: *` against an existing key.
        """
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.parquet"
            source.write_bytes(b"d03-conditional-write-conflict\n")
            object_key = f"{self.key_prefix}/conflict.parquet"
            self.make_store().put(source, object_key)

            racing = _HeadSuppressingClient(self.client)
            with self.assertRaises(Exception) as caught:
                # Bypass the store so the raw error surfaces unchanged.
                content_hash, checksum = sha256_hex_and_base64(source)
                with source.open("rb") as body:
                    racing.put_object(
                        Bucket=self.bucket,
                        Key=object_key,
                        Body=body,
                        ContentLength=source.stat().st_size,
                        ContentType="application/vnd.apache.parquet",
                        ServerSideEncryption="AES256",
                        ChecksumAlgorithm="SHA256",
                        ChecksumSHA256=checksum,
                        IfNoneMatch="*",
                        Metadata={"sha256": content_hash},
                    )

            error = caught.exception
            self.assertEqual(
                error.response["ResponseMetadata"]["HTTPStatusCode"],  # type: ignore[attr-defined]
                412,
            )
            self.assertTrue(S3ObjectStore._is_precondition_failed(error))
            # It must not be swept into the generic retry loop.
            self.assertFalse(S3ObjectStore._is_retryable(error))
            self.assertFalse(S3ObjectStore._is_missing(error))

    def test_real_412_reconciles_to_the_identical_stored_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.parquet"
            source.write_bytes(b"d03-lost-response-object\n")
            object_key = f"{self.key_prefix}/lost-response.parquet"
            first = self.make_store().put(source, object_key)

            # The HEAD probe reports the key as absent, so the store believes
            # it must write; the real service then rejects the conditional put.
            racing = _HeadSuppressingClient(self.client)
            second = self.make_store(racing).put(source, object_key)

            self.assertEqual(racing.put_calls, 1)
            self.assertEqual(len(racing.put_errors), 1)
            self.assertTrue(S3ObjectStore._is_precondition_failed(racing.put_errors[0]))
            self.assertEqual(second.content_hash, first.content_hash)
            self.assertEqual(second.provider_version_id, first.provider_version_id)
            self.assertEqual(second.byte_size, first.byte_size)

    def test_real_412_refuses_to_reconcile_different_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = root / "original.parquet"
            original.write_bytes(b"d03-original-bytes\n")
            different = root / "different.parquet"
            different.write_bytes(b"d03-DIFFERENT-bytes-entirely\n")
            object_key = f"{self.key_prefix}/immutable.parquet"
            published = self.make_store().put(original, object_key)

            racing = _HeadSuppressingClient(self.client)
            with self.assertRaisesRegex(FileExistsError, "불변 객체"):
                self.make_store(racing).put(different, object_key)

            self.assertEqual(racing.put_calls, 1)
            # The immutable object is untouched.
            self.assertEqual(
                self.make_store().verify(object_key, published.content_hash).ok,
                True,
            )
            with self.make_store().open(object_key) as body:
                self.assertEqual(body.read(), original.read_bytes())

    def test_verify_against_real_s3_reports_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.parquet"
            source.write_bytes(b"d03-verify-object\n")
            object_key = f"{self.key_prefix}/verify.parquet"
            store = self.make_store()
            receipt = store.put(source, object_key)

            good = store.verify(object_key, receipt.content_hash)
            bad = store.verify(object_key, "0" * 64)
            missing = store.verify(f"{self.key_prefix}/absent.parquet", "0" * 64)

            self.assertTrue(good.ok)
            self.assertEqual(good.byte_size, source.stat().st_size)
            self.assertFalse(bad.ok)
            self.assertEqual(bad.message, "sha256 metadata mismatch")
            self.assertFalse(missing.ok)
            self.assertEqual(missing.message, "object missing")

    def test_prefixed_store_round_trips_against_real_s3(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.parquet"
            source.write_bytes(b"d03-prefixed-object\n")
            store = S3ObjectStore(
                self.bucket,
                client=self.client,
                prefix=self.key_prefix,
                max_attempts=3,
                retry_delay_seconds=0.01,
            )

            receipt = store.put(source, "prefixed.parquet")

            self.assertEqual(receipt.object_key, f"{self.key_prefix}/prefixed.parquet")
            self.assertTrue(store.exists("prefixed.parquet"))
            self.assertTrue(store.exists(f"{self.key_prefix}/prefixed.parquet"))
            self.client.head_object(
                Bucket=self.bucket,
                Key=f"{self.key_prefix}/prefixed.parquet",
            )


if __name__ == "__main__":
    unittest.main()
