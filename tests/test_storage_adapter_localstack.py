"""LocalStack-backed integration coverage for the immutable S3 adapter."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from market_pipeline_lib.contracts import sha256_file
from market_pipeline_lib.storage import LocalObjectStore, S3ObjectStore


LOCALSTACK_ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT_URL")


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

    def test_local_and_localstack_publish_same_key_checksum_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.parquet"
            source.write_bytes(b"deterministic-d03-localstack-object\n")
            object_key = "market-data/d03/localstack-object.parquet"
            local = LocalObjectStore(root / "objects")
            remote = S3ObjectStore(
                self.bucket,
                client=self.client,
                max_attempts=3,
                retry_delay_seconds=0.01,
            )

            local_receipt = local.put(source, object_key)
            remote_receipt = remote.put(source, object_key)

            self.assertEqual(local_receipt.object_key, remote_receipt.object_key)
            self.assertEqual(local_receipt.content_hash, remote_receipt.content_hash)
            self.assertEqual(remote_receipt.content_hash, sha256_file(source))
            head = self.client.head_object(Bucket=self.bucket, Key=object_key)
            self.assertEqual(head["Metadata"]["sha256"], local_receipt.content_hash)
            with remote.open(object_key) as body:
                self.assertEqual(body.read(), source.read_bytes())


if __name__ == "__main__":
    unittest.main()
