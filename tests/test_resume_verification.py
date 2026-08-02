import hashlib
import tempfile
import unittest
from pathlib import Path

from market_pipeline_lib.resume_verification import (
    ResumeFingerprintMismatch,
    ResumeVerdict,
    require_matching_fingerprint,
    verify_resumable_file,
    verify_resumable_files,
)

PAYLOAD = b"PAR1" + b"complete staging fragment payload" * 8


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class VerifyResumableFileTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.root = Path(self._directory.name)

    def write(self, name: str, payload: bytes) -> Path:
        path = self.root / name
        path.write_bytes(payload)
        return path

    def test_matching_hash_is_accepted(self):
        path = self.write("complete.parquet", PAYLOAD)

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(PAYLOAD),
            expected_byte_size=len(PAYLOAD),
        )

        self.assertIsInstance(verdict, ResumeVerdict)
        self.assertTrue(verdict.resumable)
        self.assertTrue(verdict)
        self.assertEqual(verdict.reason, "")
        self.assertEqual(verdict.path, path)

    def test_mismatched_hash_is_rejected(self):
        path = self.write("rewritten.parquet", PAYLOAD)
        other = PAYLOAD[:-1] + b"X"
        self.assertEqual(len(other), len(PAYLOAD))

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(other),
            expected_byte_size=len(PAYLOAD),
        )

        self.assertFalse(verdict.resumable)
        self.assertFalse(verdict)
        self.assertEqual(verdict.reason, "HASH_MISMATCH")

    def test_truncated_file_is_rejected_before_hashing(self):
        truncated = PAYLOAD[: len(PAYLOAD) // 2]
        path = self.write("truncated.parquet", truncated)

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(PAYLOAD),
            expected_byte_size=len(PAYLOAD),
        )

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "SIZE_MISMATCH")
        self.assertEqual(verdict.actual_byte_size, len(truncated))
        # A short read must not be reported as a hash match by accident.
        self.assertNotEqual(verdict.actual_sha256, sha256_bytes(PAYLOAD))
        self.assertEqual(verdict.actual_sha256, "")

    def test_zero_byte_file_is_rejected_without_expected_size(self):
        path = self.write("empty.parquet", b"")

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(b""),
        )

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "EMPTY")

    def test_file_below_minimum_byte_size_is_rejected(self):
        path = self.write("stub.parquet", b"PAR1")

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(b"PAR1"),
            minimum_byte_size=64,
        )

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "TOO_SMALL")

    def test_missing_file_is_rejected(self):
        verdict = verify_resumable_file(
            self.root / "never-written.parquet",
            expected_sha256=sha256_bytes(PAYLOAD),
        )

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "MISSING")
        self.assertEqual(verdict.actual_byte_size, 0)

    def test_directory_in_place_of_file_is_rejected(self):
        directory = self.root / "shard=s00-of-2"
        directory.mkdir()

        verdict = verify_resumable_file(
            directory,
            expected_sha256=sha256_bytes(PAYLOAD),
        )

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "MISSING")

    def test_blank_expected_hash_is_rejected(self):
        path = self.write("unhashed.parquet", PAYLOAD)

        verdict = verify_resumable_file(path, expected_sha256="")

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "NO_EXPECTED_HASH")

    def test_hash_comparison_is_case_insensitive(self):
        path = self.write("upper.parquet", PAYLOAD)

        verdict = verify_resumable_file(
            path,
            expected_sha256=sha256_bytes(PAYLOAD).upper(),
        )

        self.assertTrue(verdict.resumable)


class VerifyResumableFilesTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.root = Path(self._directory.name)

    def test_all_intact_fragments_are_accepted(self):
        entries = []
        for index in range(3):
            payload = PAYLOAD + bytes([index])
            path = self.root / f"part-{index}.parquet"
            path.write_bytes(payload)
            entries.append((path, sha256_bytes(payload), len(payload)))

        verdict = verify_resumable_files(entries)

        self.assertTrue(verdict.resumable)
        self.assertEqual(verdict.reason, "")

    def test_one_truncated_fragment_rejects_the_whole_set(self):
        entries = []
        for index in range(3):
            payload = PAYLOAD + bytes([index])
            path = self.root / f"part-{index}.parquet"
            path.write_bytes(payload if index != 1 else payload[:10])
            entries.append((path, sha256_bytes(payload), len(payload)))

        verdict = verify_resumable_files(entries)

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "SIZE_MISMATCH")
        self.assertEqual(verdict.path, self.root / "part-1.parquet")

    def test_empty_entry_set_is_not_resumable(self):
        verdict = verify_resumable_files([])

        self.assertFalse(verdict.resumable)
        self.assertEqual(verdict.reason, "NO_RECORDED_OBJECTS")


class RequireMatchingFingerprintTests(unittest.TestCase):
    def test_identical_fingerprints_pass(self):
        require_matching_fingerprint(
            "a" * 64,
            expected="a" * 64,
            location="staging/shard=s00-of-2/state.json",
        )

    def test_different_fingerprint_raises(self):
        with self.assertRaises(ResumeFingerprintMismatch) as caught:
            require_matching_fingerprint(
                "a" * 64,
                expected="b" * 64,
                location="staging/shard=s00-of-2/state.json",
            )

        self.assertIn("staging/shard=s00-of-2/state.json", str(caught.exception))

    def test_absent_recorded_fingerprint_raises(self):
        with self.assertRaises(ResumeFingerprintMismatch):
            require_matching_fingerprint(
                None,
                expected="b" * 64,
                location="staging/shard=s00-of-2/state.json",
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
