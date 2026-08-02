"""Interchangeable immutable object-store implementations."""

from __future__ import annotations

import base64
import hashlib
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TypeVar, cast, runtime_checkable

from .contracts import sha256_file
from .fs_paths import long_path, short_temp_path
from .rate_limit import SYSTEM_CLOCK, Clock


_ResultT = TypeVar("_ResultT")

# SSE-S3. The adapter both requests it on write and refuses to hand back a
# receipt for an object whose HEAD does not report it, so an unencrypted
# object can never be registered in `storage.objects` as published.
SSE_ALGORITHM = "AES256"


def sha256_hex_and_base64(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, str]:
    """Return (hex, base64) SHA-256 digests of one file in a single read.

    The hex form goes into user metadata (`sha256`) and the receipt; the
    base64 form is what S3's `ChecksumSHA256` header takes, which makes the
    service itself verify the body it received.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest(), base64.b64encode(digest.digest()).decode("ascii")


@dataclass(frozen=True)
class VerificationResult:
    ok: bool
    content_hash: str
    byte_size: int
    message: str = ""


@dataclass(frozen=True)
class ObjectReceipt:
    #: Both fields feed `storage.objects`, where they are NOT NULL, so neither adapter
    #: may leave them unset; see `LOCAL_BUCKET_NAME`.
    storage_provider: str
    bucket_name: str
    object_key: str
    provider_version_id: str
    content_hash: str
    byte_size: int
    local_path: str | None = None
    etag: str | None = None


@runtime_checkable
class ObjectStore(Protocol):
    def put(self, source_path: Path, object_key: str) -> ObjectReceipt: ...

    def exists(self, object_key: str) -> bool: ...

    def open(self, object_key: str) -> BinaryIO: ...

    def verify(
        self,
        object_key: str,
        expected_sha256: str,
    ) -> VerificationResult: ...


#: `storage.objects.bucket_name` is ``varchar(160) NOT NULL`` in the applied baseline, so
#: every published object needs one, including objects published to a filesystem.
#:
#: A constant is the correct value rather than a placeholder.  The uniqueness index is
#: ``(storage_provider, bucket_name, object_key, provider_version_id)`` and
#: `LocalObjectStore` sets ``provider_version_id`` to the content hash, so two rows that
#: collide under this name have the same key *and* the same bytes -- they are the same
#: object, however many local roots produced it.  A root-derived name would instead
#: register one object under N different identities as soon as two checkouts were applied
#: to one database.  `bucket_name` is still a constructor argument for a deployment that
#: genuinely runs separate local namespaces against a shared catalog.
LOCAL_BUCKET_NAME = "local"


class LocalObjectStore:
    """Publish immutable objects atomically beneath one local root."""

    def __init__(self, root: Path, *, bucket_name: str = LOCAL_BUCKET_NAME) -> None:
        if not bucket_name.strip():
            raise ValueError("bucket_name은 비어 있을 수 없습니다.")
        self.root = root.expanduser().resolve()
        self.bucket_name = bucket_name

    def path_for(self, key: str) -> Path:
        normalized = key.replace("\\", "/").lstrip("/")
        candidate = (self.root / Path(normalized)).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"object_key가 저장소 루트를 벗어납니다: {key}") from exc
        # Canonical object keys are deep by contract, so the absolute path can
        # exceed the Windows MAX_PATH before the file name is even appended.
        # Containment is checked above, on the plain path, before switching to
        # the extended-length form used for the actual filesystem calls.
        return Path(long_path(candidate))

    def put(self, source_path: Path, object_key: str) -> ObjectReceipt:
        source = source_path.expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(f"publish할 파일이 없습니다: {source}")
        content_hash = sha256_file(source)
        byte_size = source.stat().st_size
        destination = self.path_for(object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            result = self.verify(object_key, content_hash)
            if not result.ok:
                raise FileExistsError(
                    f"불변 객체 경로에 다른 바이트가 이미 있습니다: {object_key}"
                )
        else:
            temporary = short_temp_path(destination)
            try:
                shutil.copyfile(source, temporary)
                if sha256_file(temporary) != content_hash:
                    raise OSError("로컬 staging 복사 후 SHA-256이 달라졌습니다.")
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
        return ObjectReceipt(
            storage_provider="LOCAL",
            bucket_name=self.bucket_name,
            object_key=object_key,
            provider_version_id=content_hash,
            content_hash=content_hash,
            byte_size=byte_size,
            local_path=str(destination),
        )

    def exists(self, object_key: str) -> bool:
        return self.path_for(object_key).is_file()

    def open(self, object_key: str) -> BinaryIO:
        return self.path_for(object_key).open("rb")

    def verify(
        self,
        object_key: str,
        expected_sha256: str,
    ) -> VerificationResult:
        path = self.path_for(object_key)
        if not path.is_file():
            return VerificationResult(False, "", 0, "object missing")
        actual = sha256_file(path)
        return VerificationResult(
            actual == expected_sha256,
            actual,
            path.stat().st_size,
            "" if actual == expected_sha256 else "sha256 mismatch",
        )


class S3ObjectStore:
    """S3-compatible implementation with receipt and SHA metadata verification."""

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        endpoint_url: str | None = None,
        client: object | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 0.1,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        if not bucket:
            raise ValueError("S3 bucket이 필요합니다.")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")
        if client is None:
            try:
                import boto3
            except ImportError as exc:
                raise RuntimeError(
                    "S3ObjectStore에는 optional dependency boto3가 필요합니다."
                ) from exc
            client = boto3.client("s3", endpoint_url=endpoint_url)
        # boto3 clients are generated at runtime and carry no static type, so
        # the attribute is `Any` rather than the `object` the parameter takes.
        self.client: Any = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.clock = clock

    def _key(self, object_key: str) -> str:
        normalized = object_key.lstrip("/")
        if self.prefix and normalized.startswith(f"{self.prefix}/"):
            return normalized
        return "/".join(
            part for part in (self.prefix, normalized) if part
        )

    @staticmethod
    def _error_details(exc: Exception) -> tuple[str, int | None]:
        response = getattr(exc, "response", {})
        if not isinstance(response, dict):
            return "", None
        error = response.get("Error", {})
        metadata = response.get("ResponseMetadata", {})
        code = str(error.get("Code", "")) if isinstance(error, dict) else ""
        status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        return code, status if isinstance(status, int) else None

    @classmethod
    def _is_missing(cls, exc: Exception) -> bool:
        code, status = cls._error_details(exc)
        return status == 404 or code in {"404", "NoSuchKey", "NotFound"}

    @classmethod
    def _is_precondition_failed(cls, exc: Exception) -> bool:
        code, status = cls._error_details(exc)
        return status == 412 or code in {"412", "PreconditionFailed"}

    @classmethod
    def _is_retryable(cls, exc: Exception) -> bool:
        code, status = cls._error_details(exc)
        return status in {429, 500, 502, 503, 504} or code in {
            "429",
            "500",
            "502",
            "503",
            "504",
            "InternalError",
            "RequestTimeout",
            "RequestTimeoutException",
            "ServiceUnavailable",
            "SlowDown",
            "Throttling",
            "ThrottlingException",
        }

    def _retry_delay(self, attempt: int) -> None:
        delay = self.retry_delay_seconds * (2 ** (attempt - 1))
        if delay > 0:
            self.clock.sleep(delay)

    def _call_with_retries(
        self,
        operation: Callable[..., _ResultT],
        **kwargs: Any,
    ) -> _ResultT:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation(**kwargs)
            except Exception as exc:
                if attempt == self.max_attempts or not self._is_retryable(exc):
                    raise
                self._retry_delay(attempt)
        raise AssertionError("retry loop must return or raise")

    def _head(self, key: str, *, version_id: str | None = None) -> dict[str, Any]:
        # `ChecksumMode="ENABLED"` is what makes S3 return `ChecksumSHA256`;
        # without it the service-side checksum can never be re-verified.
        parameters: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "ChecksumMode": "ENABLED",
        }
        if version_id:
            parameters["VersionId"] = version_id
        return self._call_with_retries(self.client.head_object, **parameters)

    def _receipt_from_head(
        self,
        *,
        key: str,
        expected_hash: str,
        expected_size: int,
        expected_checksum: str,
        head: dict[str, Any],
        conflict: bool,
    ) -> ObjectReceipt:
        """Turn a verified HEAD into a receipt, or refuse to issue one.

        Four independent facts are checked before an object counts as
        published: the sha256 recorded in user metadata, the byte size, the
        SSE-S3 state, and — when the service reports one — the server-computed
        `ChecksumSHA256`. A mismatch on the first two under `conflict` means a
        *different* object already owns the immutable key; anything else means
        our own write cannot be trusted.
        """
        metadata = head.get("Metadata", {})
        actual_hash = metadata.get("sha256", "") if isinstance(metadata, dict) else ""
        actual_size = int(head.get("ContentLength", 0))
        if actual_hash != expected_hash or actual_size != expected_size:
            if conflict:
                raise FileExistsError(f"불변 객체 경로에 다른 바이트가 이미 있습니다: {key}")
            raise RuntimeError(f"S3 업로드 검증 실패: s3://{self.bucket}/{key}")
        if head.get("ServerSideEncryption") != SSE_ALGORITHM:
            raise RuntimeError(
                f"SSE-S3({SSE_ALGORITHM}) 암호화가 확인되지 않았습니다: s3://{self.bucket}/{key}"
            )
        actual_checksum = head.get("ChecksumSHA256")
        if actual_checksum is not None and actual_checksum != expected_checksum:
            raise RuntimeError(
                f"S3 ChecksumSHA256이 일치하지 않습니다: s3://{self.bucket}/{key}"
            )
        etag = str(head.get("ETag", "")).strip('"') or None
        provider_version_id = head.get("VersionId") or etag
        if not provider_version_id:
            # `storage.objects.provider_version_id` is NOT NULL and is half of the
            # immutability key.  An object the service will not identify cannot be
            # registered as published.
            raise RuntimeError(
                f"S3가 VersionId도 ETag도 반환하지 않아 객체를 식별할 수 없습니다: s3://{self.bucket}/{key}"
            )
        return ObjectReceipt(
            storage_provider="S3_COMPATIBLE",
            bucket_name=self.bucket,
            object_key=key,
            provider_version_id=str(provider_version_id),
            content_hash=actual_hash,
            byte_size=actual_size,
            etag=etag,
        )

    def put(self, source_path: Path, object_key: str) -> ObjectReceipt:
        source = source_path.expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(f"publish할 파일이 없습니다: {source}")
        content_hash, checksum = sha256_hex_and_base64(source)
        byte_size = source.stat().st_size
        key = self._key(object_key)
        try:
            existing = self._head(key)
        except Exception as exc:
            if not self._is_missing(exc):
                raise
        else:
            return self._receipt_from_head(
                key=key,
                expected_hash=content_hash,
                expected_size=byte_size,
                expected_checksum=checksum,
                head=existing,
                conflict=True,
            )

        version_id: str | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                # Reopen the body for every attempt. A failed SDK call may have
                # consumed the stream even when the response never arrived.
                with source.open("rb") as body:
                    response = self.client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=body,
                        ContentLength=byte_size,
                        ContentType="application/vnd.apache.parquet",
                        ServerSideEncryption=SSE_ALGORITHM,
                        ChecksumAlgorithm="SHA256",
                        ChecksumSHA256=checksum,
                        IfNoneMatch="*",
                        Metadata={"sha256": content_hash},
                    )
                version_id = (
                    response.get("VersionId") if isinstance(response, dict) else None
                )
                break
            except Exception as exc:
                if self._is_precondition_failed(exc):
                    # Either a concurrent writer won the race or our own
                    # earlier attempt landed and lost its response. Both are
                    # reconciled the same way: read back what is actually
                    # stored and accept it only if it is byte-identical.
                    raced = self._head(key)
                    return self._receipt_from_head(
                        key=key,
                        expected_hash=content_hash,
                        expected_size=byte_size,
                        expected_checksum=checksum,
                        head=raced,
                        conflict=True,
                    )
                if attempt == self.max_attempts or not self._is_retryable(exc):
                    raise
                self._retry_delay(attempt)
        return self._receipt_from_head(
            key=key,
            expected_hash=content_hash,
            expected_size=byte_size,
            expected_checksum=checksum,
            # Pin the read to the version we just wrote where the bucket is
            # versioned, so a concurrent overwrite cannot be mistaken for ours.
            head=self._head(key, version_id=version_id),
            conflict=False,
        )

    def exists(self, object_key: str) -> bool:
        try:
            self._head(self._key(object_key))
            return True
        except Exception as exc:
            if self._is_missing(exc):
                return False
            raise

    def open(self, object_key: str) -> BinaryIO:
        body = self._call_with_retries(
            self.client.get_object,
            Bucket=self.bucket,
            Key=self._key(object_key),
        )["Body"]
        return cast(BinaryIO, body)

    def verify(
        self,
        object_key: str,
        expected_sha256: str,
    ) -> VerificationResult:
        try:
            head = self._head(self._key(object_key))
        except Exception as exc:
            if self._is_missing(exc):
                return VerificationResult(False, "", 0, "object missing")
            raise
        actual = head.get("Metadata", {}).get("sha256", "")
        return VerificationResult(
            actual == expected_sha256,
            actual,
            int(head["ContentLength"]),
            "" if actual == expected_sha256 else "sha256 metadata mismatch",
        )
