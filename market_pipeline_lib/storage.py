"""Interchangeable immutable object-store implementations."""

from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Callable, Protocol, TypeVar, runtime_checkable

from .contracts import sha256_file


_ResultT = TypeVar("_ResultT")


@dataclass(frozen=True)
class VerificationResult:
    ok: bool
    content_hash: str
    byte_size: int
    message: str = ""


@dataclass(frozen=True)
class ObjectReceipt:
    storage_provider: str
    bucket_name: str | None
    object_key: str
    provider_version_id: str | None
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


class LocalObjectStore:
    """Publish immutable objects atomically beneath one local root."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def path_for(self, key: str) -> Path:
        normalized = key.replace("\\", "/").lstrip("/")
        candidate = (self.root / Path(normalized)).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"object_key가 저장소 루트를 벗어납니다: {key}") from exc
        return candidate

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
            temporary = destination.with_name(
                f".{destination.name}.{os.getpid()}.staged"
            )
            try:
                shutil.copyfile(source, temporary)
                if sha256_file(temporary) != content_hash:
                    raise OSError("로컬 staging 복사 후 SHA-256이 달라졌습니다.")
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
        return ObjectReceipt(
            storage_provider="LOCAL",
            bucket_name=None,
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
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds

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
        if delay:
            time.sleep(delay)

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

    def _head(self, key: str) -> dict[str, Any]:
        return self._call_with_retries(
            self.client.head_object,
            Bucket=self.bucket,
            Key=key,
        )

    def _receipt_from_head(
        self,
        *,
        key: str,
        expected_hash: str,
        expected_size: int,
        head: dict[str, Any],
        conflict: bool,
    ) -> ObjectReceipt:
        metadata = head.get("Metadata", {})
        actual_hash = metadata.get("sha256", "") if isinstance(metadata, dict) else ""
        actual_size = int(head.get("ContentLength", 0))
        if actual_hash != expected_hash or actual_size != expected_size:
            message = f"불변 객체 경로에 다른 바이트가 이미 있습니다: {key}"
            if conflict:
                raise FileExistsError(message)
            raise RuntimeError(f"S3 업로드 검증 실패: s3://{self.bucket}/{key}")
        etag = str(head.get("ETag", "")).strip('"') or None
        return ObjectReceipt(
            storage_provider="S3_COMPATIBLE",
            bucket_name=self.bucket,
            object_key=key,
            provider_version_id=head.get("VersionId") or etag,
            content_hash=actual_hash,
            byte_size=actual_size,
            etag=etag,
        )

    def put(self, source_path: Path, object_key: str) -> ObjectReceipt:
        source = source_path.expanduser().resolve()
        if not source.is_file():
            raise FileNotFoundError(f"publish할 파일이 없습니다: {source}")
        content_hash = sha256_file(source)
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
                head=existing,
                conflict=True,
            )

        for attempt in range(1, self.max_attempts + 1):
            try:
                # Reopen the body for every attempt. A failed SDK call may have
                # consumed the stream even when the response never arrived.
                with source.open("rb") as body:
                    self.client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=body,
                        ContentLength=byte_size,
                        ContentType="application/vnd.apache.parquet",
                        IfNoneMatch="*",
                        Metadata={"sha256": content_hash},
                    )
                break
            except Exception as exc:
                if self._is_precondition_failed(exc):
                    raced = self._head(key)
                    return self._receipt_from_head(
                        key=key,
                        expected_hash=content_hash,
                        expected_size=byte_size,
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
            head=self._head(key),
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
        return body

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
