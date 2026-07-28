"""Interchangeable immutable object-store implementations."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol, runtime_checkable

from .contracts import sha256_file


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
    ) -> None:
        if not bucket:
            raise ValueError("S3 bucket이 필요합니다.")
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

    def _key(self, object_key: str) -> str:
        normalized = object_key.lstrip("/")
        if self.prefix and normalized.startswith(f"{self.prefix}/"):
            return normalized
        return "/".join(
            part for part in (self.prefix, normalized) if part
        )

    def put(self, source_path: Path, object_key: str) -> ObjectReceipt:
        source = source_path.expanduser().resolve()
        content_hash = sha256_file(source)
        byte_size = source.stat().st_size
        key = self._key(object_key)
        self.client.upload_file(
            str(source),
            self.bucket,
            key,
            ExtraArgs={"Metadata": {"sha256": content_hash}},
        )
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        remote_hash = head.get("Metadata", {}).get("sha256", "")
        if int(head["ContentLength"]) != byte_size or remote_hash != content_hash:
            raise RuntimeError(f"S3 업로드 검증 실패: s3://{self.bucket}/{key}")
        return ObjectReceipt(
            storage_provider="S3_COMPATIBLE",
            bucket_name=self.bucket,
            object_key=key,
            provider_version_id=(
                head.get("VersionId")
                or str(head.get("ETag", "")).strip('"')
                or None
            ),
            content_hash=content_hash,
            byte_size=byte_size,
            etag=str(head.get("ETag", "")).strip('"') or None,
        )

    def exists(self, object_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(object_key))
            return True
        except Exception:
            return False

    def open(self, object_key: str) -> BinaryIO:
        body = self.client.get_object(
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
            head = self.client.head_object(
                Bucket=self.bucket,
                Key=self._key(object_key),
            )
        except Exception as exc:
            return VerificationResult(False, "", 0, str(exc))
        actual = head.get("Metadata", {}).get("sha256", "")
        return VerificationResult(
            actual == expected_sha256,
            actual,
            int(head["ContentLength"]),
            "" if actual == expected_sha256 else "sha256 metadata mismatch",
        )
