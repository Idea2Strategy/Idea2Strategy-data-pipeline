"""Persistent, atomic checkpoints for the daily market-data pipeline."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = PROJECT_ROOT / "pipeline_state" / "daily_pipeline_state.json"
STATE_REPLACE_ATTEMPTS = 6
STATE_REPLACE_RETRY_SECONDS = 0.1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def replace_with_retry(
    source: Path,
    destination: Path,
    attempts: int = STATE_REPLACE_ATTEMPTS,
    retry_seconds: float = STATE_REPLACE_RETRY_SECONDS,
) -> None:
    """Replace a state file while tolerating brief Windows file locks."""
    if attempts < 1:
        raise ValueError("attempts must be positive")

    for attempt in range(attempts):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(retry_seconds * (2**attempt))


class PipelineStateStore:
    def __init__(self, path: Path = DEFAULT_STATE_PATH) -> None:
        self.path = path
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"version": 1, "checkpoints": {}}
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"파이프라인 상태 파일을 읽을 수 없습니다: {exc}") from exc
        if not isinstance(loaded, dict) or loaded.get("version") != 1:
            raise RuntimeError("지원하지 않는 파이프라인 상태 파일 형식입니다.")
        loaded.setdefault("checkpoints", {})
        return loaded

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.tmp"
        )
        self.data["updated_at_utc"] = utc_now_iso()
        try:
            temporary_path.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            replace_with_retry(temporary_path, self.path)
        finally:
            temporary_path.unlink(missing_ok=True)

    def begin_run(self, storage_format: str, target_session_utc: str) -> None:
        self.data["last_run"] = {
            "status": "running",
            "storage_format": storage_format,
            "target_session_utc": target_session_utc,
            "started_at_utc": utc_now_iso(),
        }
        self.save()

    def finish_run(self, status: str, failures: list[dict[str, Any]]) -> None:
        run = self.data.setdefault("last_run", {})
        run["status"] = status
        run["finished_at_utc"] = utc_now_iso()
        run["failure_count"] = len(failures)
        self.data["last_failures"] = failures
        self.save()

    def _entry(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        create: bool = False,
    ) -> dict[str, Any]:
        checkpoints = self.data.setdefault("checkpoints", {})
        if create:
            return (
                checkpoints.setdefault(storage_format, {})
                .setdefault(symbol, {})
                .setdefault(stage, {})
            )
        return (
            checkpoints.get(storage_format, {})
            .get(symbol, {})
            .get(stage, {})
        )

    def is_complete(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        target_session_utc: str,
        output_path: Path,
    ) -> bool:
        entry = self._entry(storage_format, symbol, stage)
        return (
            output_path.is_file()
            and entry.get("status") == "success"
            and entry.get("target_session_utc") == target_session_utc
        )

    def is_complete_outputs(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        target_session_utc: str,
        output_paths: list[Path],
    ) -> bool:
        """Require every output of a multi-file stage before it may be skipped."""
        entry = self._entry(storage_format, symbol, stage)
        return (
            bool(output_paths)
            and all(path.is_file() for path in output_paths)
            and entry.get("status") == "success"
            and entry.get("target_session_utc") == target_session_utc
        )

    def mark_stage(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        status: str,
        target_session_utc: str,
        attempt: int,
        error: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        entry = self._entry(storage_format, symbol, stage, create=True)
        entry.update(
            {
                "status": status,
                "target_session_utc": target_session_utc,
                "attempt": attempt,
                "updated_at_utc": utc_now_iso(),
                "error": error,
            }
        )
        if details:
            entry.update(details)
        self.save()

    @staticmethod
    def partition_key(
        pipeline: str,
        layer: str,
        resolution: str,
        partition_start: str,
        partition_end: str,
        shard_key: str,
        revision: int,
    ) -> str:
        """Return the DB-aligned immutable partition checkpoint key."""
        if revision <= 0:
            raise ValueError("revision must be positive")
        return "|".join(
            (
                pipeline,
                layer,
                resolution,
                partition_start,
                partition_end,
                shard_key,
                str(revision),
            )
        )

    def is_partition_complete(
        self,
        pipeline: str,
        layer: str,
        resolution: str,
        partition_start: str,
        partition_end: str,
        shard_key: str,
        revision: int,
        *,
        expected_content_hash: str | None = None,
    ) -> bool:
        key = self.partition_key(
            pipeline,
            layer,
            resolution,
            partition_start,
            partition_end,
            shard_key,
            revision,
        )
        entry = self.data.setdefault("partition_checkpoints", {}).get(key, {})
        return (
            entry.get("status") == "success"
            and (
                expected_content_hash is None
                or entry.get("content_hash") == expected_content_hash
            )
        )

    def mark_partition(
        self,
        pipeline: str,
        layer: str,
        resolution: str,
        partition_start: str,
        partition_end: str,
        shard_key: str,
        revision: int,
        *,
        status: str,
        object_ids: list[str] | None = None,
        content_hash: str | None = None,
        error: str = "",
    ) -> None:
        key = self.partition_key(
            pipeline,
            layer,
            resolution,
            partition_start,
            partition_end,
            shard_key,
            revision,
        )
        self.data.setdefault("partition_checkpoints", {})[key] = {
            "status": status,
            "object_ids": object_ids or [],
            "content_hash": content_hash,
            "error": error,
            "updated_at_utc": utc_now_iso(),
        }
        self.save()

