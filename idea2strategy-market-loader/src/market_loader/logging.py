from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any, ClassVar

SENSITIVE_KEYS = re.compile(
    r"(secret|password|passwd|credential|access.?key|api.?key|authorization|dsn)",
    re.IGNORECASE,
)


def redact(value: Any, key: str = "") -> Any:
    if SENSITIVE_KEYS.search(key):
        return "***"
    if isinstance(value, dict):
        return {item_key: redact(item_value, item_key) for item_key, item_value in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r"(?i)(password|secret|token)=([^&\s]+)", r"\1=***", value)
        value = re.sub(r"(?i)(postgres(?:ql)?://[^:]+:)[^@]+@", r"\1***@", value)
    return value


class JsonFormatter(logging.Formatter):
    _standard: ClassVar[set[str]] = set(logging.makeLogRecord({}).__dict__)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
            "run_id": None,
            "partition_key": None,
            "manifest_id": None,
            "attempt": None,
            "duration_ms": None,
        }
        for key, value in record.__dict__.items():
            if key not in self._standard and key not in {"message", "asctime"}:
                payload[key] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(redact(payload), separators=(",", ":"), default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
