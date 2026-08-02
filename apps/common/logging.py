"""Structured JSON logging with credential redaction.

Modelled on `idea2strategy-market-loader/src/market_loader/logging.py` so both
codebases emit the same shape; that module belongs to the loader project and is
never imported or edited from here.

Nothing that matches `SENSITIVE_KEYS` is ever written to a log stream, and
connection strings embedded in free text have their password component removed.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any, ClassVar, TextIO

SENSITIVE_KEYS = re.compile(
    r"(secret|password|passwd|credential|access.?key|api.?key|authorization|token|dsn|"
    r"session.?key|private.?key|connection.?string)",
    re.IGNORECASE,
)

_INLINE_SECRET = re.compile(r"(?i)\b(password|secret|token|api[_-]?key)=([^&\s]+)")
_DSN_PASSWORD = re.compile(r"(?i)((?:postgres(?:ql)?|redis|amqp|mysql)://[^:/\s]+:)[^@\s]+@")
_AWS_ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")

REDACTED = "***"


def redact(value: Any, key: str = "") -> Any:
    """Return `value` with every credential-shaped element replaced."""

    if SENSITIVE_KEYS.search(key):
        return REDACTED
    if isinstance(value, dict):
        return {item_key: redact(item_value, str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        value = _INLINE_SECRET.sub(r"\1=" + REDACTED, value)
        value = _DSN_PASSWORD.sub(r"\1" + REDACTED + "@", value)
        value = _AWS_ACCESS_KEY.sub(REDACTED, value)
    return value


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with the pipeline correlation fields pinned."""

    _standard: ClassVar[set[str]] = set(logging.makeLogRecord({}).__dict__) | {"taskName"}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            "run_id": None,
            "command_id": None,
            "message_id": None,
            "attempt": None,
            "duration_ms": None,
        }
        for key, value in record.__dict__.items():
            if key not in self._standard and key not in {"message", "asctime"}:
                payload[key] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception_type"] = record.exc_info[0].__name__
            payload["exception_message"] = str(record.exc_info[1])
        redacted = redact(payload)
        return json.dumps(redacted, separators=(",", ":"), default=str, ensure_ascii=False)


def configure_logging(level: str = "INFO", *, stream: TextIO | None = None) -> None:
    """Install the JSON formatter on the root logger."""

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
