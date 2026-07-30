from __future__ import annotations

import json
import logging

from market_loader.logging import JsonFormatter, redact


def test_secret_values_are_redacted() -> None:
    payload = redact(
        {
            "ALPACA_API_SECRET": "top-secret",
            "PGPASSWORD": "password",
            "url": "postgresql://user:password@localhost/database",
        }
    )
    assert payload["ALPACA_API_SECRET"] == "***"  # noqa: S105
    assert payload["PGPASSWORD"] == "***"
    assert "password@" not in payload["url"]


def test_json_formatter_has_required_common_fields() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "event-name", (), None)
    payload = json.loads(JsonFormatter().format(record))
    assert payload["event"] == "event-name"
    for field in (
        "timestamp",
        "level",
        "run_id",
        "partition_key",
        "manifest_id",
        "attempt",
        "duration_ms",
    ):
        assert field in payload
