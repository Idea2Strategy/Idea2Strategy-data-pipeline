"""Environment configuration for `pipeline-worker`.

Every variable the process reads is declared here and documented in
`ENVIRONMENT_VARIABLES`.  Required variables have **no default**: an absent or
blank value aborts boot with `ConfigurationError` naming every missing variable
at once.  A silently defaulted queue URL or catalog root is exactly the class of
defect this bundle is correcting.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.common.errors import ConfigurationError

#: Variables that must be present and non-blank before the worker boots.
REQUIRED_ENVIRONMENT_VARIABLES: tuple[str, ...] = (
    "PIPELINE_WORKER_ENVIRONMENT",
    "PIPELINE_WORKER_MESSAGE_SOURCE",
    "PIPELINE_WORKER_CATALOG_ROOT",
    "PIPELINE_WORKER_OBJECT_STORE_ROOT",
)

#: Message-source adapters this app knows about.
MESSAGE_SOURCES: tuple[str, ...] = ("inprocess", "sqs")

#: name -> (required?, default, description).  Rendered by ``--print-config-help``.
ENVIRONMENT_VARIABLES: tuple[tuple[str, bool, str, str], ...] = (
    (
        "PIPELINE_WORKER_ENVIRONMENT",
        True,
        "",
        "Deployment environment label (local, dev, stage, prod). Emitted on every log line.",
    ),
    (
        "PIPELINE_WORKER_MESSAGE_SOURCE",
        True,
        "",
        "Message-source adapter: 'inprocess' (works today) or 'sqs' (DP5, not yet implemented).",
    ),
    (
        "PIPELINE_WORKER_CATALOG_ROOT",
        True,
        "",
        "Filesystem root of the JSONL market-data catalog the worker validates and updates.",
    ),
    (
        "PIPELINE_WORKER_OBJECT_STORE_ROOT",
        True,
        "",
        "Filesystem root of the local immutable object store backing storage.objects.",
    ),
    (
        "PIPELINE_WORKER_QUEUE_URL",
        False,
        "",
        "SQS queue URL. Required when PIPELINE_WORKER_MESSAGE_SOURCE=sqs; rejected otherwise.",
    ),
    (
        "PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL",
        False,
        "",
        "SQS dead-letter queue URL. Required when PIPELINE_WORKER_MESSAGE_SOURCE=sqs: without "
        "somewhere to park a poison message the worker would retry it forever.",
    ),
    (
        "PIPELINE_WORKER_AWS_ENDPOINT_URL",
        False,
        "",
        "Override the AWS endpoint (LocalStack, VPC endpoint). Empty means the real AWS endpoint.",
    ),
    (
        "PIPELINE_WORKER_AWS_REGION",
        False,
        "us-east-1",
        "AWS region for the SQS client.",
    ),
    (
        "PIPELINE_WORKER_MAX_RECEIVE_COUNT",
        False,
        "5",
        "Deliveries a message may fail before it is parked on the dead-letter queue. >= 1.",
    ),
    (
        "PIPELINE_WORKER_VISIBILITY_TIMEOUT_SECONDS",
        False,
        "60",
        "Seconds a received SQS message stays invisible to other consumers. 0..43200.",
    ),
    (
        "PIPELINE_WORKER_HEALTH_HOST",
        False,
        "127.0.0.1",
        "Interface the readiness HTTP endpoint binds to.",
    ),
    (
        "PIPELINE_WORKER_HEALTH_PORT",
        False,
        "",
        "When set, serve GET /health (liveness) and GET /ready (readiness) on this port. "
        "0 picks an ephemeral port, which is what the test suite uses.",
    ),
    (
        "PIPELINE_WORKER_REALTIME_INGEST",
        False,
        "",
        "JSON object configuring the D12/D90 realtime bar consumer. Required keys: "
        "instrument_map_path, price_type, data_layer, resolution, event_type, "
        "source_provider, source_feed, source_resolution, partition_granularity, "
        "shard_count, staging_root, value_fields. "
        "Absent means INGEST_REALTIME_BARS commands are refused rather than half-handled.",
    ),
    (
        "PIPELINE_WORKER_LOG_LEVEL",
        False,
        "INFO",
        "Root log level: DEBUG, INFO, WARNING, ERROR or CRITICAL.",
    ),
    (
        "PIPELINE_WORKER_POLL_INTERVAL_SECONDS",
        False,
        "1.0",
        "Long-poll wait per receive cycle, in seconds. Must be > 0.",
    ),
    (
        "PIPELINE_WORKER_MAX_MESSAGES_PER_POLL",
        False,
        "10",
        "Maximum messages fetched per receive cycle. 1..10, matching the SQS ceiling.",
    ),
    (
        "PIPELINE_WORKER_RETRY_DELAY_SECONDS",
        False,
        "30.0",
        "Delay before a failed message becomes visible again. Must be >= 0.",
    ),
    (
        "PIPELINE_WORKER_SHUTDOWN_GRACE_SECONDS",
        False,
        "30.0",
        "Time allowed to finish in-flight work after SIGINT/SIGTERM. Must be >= 0.",
    ),
    (
        "PIPELINE_WORKER_IDEMPOTENCY_CACHE_SIZE",
        False,
        "10000",
        "Number of recently handled command ids retained for duplicate suppression.",
    ),
    (
        "PIPELINE_WORKER_EXIT_AFTER_IDLE_POLLS",
        False,
        "0",
        "Exit successfully after this many consecutive empty polls. 0 keeps the worker "
        "long-running; a positive value supports desired-zero ECS RunTask execution.",
    ),
    (
        "PIPELINE_WORKER_HEALTH_FILE",
        False,
        "",
        "Optional path. When set, a readiness JSON document is written there while ready "
        "and removed on shutdown, so a container probe can read it.",
    ),
)


def _blank(value: str | None) -> bool:
    return value is None or not value.strip()


def _require_float(values: Mapping[str, str], name: str, default: str, *, minimum: float) -> float:
    raw = values.get(name) or default
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from exc
    if parsed < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}, got {parsed}")
    return parsed


def _require_int(
    values: Mapping[str, str], name: str, default: str, *, minimum: int, maximum: int
) -> int:
    raw = values.get(name) or default
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc
    if not minimum <= parsed <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}, got {parsed}")
    return parsed


#: Keys `PIPELINE_WORKER_REALTIME_INGEST` must carry.  There is no default for any of
#: them: `PT1M`/`close` being assumed is precisely the D12 defect being corrected.
REALTIME_SETTING_KEYS: tuple[str, ...] = (
    "instrument_map_path",
    "price_type",
    "data_layer",
    "resolution",
    "event_type",
    # C's own `provider`/`feed` vocabulary, which is NOT D's `feed_code`: the
    # market-gateway emits `provider="ALPACA"`, `feed="SIP"`
    # (`AlpacaMarketEventNormalizer.java:16-17`) while D's RAW feed_code is
    # `ALPACA_SIP_RAW_30M`.  Defaulting either would let another feed's prices be
    # filed under this dataset, so both are declared.
    "source_provider",
    "source_feed",
    "source_resolution",
    "partition_granularity",
    "shard_count",
    "staging_root",
    "value_fields",
)

#: Bar columns `value_fields` must name a source for, plus the two nullable ones.
REALTIME_REQUIRED_VALUE_FIELDS: tuple[str, ...] = ("open", "high", "low", "close", "volume")
REALTIME_OPTIONAL_VALUE_FIELDS: tuple[str, ...] = ("trade_count", "vwap")


@dataclass(frozen=True)
class RealtimeIngestSettings:
    """Validated `PIPELINE_WORKER_REALTIME_INGEST` document."""

    instrument_map_path: Path
    staging_root: Path
    price_type: str
    data_layer: str
    resolution: str
    event_type: str
    source_provider: str
    source_feed: str
    source_resolution: str
    partition_granularity: str
    shard_count: int
    value_fields: Mapping[str, str]

    @classmethod
    def parse(cls, raw: str) -> RealtimeIngestSettings:
        try:
            document = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                f"PIPELINE_WORKER_REALTIME_INGEST is not valid JSON: {exc}"
            ) from exc
        if not isinstance(document, dict):
            raise ConfigurationError("PIPELINE_WORKER_REALTIME_INGEST must be a JSON object")
        missing = [key for key in REALTIME_SETTING_KEYS if key not in document]
        if missing:
            raise ConfigurationError(
                f"PIPELINE_WORKER_REALTIME_INGEST is missing required key(s): {sorted(missing)}"
            )
        unknown = sorted(set(document) - set(REALTIME_SETTING_KEYS))
        if unknown:
            raise ConfigurationError(f"PIPELINE_WORKER_REALTIME_INGEST has unknown key(s): {unknown}")

        fields = document["value_fields"]
        if not isinstance(fields, dict):
            raise ConfigurationError("PIPELINE_WORKER_REALTIME_INGEST.value_fields must be an object")
        absent = [name for name in REALTIME_REQUIRED_VALUE_FIELDS if not fields.get(name)]
        if absent:
            raise ConfigurationError(
                f"PIPELINE_WORKER_REALTIME_INGEST.value_fields is missing {sorted(absent)}"
            )
        stray = sorted(set(fields) - set(REALTIME_REQUIRED_VALUE_FIELDS) - set(REALTIME_OPTIONAL_VALUE_FIELDS))
        if stray:
            raise ConfigurationError(
                f"PIPELINE_WORKER_REALTIME_INGEST.value_fields names unknown bar column(s): {stray}"
            )

        shard_count = document["shard_count"]
        if isinstance(shard_count, bool) or not isinstance(shard_count, int) or shard_count < 1:
            raise ConfigurationError(
                f"PIPELINE_WORKER_REALTIME_INGEST.shard_count must be a positive integer, got {shard_count!r}"
            )
        return cls(
            instrument_map_path=Path(_setting_text(document, "instrument_map_path")),
            staging_root=Path(_setting_text(document, "staging_root")),
            price_type=_setting_text(document, "price_type"),
            data_layer=_setting_text(document, "data_layer"),
            resolution=_setting_text(document, "resolution"),
            event_type=_setting_text(document, "event_type"),
            source_provider=_setting_text(document, "source_provider"),
            source_feed=_setting_text(document, "source_feed"),
            source_resolution=_setting_text(document, "source_resolution"),
            partition_granularity=_setting_text(document, "partition_granularity"),
            shard_count=shard_count,
            value_fields={name: str(value) for name, value in fields.items()},
        )

    def describe(self) -> dict[str, object]:
        return {
            "price_type": self.price_type,
            "data_layer": self.data_layer,
            "resolution": self.resolution,
            "event_type": self.event_type,
            "source_provider": self.source_provider,
            "source_feed": self.source_feed,
            "source_resolution": self.source_resolution,
            "partition_granularity": self.partition_granularity,
            "shard_count": self.shard_count,
        }


def _setting_text(document: Mapping[str, Any], key: str) -> str:
    value = document[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f"PIPELINE_WORKER_REALTIME_INGEST.{key} must be a non-empty string, got {value!r}"
        )
    return value.strip()


@dataclass(frozen=True)
class WorkerConfig:
    """Validated `pipeline-worker` configuration."""

    environment: str
    message_source: str
    catalog_root: Path
    object_store_root: Path
    queue_url: str | None
    dead_letter_queue_url: str | None
    aws_endpoint_url: str | None
    aws_region: str
    log_level: str
    poll_interval_seconds: float
    max_messages_per_poll: int
    retry_delay_seconds: float
    shutdown_grace_seconds: float
    idempotency_cache_size: int
    max_receive_count: int
    visibility_timeout_seconds: int
    health_file: Path | None
    health_host: str
    health_port: int | None
    realtime: RealtimeIngestSettings | None
    exit_after_idle_polls: int = 0

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> WorkerConfig:
        values: Mapping[str, str] = os.environ if environment is None else environment

        missing = [name for name in REQUIRED_ENVIRONMENT_VARIABLES if _blank(values.get(name))]
        if missing:
            raise ConfigurationError.missing(
                missing,
                hint="see apps/pipeline_worker/config.py ENVIRONMENT_VARIABLES",
            )

        message_source = values["PIPELINE_WORKER_MESSAGE_SOURCE"].strip().lower()
        if message_source not in MESSAGE_SOURCES:
            raise ConfigurationError(
                f"PIPELINE_WORKER_MESSAGE_SOURCE must be one of {list(MESSAGE_SOURCES)}, "
                f"got {message_source!r}"
            )

        queue_url_raw = values.get("PIPELINE_WORKER_QUEUE_URL")
        queue_url = None if _blank(queue_url_raw) else str(queue_url_raw).strip()
        dead_letter_raw = values.get("PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL")
        dead_letter_queue_url = None if _blank(dead_letter_raw) else str(dead_letter_raw).strip()
        if message_source == "sqs" and queue_url is None:
            raise ConfigurationError.missing(
                ["PIPELINE_WORKER_QUEUE_URL"],
                hint="required when PIPELINE_WORKER_MESSAGE_SOURCE=sqs",
            )
        if message_source == "sqs" and dead_letter_queue_url is None:
            raise ConfigurationError.missing(
                ["PIPELINE_WORKER_DEAD_LETTER_QUEUE_URL"],
                hint=(
                    "required when PIPELINE_WORKER_MESSAGE_SOURCE=sqs; without it a poison "
                    "message can only be retried forever or silently dropped"
                ),
            )
        if message_source != "sqs" and queue_url is not None:
            raise ConfigurationError(
                "PIPELINE_WORKER_QUEUE_URL is set but PIPELINE_WORKER_MESSAGE_SOURCE is "
                f"{message_source!r}; refusing to start with an ignored queue configuration"
            )

        log_level = (values.get("PIPELINE_WORKER_LOG_LEVEL") or "INFO").strip().upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError(f"PIPELINE_WORKER_LOG_LEVEL is not a log level: {log_level!r}")

        health_file_raw = values.get("PIPELINE_WORKER_HEALTH_FILE")
        health_file = None if _blank(health_file_raw) else Path(str(health_file_raw).strip())
        health_port_raw = values.get("PIPELINE_WORKER_HEALTH_PORT")
        health_port = (
            None
            if _blank(health_port_raw)
            else _require_int(values, "PIPELINE_WORKER_HEALTH_PORT", "", minimum=0, maximum=65535)
        )

        endpoint_raw = values.get("PIPELINE_WORKER_AWS_ENDPOINT_URL")
        realtime_raw = values.get("PIPELINE_WORKER_REALTIME_INGEST")

        return cls(
            environment=values["PIPELINE_WORKER_ENVIRONMENT"].strip(),
            message_source=message_source,
            catalog_root=Path(values["PIPELINE_WORKER_CATALOG_ROOT"].strip()),
            object_store_root=Path(values["PIPELINE_WORKER_OBJECT_STORE_ROOT"].strip()),
            queue_url=queue_url,
            dead_letter_queue_url=dead_letter_queue_url,
            aws_endpoint_url=None if _blank(endpoint_raw) else str(endpoint_raw).strip(),
            aws_region=(values.get("PIPELINE_WORKER_AWS_REGION") or "us-east-1").strip(),
            log_level=log_level,
            poll_interval_seconds=_require_float(
                values, "PIPELINE_WORKER_POLL_INTERVAL_SECONDS", "1.0", minimum=0.001
            ),
            max_messages_per_poll=_require_int(
                values, "PIPELINE_WORKER_MAX_MESSAGES_PER_POLL", "10", minimum=1, maximum=10
            ),
            retry_delay_seconds=_require_float(
                values, "PIPELINE_WORKER_RETRY_DELAY_SECONDS", "30.0", minimum=0.0
            ),
            shutdown_grace_seconds=_require_float(
                values, "PIPELINE_WORKER_SHUTDOWN_GRACE_SECONDS", "30.0", minimum=0.0
            ),
            idempotency_cache_size=_require_int(
                values,
                "PIPELINE_WORKER_IDEMPOTENCY_CACHE_SIZE",
                "10000",
                minimum=1,
                maximum=1_000_000,
            ),
            max_receive_count=_require_int(
                values, "PIPELINE_WORKER_MAX_RECEIVE_COUNT", "5", minimum=1, maximum=1_000
            ),
            visibility_timeout_seconds=_require_int(
                values,
                "PIPELINE_WORKER_VISIBILITY_TIMEOUT_SECONDS",
                "60",
                minimum=0,
                maximum=43_200,
            ),
            health_file=health_file,
            health_host=(values.get("PIPELINE_WORKER_HEALTH_HOST") or "127.0.0.1").strip(),
            health_port=health_port,
            realtime=None if _blank(realtime_raw) else RealtimeIngestSettings.parse(str(realtime_raw)),
            exit_after_idle_polls=_require_int(
                values,
                "PIPELINE_WORKER_EXIT_AFTER_IDLE_POLLS",
                "0",
                minimum=0,
                maximum=1_000_000,
            ),
        )

    def describe(self) -> dict[str, object]:
        """Log-safe description.  Queue URLs are reported by presence only."""

        return {
            "environment": self.environment,
            "message_source": self.message_source,
            "catalog_root": str(self.catalog_root),
            "object_store_root": str(self.object_store_root),
            "queue_configured": self.queue_url is not None,
            "dead_letter_queue_configured": self.dead_letter_queue_url is not None,
            "aws_endpoint_override": self.aws_endpoint_url is not None,
            "aws_region": self.aws_region,
            "log_level": self.log_level,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_messages_per_poll": self.max_messages_per_poll,
            "retry_delay_seconds": self.retry_delay_seconds,
            "shutdown_grace_seconds": self.shutdown_grace_seconds,
            "max_receive_count": self.max_receive_count,
            "visibility_timeout_seconds": self.visibility_timeout_seconds,
            "health_file": str(self.health_file) if self.health_file else None,
            "health_port": self.health_port,
            "realtime": None if self.realtime is None else self.realtime.describe(),
            "exit_after_idle_polls": self.exit_after_idle_polls,
        }


def environment_variable_help() -> str:
    lines = ["pipeline-worker environment variables:", ""]
    for name, required, default, description in ENVIRONMENT_VARIABLES:
        marker = "required" if required else f"optional (default {default!r})" if default else "optional"
        lines.append(f"  {name}")
        lines.append(f"      {marker}")
        lines.append(f"      {description}")
    return "\n".join(lines)
