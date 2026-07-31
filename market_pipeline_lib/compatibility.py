"""Compatibility validation for the D-owned COM06 fixture proposal.

These validators intentionally live beside the implementation and tests.  They
do not make the proposal a canonical root contract; they provide executable
producer evidence until the protected contract registry is approved.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .contracts import canonical_dataset_hash


SCHEMA_VERSION = 1
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
RESULT_EVENT_TYPES = {
    "QUEUED": "BACKTEST_QUEUED",
    "RUNNING": "BACKTEST_RUNNING",
    "COMPLETE": "BACKTEST_COMPLETE",
    "FAILED": "BACKTEST_FAILED",
    "UNAVAILABLE": "BACKTEST_UNAVAILABLE",
}
RESULT_REQUIRED_FIELDS = {
    "QUEUED": ("queued_at",),
    "RUNNING": ("started_at", "attempt"),
    "COMPLETE": ("completed_at", "result_manifest_id"),
    "FAILED": ("failed_at", "failure_code", "retryable"),
    "UNAVAILABLE": ("decided_at", "reason_code", "missing_requirements"),
}


class ContractValidationError(ValueError):
    """Raised when a compatibility document cannot be consumed safely."""


def _require_fields(document: Mapping[str, Any], fields: Sequence[str], label: str) -> None:
    missing = [field for field in fields if field not in document]
    if missing:
        raise ContractValidationError(f"{label} missing required field: {missing[0]}")


def _require_string(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise ContractValidationError(f"{label}.{field} must be a non-empty string")
    return value


def _require_uuid(document: Mapping[str, Any], field: str, label: str) -> None:
    value = _require_string(document, field, label)
    try:
        uuid.UUID(value)
    except ValueError as exc:
        raise ContractValidationError(f"{label}.{field} must be a UUID") from exc


def _require_hash(document: Mapping[str, Any], field: str, label: str) -> str:
    value = _require_string(document, field, label)
    if not HEX_SHA256.fullmatch(value):
        raise ContractValidationError(f"{label}.{field} must be lowercase SHA-256")
    return value


def _require_timestamp(document: Mapping[str, Any], field: str, label: str) -> None:
    value = _require_string(document, field, label)
    if not value.endswith("Z"):
        raise ContractValidationError(f"{label}.{field} must be UTC with a Z suffix")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractValidationError(f"{label}.{field} must be ISO-8601") from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ContractValidationError(f"{label}.{field} must be UTC")


def _require_schema(document: Mapping[str, Any], contract_id: str, label: str) -> None:
    if document.get("contract_id") != contract_id:
        raise ContractValidationError(f"{label}.contract_id must be {contract_id}")
    if document.get("schema_version") != SCHEMA_VERSION:
        raise ContractValidationError(f"{label}.schema_version must be {SCHEMA_VERSION}")


def _validate_envelope(document: Mapping[str, Any], label: str) -> None:
    _require_fields(
        document,
        ("message_id", "occurred_at", "correlation_id", "idempotency_key"),
        label,
    )
    _require_uuid(document, "message_id", label)
    _require_uuid(document, "correlation_id", label)
    _require_timestamp(document, "occurred_at", label)
    _require_string(document, "idempotency_key", label)


def validate_dataset_manifest(document: Mapping[str, Any]) -> None:
    label = "dataset_manifest"
    _require_schema(document, "com06.dataset-manifest", label)
    _require_fields(
        document,
        (
            "manifest_id",
            "dataset_id",
            "revision",
            "status",
            "dataset_hash",
            "schema_id",
            "period_start",
            "period_end",
            "available_at",
            "objects",
        ),
        label,
    )
    _require_uuid(document, "manifest_id", label)
    _require_uuid(document, "dataset_id", label)
    if not isinstance(document["revision"], int) or document["revision"] < 1:
        raise ContractValidationError(f"{label}.revision must be positive")
    if document["status"] != "AVAILABLE":
        raise ContractValidationError(f"{label}.status must be AVAILABLE")
    _require_string(document, "schema_id", label)
    for field in ("period_start", "period_end", "available_at"):
        _require_timestamp(document, field, label)

    objects = document["objects"]
    if not isinstance(objects, list) or not objects:
        raise ContractValidationError(f"{label}.objects must not be empty")
    required_object_fields = (
        "storage_object_id",
        "object_key",
        "content_hash",
        "object_kind",
        "partition_granularity",
        "partition_start",
        "partition_end",
        "period_start",
        "period_end",
        "shard_key",
        "part_number",
        "row_count",
        "schema_version",
    )
    for index, item in enumerate(objects):
        object_label = f"{label}.objects[{index}]"
        if not isinstance(item, Mapping):
            raise ContractValidationError(f"{object_label} must be an object")
        _require_fields(item, required_object_fields, object_label)
        _require_uuid(item, "storage_object_id", object_label)
        _require_string(item, "object_key", object_label)
        _require_hash(item, "content_hash", object_label)
        for field in ("period_start", "period_end"):
            _require_timestamp(item, field, object_label)
        for field in ("part_number", "row_count"):
            if not isinstance(item[field], int) or item[field] < 1:
                raise ContractValidationError(f"{object_label}.{field} must be positive")

    declared_hash = _require_hash(document, "dataset_hash", label)
    actual_hash = canonical_dataset_hash(objects)
    if declared_hash != actual_hash:
        raise ContractValidationError(
            f"{label}.dataset_hash does not match canonical object metadata"
        )


def validate_backtest_request(document: Mapping[str, Any]) -> None:
    label = "backtest_request"
    _require_schema(document, "com06.backtest-request", label)
    _validate_envelope(document, label)
    _require_fields(
        document,
        (
            "event_type",
            "backtest_run_id",
            "strategy_version_id",
            "strategy_snapshot_hash",
            "compiled_plan_hash",
            "dataset_manifest_id",
            "dataset_hash",
            "execution_policy_version",
            "requested_at",
        ),
        label,
    )
    if document["event_type"] != "BACKTEST_REQUESTED":
        raise ContractValidationError(f"{label}.event_type must be BACKTEST_REQUESTED")
    for field in ("backtest_run_id", "strategy_version_id", "dataset_manifest_id"):
        _require_uuid(document, field, label)
    for field in ("strategy_snapshot_hash", "compiled_plan_hash", "dataset_hash"):
        _require_hash(document, field, label)
    _require_string(document, "execution_policy_version", label)
    _require_timestamp(document, "requested_at", label)


def validate_backtest_result(document: Mapping[str, Any]) -> None:
    label = "backtest_result"
    _require_schema(document, "com06.backtest-result", label)
    _validate_envelope(document, label)
    _require_fields(document, ("event_type", "backtest_run_id", "status"), label)
    _require_uuid(document, "backtest_run_id", label)
    status = document["status"]
    if status not in RESULT_EVENT_TYPES:
        raise ContractValidationError(f"{label}.status is unsupported")
    if document["event_type"] != RESULT_EVENT_TYPES[status]:
        raise ContractValidationError(
            f"{label}.event_type must be {RESULT_EVENT_TYPES[status]} for {status}"
        )
    _require_fields(document, RESULT_REQUIRED_FIELDS[status], label)

    timestamp_field = {
        "QUEUED": "queued_at",
        "RUNNING": "started_at",
        "COMPLETE": "completed_at",
        "FAILED": "failed_at",
        "UNAVAILABLE": "decided_at",
    }[status]
    _require_timestamp(document, timestamp_field, label)
    if status == "RUNNING" and (
        not isinstance(document["attempt"], int) or document["attempt"] < 1
    ):
        raise ContractValidationError(f"{label}.attempt must be positive")
    if status == "COMPLETE":
        _require_uuid(document, "result_manifest_id", label)
    if status == "FAILED":
        _require_string(document, "failure_code", label)
        if not isinstance(document["retryable"], bool):
            raise ContractValidationError(f"{label}.retryable must be boolean")
    if status == "UNAVAILABLE":
        _require_string(document, "reason_code", label)
        missing = document["missing_requirements"]
        if not isinstance(missing, list) or not missing or not all(
            isinstance(value, str) and value for value in missing
        ):
            raise ContractValidationError(
                f"{label}.missing_requirements must contain strings"
            )
