"""Event-shape validation helpers.

Every handler validates the event it was handed and raises `MalformedEventError`
on anything it does not recognise.  Returning an empty success for an
unrecognised event is a forbidden pattern in this bundle: it turns a producer
defect into silent data loss.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, TypeVar

from apps.common.errors import MalformedEventError

_T = TypeVar("_T")

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MalformedEventError(f"{label} must be an object, got {type(value).__name__}")
    for key in value:
        if not isinstance(key, str):
            raise MalformedEventError(f"{label} keys must be strings, got {type(key).__name__}")
    return value


def require_field(document: Mapping[str, Any], field: str, label: str) -> Any:
    if field not in document:
        raise MalformedEventError(f"{label} is missing required field {field!r}")
    return document[field]


def require_string(document: Mapping[str, Any], field: str, label: str) -> str:
    value = require_field(document, field, label)
    if not isinstance(value, str) or not value.strip():
        raise MalformedEventError(f"{label}.{field} must be a non-empty string")
    return value.strip()


def require_identifier(document: Mapping[str, Any], field: str, label: str) -> str:
    value = require_string(document, field, label)
    if not _IDENTIFIER.fullmatch(value):
        raise MalformedEventError(
            f"{label}.{field} must be a 1-128 character identifier "
            "([A-Za-z0-9] followed by [A-Za-z0-9._:-])"
        )
    return value


def require_enum(document: Mapping[str, Any], field: str, allowed: Sequence[str], label: str) -> str:
    value = require_string(document, field, label)
    if value not in allowed:
        raise MalformedEventError(
            f"{label}.{field} must be one of {sorted(allowed)}, got {value!r}"
        )
    return value


def require_sequence(document: Mapping[str, Any], field: str, label: str) -> Sequence[Any]:
    value = require_field(document, field, label)
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise MalformedEventError(f"{label}.{field} must be an array")
    return value


def require_utc_timestamp(document: Mapping[str, Any], field: str, label: str) -> datetime:
    value = require_string(document, field, label)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MalformedEventError(f"{label}.{field} must be an ISO-8601 timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise MalformedEventError(f"{label}.{field} must be UTC")
    return parsed


def reject_unknown_fields(
    document: Mapping[str, Any], allowed: Sequence[str], label: str
) -> None:
    """Fail closed on fields this handler's contract version does not define."""

    unexpected = sorted(set(document) - set(allowed))
    if unexpected:
        raise MalformedEventError(f"{label} has unknown field(s): {', '.join(unexpected)}")
