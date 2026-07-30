from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from market_loader.errors import InputError


@dataclass(frozen=True, slots=True)
class TimeRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise InputError("time range must satisfy start < end")


def chunk_ranges(start: date, end: date, chunk_days: int) -> Iterator[TimeRange]:
    if chunk_days <= 0:
        raise InputError("chunk_days must be positive")
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=chunk_days), end)
        yield TimeRange(cursor, next_cursor)
        cursor = next_cursor


def year_ranges(start: date, end: date) -> Iterator[TimeRange]:
    cursor = start
    while cursor < end:
        boundary = min(date(cursor.year + 1, 1, 1), end)
        yield TimeRange(cursor, boundary)
        cursor = boundary


def shard_for(instrument_id: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise InputError("shard_count must be positive")
    canonical = str(uuid.UUID(instrument_id))
    first_unsigned_64_bits = int.from_bytes(
        hashlib.sha256(canonical.encode("utf-8")).digest()[:8], "big", signed=False
    )
    return first_unsigned_64_bits % shard_count


def canonical_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def idempotency_key(payload: dict[str, Any]) -> str:
    normalized = {
        key: sorted(value) if isinstance(value, list) else value for key, value in payload.items()
    }
    return canonical_hash(normalized)


def partition_key(adjustment: str, resolution: str, year: int, shard: int) -> str:
    return f"adjustment={adjustment}/resolution={resolution}/year={year}/shard={shard:02d}"
