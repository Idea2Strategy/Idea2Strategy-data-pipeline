from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any
from uuid import UUID

from market_loader.model.partition import canonical_hash


@dataclass(frozen=True, slots=True)
class ManifestObject:
    content_sha256: str
    row_count: int
    period_start: str
    period_end: str
    shard: int
    part: int


def object_key(
    *,
    prefix: str,
    adjustment: str,
    resolution: str,
    revision: int,
    year: int,
    shard: int,
    shard_count: int,
    manifest_id: UUID,
    part: int = 1,
) -> str:
    segments = [
        prefix.strip("/"),
        "provider=alpaca",
        "feed=sip",
        f"adjustment={adjustment}",
        "session=regular",
        f"resolution={resolution}",
        f"revision={revision:08d}",
        f"year={year:04d}",
        f"shard={shard:02d}-of-{shard_count:02d}",
        f"manifest_id={manifest_id}",
        f"part-{part:05d}.parquet",
    ]
    return "/".join(segments)


def manifest_hash(
    *,
    feed_code: str,
    adjustment: str,
    resolution: str,
    period_start: date,
    period_end: date,
    revision: int,
    schema_version: str,
    processing_version: str,
    objects: list[ManifestObject],
) -> str:
    payload: dict[str, Any] = {
        "provider": "ALPACA",
        "feed": feed_code,
        "adjustment": adjustment,
        "session": "XNYS_REGULAR",
        "resolution": resolution,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "revision": revision,
        "schema_version": schema_version,
        "processing_version": processing_version,
        "objects": [
            asdict(item) for item in sorted(objects, key=lambda value: (value.shard, value.part))
        ],
    }
    return canonical_hash(payload)
