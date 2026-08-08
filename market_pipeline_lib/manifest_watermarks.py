"""Advance durable feed watermarks from committed AVAILABLE manifests.

An AVAILABLE manifest is the canonical proof that immutable market-data objects
were ingested and published successfully.  This adapter projects that proof onto
``market_data.stream_watermarks`` without bypassing either publication status or
the repository's monotonic-update guard.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

from .watermarks import StreamPosition, StreamWatermark, WatermarkRepository


class ManifestCatalog(Protocol):
    def records(self, table: str) -> list[dict[str, Any]]: ...


def _instant(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        moment = datetime.fromisoformat(text)
    else:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return moment.astimezone(UTC)


def advance_available_manifest_watermarks(
    catalog: ManifestCatalog,
    repository: WatermarkRepository,
    *,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Project the newest successful manifest of each active feed to a watermark.

    Provider rights/status and feed retirement are checked before a manifest can
    contribute.  QUARANTINED/BUILDING manifests are ignored, and the repository
    remains responsible for the atomic no-regression rule.
    """

    observed = (observed_at or datetime.now(UTC)).astimezone(UTC)
    active_provider_ids = {
        str(row["id"])
        for row in catalog.records("market_data.providers")
        if row.get("status") == "ACTIVE"
    }
    active_feed_ids = {
        str(row["id"])
        for row in catalog.records("market_data.feeds")
        if str(row.get("provider_id")) in active_provider_ids
        and row.get("retired_at") is None
    }

    latest: dict[str, Mapping[str, object]] = {}
    for manifest in catalog.records("market_data.dataset_manifests"):
        feed_id = str(manifest.get("feed_id", ""))
        if feed_id not in active_feed_ids or manifest.get("status") != "AVAILABLE":
            continue
        period_end = _instant(manifest.get("period_end"), "period_end")
        available_at = _instant(manifest.get("available_at"), "available_at")
        candidate = {**manifest, "period_end": period_end, "available_at": available_at}
        current = latest.get(feed_id)
        if current is None or period_end > current["period_end"]:
            latest[feed_id] = candidate

    rows: list[dict[str, str]] = []
    advanced = 0
    for feed_id in sorted(latest):
        manifest = latest[feed_id]
        before = repository.load(feed_id)
        stored = repository.advance(
            StreamWatermark(
                feed_id=feed_id,
                position=StreamPosition(manifest["period_end"]),
                ingested_at=manifest["available_at"],
                updated_at=observed,
            )
        )
        if before is None or stored.position > before.position:
            advanced += 1
        rows.append(
            {
                "feed_id": feed_id,
                "last_source_event_at": stored.position.isoformat(),
                "last_ingested_at": stored.ingested_at.isoformat().replace("+00:00", "Z"),
            }
        )

    return {
        "status": "SUCCEEDED",
        "active_feed_count": len(active_feed_ids),
        "advanced_feed_count": advanced,
        "watermarks": rows,
    }
