from __future__ import annotations

from datetime import date
from uuid import UUID

from market_loader.model.manifest import ManifestObject, manifest_hash, object_key


def test_object_key_snapshot() -> None:
    result = object_key(
        prefix="historical",
        adjustment="raw",
        resolution="30m",
        revision=1,
        year=2024,
        shard=3,
        shard_count=8,
        manifest_id=UUID("11111111-1111-1111-1111-111111111111"),
    )
    assert result == (
        "historical/provider=alpaca/feed=sip/adjustment=raw/session=regular/"
        "resolution=30m/revision=00000001/year=2024/shard=03-of-08/"
        "manifest_id=11111111-1111-1111-1111-111111111111/part-00001.parquet"
    )
    assert "\\" not in result


def test_manifest_hash_is_deterministic_and_adjustment_sensitive() -> None:
    objects = [
        ManifestObject("b" * 64, 2, "2024-01-01", "2025-01-01", 1, 1),
        ManifestObject("a" * 64, 1, "2024-01-01", "2025-01-01", 0, 1),
    ]
    arguments = {
        "feed_code": "ALPACA_SIP_RAW_30M",
        "adjustment": "raw",
        "resolution": "30m",
        "period_start": date(2024, 1, 1),
        "period_end": date(2025, 1, 1),
        "revision": 1,
        "schema_version": "market-bars/1",
        "processing_version": "market-loader/1.0.0",
    }
    first = manifest_hash(objects=objects, **arguments)
    assert first == manifest_hash(objects=list(reversed(objects)), **arguments)
    assert first != manifest_hash(
        objects=objects,
        **{
            **arguments,
            "feed_code": "ALPACA_SIP_ALL_30M",
            "adjustment": "all",
        },
    )
