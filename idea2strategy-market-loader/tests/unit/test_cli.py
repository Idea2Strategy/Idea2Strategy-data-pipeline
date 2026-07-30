from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_loader.cli import _historical_sip_probe_window


def test_historical_sip_probe_avoids_recent_entitlement_window() -> None:
    now = datetime(2026, 7, 30, 5, 18, tzinfo=UTC)

    start, end = _historical_sip_probe_window(now)

    assert now - end == timedelta(days=1)
    assert end - start == timedelta(days=3)
