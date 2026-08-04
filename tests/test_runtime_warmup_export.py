from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from market_pipeline_lib.cli import build_parser
from market_pipeline_lib.contracts import DATASET_CONTRACTS
from market_pipeline_lib.realtime_warmup import (
    FeatureRequirement,
    WarmupPublicationSpec,
    WarmupReadiness,
)
from market_pipeline_lib.runtime_warmup_export import (
    RuntimeWarmupError,
    publish_trading_warmup,
)

FIXTURE = Path(__file__).parent / "fixtures" / "d90" / "provider-neutral-market-events.json"
INSTRUMENT_ID = "8a35e6b5-cf84-4f63-920d-57c1f1b95df0"
CONTRACT = DATASET_CONTRACTS[("raw", "RAW", "30m")]
SPEC = WarmupPublicationSpec(
    contract=CONTRACT,
    event_type="BAR_1M",
    granularity="DAY",
    revision=1,
    shard_count=1,
)
NOW = datetime(2026, 7, 31, 21, 10, tzinfo=UTC)


def readiness(state: str = "READY") -> WarmupReadiness:
    return WarmupReadiness(
        state=state,
        session_date_et="2026-07-31",
        feed_id=CONTRACT.feed_code,
        evaluated_at=datetime(2026, 7, 31, 21, 5, tzinfo=UTC),
        reason_code=None if state == "READY" else "D90_WATERMARK_STALE",
        detail=None if state == "READY" else "watermark did not reach the session close",
    )


def requirement() -> FeatureRequirement:
    return FeatureRequirement(
        requirement_id="close-entry",
        feature_id="close",
        feature_version="1.0.0",
        resolution="PT1M",
        value_field="close",
        instruments=(INSTRUMENT_ID,),
        required_observations=1,
    )


def test_ready_one_shot_publishes_and_verifies_receipt(tmp_path: Path) -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    output = tmp_path / "runtime" / "trading" / "warmup"

    report = publish_trading_warmup(
        output=output,
        session_date=date(2026, 7, 31),
        spec=SPEC,
        readiness=readiness(),
        requirements=(requirement(),),
        events=document,
        now=NOW,
        max_readiness_age=timedelta(minutes=10),
    )

    assert report["status"] == "PUBLISHED"
    receipt = json.loads((output / "publication-receipt.json").read_text())
    assert receipt["manifest"]["revision"] == 1
    assert receipt["manifest"]["sha256"]
    assert {item["schema_version"] for item in receipt["objects"]} == {
        "warmup-bars-v1",
        "feature-object-v1",
    }

    replay = publish_trading_warmup(
        output=output,
        session_date=date(2026, 7, 31),
        spec=SPEC,
        readiness=readiness(),
        requirements=(requirement(),),
        events=document,
        now=NOW,
        max_readiness_age=timedelta(minutes=10),
    )
    assert replay["status"] == "ALREADY_APPLIED"


def test_stale_readiness_fails_before_publication(tmp_path: Path) -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))

    with pytest.raises(RuntimeWarmupError, match="stale"):
        publish_trading_warmup(
            output=tmp_path / "warmup",
            session_date=date(2026, 7, 31),
            spec=SPEC,
            readiness=readiness(),
            requirements=(requirement(),),
            events=document,
            now=NOW,
            max_readiness_age=timedelta(minutes=1),
        )

    assert not (tmp_path / "warmup").exists()


def test_ready_requires_events_and_feature_requirements(tmp_path: Path) -> None:
    with pytest.raises(RuntimeWarmupError, match="events"):
        publish_trading_warmup(
            output=tmp_path / "warmup",
            session_date=date(2026, 7, 31),
            spec=SPEC,
            readiness=readiness(),
            requirements=(requirement(),),
            events=None,
            now=NOW,
            max_readiness_age=timedelta(minutes=10),
        )


def test_readiness_cannot_pin_a_different_manifest(tmp_path: Path) -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    pinned = WarmupReadiness(
        state="READY",
        session_date_et="2026-07-31",
        feed_id=CONTRACT.feed_code,
        evaluated_at=datetime(2026, 7, 31, 21, 5, tzinfo=UTC),
        manifest_id="00000000-0000-4000-8000-000000000001",
    )

    with pytest.raises(RuntimeWarmupError, match="manifest_id"):
        publish_trading_warmup(
            output=tmp_path / "warmup",
            session_date=date(2026, 7, 31),
            spec=SPEC,
            readiness=pinned,
            requirements=(requirement(),),
            events=document,
            now=NOW,
            max_readiness_age=timedelta(minutes=10),
        )

    assert not (tmp_path / "warmup").exists()


def test_blocked_publication_forbids_events_and_writes_a_verified_manifest(tmp_path: Path) -> None:
    output = tmp_path / "warmup"
    report = publish_trading_warmup(
        output=output,
        session_date=date(2026, 7, 31),
        spec=SPEC,
        readiness=readiness("BLOCKED"),
        requirements=(),
        events=None,
        now=NOW,
        max_readiness_age=timedelta(minutes=10),
    )

    assert report["status"] == "PUBLISHED"
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["status"] == "QUARANTINED"
    receipt = json.loads((output / "publication-receipt.json").read_text())
    assert receipt["objects"] == []


def test_warmup_cli_requires_every_semantic_input() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["publish-trading-warmup", "--output", "warmup"])

    args = parser.parse_args(
        [
            "publish-trading-warmup",
            "--output",
            "warmup",
            "--session-date",
            "2026-07-31",
            "--events",
            "events.json",
            "--requirements",
            "requirements.json",
            "--readiness",
            "readiness.json",
            "--adjustment",
            "raw",
            "--layer",
            "RAW",
            "--resolution",
            "30m",
            "--event-type",
            "BAR_1M",
            "--granularity",
            "DAY",
            "--revision",
            "1",
            "--shard-count",
            "1",
            "--max-readiness-age-seconds",
            "600",
        ]
    )
    assert args.max_readiness_age_seconds == 600
