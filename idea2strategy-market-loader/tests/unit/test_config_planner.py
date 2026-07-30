from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from market_loader.config import EnvironmentSettings, load_config
from market_loader.errors import ConfigurationError, RightsApprovalError
from market_loader.model.catalog import UniverseInstrument
from market_loader.pipeline.planner import create_plan


def _instrument(status: str = "ACTIVE") -> UniverseInstrument:
    return UniverseInstrument(
        provider_symbol="AAPL",
        asset_type="STOCK",
        primary_exchange_mic="XNAS",
        effective_from=date(2020, 1, 1),
        effective_to=None,
        support_status=status,
        instrument_id="11111111-1111-1111-1111-111111111111",
    )


def test_load_config_resolves_staging_relative_to_config() -> None:
    root = Path(__file__).parents[2]
    loaded = load_config(root / "config.example.yaml")
    assert loaded.project.schema_version == "market-bars/1"
    assert loaded.storage.staging_directory == (root / ".staging").resolve()


def test_invalid_config_and_rights_gate(tmp_path: Path) -> None:
    invalid = tmp_path / "bad.yaml"
    invalid.write_text("data: []\n", encoding="utf-8")
    with pytest.raises(ConfigurationError):
        load_config(invalid)
    with pytest.raises(RightsApprovalError):
        EnvironmentSettings(_env_file=None).require_rights_approval()
    approved = EnvironmentSettings(
        _env_file=None,
        PROVIDER_RIGHTS_APPROVED=True,
        PROVIDER_RIGHTS_VERSION="approval/2026-01",
    )
    approved.require_rights_approval()


def test_plan_counts_batches_chunks_years_and_input_errors() -> None:
    root = Path(__file__).parents[2]
    config = load_config(root / "config.example.yaml")
    result = create_plan(
        config,
        [_instrument()],
        date(2024, 1, 1),
        date(2025, 1, 1),
        adjustments=["raw"],
        resolutions=["30m", "1d"],
    )
    assert result.api_chunk_count == 3
    assert result.expected_api_requests == 3
    assert result.expected_manifests == 2
    assert result.expected_s3_objects == 16
    assert result.as_dict()["symbol_count"] == 1
    invalid = create_plan(
        config,
        [_instrument("INACTIVE")],
        date(2024, 1, 1),
        date(2025, 1, 1),
        adjustments=["bad"],
        resolutions=["tick"],
    )
    assert len(invalid.input_errors) == 3
