from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date

from market_loader.config import AppConfig
from market_loader.model.catalog import UniverseInstrument
from market_loader.model.partition import chunk_ranges, year_ranges


@dataclass(frozen=True, slots=True)
class Plan:
    symbol_count: int
    adjustments: list[str]
    resolutions: list[str]
    api_chunk_count: int
    year_count: int
    expected_api_requests: int
    expected_manifests: int
    expected_s3_objects: int
    skipped_partitions: int
    input_errors: list[str]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def create_plan(
    config: AppConfig,
    universe: list[UniverseInstrument],
    start: date,
    end: date,
    *,
    adjustments: list[str] | None = None,
    resolutions: list[str] | None = None,
    max_symbols: int | None = None,
) -> Plan:
    selected = [
        item
        for item in universe
        if item.support_status == "ACTIVE" and item.active_during(start, end)
    ]
    if max_symbols is not None:
        selected = selected[:max_symbols]
    chosen_adjustments = sorted(set(adjustments or config.data.adjustments))
    chosen_resolutions = sorted(set(resolutions or config.data.output_resolutions))
    errors: list[str] = []
    if not selected:
        errors.append("no active instruments overlap the requested range")
    if not set(chosen_adjustments) <= {"raw", "all"}:
        errors.append("adjustments must contain only raw or all")
    if not set(chosen_resolutions) <= {"30m", "1h", "4h", "1d"}:
        errors.append("unsupported resolution")
    chunks = list(chunk_ranges(start, end, config.alpaca.chunk_days))
    years = list(year_ranges(start, end))
    batches = math.ceil(len(selected) / config.alpaca.symbols_per_request) if selected else 0
    expected_manifests = len(chosen_adjustments) * len(chosen_resolutions) * len(years)
    return Plan(
        symbol_count=len(selected),
        adjustments=chosen_adjustments,
        resolutions=chosen_resolutions,
        api_chunk_count=len(chunks),
        year_count=len(years),
        expected_api_requests=len(chosen_adjustments) * len(chunks) * batches,
        expected_manifests=expected_manifests,
        expected_s3_objects=expected_manifests * config.data.shard_count,
        skipped_partitions=0,
        input_errors=errors,
    )
