from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from market_loader.alpaca.client import AlpacaBarsClient
from market_loader.calendar.xnys import XnysCalendar
from market_loader.config import AppConfig
from market_loader.database.connection import Database
from market_loader.database.repositories import MarketRepository
from market_loader.model.bar import Bar
from market_loader.model.catalog import UniverseInstrument, universe_hash
from market_loader.model.manifest import manifest_hash
from market_loader.model.partition import (
    chunk_ranges,
    idempotency_key,
    partition_key,
    shard_for,
    year_ranges,
)
from market_loader.pipeline.collector import collect_chunk
from market_loader.pipeline.normalizer import normalize_bars
from market_loader.pipeline.publisher import Publisher
from market_loader.pipeline.registrar import register_manifest
from market_loader.pipeline.resampler import resample_bars
from market_loader.pipeline.validator import validate_bars

LOGGER = logging.getLogger(__name__)
RESOLUTION_ORDER = ("30m", "1h", "4h", "1d")


@dataclass(frozen=True, slots=True)
class BackfillResult:
    run_id: UUID
    reused: bool
    manifest_count: int
    object_count: int
    row_count: int


class BackfillEngine:
    def __init__(
        self,
        *,
        config: AppConfig,
        database: Database,
        alpaca: AlpacaBarsClient,
        calendar: XnysCalendar,
        publisher: Publisher,
    ) -> None:
        self.config = config
        self.database = database
        self.alpaca = alpaca
        self.calendar = calendar
        self.publisher = publisher

    def run(
        self,
        *,
        universe: list[UniverseInstrument],
        start: date,
        end: date,
        adjustments: list[str],
        resolutions: list[str],
        max_symbols: int | None,
    ) -> BackfillResult:
        if any(value != "30m" for value in resolutions) and "30m" not in resolutions:
            raise ValueError("derived resolutions require publishing their 30m source manifest")
        with self.database.transaction() as connection:
            repository = MarketRepository(connection)
            repository.assert_schema()
            resolved = repository.resolve_instruments(universe)
        selected = [
            item
            for item in resolved
            if item.support_status == "ACTIVE" and item.active_during(start, end)
        ]
        if max_symbols is not None:
            selected = selected[:max_symbols]
        is_sample = len(selected) <= 5 and (end - start).days <= 366
        if not is_sample:
            with self.database.transaction() as connection:
                if not MarketRepository(connection).has_successful_sample_run():
                    raise RuntimeError(
                        "full write blocked until a successful <=5-symbol, "
                        "<=1-year sample run exists"
                    )
        payload: dict[str, Any] = {
            "pipeline_type": "HISTORICAL_BACKFILL",
            "provider": "ALPACA",
            "feed": "SIP",
            "adjustments": sorted(adjustments),
            "resolutions": sorted(resolutions),
            "session": "XNYS_REGULAR",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "universe_hash": universe_hash(selected),
            "schema_version": self.config.project.schema_version,
            "processing_version": self.config.project.processing_version,
            "shard_count": self.config.data.shard_count,
        }
        partitions = [
            partition_key(adjustment, resolution, period.start.year, shard)
            for period in year_ranges(start, end)
            for adjustment in adjustments
            for resolution in resolutions
            for shard in range(self.config.data.shard_count)
        ]
        with self.database.transaction() as connection:
            run_id, reused = MarketRepository(connection).create_run(
                idempotency_key=idempotency_key(payload),
                processing_version=self.config.project.processing_version,
                input_config={
                    **payload,
                    "symbol_count": len(selected),
                    "universe": [
                        {
                            **asdict(item),
                            "effective_from": item.effective_from.isoformat(),
                            "effective_to": (
                                item.effective_to.isoformat() if item.effective_to else None
                            ),
                        }
                        for item in selected
                    ],
                },
                partition_keys=partitions,
            )
        if reused:
            return BackfillResult(run_id, True, 0, 0, 0)
        manifest_count = object_count = row_count = 0
        try:
            for period in year_ranges(start, end):
                windows = self.calendar.sessions(period.start, period.end)
                for adjustment in adjustments:
                    source = self._collect_period(selected, period.start, period.end, adjustment)
                    source = normalize_bars(
                        source,
                        datetime.combine(period.start, datetime.min.time(), tzinfo=UTC),
                        datetime.combine(period.end, datetime.min.time(), tzinfo=UTC),
                    )
                    source_manifest: UUID | None = None
                    for resolution in [value for value in RESOLUTION_ORDER if value in resolutions]:
                        resolution_partitions = [
                            partition_key(adjustment, resolution, period.start.year, shard)
                            for shard in range(self.config.data.shard_count)
                        ]
                        with self.database.transaction() as connection:
                            completed_manifest = MarketRepository(
                                connection
                            ).successful_manifest_for_partitions(run_id, resolution_partitions)
                        if completed_manifest is not None:
                            if resolution == "30m":
                                source_manifest = completed_manifest
                            continue
                        bars = (
                            source
                            if resolution == "30m"
                            else resample_bars(source, windows, resolution)
                        )
                        validation = validate_bars(bars, windows, derived=resolution != "30m")
                        feed_code = f"ALPACA_SIP_{adjustment.upper()}_{resolution.upper()}"
                        data_layer = (
                            "RAW"
                            if adjustment == "raw" and resolution == "30m"
                            else "ADJUSTED"
                            if adjustment == "all" and resolution == "30m"
                            else "DERIVED"
                        )
                        with self.database.transaction() as connection:
                            manifest_id, revision, previous_id = MarketRepository(
                                connection
                            ).create_manifest(
                                feed_code=feed_code,
                                data_layer=data_layer,
                                resolution=resolution,
                                period_start=period.start,
                                period_end=period.end,
                                processing_version=self.config.project.processing_version,
                            )
                        by_shard: dict[int, list[Bar]] = {
                            shard: [] for shard in range(self.config.data.shard_count)
                        }
                        for bar in bars:
                            by_shard[
                                shard_for(bar.instrument_id, self.config.data.shard_count)
                            ].append(bar)
                        published = []
                        for shard, shard_bars in by_shard.items():
                            key = partition_key(adjustment, resolution, period.start.year, shard)
                            published.append(
                                self.publisher.publish_shard(
                                    run_id=run_id,
                                    manifest_id=manifest_id,
                                    revision=revision,
                                    adjustment=adjustment,
                                    resolution=resolution,
                                    period_start=period.start,
                                    period_end=period.end,
                                    shard=shard,
                                    bars=shard_bars,
                                    partition_key=key,
                                )
                            )
                        digest = manifest_hash(
                            feed_code=feed_code,
                            adjustment=adjustment,
                            resolution=resolution,
                            period_start=period.start,
                            period_end=period.end,
                            revision=revision,
                            schema_version=self.config.project.schema_version,
                            processing_version=self.config.project.processing_version,
                            objects=[item.manifest_object for item in published],
                        )
                        with self.database.transaction() as connection:
                            register_manifest(
                                MarketRepository(connection),
                                manifest_id=manifest_id,
                                previous_manifest_id=previous_id,
                                manifest_hash=digest,
                                quality_status=validation.status,
                                published=published,
                                source_manifest_id=source_manifest if resolution != "30m" else None,
                                run_id=run_id,
                                warning_codes=validation.warnings,
                            )
                        if resolution == "30m":
                            source_manifest = manifest_id
                        manifest_count += 1
                        object_count += len(published)
                        row_count += len(bars)
            summary = {
                "manifest_count": manifest_count,
                "object_count": object_count,
                "row_count": row_count,
            }
            with self.database.transaction() as connection:
                MarketRepository(connection).complete_run(run_id, succeeded=True, summary=summary)
            self._write_report(run_id, summary)
            return BackfillResult(run_id, False, manifest_count, object_count, row_count)
        except Exception:
            with self.database.transaction() as connection:
                MarketRepository(connection).complete_run(
                    run_id,
                    succeeded=False,
                    summary={
                        "manifest_count": manifest_count,
                        "object_count": object_count,
                        "row_count": row_count,
                    },
                )
            raise

    def _collect_period(
        self,
        instruments: list[UniverseInstrument],
        start: date,
        end: date,
        adjustment: str,
    ) -> list[Bar]:
        result: list[Bar] = []
        for chunk in chunk_ranges(start, end, self.config.alpaca.chunk_days):
            active = [item for item in instruments if item.active_during(chunk.start, chunk.end)]
            batch_size = self.config.alpaca.symbols_per_request
            for offset in range(0, len(active), batch_size):
                result.extend(
                    collect_chunk(
                        self.alpaca,
                        self.calendar,
                        active[offset : offset + batch_size],
                        chunk.start,
                        chunk.end,
                        adjustment,
                    )
                )
        return result

    @staticmethod
    def _write_report(run_id: UUID, summary: dict[str, int]) -> None:
        report_root = Path("reports") / str(run_id)
        report_root.mkdir(parents=True, exist_ok=True)
        (report_root / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (report_root / "partitions.jsonl").touch()
        (report_root / "quality-incidents.jsonl").touch()
