"""The `INGEST_REALTIME_BARS` port: the worker's half of D12/D90.

`pipeline-worker` hosts the realtime consumer.  The queue side is
:class:`~apps.pipeline_worker.messaging.SqsMessageSource`; this module is what
happens to the events once they arrive.

There is exactly one adapter that does the work
(:class:`EngineRealtimeIngestPort`) and one that refuses
(:class:`UnconfiguredRealtimeIngestPort`).  The refusal is deliberate and loud:
without `PIPELINE_WORKER_REALTIME_INGEST` the worker does not know which dataset
contract the events belong to, which event type carries a bar, or which
``values`` key holds each price -- and guessing `PT1M`/`close` is the exact D12
defect this stage exists to remove.  A refused command is retried, never
answered with an empty success.

The ingestor and its watermark ledger are built once and reused, so a redelivered
batch is recognised as a replay rather than published twice.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from apps.common.errors import MalformedEventError, PortNotConfiguredError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from apps.pipeline_worker.config import RealtimeIngestSettings, WorkerConfig
    from market_pipeline_lib.realtime_ingest import RealtimeIngestor


@runtime_checkable
class RealtimeIngestPort(Protocol):
    """Accepts a batch of provider-neutral market events."""

    def ingest(self, events: Sequence[Mapping[str, Any]], *, flush: bool) -> Mapping[str, Any]:
        """Ingest `events`; publish immediately when `flush` is true."""


class UnconfiguredRealtimeIngestPort:
    """Refuses, naming the variable that would enable the port."""

    def ingest(self, events: Sequence[Mapping[str, Any]], *, flush: bool) -> Mapping[str, Any]:
        raise PortNotConfiguredError(
            "INGEST_REALTIME_BARS needs PIPELINE_WORKER_REALTIME_INGEST, which declares the "
            "dataset contract, the event type, the source resolution, the partition "
            "granularity and the value-field mapping. None of those has a default: a worker "
            "that assumed PT1M/close would silently mislabel every bar it published. "
            "Run `pipeline-worker --print-env` for the full document shape."
        )


class EngineRealtimeIngestPort:
    """Drives :class:`~market_pipeline_lib.realtime_ingest.RealtimeIngestor`."""

    def __init__(self, config: WorkerConfig) -> None:
        settings = config.realtime
        if settings is None:  # pragma: no cover - the executor picks the other adapter
            raise PortNotConfiguredError("PIPELINE_WORKER_REALTIME_INGEST is not set")
        self._config = config
        self._settings: RealtimeIngestSettings = settings
        self._ingestor: RealtimeIngestor | None = None

    def _build(self) -> RealtimeIngestor:
        # Imported lazily: pyarrow, pandas and pandas_market_calendars cost about a
        # second, and a worker that never receives a realtime command should not pay it.
        from market_pipeline_lib.contracts import DATASET_CONTRACTS, stable_shard_key
        from market_pipeline_lib.engine import MarketPipelineEngine, PipelineConfig
        from market_pipeline_lib.realtime_ingest import (
            BarFieldMap,
            RealtimeIngestor,
            RealtimeIngestSpec,
        )
        from market_pipeline_lib.watermarks import InMemoryWatermarkRepository, WatermarkLedger

        settings = self._settings
        key = (settings.price_type, settings.data_layer, settings.resolution)
        contract = DATASET_CONTRACTS.get(key)
        if contract is None:
            raise PortNotConfiguredError(
                f"PIPELINE_WORKER_REALTIME_INGEST names no known dataset contract: {key}; "
                f"known contracts are {sorted(DATASET_CONTRACTS)}"
            )
        engine = MarketPipelineEngine(
            PipelineConfig(
                local_root=self._config.object_store_root,
                staging_root=settings.staging_root,
                instrument_map_path=settings.instrument_map_path,
                shard_count=settings.shard_count,
            ),
            catalog=self._catalog(),
        )
        spec = RealtimeIngestSpec(
            contract=contract,
            event_type=settings.event_type,
            source_provider=settings.source_provider,
            source_feed=settings.source_feed,
            source_resolution=settings.source_resolution,
            partition_granularity=settings.partition_granularity,
            fields=BarFieldMap(**dict(settings.value_fields)),
        )
        shard_keys = tuple(
            sorted(
                {
                    stable_shard_key(mapping.instrument_id, settings.shard_count)
                    for mapping in engine.mappings.values()
                }
            )
        )
        ledger = WatermarkLedger(
            feed_id=engine.feed_ids[contract.feed_code],
            shard_keys=shard_keys,
            # A durable `SqlWatermarkRepository` drops in here once the worker is
            # given a database URL; the process-local one is honest about what it
            # is, and `checkpoint` still refuses to invent a floor.
            repository=InMemoryWatermarkRepository(),
        )
        return RealtimeIngestor(engine, spec, ledger=ledger)

    def _catalog(self) -> Any:
        from market_pipeline_lib.catalog import LocalCatalog

        return LocalCatalog(self._config.catalog_root)

    @property
    def ingestor(self) -> RealtimeIngestor:
        if self._ingestor is None:
            self._ingestor = self._build()
        return self._ingestor

    def ingest(self, events: Sequence[Mapping[str, Any]], *, flush: bool) -> Mapping[str, Any]:
        from market_pipeline_lib.realtime_ingest import RealtimeIngestError

        ingestor = self.ingestor
        try:
            decisions = ingestor.submit_batch(events)
        except RealtimeIngestError as error:
            # A structurally bad event cannot be fixed by another delivery.
            raise MalformedEventError(str(error)) from error

        accepted = sum(1 for decision in decisions if decision.accepted)
        summary: dict[str, Any] = {
            "accepted": accepted,
            "skipped": len(decisions) - accepted,
            "pending_rows": ingestor.pending_rows,
            "status": "BUFFERED",
        }
        if not flush:
            return summary
        result = ingestor.flush()
        summary.update(
            {
                "status": result.status,
                "row_count": result.row_count,
                "object_keys": list(result.object_keys),
                "manifest_ids": list(result.manifest_ids),
                "partitions": list(result.partitions),
                "incident_count": result.incident_count,
                "watermark_position": result.watermark_position,
                "pending_rows": ingestor.pending_rows,
            }
        )
        return summary
