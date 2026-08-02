"""Backfill, incremental, derivation, and compaction orchestration."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Protocol

import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .catalog import CatalogCapability, LocalCatalog, MarketDataCatalog
from .contracts import (
    ADJUSTED_FEED,
    CALENDAR_NAME,
    DATASET_CONTRACTS,
    ET,
    FEED_METADATA,
    PROVIDER_CODE,
    RAW_FEED,
    SCHEMA_VERSION,
    DatasetContract,
    Granularity,
    InstrumentMapping,
    canonical_dataset_hash,
    deterministic_uuid,
    iso_utc,
    load_instrument_map,
    logical_dataset_id,
    object_key,
    partition_bounds,
    partition_utc_bounds,
    sha256_file,
    stable_shard_key,
)
from .fs_paths import short_temp_path
from .processing import (
    derive_regular_bars,
    estimate_rows_for_size,
    filter_table_period,
    normalize_legacy_frame,
    normalize_provider_frame,
    quality_issues,
    scan_tables,
    sort_bar_table,
    split_table_by_time,
    write_parquet,
)
from .quality import (
    ImpactScope,
    QualityIncident,
    record_issue_incidents,
    record_quality_incidents,
)
from .storage import LocalObjectStore, ObjectStore


def legacy_staging_filename(source_path: Path, batch_number: int) -> str:
    """Name one legacy migration staging fragment.

    Deterministic, so `--resume` can recognise an already written fragment,
    and short: the fragment lives under
    ``<staging_root>/<run_id>/legacy/contract=…/year=…/shard=…/instrument=<uuid>/``
    which is already ~200 characters, so a 65-character name plus an atomic
    temp suffix used to overflow the Windows MAX_PATH limit.
    """
    token = deterministic_uuid("legacy-source", str(source_path)).replace("-", "")[:12]
    return f"s{token}-b{batch_number:06d}.parquet"


class BarSource(Protocol):
    def fetch(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        price_type: str,
    ) -> pd.DataFrame | None: ...


class AlpacaBarSource:
    """Provider adapter; credentials remain in environment/process memory."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        *,
        chunk_retries: int = 3,
        request_delay: float = 0.35,
    ) -> None:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.trading.client import TradingClient
        from data_collection.collect_sip_1min import InactiveSymbolCache

        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.asset_client = TradingClient(api_key, secret_key)
        self.inactive_cache = InactiveSymbolCache()
        self.chunk_retries = chunk_retries
        self.request_delay = request_delay

    def should_skip_inactive(
        self,
        symbol: str,
        last_bar: datetime,
        end: datetime,
    ) -> bool:
        from data_collection.collect_sip_1min import (
            should_skip_inactive_symbol,
        )

        return should_skip_inactive_symbol(
            self.asset_client,
            self.inactive_cache,
            symbol,
            last_bar,
            end,
        )

    def fetch(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        price_type: str,
    ) -> pd.DataFrame | None:
        from alpaca.data.enums import Adjustment, DataFeed
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(30, TimeFrameUnit.Minute),
            start=start,
            end=end,
            adjustment=(
                Adjustment.ALL if price_type == "adjusted" else Adjustment.RAW
            ),
            feed=DataFeed.SIP,
        )
        for attempt in range(1, self.chunk_retries + 1):
            try:
                response = self.client.get_stock_bars(request)
                if self.request_delay:
                    time.sleep(self.request_delay)
                return response.df
            except Exception:
                if attempt == self.chunk_retries:
                    return None
                time.sleep(2 ** (attempt - 1))
        return None


@dataclass(frozen=True)
class PipelineConfig:
    local_root: Path
    staging_root: Path
    instrument_map_path: Path
    shard_count: int = 16
    target_size_mib: int = 256
    max_size_mib: int = 512
    calendar: str = CALENDAR_NAME
    revision: int | None = None
    resume: bool = False
    dry_run: bool = False

    def validate(self) -> None:
        if self.shard_count <= 0:
            raise ValueError("shard-count는 양수여야 합니다.")
        if self.target_size_mib <= 0:
            raise ValueError("target-size-mib는 양수여야 합니다.")
        if self.max_size_mib < self.target_size_mib:
            raise ValueError("max-size-mib는 target-size-mib 이상이어야 합니다.")
        if self.revision is not None and self.revision <= 0:
            raise ValueError("revision은 1 이상이어야 합니다.")
        mcal.get_calendar(self.calendar)

    @property
    def fingerprint(self) -> str:
        payload = {
            "instrument_map_sha256": (
                hashlib.sha256(self.instrument_map_path.read_bytes()).hexdigest()
                if self.instrument_map_path.is_file()
                else "missing"
            ),
            "shard_count": self.shard_count,
            "target_size_mib": self.target_size_mib,
            "max_size_mib": self.max_size_mib,
            "calendar": self.calendar,
            "revision": self.revision,
            "schema_version": SCHEMA_VERSION,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()


@dataclass
class NewObject:
    storage: dict[str, Any]
    relation: dict[str, Any]
    source_dataset_object_ids: list[str]


def _bar_duration(resolution: str) -> timedelta:
    return {
        "1m": timedelta(minutes=1),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }[resolution]


def _table_period_end(
    table: pa.Table,
    contract: DatasetContract,
) -> datetime:
    timestamps = table.column("bar_start_at").to_pandas()
    maximum = pd.Timestamp(timestamps.max())
    if not contract.has_source_minutes:
        return maximum.to_pydatetime() + _bar_duration(contract.resolution)
    frame = table.select(["bar_start_at", "source_minutes"]).to_pandas()
    matching = frame[pd.to_datetime(frame["bar_start_at"], utc=True) == maximum]
    minutes = int(matching["source_minutes"].max())
    return maximum.to_pydatetime() + timedelta(minutes=minutes)


#: Why a collection failure cannot be scoped to a shard: the fetch never returned, so
#: no object, partition or shard exists to attach the incident to.  The recorded period
#: is still the failed request window, not the manifest's.
_COLLECTION_FAILURE_REASON = (
    "수집 단계 실패는 객체가 생성되기 전에 발생하므로 shard/partition으로 "
    "좁힐 수 없습니다. 기록된 period는 실패한 요청 구간입니다."
)


def _parse_iso(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _iso_or(value: Any, fallback: str | datetime) -> datetime:
    return _parse_iso(value) if value else _parse_iso(fallback)


def _overlaps(
    left_start: str,
    left_end: str,
    right_start: date,
    right_end: date,
) -> bool:
    left_s = date.fromisoformat(str(left_start)[:10])
    left_e = date.fromisoformat(str(left_end)[:10])
    return left_s < right_end and right_start < left_e


class MarketPipelineEngine:
    def __init__(
        self,
        config: PipelineConfig,
        *,
        object_store: ObjectStore | None = None,
        catalog: MarketDataCatalog | None = None,
        source: BarSource | None = None,
    ) -> None:
        config.validate()
        self.config = config
        self.object_store = object_store or LocalObjectStore(config.local_root)
        self.catalog = catalog or LocalCatalog(
            config.local_root / "catalog-export",
            create=not config.dry_run,
        )
        self.source = source
        self.mappings = load_instrument_map(config.instrument_map_path)
        self.feed_ids = {
            code: deterministic_uuid("feed", PROVIDER_CODE, code)
            for code in FEED_METADATA
        }
        if not config.dry_run:
            self._ensure_provider_metadata()

    def _ensure_provider_metadata(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        provider_id = deterministic_uuid("provider", PROVIDER_CODE)
        provider = {
            "id": provider_id,
            "code": PROVIDER_CODE,
            "display_name": "Alpaca",
            "rights_version": "UNVERIFIED",
            "status": "REVIEW_REQUIRED",
            "created_at": now,
        }
        self.catalog.upsert("market_data.providers", provider)
        for code, feed_id in self.feed_ids.items():
            resolution, feed_version = FEED_METADATA[code]
            self.catalog.upsert(
                "market_data.feeds",
                {
                    "id": feed_id,
                    "provider_id": provider_id,
                    "code": code,
                    "data_kind": "BARS",
                    "resolution": resolution,
                    "timezone_name": "America/New_York",
                    "feed_version": feed_version,
                    "created_at": now,
                    "retired_at": None,
                },
            )

    def _materialize_object(
        self,
        object_key_value: str,
        expected_hash: str | None = None,
    ) -> Path:
        """Return a local readable copy without leaking store-specific logic."""
        if isinstance(self.object_store, LocalObjectStore):
            return self.object_store.path_for(object_key_value)
        destination = (
            self.config.staging_root
            / "_materialized"
            / f"{deterministic_uuid(object_key_value)}.parquet"
        )
        if destination.is_file() and (
            expected_hash is None
            or sha256_file(destination) == expected_hash
        ):
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = short_temp_path(destination)
        try:
            with self.object_store.open(object_key_value) as source:
                with temporary.open("wb") as output:
                    shutil.copyfileobj(source, output)
            if expected_hash and sha256_file(temporary) != expected_hash:
                raise OSError(
                    f"materialize SHA-256 불일치: {object_key_value}"
                )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    def _run_record(self, code: str, idempotency_key: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        run_id = deterministic_uuid("pipeline-run", code, idempotency_key)
        existing = self.catalog.pipeline_run(run_id)
        if existing and existing["status"] == "SUCCEEDED":
            return {**existing, "_reused": True}
        record = {
            "id": run_id,
            "pipeline_code": code,
            "pipeline_version": SCHEMA_VERSION,
            "idempotency_key": idempotency_key,
            "status": "RUNNING",
            "input_hash": hashlib.sha256(idempotency_key.encode()).hexdigest(),
            "output_hash": None,
            "started_at": now,
            "completed_at": None,
            "failure_code": None,
        }
        self.catalog.begin_pipeline_run(record)
        return record

    def _next_revision(
        self,
        contract: DatasetContract,
        year: int,
    ) -> tuple[int, dict[str, Any] | None]:
        latest = self.catalog.latest_available_manifest(
            feed_id=self.feed_ids[contract.feed_code],
            data_layer=contract.data_layer,
            resolution=contract.resolution,
            year=year,
        )
        if self.config.revision is not None:
            revision = self.config.revision
            if latest and revision < int(latest["revision_number"]):
                raise ValueError("최신 Revision보다 작은 Revision을 만들 수 없습니다.")
            return revision, latest
        return (int(latest["revision_number"]) + 1 if latest else 1), latest

    def _candidate_tables(
        self,
        table: pa.Table,
    ) -> list[pa.Table]:
        if table.num_rows == 0:
            return []
        target_rows = estimate_rows_for_size(table, self.config.target_size_mib)
        candidates = split_table_by_time(table, target_rows)
        accepted: list[pa.Table] = []
        queue = list(candidates)
        scratch = self.config.staging_root / "_size-check"
        scratch.mkdir(parents=True, exist_ok=True)
        while queue:
            candidate = queue.pop(0)
            path = scratch / f"{deterministic_uuid(candidate.num_rows, len(queue))}.parquet"
            write_parquet(candidate, path)
            too_large = (
                path.stat().st_size > self.config.max_size_mib * 1024 * 1024
            )
            path.unlink(missing_ok=True)
            if too_large and candidate.num_rows > 1:
                halves = split_table_by_time(
                    candidate,
                    max(1, candidate.num_rows // 2),
                )
                if len(halves) == 1:
                    raise ValueError(
                        "하나의 timestamp 경계가 max-size-mib를 초과합니다."
                    )
                queue = halves + queue
            else:
                accepted.append(candidate)
        return accepted

    def _write_new_objects(
        self,
        contract: DatasetContract,
        year: int,
        revision: int,
        manifest_id: str,
        groups: Iterable[
            tuple[
                Granularity,
                date,
                date,
                str,
                pa.Table,
                list[str],
            ]
        ],
    ) -> tuple[list[NewObject], list[dict[str, Any]]]:
        dataset_id = logical_dataset_id(contract, year)
        new_objects: list[NewObject] = []
        incidents: list[dict[str, Any]] = []
        part_counters: dict[tuple[str, str, str], int] = {}
        for (
            granularity,
            partition_start,
            partition_end,
            shard_key,
            table,
            source_ids,
        ) in groups:
            # Validate first, then sort.  `sort_bar_table` repairs out-of-order input
            # and destroys the evidence in the same step, so running it before
            # `quality_issues` made `OUT_OF_ORDER_BARS` unreachable in production --
            # the D10 ordering defect spec section 1 records.
            issues = quality_issues(
                table,
                contract,
                partition_start=partition_start,
                partition_end=partition_end,
                calendar_name=self.config.calendar,
            )
            table = sort_bar_table(table)
            incidents.extend(
                {
                    **issue,
                    "shard_key": shard_key,
                    "partition_start": partition_start.isoformat(),
                    "partition_end": partition_end.isoformat(),
                }
                for issue in issues
            )
            if any(issue["severity"] == "ERROR" for issue in issues):
                continue
            key = (
                granularity,
                partition_start.isoformat(),
                shard_key,
            )
            for candidate in self._candidate_tables(table):
                try:
                    part_counters[key] = part_counters.get(key, 0) + 1
                    part_number = part_counters[key]
                    key_path = object_key(
                        contract,
                        dataset_id,
                        revision,
                        granularity,
                        partition_start,
                        partition_end,
                        shard_key,
                        part_number,
                    )
                    scratch = (
                        self.config.staging_root
                        / "_publish"
                        / deterministic_uuid(key_path)
                        / "object.parquet"
                    )
                    write_parquet(candidate, scratch)
                    receipt = self.object_store.put(scratch, key_path)
                    verification = self.object_store.verify(
                        key_path,
                        receipt.content_hash,
                    )
                    if not verification.ok:
                        raise OSError(
                            f"publish 후 객체 검증 실패: {key_path}"
                        )
                    timestamps = candidate.column("bar_start_at").to_pandas()
                    period_start = pd.Timestamp(
                        timestamps.min()
                    ).to_pydatetime()
                    period_end = _table_period_end(candidate, contract)
                    object_id = deterministic_uuid(
                        "storage-object",
                        receipt.content_hash,
                        key_path,
                    )
                    relation_id = deterministic_uuid(
                        "dataset-object",
                        manifest_id,
                        object_id,
                    )
                    instrument_ids = candidate.column(
                        "instrument_id"
                    ).to_pylist()
                    now = datetime.now(timezone.utc).isoformat()
                    storage = {
                        "id": object_id,
                        "status": "AVAILABLE",
                        "storage_provider": receipt.storage_provider,
                        "bucket_name": receipt.bucket_name,
                        "object_key": receipt.object_key,
                        "provider_version_id": receipt.provider_version_id,
                        "content_hash": receipt.content_hash,
                        "byte_size": receipt.byte_size,
                        "file_format": "PARQUET",
                        "compression_codec": "UNCOMPRESSED",
                        "media_type": "application/vnd.apache.parquet",
                        "schema_version": SCHEMA_VERSION,
                        "row_count": candidate.num_rows,
                        "period_start": iso_utc(period_start),
                        "period_end": iso_utc(period_end),
                        "encryption_key_ref": None,
                        "retention_policy_version": "UNSPECIFIED",
                        "retention_until": None,
                        "legal_hold": False,
                        "created_at": now,
                        "verified_at": now,
                        "quarantined_at": None,
                        "superseded_at": None,
                        "deleted_at": None,
                    }
                    relation = {
                        "id": relation_id,
                        "dataset_manifest_id": manifest_id,
                        "object_id": object_id,
                        "object_kind": "MARKET_BARS",
                        "partition_granularity": granularity,
                        "partition_start": partition_start.isoformat(),
                        "partition_end": partition_end.isoformat(),
                        "period_start": iso_utc(period_start),
                        "period_end": iso_utc(period_end),
                        "shard_key": shard_key,
                        "part_number": part_number,
                        "row_count": candidate.num_rows,
                        "min_instrument_id": min(instrument_ids),
                        "max_instrument_id": max(instrument_ids),
                    }
                    new_objects.append(
                        NewObject(storage, relation, source_ids)
                    )
                except Exception as exc:
                    incidents.append(
                        {
                            "severity": "ERROR",
                            "code": "OBJECT_PUBLISH_FAILED",
                            "message": str(exc),
                            "shard_key": shard_key,
                            "partition_start": partition_start.isoformat(),
                            "partition_end": partition_end.isoformat(),
                        }
                    )
        return new_objects, incidents

    @staticmethod
    def _collection_failure_incident(
        issue: dict[str, Any],
        manifest: dict[str, Any],
        detected_at: datetime,
    ) -> QualityIncident:
        """A failure that happened before any object existed, scoped as narrowly as it can be.

        A provider fetch that never returned, or a derived contract with no source
        manifest, produces no `dataset_objects` row, so there is no shard or partition
        to attach.  `ImpactScope.manifest_wide` is the declared escape hatch and
        requires a written reason; the recorded period is still the failed window the
        caller reports, clamped to the manifest, not the manifest period itself.
        """

        reason = issue.get("manifest_wide_reason")
        if not reason:
            raise ValueError(
                f"{issue['code']} has no shard_key, so recording it needs an explicit "
                "manifest_wide_reason; widening an incident silently is the D10 defect"
            )
        lower = _iso_or(issue.get("period_start"), manifest["period_start"])
        upper = _iso_or(issue.get("period_end"), manifest["period_end"])
        return QualityIncident(
            incident_code=issue["code"],
            severity=issue["severity"],
            scope=ImpactScope.manifest_wide(
                period_start=max(lower, _parse_iso(manifest["period_start"])),
                period_end=min(upper, _parse_iso(manifest["period_end"])),
                reason=str(reason),
            ),
            detected_at=detected_at,
            message=str(issue.get("message", "")),
        )

    @staticmethod
    def _canonical_object(relation: dict[str, Any], storage: dict[str, Any]) -> dict[str, Any]:
        return {
            "content_hash": storage["content_hash"],
            "object_kind": relation["object_kind"],
            "partition_granularity": relation["partition_granularity"],
            "partition_start": relation["partition_start"],
            "partition_end": relation["partition_end"],
            "period_start": relation["period_start"],
            "period_end": relation["period_end"],
            "shard_key": relation["shard_key"],
            "part_number": relation["part_number"],
            "row_count": relation["row_count"],
            "schema_version": storage["schema_version"],
        }

    def publish_dataset(
        self,
        contract: DatasetContract,
        year: int,
        groups: Iterable[
            tuple[Granularity, date, date, str, pa.Table, list[str]]
        ],
        *,
        replace_periods: list[tuple[date, date]] | None = None,
        dataset_source_manifest_id: str | None = None,
        relation_type: str = "RESAMPLED_FROM",
        additional_incidents: Iterable[dict[str, Any]] = (),
    ) -> dict[str, Any]:
        """Publish one dataset revision: objects to the store, rows to the catalog.

        The object-store writes happen first and are not transactional -- objects are
        immutable and content-addressed, so an orphan costs storage and nothing else.
        Every catalog write then happens inside a single `catalog.transaction()`: the
        BUILDING manifest, the staged objects, the lineage, the incidents, the final
        manifest and the supersede of the previous revision either all commit or none
        do.  Without that, a crash between the last two writes leaves two AVAILABLE
        manifests for one period, or a permanently BUILDING manifest owning objects.
        """

        revision, previous = self._next_revision(contract, year)
        manifest_id = deterministic_uuid(
            "manifest",
            contract.logical_code,
            year,
            revision,
        )
        now = datetime.now(timezone.utc).isoformat()
        year_start, year_end = partition_bounds(date(year, 1, 1), "YEAR")
        building = {
            "id": manifest_id,
            "feed_id": self.feed_ids[contract.feed_code],
            "instrument_id": None,
            "data_layer": contract.data_layer,
            "resolution": contract.resolution,
            "revision_number": revision,
            "status": "BUILDING",
            "period_start": iso_utc(partition_utc_bounds(year_start, "YEAR")[0]),
            "period_end": iso_utc(partition_utc_bounds(year_start, "YEAR")[1]),
            "schema_version": SCHEMA_VERSION,
            "dataset_hash": hashlib.sha256(b"BUILDING").hexdigest(),
            "supersedes_manifest_id": previous["id"] if previous else None,
            "created_at": now,
            "available_at": None,
        }
        new_objects, incidents = self._write_new_objects(
            contract,
            year,
            revision,
            manifest_id,
            groups,
        )
        incidents.extend(dict(value) for value in additional_incidents)
        retained: list[NewObject] = []
        replace_periods = replace_periods or []
        if previous:
            # Carry every object the new revision does not replace forward.  This loop
            # used to be gated on the catalog's concrete type, so against any catalog
            # other than the local one a revision silently published *only* the
            # replaced partitions and dropped the rest of the dataset -- the latent
            # data-loss bug spec section 1 records.
            for old in self.catalog.objects_for_manifest(previous["id"]):
                relation = {
                    key: value for key, value in old.items() if key != "storage"
                }
                if any(
                    _overlaps(
                        relation["partition_start"],
                        relation["partition_end"],
                        start,
                        end,
                    )
                    for start, end in replace_periods
                ):
                    continue
                relation["id"] = deterministic_uuid(
                    "dataset-object",
                    manifest_id,
                    relation["object_id"],
                )
                relation["dataset_manifest_id"] = manifest_id
                retained.append(NewObject(old["storage"], relation, []))
        all_objects = retained + new_objects
        error_incidents = [
            incident for incident in incidents if incident["severity"] == "ERROR"
        ]
        status = "AVAILABLE" if all_objects and not error_incidents else "QUARANTINED"
        canonical = [
            self._canonical_object(item.relation, item.storage)
            for item in all_objects
        ]
        observed_start = min(
            (item.relation["period_start"] for item in all_objects),
            default=building["period_start"],
        )
        observed_end = max(
            (item.relation["period_end"] for item in all_objects),
            default=building["period_start"],
        )
        manifest = {
            **building,
            "status": status,
            "period_start": observed_start,
            "period_end": observed_end,
            "dataset_hash": canonical_dataset_hash(canonical),
            "available_at": now if status == "AVAILABLE" else None,
        }
        records_outputs = self.catalog.supports(CatalogCapability.PIPELINE_RUN_OUTPUTS)
        with self.catalog.transaction():
            self.catalog.publish_manifest(building)
            for item in all_objects:
                self.catalog.stage_object(item.storage, item.relation)
                if records_outputs and item in new_objects:
                    self.catalog.record_pipeline_output(
                        pipeline_run_id=self.active_run_id,
                        dataset_manifest_id=manifest_id,
                        dataset_object_id=item.relation["id"],
                    )
                for source_id in item.source_dataset_object_ids:
                    self.catalog.record_object_lineage(
                        {
                            "derived_dataset_object_id": item.relation["id"],
                            "source_dataset_object_id": source_id,
                            "pipeline_run_id": self.active_run_id,
                            "relation_type": relation_type,
                            "created_at": now,
                        }
                    )
            # Data-derived findings already carry their own instrument, period and
            # affected bar count from `quality_issues`; `record_issue_incidents` keeps
            # that scope instead of the previous hand-built row, which flattened every
            # finding to `instrument_id=None` plus the manifest's whole period.
            detected_at = datetime.fromisoformat(now)
            record_issue_incidents(
                self.catalog,
                [issue for issue in incidents if issue.get("shard_key")],
                dataset_manifest_id=manifest_id,
                detected_at=detected_at,
            )
            # Collection-time failures produced no object, so there is no shard to
            # scope them to.  They are widened deliberately, with the reason and the
            # failed window written down, never by omission.
            record_quality_incidents(
                self.catalog,
                [
                    self._collection_failure_incident(issue, building, detected_at)
                    for issue in incidents
                    if not issue.get("shard_key")
                ],
                dataset_manifest_id=manifest_id,
            )
            self.catalog.publish_manifest(manifest)
            if status == "AVAILABLE" and previous:
                self.catalog.publish_manifest(
                    {
                        **previous,
                        "status": "SUPERSEDED",
                        "available_at": previous.get("available_at"),
                    }
                )
            if dataset_source_manifest_id:
                self.catalog.record_dataset_lineage(
                    {
                        "derived_manifest_id": manifest_id,
                        "source_manifest_id": dataset_source_manifest_id,
                        "relation_type": relation_type,
                    }
                )
        return {
            "manifest": manifest,
            "new_object_count": len(new_objects),
            "retained_object_count": len(retained),
            "incident_count": len(incidents),
        }

    def start_run(self, code: str, idempotency_key: str) -> dict[str, Any]:
        """Begin -- or reuse -- one `market_data.pipeline_runs` row, and make it active.

        The public entry point every caller that publishes datasets goes through,
        including `backfill`, `migrate_legacy`, `incremental`, `derive`, `compact` and
        the realtime ingest path.  The run id is derived from `code` and
        `idempotency_key`, so re-running the same job finds the same row; when that row
        already reached SUCCEEDED the returned mapping carries ``_reused: True`` and the
        caller should stop rather than redo the work.

        `publish_dataset` needs an active run for `dataset_object_lineage`, so the
        active run is set here rather than left to each caller to remember.
        """

        run = self._run_record(code, idempotency_key)
        self._active_run_id = run["id"]
        return run

    @property
    def active_run_id(self) -> str:
        """The run `publish_dataset` will attribute lineage to.

        Raises rather than defaulting: lineage attributed to a run that was never
        started is worse than no lineage, because it looks like provenance.
        """

        value = getattr(self, "_current_run_id", None)
        if value is None:
            raise RuntimeError("pipeline run이 시작되지 않았습니다.")
        return str(value)

    @property
    def _active_run_id(self) -> str:
        """Deprecated alias for `active_run_id`; `start_run` is the way in.

        Kept because tests and callers that predate `start_run` set it directly to
        attribute a hand-built `publish_dataset` call to a run they created themselves.
        """

        return self.active_run_id

    @_active_run_id.setter
    def _active_run_id(self, value: str) -> None:
        self._current_run_id = value

    def plan(
        self,
        *,
        start: datetime,
        end: datetime,
        price_types: Iterable[str],
        resolutions: Iterable[str] = ("30m", "1h", "4h", "1d"),
        symbols: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        price_types = tuple(price_types)
        resolutions = tuple(resolutions)
        selected_symbols = self._selected_mappings(symbols)
        chunks = 0
        cursor = start
        while cursor < end:
            cursor = min(cursor + timedelta(days=180), end)
            chunks += 1
        return {
            "mode": "dry-run",
            "provider": PROVIDER_CODE,
            "feed_codes": [
                RAW_FEED if value == "raw" else ADJUSTED_FEED
                for value in price_types
            ],
            "resolutions": list(resolutions),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "symbols": len(selected_symbols),
            "api_chunks_per_symbol": chunks,
            "estimated_api_requests": chunks
            * len(selected_symbols)
            * len(price_types),
            "shard_count": self.config.shard_count,
            "external_writes": False,
        }

    def _selected_mappings(
        self,
        symbols: Iterable[str] | None,
    ) -> list[InstrumentMapping]:
        if symbols is None:
            return sorted(self.mappings.values(), key=lambda item: item.instrument_id)
        requested = {
            value.strip().upper().replace("/", ".")
            for value in symbols
            if value.strip()
        }
        missing = requested.difference(self.mappings)
        if missing:
            raise ValueError(f"instrument map에 없는 symbol: {sorted(missing)}")
        return sorted(
            (self.mappings[symbol] for symbol in requested),
            key=lambda item: item.instrument_id,
        )

    def _staging_path(
        self,
        run_code: str,
        price_type: str,
        year: int,
        shard: str,
        mapping: InstrumentMapping,
        chunk_start: datetime,
    ) -> Path:
        return (
            self.config.staging_root
            / run_code
            / f"price_type={price_type}"
            / f"year={year}"
            / f"shard={shard}"
            / f"instrument={mapping.instrument_id}"
            / f"chunk={chunk_start.strftime('%Y%m%dT%H%M%SZ')}.parquet"
        )

    def collect_staging(
        self,
        *,
        run_code: str,
        start: datetime,
        end: datetime,
        price_types: Iterable[str],
        symbols: Iterable[str] | None = None,
        chunk_days: int = 180,
        check_inactive: bool = False,
    ) -> dict[str, Any]:
        if self.source is None:
            raise RuntimeError("Alpaca BarSource가 설정되지 않았습니다.")
        mappings = self._selected_mappings(symbols)
        failures: list[dict[str, Any]] = []
        inactive_symbols: list[dict[str, Any]] = []
        fragments: list[str] = []
        for price_type in price_types:
            for mapping in mappings:
                if check_inactive and hasattr(
                    self.source, "should_skip_inactive"
                ):
                    last_bar = self._latest_published_bar(
                        mapping.instrument_id,
                        price_type,
                        end.year,
                    )
                    if last_bar is not None and self.source.should_skip_inactive(  # type: ignore[attr-defined]
                        mapping.provider_symbol,
                        last_bar,
                        end,
                    ):
                        inactive_symbols.append(
                            {
                                "provider_symbol": mapping.provider_symbol,
                                "instrument_id": mapping.instrument_id,
                                "last_bar": last_bar.isoformat(),
                            }
                        )
                        continue
                cursor = start
                while cursor < end:
                    chunk_end = min(cursor + timedelta(days=chunk_days), end)
                    expected_candidates = list(
                        self.config.staging_root.glob(
                            (
                                f"{run_code}/price_type={price_type}/year=*/shard=*/"
                                f"instrument={mapping.instrument_id}/"
                                f"chunk={cursor.strftime('%Y%m%dT%H%M%SZ')}.parquet"
                            )
                        )
                    )
                    if self.config.resume and expected_candidates:
                        fragments.extend(str(path) for path in expected_candidates)
                        cursor = chunk_end
                        continue
                    try:
                        frame = self.source.fetch(
                            mapping.provider_symbol,
                            cursor,
                            chunk_end,
                            price_type,
                        )
                    except Exception as exc:
                        failures.append(
                            {
                                "symbol": mapping.provider_symbol,
                                "price_type": price_type,
                                "start": cursor.isoformat(),
                                "end": chunk_end.isoformat(),
                                "code": "ALPACA_FETCH_FAILED",
                                "message": str(exc),
                            }
                        )
                        cursor = chunk_end
                        continue
                    if frame is None:
                        failures.append(
                            {
                                "symbol": mapping.provider_symbol,
                                "price_type": price_type,
                                "start": cursor.isoformat(),
                                "end": chunk_end.isoformat(),
                                "code": "ALPACA_FETCH_FAILED",
                            }
                        )
                        cursor = chunk_end
                        continue
                    try:
                        table = normalize_provider_frame(frame, mapping)
                    except (OSError, TypeError, ValueError) as exc:
                        failures.append(
                            {
                                "symbol": mapping.provider_symbol,
                                "price_type": price_type,
                                "start": cursor.isoformat(),
                                "end": chunk_end.isoformat(),
                                "code": "SOURCE_NORMALIZATION_FAILED",
                                "message": str(exc),
                            }
                        )
                        cursor = chunk_end
                        continue
                    if table.num_rows:
                        years = sorted(
                            set(table.column("session_date_et").to_pylist())
                        )
                        for year in sorted({value.year for value in years}):
                            mask = pc.equal(
                                pc.year(table.column("session_date_et")),
                                pa.scalar(year),
                            )
                            year_table = table.filter(mask)
                            if not year_table.num_rows:
                                continue
                            shard = stable_shard_key(
                                mapping.instrument_id,
                                self.config.shard_count,
                            )
                            path = self._staging_path(
                                run_code,
                                price_type,
                                year,
                                shard,
                                mapping,
                                cursor,
                            )
                            write_parquet(year_table, path)
                            fragments.append(str(path))
                    cursor = chunk_end
        return {
            "fragment_count": len(fragments),
            "failures": failures,
            "inactive_symbols": inactive_symbols,
            "staging_root": str(self.config.staging_root / run_code),
        }

    def _latest_published_bar(
        self,
        instrument_id: str,
        price_type: str,
        year: int,
    ) -> datetime | None:
        native_layer = "RAW" if price_type == "raw" else "ADJUSTED"
        feed_code = RAW_FEED if price_type == "raw" else ADJUSTED_FEED
        latest_value: pd.Timestamp | None = None
        for candidate_year in (year, year - 1):
            manifest = self.catalog.latest_available_manifest(
                feed_id=self.feed_ids[feed_code],
                data_layer=native_layer,
                resolution="30m",
                year=candidate_year,
            )
            if manifest is None:
                continue
            for item in self.catalog.objects_for_manifest(manifest["id"]):
                path = self._materialize_object(
                    item["storage"]["object_key"],
                    item["storage"]["content_hash"],
                )
                table = ds.dataset(str(path), format="parquet").to_table(
                    columns=["bar_start_at"],
                    filter=ds.field("instrument_id") == instrument_id,
                )
                if not table.num_rows:
                    continue
                maximum = pd.Timestamp(
                    pc.max(table.column("bar_start_at")).as_py()
                )
                if latest_value is None or maximum > latest_value:
                    latest_value = maximum
            if latest_value is not None:
                break
        return latest_value.to_pydatetime() if latest_value is not None else None

    @staticmethod
    def _legacy_source_root(
        input_root: Path,
        contract: DatasetContract,
    ) -> tuple[Path, str]:
        if contract.data_layer in {"RAW", "ADJUSTED"}:
            return (
                input_root
                / "sip_market_data"
                / contract.price_type
                / "parquet",
                "_30min_sip_historical",
            )
        interval = {
            "1h": "1hour",
            "4h": "4hour",
            "1d": "1day",
        }[contract.resolution]
        return (
            input_root
            / f"regular_sip_{interval}_market_data"
            / contract.price_type
            / "parquet",
            f"_{interval}_sip_historical",
        )

    def _mapping_for_legacy_filename(
        self,
        path: Path,
        marker: str,
    ) -> InstrumentMapping | None:
        stem = path.stem
        if not stem.endswith(marker):
            return None
        raw = stem[: -len(marker)].upper()
        candidates = (
            raw.replace("/", "."),
            raw.replace("-", "."),
            raw,
        )
        return next(
            (self.mappings[value] for value in candidates if value in self.mappings),
            None,
        )

    def _legacy_staging_groups(
        self,
        run_id: str,
        contract: DatasetContract,
        year: int,
        *,
        source_manifest_id: str | None = None,
    ) -> Iterator[
        tuple[Granularity, date, date, str, pa.Table, list[str]]
    ]:
        base = (
            self.config.staging_root
            / run_id
            / "legacy"
            / f"contract={contract.logical_code.replace(':', '_')}"
            / f"year={year}"
        )
        if not base.is_dir():
            return
        source_objects = (
            self.catalog.objects_for_manifest(source_manifest_id)
            if source_manifest_id
            else []
        )
        year_start, year_end = partition_bounds(date(year, 1, 1), "YEAR")
        for shard_root in sorted(base.glob("shard=*")):
            shard = shard_root.name.split("=", 1)[1]
            paths = sorted(shard_root.rglob("*.parquet"))
            for table in self._month_tables(paths, year):
                timestamps = table.column("bar_start_at").to_pandas()
                observed_start = pd.Timestamp(timestamps.min()).isoformat()
                observed_end = pd.Timestamp(
                    _table_period_end(table, contract)
                ).isoformat()
                source_ids = [
                    item["id"]
                    for item in source_objects
                    if item["shard_key"] == shard
                    and item["period_start"] < observed_end
                    and observed_start < item["period_end"]
                ]
                yield (
                    "YEAR",
                    year_start,
                    year_end,
                    shard,
                    table,
                    source_ids,
                )

    def migrate_legacy(
        self,
        *,
        input_root: Path,
        start_year: int,
        end_year: int,
        price_types: Iterable[str],
        resolutions: Iterable[str] = ("30m", "1h", "4h", "1d"),
    ) -> dict[str, Any]:
        """Convert legacy per-symbol ten-year files without deleting them."""
        if end_year < start_year:
            raise ValueError("end-year는 start-year 이상이어야 합니다.")
        price_types = tuple(price_types)
        resolutions = tuple(resolutions)
        contracts = [
            contract
            for contract in DATASET_CONTRACTS.values()
            if contract.price_type in price_types
            and contract.resolution in resolutions
        ]
        inventory = []
        for contract in contracts:
            root, marker = self._legacy_source_root(input_root, contract)
            files = sorted(root.glob(f"*{marker}.parquet")) if root.is_dir() else []
            inventory.append(
                {
                    "contract": contract.logical_code,
                    "source_root": str(root),
                    "file_count": len(files),
                }
            )
        if self.config.dry_run:
            return {
                "status": "DRY_RUN",
                "input_root": str(input_root),
                "start_year": start_year,
                "end_year": end_year,
                "inventory": inventory,
            }
        key = (
            f"{self.config.fingerprint}:migrate:{input_root.resolve()}:"
            f"{start_year}:{end_year}:{','.join(price_types)}:"
            f"{','.join(resolutions)}"
        )
        run = self.start_run("LEGACY_MARKET_DATA_MIGRATION", key)
        if run.get("_reused"):
            return {
                "status": "SUCCEEDED",
                "pipeline_run_id": run["id"],
                "reused": True,
                "inventory": inventory,
            }
        failures = []
        for contract in contracts:
            root, marker = self._legacy_source_root(input_root, contract)
            if not root.is_dir():
                continue
            for path in sorted(root.glob(f"*{marker}.parquet")):
                mapping = self._mapping_for_legacy_filename(path, marker)
                if mapping is None:
                    failures.append(
                        {
                            "code": "UNMAPPED_INSTRUMENT",
                            "path": str(path),
                            "contract": contract.logical_code,
                        }
                    )
                    continue
                try:
                    parquet_file = pq.ParquetFile(path)
                    for batch_number, batch in enumerate(
                        parquet_file.iter_batches(batch_size=65_536),
                        1,
                    ):
                        table = normalize_legacy_frame(
                            pa.Table.from_batches([batch]).to_pandas(),
                            mapping,
                            contract,
                        )
                        for year in range(start_year, end_year + 1):
                            year_start, year_end = partition_utc_bounds(
                                date(year, 1, 1),
                                "YEAR",
                            )
                            year_table = filter_table_period(
                                table,
                                year_start,
                                year_end,
                            )
                            if not year_table.num_rows:
                                continue
                            shard = stable_shard_key(
                                mapping.instrument_id,
                                self.config.shard_count,
                            )
                            destination = (
                                self.config.staging_root
                                / run["id"]
                                / "legacy"
                                / (
                                    "contract="
                                    + contract.logical_code.replace(":", "_")
                                )
                                / f"year={year}"
                                / f"shard={shard}"
                                / f"instrument={mapping.instrument_id}"
                                / legacy_staging_filename(
                                    path.resolve(),
                                    batch_number,
                                )
                            )
                            if not (
                                self.config.resume and destination.is_file()
                            ):
                                write_parquet(year_table, destination)
                except Exception as exc:
                    failures.append(
                        {
                            "code": "LEGACY_READ_FAILED",
                            "path": str(path),
                            "contract": contract.logical_code,
                            "message": str(exc),
                        }
                    )
        results = []
        native_manifests: dict[tuple[str, int], str] = {}
        ordered = sorted(
            contracts,
            key=lambda value: (
                value.price_type,
                value.data_layer == "DERIVED",
                value.resolution,
            ),
        )
        for contract in ordered:
            for year in range(start_year, end_year + 1):
                source_manifest_id = native_manifests.get(
                    (contract.price_type, year)
                )
                if contract.data_layer == "DERIVED" and source_manifest_id is None:
                    source_layer = (
                        "RAW"
                        if contract.price_type == "raw"
                        else "ADJUSTED"
                    )
                    source_manifest = (
                        self.catalog.latest_available_manifest(
                            feed_id=self.feed_ids[contract.feed_code],
                            data_layer=source_layer,
                            resolution="30m",
                            year=year,
                        )
                    )
                    source_manifest_id = (
                        source_manifest["id"] if source_manifest else None
                    )
                result = self.publish_dataset(
                    contract,
                    year,
                    self._legacy_staging_groups(
                        run["id"],
                        contract,
                        year,
                        source_manifest_id=source_manifest_id,
                    ),
                    replace_periods=[
                        partition_bounds(date(year, 1, 1), "YEAR")
                    ],
                    dataset_source_manifest_id=(
                        source_manifest_id
                        if contract.data_layer == "DERIVED"
                        else None
                    ),
                    relation_type="MIGRATED_FROM",
                    additional_incidents=(
                        [
                            {
                                "severity": "ERROR",
                                "code": "SOURCE_MANIFEST_MISSING",
                                "message": (
                                    "파생 legacy 객체와 연결할 "
                                    "공급자 30m Manifest가 없습니다."
                                ),
                                "partition_start": f"{year}-01-01",
                                "partition_end": f"{year + 1}-01-01",
                                "shard_key": None,
                                "manifest_wide_reason": (
                                    "원본 manifest 자체가 없으므로 shard나 "
                                    "instrument 단위로 좁힐 근거가 존재하지 않습니다."
                                ),
                            }
                        ]
                        if contract.data_layer == "DERIVED"
                        and source_manifest_id is None
                        else []
                    ),
                )
                results.append(result)
                if (
                    contract.data_layer in {"RAW", "ADJUSTED"}
                    and result["manifest"]["status"] == "AVAILABLE"
                ):
                    native_manifests[(contract.price_type, year)] = result[
                        "manifest"
                    ]["id"]
        failed = bool(failures) or any(
            result["manifest"]["status"] != "AVAILABLE" for result in results
        )
        output_hash = hashlib.sha256(
            json.dumps(
                [result["manifest"]["dataset_hash"] for result in results],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        self.catalog.finish_pipeline_run(
            run["id"],
            status="FAILED" if failed else "SUCCEEDED",
            output_hash=output_hash,
            failure_code="PARTIAL_MIGRATION_FAILURE" if failed else None,
        )
        return {
            "status": "FAILED" if failed else "SUCCEEDED",
            "pipeline_run_id": run["id"],
            "inventory": inventory,
            "failures": failures,
            "results": results,
        }

    def _month_tables(
        self,
        paths: list[Path],
        year: int,
        *,
        derive_resolution: str | None = None,
    ) -> Iterator[pa.Table]:
        for month in range(1, 13):
            start = date(year, month, 1)
            end = (
                date(year + 1, 1, 1)
                if month == 12
                else date(year, month + 1, 1)
            )
            expression = (
                (ds.field("session_date_et") >= pa.scalar(start))
                & (ds.field("session_date_et") < pa.scalar(end))
            )
            batches = list(
                scan_tables(paths, filter_expression=expression)
            )
            if not batches:
                continue
            table = sort_bar_table(pa.concat_tables(batches))
            if derive_resolution:
                table = derive_regular_bars(
                    table,
                    derive_resolution,
                    self.config.calendar,
                )
            if table.num_rows:
                yield table

    def _annual_groups_from_staging(
        self,
        run_code: str,
        contract: DatasetContract,
        year: int,
        *,
        source_manifest_id: str | None = None,
    ) -> Iterator[
        tuple[Granularity, date, date, str, pa.Table, list[str]]
    ]:
        base = (
            self.config.staging_root
            / run_code
            / f"price_type={contract.price_type}"
            / f"year={year}"
        )
        year_start, year_end = partition_bounds(date(year, 1, 1), "YEAR")
        if not base.is_dir():
            return
        source_objects = (
            self.catalog.objects_for_manifest(source_manifest_id)
            if source_manifest_id
            else []
        )
        for shard_root in sorted(base.glob("shard=*")):
            paths = sorted(shard_root.rglob("*.parquet"))
            if not paths:
                continue
            shard_key = shard_root.name.split("=", 1)[1]
            resolution = (
                contract.resolution if contract.data_layer == "DERIVED" else None
            )
            for table in self._month_tables(
                paths,
                year,
                derive_resolution=resolution,
            ):
                timestamps = table.column("bar_start_at").to_pandas()
                observed_start = pd.Timestamp(timestamps.min()).isoformat()
                observed_end = pd.Timestamp(
                    _table_period_end(table, contract)
                ).isoformat()
                source_ids = [
                    item["id"]
                    for item in source_objects
                    if item["shard_key"] == shard_key
                    and item["period_start"] < observed_end
                    and observed_start < item["period_end"]
                ]
                yield (
                    "YEAR",
                    year_start,
                    year_end,
                    shard_key,
                    table,
                    source_ids,
                )

    def backfill(
        self,
        *,
        start: datetime,
        end: datetime,
        price_types: Iterable[str],
        resolutions: Iterable[str] = ("30m", "1h", "4h", "1d"),
        symbols: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        price_types = tuple(price_types)
        resolutions = tuple(resolutions)
        if self.config.dry_run:
            return self.plan(
                start=start,
                end=end,
                price_types=price_types,
                resolutions=resolutions,
                symbols=symbols,
            )
        key = (
            f"{self.config.fingerprint}:backfill:{start.isoformat()}:"
            f"{end.isoformat()}:{','.join(price_types)}:"
            f"{','.join(resolutions)}"
        )
        run = self.start_run("ALPACA_SIP_BACKFILL", key)
        if run.get("_reused"):
            return {
                "status": "SUCCEEDED",
                "pipeline_run_id": run["id"],
                "reused": True,
            }
        staging = self.collect_staging(
            run_code=run["id"],
            start=start,
            end=end,
            price_types=price_types,
            symbols=symbols,
        )
        results = []
        for price_type in price_types:
            native_layer = "RAW" if price_type == "raw" else "ADJUSTED"
            native = DATASET_CONTRACTS[(price_type, native_layer, "30m")]
            first_year = start.astimezone(ET).year
            last_year = (end - timedelta(microseconds=1)).astimezone(ET).year
            for year in range(first_year, last_year + 1):
                native_result = self.publish_dataset(
                    native,
                    year,
                    self._annual_groups_from_staging(
                        run["id"],
                        native,
                        year,
                    ),
                    replace_periods=[
                        partition_bounds(date(year, 1, 1), "YEAR")
                    ],
                    additional_incidents=[
                        {
                            "severity": "ERROR",
                            "code": failure["code"],
                            "message": (
                                f"{failure['symbol']} "
                                f"{failure['start']}~{failure['end']}"
                            ),
                            "partition_start": f"{year}-01-01",
                            "partition_end": f"{year + 1}-01-01",
                            "shard_key": None,
                            "period_start": failure["start"],
                            "period_end": failure["end"],
                            "manifest_wide_reason": _COLLECTION_FAILURE_REASON,
                        }
                        for failure in staging["failures"]
                        if failure["price_type"] == price_type
                        and int(str(failure["start"])[:4]) <= year
                        <= int(str(failure["end"])[:4])
                    ],
                )
                results.append(native_result)
                for resolution in (
                    value
                    for value in ("1h", "4h", "1d")
                    if value in resolutions
                ):
                    derived = DATASET_CONTRACTS[
                        (price_type, "DERIVED", resolution)
                    ]
                    result = self.publish_dataset(
                        derived,
                        year,
                        self._annual_groups_from_staging(
                            run["id"],
                            derived,
                            year,
                            source_manifest_id=native_result["manifest"]["id"],
                        ),
                        replace_periods=[
                            partition_bounds(date(year, 1, 1), "YEAR")
                        ],
                        dataset_source_manifest_id=native_result["manifest"]["id"],
                        relation_type=(
                            "REGULAR_SESSION_FILTERED_FROM"
                            if resolution == "1h"
                            else "RESAMPLED_FROM"
                        ),
                        additional_incidents=[
                            {
                                "severity": "ERROR",
                                "code": failure["code"],
                                "message": (
                                    f"{failure['symbol']} "
                                    f"{failure['start']}~{failure['end']}"
                                ),
                                "partition_start": f"{year}-01-01",
                                "partition_end": f"{year + 1}-01-01",
                                "shard_key": None,
                                "period_start": failure["start"],
                                "period_end": failure["end"],
                                "manifest_wide_reason": _COLLECTION_FAILURE_REASON,
                            }
                            for failure in staging["failures"]
                            if failure["price_type"] == price_type
                            and int(str(failure["start"])[:4]) <= year
                            <= int(str(failure["end"])[:4])
                        ],
                    )
                    results.append(result)
        failed = bool(staging["failures"]) or any(
            result["manifest"]["status"] != "AVAILABLE" for result in results
        )
        output_hash = hashlib.sha256(
            json.dumps(
                [
                    result["manifest"]["dataset_hash"]
                    for result in results
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        self.catalog.finish_pipeline_run(
            run["id"],
            status="FAILED" if failed else "SUCCEEDED",
            output_hash=output_hash,
            failure_code="PARTIAL_BACKFILL_FAILURE" if failed else None,
        )
        summary = {
            "status": "FAILED" if failed else "SUCCEEDED",
            "pipeline_run_id": run["id"],
            "staging": staging,
            "manifest_count": len(results),
            "available_manifests": sum(
                result["manifest"]["status"] == "AVAILABLE"
                for result in results
            ),
            "results": results,
        }
        self.catalog.write_summary(summary)
        return summary

    def derive(
        self,
        *,
        years: Iterable[int],
        price_types: Iterable[str],
        resolutions: Iterable[str] = ("1h", "4h", "1d"),
    ) -> dict[str, Any]:
        """Rebuild derived annual objects from published native 30m manifests."""
        years = tuple(sorted(set(years)))
        price_types = tuple(price_types)
        resolutions = tuple(resolutions)
        if self.config.dry_run:
            operations = []
            for price_type in price_types:
                native_layer = "RAW" if price_type == "raw" else "ADJUSTED"
                for year in years:
                    native = self.catalog.latest_available_manifest(
                        feed_id=self.feed_ids[
                            RAW_FEED if price_type == "raw" else ADJUSTED_FEED
                        ],
                        data_layer=native_layer,
                        resolution="30m",
                        year=year,
                    )
                    operations.append(
                        {
                            "price_type": price_type,
                            "year": year,
                            "source_manifest_id": native["id"] if native else None,
                            "resolutions": list(resolutions),
                        }
                    )
            return {"status": "DRY_RUN", "operations": operations}
        key = (
            f"{self.config.fingerprint}:derive:{','.join(map(str, years))}:"
            f"{','.join(price_types)}:{','.join(resolutions)}"
        )
        run = self.start_run("MARKET_DATA_DERIVE", key)
        if run.get("_reused"):
            return {
                "status": "SUCCEEDED",
                "pipeline_run_id": run["id"],
                "reused": True,
            }
        results = []
        for price_type in price_types:
            native_layer = "RAW" if price_type == "raw" else "ADJUSTED"
            feed_code = RAW_FEED if price_type == "raw" else ADJUSTED_FEED
            for year in years:
                native = self.catalog.latest_available_manifest(
                    feed_id=self.feed_ids[feed_code],
                    data_layer=native_layer,
                    resolution="30m",
                    year=year,
                )
                if native is None:
                    results.append(
                        {
                            "status": "QUARANTINED",
                            "price_type": price_type,
                            "year": year,
                            "code": "SOURCE_MANIFEST_MISSING",
                        }
                    )
                    continue
                source_objects = self.catalog.objects_for_manifest(native["id"])
                for resolution in resolutions:
                    contract = DATASET_CONTRACTS[
                        (price_type, "DERIVED", resolution)
                    ]
                    groups = []
                    for shard in sorted(
                        {item["shard_key"] for item in source_objects}
                    ):
                        shard_objects = [
                            item
                            for item in source_objects
                            if item["shard_key"] == shard
                        ]
                        paths = [
                            self._materialize_object(
                                item["storage"]["object_key"],
                                item["storage"]["content_hash"],
                            )
                            for item in shard_objects
                        ]
                        for table in self._month_tables(
                            paths,
                            year,
                            derive_resolution=resolution,
                        ):
                            timestamps = table.column("bar_start_at").to_pandas()
                            observed_start = pd.Timestamp(
                                timestamps.min()
                            ).isoformat()
                            observed_end = pd.Timestamp(
                                _table_period_end(table, contract)
                            ).isoformat()
                            source_ids = [
                                item["id"]
                                for item in shard_objects
                                if item["period_start"] < observed_end
                                and observed_start < item["period_end"]
                            ]
                            year_start, year_end = partition_bounds(
                                date(year, 1, 1),
                                "YEAR",
                            )
                            groups.append(
                                (
                                    "YEAR",
                                    year_start,
                                    year_end,
                                    shard,
                                    table,
                                    source_ids,
                                )
                            )
                    results.append(
                        self.publish_dataset(
                            contract,
                            year,
                            groups,
                            replace_periods=[
                                partition_bounds(date(year, 1, 1), "YEAR")
                            ],
                            dataset_source_manifest_id=native["id"],
                            relation_type="RESAMPLED_FROM",
                        )
                    )
        success = all(
            result.get("manifest", {}).get("status") == "AVAILABLE"
            for result in results
        )
        output_hash = hashlib.sha256(
            json.dumps(results, sort_keys=True, default=str).encode()
        ).hexdigest()
        self.catalog.finish_pipeline_run(
            run["id"],
            status="SUCCEEDED" if success else "FAILED",
            output_hash=output_hash,
            failure_code=None if success else "DERIVE_FAILED",
        )
        return {
            "status": "SUCCEEDED" if success else "FAILED",
            "pipeline_run_id": run["id"],
            "results": results,
        }

    def incremental(
        self,
        *,
        sessions: Iterable[date],
        price_types: Iterable[str],
        resolutions: Iterable[str] = ("30m", "1h", "4h", "1d"),
        symbols: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        sessions = tuple(sorted(set(sessions)))
        price_types = tuple(price_types)
        resolutions = tuple(resolutions)
        if not sessions:
            raise ValueError("incremental session이 비었습니다.")
        schedule = mcal.get_calendar(self.config.calendar).schedule(
            start_date=sessions[0],
            end_date=sessions[-1],
            tz="UTC",
        )
        valid_sessions = {
            pd.Timestamp(index).date(): (
                pd.Timestamp(row["market_open"]).to_pydatetime(),
                pd.Timestamp(row["market_close"]).to_pydatetime(),
            )
            for index, row in schedule.iterrows()
        }
        missing = set(sessions).difference(valid_sessions)
        if missing:
            raise ValueError(f"완료 거래 세션이 아닌 날짜: {sorted(missing)}")
        start = min(value[0] for value in valid_sessions.values()) - timedelta(hours=12)
        end = max(value[1] for value in valid_sessions.values()) + timedelta(hours=8)
        if self.config.dry_run:
            return self.plan(
                start=start,
                end=end,
                price_types=tuple(price_types),
                resolutions=resolutions,
                symbols=symbols,
            )
        adjustment_revision_backfill = None
        if "adjusted" in price_types:
            revision_detected = self._detect_adjusted_revision(
                before_session=sessions[0],
                symbols=symbols,
            )
            if revision_detected:
                adjusted_manifests = [
                    row
                    for row in self.catalog.records(
                        "market_data.dataset_manifests"
                    )
                    if row["feed_id"] == self.feed_ids[ADJUSTED_FEED]
                    and row["data_layer"] == "ADJUSTED"
                    and row["resolution"] == "30m"
                    and row["status"] == "AVAILABLE"
                ]
                retained_years = sorted(
                    {
                        int(str(row["period_start"])[:4])
                        for row in adjusted_manifests
                    }
                )
                if retained_years:
                    next_revision = (
                        max(
                            int(row["revision_number"])
                            for row in adjusted_manifests
                        )
                        + 1
                    )
                    correction_config = replace(
                        self.config,
                        revision=next_revision,
                        dry_run=False,
                    )
                    correction_engine = MarketPipelineEngine(
                        correction_config,
                        object_store=self.object_store,
                        catalog=self.catalog,
                        source=self.source,
                    )
                    correction_start = datetime(
                        retained_years[0],
                        1,
                        1,
                        tzinfo=ET,
                    ).astimezone(timezone.utc)
                    correction_end = datetime(
                        retained_years[-1] + 1,
                        1,
                        1,
                        tzinfo=ET,
                    ).astimezone(timezone.utc)
                    adjustment_revision_backfill = (
                        correction_engine.backfill(
                            start=correction_start,
                            end=correction_end,
                            price_types=("adjusted",),
                            resolutions=resolutions,
                            symbols=None,
                        )
                    )
                    if (
                        adjustment_revision_backfill.get("status")
                        != "SUCCEEDED"
                    ):
                        return {
                            "status": "FAILED",
                            "code": "ADJUSTMENT_REVISION_BACKFILL_FAILED",
                            "adjustment_revision_backfill": (
                                adjustment_revision_backfill
                            ),
                        }
        key = (
            f"{self.config.fingerprint}:incremental:"
            f"{sessions[0]}:{sessions[-1]}:{','.join(price_types)}:"
            f"{','.join(resolutions)}"
        )
        run = self.start_run("ALPACA_SIP_INCREMENTAL", key)
        if run.get("_reused"):
            return {
                "status": "SUCCEEDED",
                "pipeline_run_id": run["id"],
                "reused": True,
                "sessions": [value.isoformat() for value in sessions],
            }
        staging = self.collect_staging(
            run_code=run["id"],
            start=start,
            end=end,
            price_types=price_types,
            symbols=symbols,
            chunk_days=7,
            check_inactive=True,
        )
        results = []
        for price_type in price_types:
            native_layer = "RAW" if price_type == "raw" else "ADJUSTED"
            contracts = [
                DATASET_CONTRACTS[(price_type, native_layer, "30m")],
                *(
                    DATASET_CONTRACTS[(price_type, "DERIVED", resolution)]
                    for resolution in ("1h", "4h", "1d")
                    if resolution in resolutions
                ),
            ]
            for contract in contracts:
                for year in sorted({value.year for value in sessions}):
                    year_sessions = [
                        value for value in sessions if value.year == year
                    ]
                    native_latest = None
                    native_source_objects: list[dict[str, Any]] = []
                    if contract.data_layer == "DERIVED":
                        native_latest = self.catalog.latest_available_manifest(
                            feed_id=self.feed_ids[contract.feed_code],
                            data_layer=native_layer,
                            resolution="30m",
                            year=year,
                        )
                        if native_latest:
                            native_source_objects = (
                                self.catalog.objects_for_manifest(
                                    native_latest["id"]
                                )
                            )
                    base = (
                        self.config.staging_root
                        / run["id"]
                        / f"price_type={price_type}"
                        / f"year={year}"
                    )
                    groups = []
                    for session_date in year_sessions:
                        for shard_root in sorted(base.glob("shard=*")):
                            paths = sorted(shard_root.rglob("*.parquet"))
                            expression = (
                                ds.field("session_date_et")
                                == pa.scalar(session_date)
                            )
                            tables = list(
                                scan_tables(paths, filter_expression=expression)
                            )
                            if not tables:
                                continue
                            table = sort_bar_table(pa.concat_tables(tables))
                            if contract.data_layer == "DERIVED":
                                table = derive_regular_bars(
                                    table,
                                    contract.resolution,
                                    self.config.calendar,
                                )
                            if table.num_rows:
                                start_date, end_date = partition_bounds(
                                    session_date,
                                    "DAY",
                                )
                                shard_key = shard_root.name.split("=", 1)[1]
                                source_ids = [
                                    item["id"]
                                    for item in native_source_objects
                                    if item["shard_key"] == shard_key
                                    and _overlaps(
                                        item["partition_start"],
                                        item["partition_end"],
                                        start_date,
                                        end_date,
                                    )
                                ]
                                groups.append(
                                    (
                                        "DAY",
                                        start_date,
                                        end_date,
                                        shard_key,
                                        table,
                                        source_ids,
                                    )
                                )
                    source_manifest = (
                        native_latest["id"] if native_latest else None
                    )
                    results.append(
                        self.publish_dataset(
                            contract,
                            year,
                            groups,
                            replace_periods=[
                                partition_bounds(value, "DAY")
                                for value in year_sessions
                            ],
                            dataset_source_manifest_id=source_manifest,
                            relation_type=(
                                "REGULAR_SESSION_FILTERED_FROM"
                                if contract.data_layer == "DERIVED"
                                else "COLLECTED_FROM"
                            ),
                            additional_incidents=[
                                {
                                    "severity": "ERROR",
                                    "code": failure["code"],
                                    "message": (
                                        f"{failure['symbol']} "
                                        f"{failure['start']}~{failure['end']}"
                                    ),
                                    "partition_start": year_sessions[
                                        0
                                    ].isoformat(),
                                    "partition_end": (
                                        year_sessions[-1]
                                        + timedelta(days=1)
                                    ).isoformat(),
                                    "shard_key": None,
                                    "period_start": failure["start"],
                                    "period_end": failure["end"],
                                    "manifest_wide_reason": (
                                        _COLLECTION_FAILURE_REASON
                                    ),
                                }
                                for failure in staging["failures"]
                                if failure["price_type"] == price_type
                                and int(str(failure["start"])[:4]) <= year
                                <= int(str(failure["end"])[:4])
                            ],
                        )
                    )
        failed = bool(staging["failures"]) or any(
            result["manifest"]["status"] != "AVAILABLE" for result in results
        )
        digest = hashlib.sha256(
            json.dumps(
                [result["manifest"]["dataset_hash"] for result in results],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        self.catalog.finish_pipeline_run(
            run["id"],
            status="FAILED" if failed else "SUCCEEDED",
            output_hash=digest,
            failure_code="PARTIAL_INCREMENTAL_FAILURE" if failed else None,
        )
        summary = {
            "status": "FAILED" if failed else "SUCCEEDED",
            "pipeline_run_id": run["id"],
            "sessions": [value.isoformat() for value in sessions],
            "staging": staging,
            "adjustment_revision_detected": (
                adjustment_revision_backfill is not None
            ),
            "adjustment_revision_backfill": adjustment_revision_backfill,
            "results": results,
        }
        self.catalog.write_summary(summary)
        return summary

    def _detect_adjusted_revision(
        self,
        *,
        before_session: date,
        symbols: Iterable[str] | None,
        overlap_sessions: int = 10,
    ) -> bool:
        """Compare recent overlapping adjusted prices before daily publish."""
        if self.source is None or overlap_sessions <= 0:
            return False
        calendar = mcal.get_calendar(self.config.calendar)
        schedule = calendar.schedule(
            start_date=before_session - timedelta(days=60),
            end_date=before_session - timedelta(days=1),
            tz="UTC",
        )
        if schedule.empty:
            return False
        overlap = schedule.tail(overlap_sessions)
        fetch_start = (
            pd.Timestamp(overlap.iloc[0]["market_open"]).to_pydatetime()
            - timedelta(hours=12)
        )
        fetch_end = (
            pd.Timestamp(overlap.iloc[-1]["market_close"]).to_pydatetime()
            + timedelta(hours=8)
        )
        mappings = self._selected_mappings(symbols)
        manifests_by_year = {}
        for year in {
            pd.Timestamp(index).date().year for index in overlap.index
        }:
            manifest = self.catalog.latest_available_manifest(
                feed_id=self.feed_ids[ADJUSTED_FEED],
                data_layer="ADJUSTED",
                resolution="30m",
                year=year,
            )
            if manifest:
                manifests_by_year[year] = manifest
        if not manifests_by_year:
            return False
        compare_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
        ]
        for mapping in mappings:
            if hasattr(self.source, "should_skip_inactive"):
                last_bar = self._latest_published_bar(
                    mapping.instrument_id,
                    "adjusted",
                    before_session.year,
                )
                if last_bar is not None and self.source.should_skip_inactive(  # type: ignore[attr-defined]
                    mapping.provider_symbol,
                    last_bar,
                    fetch_end,
                ):
                    continue
            fetched = self.source.fetch(
                mapping.provider_symbol,
                fetch_start,
                fetch_end,
                "adjusted",
            )
            if fetched is None or fetched.empty:
                continue
            new_table = normalize_provider_frame(fetched, mapping)
            new_frame = new_table.to_pandas()
            old_tables = []
            for manifest in manifests_by_year.values():
                for item in self.catalog.objects_for_manifest(manifest["id"]):
                    path = self._materialize_object(
                        item["storage"]["object_key"],
                        item["storage"]["content_hash"],
                    )
                    table = ds.dataset(str(path), format="parquet").to_table(
                        filter=(
                            ds.field("instrument_id")
                            == mapping.instrument_id
                        )
                    )
                    if table.num_rows:
                        old_tables.append(table)
            if not old_tables:
                continue
            old_frame = pa.concat_tables(old_tables).to_pandas()
            merged = old_frame.merge(
                new_frame,
                on=["instrument_id", "bar_start_at"],
                suffixes=("_old", "_new"),
            )
            if merged.empty:
                continue
            for column in compare_columns:
                old_values = pd.to_numeric(
                    merged[f"{column}_old"],
                    errors="coerce",
                )
                new_values = pd.to_numeric(
                    merged[f"{column}_new"],
                    errors="coerce",
                )
                equal = (
                    old_values.eq(new_values)
                    | (old_values.isna() & new_values.isna())
                )
                if not bool(equal.all()):
                    return True
        return False

    def compact(
        self,
        contract: DatasetContract,
        *,
        granularity: Granularity,
        period: date,
    ) -> dict[str, Any]:
        if granularity == "DAY":
            raise ValueError("DAY는 수집 단위이며 Compaction 대상이 아닙니다.")
        start, end = partition_bounds(period, granularity)
        if granularity == "WEEK" and (
            start.month != (end - timedelta(days=1)).month
            or start.year != (end - timedelta(days=1)).year
        ):
            return {
                "status": "SKIPPED",
                "reason": "월 경계를 넘는 주는 DAY로 유지해 MONTH 경계를 보존합니다.",
                "partition_start": start.isoformat(),
                "partition_end": end.isoformat(),
            }
        latest = self.catalog.latest_available_manifest(
            feed_id=self.feed_ids[contract.feed_code],
            data_layer=contract.data_layer,
            resolution=contract.resolution,
            year=start.year,
        )
        if latest is None:
            raise ValueError("Compaction할 AVAILABLE Manifest가 없습니다.")
        members = [
            item
            for item in self.catalog.objects_for_manifest(latest["id"])
            if _overlaps(
                item["partition_start"],
                item["partition_end"],
                start,
                end,
            )
        ]
        existing_target = [
            item
            for item in members
            if item["partition_granularity"] == granularity
            and item["partition_start"] == start.isoformat()
            and item["partition_end"] == end.isoformat()
        ]
        if existing_target:
            return {
                "status": "SUCCEEDED",
                "reused": True,
                "manifest_id": latest["id"],
                "object_count": len(existing_target),
            }
        rank = {"DAY": 0, "WEEK": 1, "MONTH": 2, "YEAR": 3}
        inputs = [
            item
            for item in members
            if rank[item["partition_granularity"]] < rank[granularity]
        ]
        schedule = mcal.get_calendar(self.config.calendar).schedule(
            start_date=start,
            end_date=end - timedelta(days=1),
            tz="UTC",
        )
        expected_sessions = {pd.Timestamp(index).date() for index in schedule.index}
        covered = set()
        for item in inputs:
            item_start = date.fromisoformat(item["partition_start"])
            item_end = date.fromisoformat(item["partition_end"])
            covered.update(
                value
                for value in expected_sessions
                if item_start <= value < item_end
            )
        missing = expected_sessions.difference(covered)
        if missing:
            if not self.config.dry_run:
                window_start, window_end = partition_utc_bounds(start, granularity)
                missing_sessions = sorted(value.isoformat() for value in missing)
                # Manifest breadth with a written reason, not by omission: the gap is
                # every shard of the partition at once, so no single shard_key or
                # instrument_id honestly describes it.  The recorded *period* is still
                # the compacted partition, never the manifest's whole year, and the
                # missing sessions are named in the message.
                record_quality_incidents(
                    self.catalog,
                    [
                        QualityIncident(
                            incident_code="COMPACTION_INPUT_INCOMPLETE",
                            severity="ERROR",
                            scope=ImpactScope.manifest_wide(
                                period_start=window_start,
                                period_end=window_end,
                                reason=(
                                    "compaction 입력 누락은 파티션의 모든 shard에 걸쳐 "
                                    "판정되므로 단일 shard/instrument로 좁힐 수 없습니다."
                                ),
                            ),
                            detected_at=datetime.now(timezone.utc),
                            message=(
                                f"{granularity} {start.isoformat()}~{end.isoformat()} "
                                f"compaction 입력에 없는 세션: {', '.join(missing_sessions)}"
                            ),
                        )
                    ],
                    dataset_manifest_id=latest["id"],
                )
            incident = {
                "status": "QUARANTINED",
                "code": "COMPACTION_INPUT_INCOMPLETE",
                "missing_sessions": sorted(value.isoformat() for value in missing),
            }
            return incident
        if self.config.dry_run:
            return {
                "status": "DRY_RUN",
                "manifest_id": latest["id"],
                "input_object_count": len(inputs),
                "partition_start": start.isoformat(),
                "partition_end": end.isoformat(),
            }
        key = (
            f"{self.config.fingerprint}:compact:{contract.logical_code}:"
            f"{granularity}:{start}:{end}"
        )
        run = self.start_run("MARKET_DATA_COMPACTION", key)
        if run.get("_reused"):
            return {
                "status": "SUCCEEDED",
                "pipeline_run_id": run["id"],
                "reused": True,
            }
        groups = []
        for shard in sorted({item["shard_key"] for item in inputs}):
            shard_inputs = [item for item in inputs if item["shard_key"] == shard]
            paths = [
                self._materialize_object(
                    item["storage"]["object_key"],
                    item["storage"]["content_hash"],
                )
                for item in shard_inputs
            ]
            slices: list[tuple[date, date]] = []
            cursor = start
            while cursor < end:
                if granularity == "YEAR":
                    _, slice_end = partition_bounds(cursor, "MONTH")
                    slice_end = min(slice_end, end)
                else:
                    slice_end = end
                slices.append((cursor, slice_end))
                cursor = slice_end
            for slice_start, slice_end in slices:
                utc_start = datetime.combine(
                    slice_start,
                    datetime.min.time(),
                    ET,
                ).astimezone(timezone.utc)
                utc_end = datetime.combine(
                    slice_end,
                    datetime.min.time(),
                    ET,
                ).astimezone(timezone.utc)
                tables = [
                    filter_table_period(table, utc_start, utc_end)
                    for table in scan_tables(paths)
                ]
                tables = [table for table in tables if table.num_rows]
                if not tables:
                    continue
                combined = sort_bar_table(pa.concat_tables(tables))
                duplicate_keys = combined.select(
                    ["instrument_id", "bar_start_at"]
                ).to_pandas().duplicated()
                if duplicate_keys.any():
                    raise ValueError("Compaction 입력에 중복 봉이 있습니다.")
                source_ids = [
                    item["id"]
                    for item in shard_inputs
                    if _overlaps(
                        item["partition_start"],
                        item["partition_end"],
                        slice_start,
                        slice_end,
                    )
                ]
                groups.append(
                    (
                        granularity,
                        start,
                        end,
                        shard,
                        combined,
                        source_ids,
                    )
                )
        result = self.publish_dataset(
            contract,
            start.year,
            groups,
            replace_periods=[(start, end)],
            dataset_source_manifest_id=latest["id"],
            relation_type="COMPACTED_FROM",
        )
        success = result["manifest"]["status"] == "AVAILABLE"
        self.catalog.finish_pipeline_run(
            run["id"],
            status="SUCCEEDED" if success else "FAILED",
            output_hash=result["manifest"]["dataset_hash"],
            failure_code=None if success else "COMPACTION_FAILED",
        )
        return {
            "status": "SUCCEEDED" if success else "FAILED",
            "pipeline_run_id": run["id"],
            **result,
        }
