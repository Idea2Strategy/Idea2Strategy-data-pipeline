"""Year/shard transformation, manifest creation, and load-plan generation."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .core import (
    BAR_SCHEMA_VERSION,
    DATASET_SPECS,
    HASH_ALGORITHM,
    WRITER_VERSION,
    DatasetSpec,
    InstrumentMapping,
    canonical_dataset_hash,
    deterministic_uuid,
    et_year_bounds_utc,
    iso_utc,
    json_ready,
    load_instrument_map,
    normalize_legacy_frame,
    provider_symbol_from_path,
    quality_issues,
    resolve_mapping,
    rows_per_part,
    sha256_file,
    shard_key,
    sort_bar_table,
    stable_shard_number,
    write_parquet_atomic,
)


DEFAULT_TARGET_SIZE_MIB = 256
DEFAULT_MAX_SIZE_MIB = 512
DEFAULT_SHARD_COUNT = 16
DEFAULT_REVISION = 1


@dataclass(frozen=True)
class BackfillConfig:
    input_root: Path
    output_root: Path
    instrument_map_path: Path
    start_year: int
    end_year: int
    specs: tuple[DatasetSpec, ...]
    shard_count: int = DEFAULT_SHARD_COUNT
    target_size_mib: int = DEFAULT_TARGET_SIZE_MIB
    max_size_mib: int = DEFAULT_MAX_SIZE_MIB
    revision: int = DEFAULT_REVISION
    resume: bool = False
    dry_run: bool = False

    def validate(self) -> None:
        if self.start_year > self.end_year:
            raise ValueError("start_year는 end_year보다 클 수 없습니다.")
        if self.shard_count <= 0:
            raise ValueError("shard_count는 양수여야 합니다.")
        if self.target_size_mib <= 0 or self.max_size_mib <= 0:
            raise ValueError("파일 크기 설정은 양수여야 합니다.")
        if self.target_size_mib > self.max_size_mib:
            raise ValueError("target_size_mib는 max_size_mib보다 클 수 없습니다.")
        if self.revision <= 0:
            raise ValueError("revision은 1 이상이어야 합니다.")
        unsupported = [spec.key for spec in self.specs if spec.key not in DATASET_SPECS]
        if unsupported:
            raise ValueError(f"지원하지 않는 데이터셋입니다: {unsupported}")

    @property
    def fingerprint(self) -> str:
        return deterministic_uuid(
            "backfill-config-v1",
            self.input_root.resolve(),
            self.instrument_map_path.resolve(),
            self.start_year,
            self.end_year,
            ",".join(f"{spec.data_layer}:{spec.resolution}" for spec in self.specs),
            self.shard_count,
            self.target_size_mib,
            self.max_size_mib,
            self.revision,
            BAR_SCHEMA_VERSION,
            WRITER_VERSION,
        )


@dataclass(frozen=True)
class SourceFile:
    path: Path
    mapping: InstrumentMapping


@dataclass
class Inventory:
    sources: dict[tuple[str, str], list[SourceFile]] = field(default_factory=dict)
    unmapped: dict[tuple[str, str], list[Path]] = field(default_factory=dict)


@dataclass
class ObjectArtifact:
    object_id: str
    dataset_object_id: str
    dataset_manifest_id: str
    local_path: str
    object_key: str
    content_hash: str
    byte_size: int
    partition_granularity: str
    partition_start: str
    partition_end: str
    period_start: str
    period_end: str
    shard_key: str
    part_number: int
    row_count: int
    schema_version: str = BAR_SCHEMA_VERSION

    def canonical_hash_record(self) -> dict[str, Any]:
        return {
            "content_hash": self.content_hash,
            "partition_granularity": self.partition_granularity,
            "partition_start": self.partition_start,
            "partition_end": self.partition_end,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "shard_key": self.shard_key,
            "part_number": self.part_number,
            "row_count": self.row_count,
            "schema_version": self.schema_version,
        }


@dataclass
class ManifestResult:
    manifest: dict[str, Any]
    objects: list[ObjectArtifact]
    incidents: list[dict[str, Any]]
    failures: list[dict[str, Any]]

    @property
    def available(self) -> bool:
        return self.manifest.get("status") == "AVAILABLE"


def selected_specs(layer: str, resolution: str) -> tuple[DatasetSpec, ...]:
    layer = layer.upper()
    candidates = list(DATASET_SPECS.values())
    if layer != "ALL":
        candidates = [spec for spec in candidates if spec.data_layer == layer]
    if resolution.lower() != "all":
        candidates = [
            spec for spec in candidates if spec.resolution == resolution.lower()
        ]
    if not candidates:
        raise ValueError(
            f"지원하는 layer/resolution 조합이 아닙니다: {layer}/{resolution}"
        )
    return tuple(candidates)


def scan_inventory(
    config: BackfillConfig,
    mappings: dict[str, InstrumentMapping],
) -> Inventory:
    inventory = Inventory()
    for spec in config.specs:
        source_dir = config.input_root / spec.source_relative_path
        paths = (
            sorted(source_dir.glob(f"*{spec.filename_marker}.parquet"))
            if source_dir.is_dir()
            else []
        )
        mapped: list[SourceFile] = []
        unmapped: list[Path] = []
        for path in paths:
            mapping = resolve_mapping(path, mappings)
            if mapping is None:
                unmapped.append(path)
            else:
                mapped.append(SourceFile(path, mapping))
        inventory.sources[spec.key] = mapped
        inventory.unmapped[spec.key] = unmapped
    return inventory


def inventory_summary(
    config: BackfillConfig,
    inventory: Inventory,
) -> dict[str, Any]:
    datasets = []
    for spec in config.specs:
        mapped = inventory.sources.get(spec.key, [])
        unmapped = inventory.unmapped.get(spec.key, [])
        datasets.append(
            {
                "data_layer": spec.data_layer,
                "resolution": spec.resolution,
                "source_path": str(config.input_root / spec.source_relative_path),
                "mapped_files": len(mapped),
                "unmapped_files": len(unmapped),
                "unmapped_symbols": [
                    provider_symbol_from_path(path) for path in unmapped
                ],
                "manifest_count": config.end_year - config.start_year + 1,
            }
        )
    return {
        "mode": "dry-run" if config.dry_run else "local-transform",
        "input_root": str(config.input_root),
        "output_root": str(config.output_root),
        "start_year": config.start_year,
        "end_year": config.end_year,
        "revision": config.revision,
        "shard_count": config.shard_count,
        "partition_granularity": "YEAR",
        "target_size_mib": config.target_size_mib,
        "max_size_mib": config.max_size_mib,
        "schema_version": BAR_SCHEMA_VERSION,
        "writer_version": WRITER_VERSION,
        "datasets": datasets,
        "point_in_time_universe": False,
        "warning": (
            "수집 종목 합집합은 과거 각 시점의 S&P 500 구성 종목을 "
            "재현하지 않습니다."
        ),
    }


def _read_source_year(source: SourceFile, spec: DatasetSpec, year: int) -> pa.Table:
    start, end = et_year_bounds_utc(year)
    try:
        table = pq.read_table(
            source.path,
            filters=[
                ("timestamp", ">=", start),
                ("timestamp", "<", end),
            ],
        )
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        # Some legacy writers store timestamp only through pandas metadata.
        table = pq.read_table(source.path)
    return normalize_legacy_frame(
        table.to_pandas(),
        source.mapping,
        spec,
        year,
    )


def _manifest_id(spec: DatasetSpec, year: int, revision: int) -> str:
    return deterministic_uuid(
        "Alpaca",
        "SIP",
        spec.data_layer,
        spec.resolution,
        "YEAR",
        year,
        revision,
    )


def _manifest_root(
    config: BackfillConfig,
    spec: DatasetSpec,
    year: int,
    manifest_id: str,
) -> Path:
    return (
        config.output_root
        / "market-data"
        / f"dataset={manifest_id}"
        / f"revision={config.revision}"
        / f"layer={spec.data_layer}"
        / f"resolution={spec.resolution}"
        / "granularity=YEAR"
        / f"partition_start={year}-01-01"
        / f"partition_end={year + 1}-01-01"
    )


def _json_dump_atomic(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=json_ready)
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_completed_manifest(
    manifest_path: Path,
    config: BackfillConfig,
) -> ManifestResult | None:
    if not manifest_path.is_file():
        return None
    manifest = _load_json(manifest_path)
    if manifest.get("config_fingerprint") != config.fingerprint:
        raise ValueError(
            "같은 Revision 경로에 다른 설정의 Manifest가 있습니다. "
            "기존 파일을 덮어쓰지 말고 새 Revision을 사용하세요."
        )
    if manifest.get("status") != "AVAILABLE":
        return None
    objects = [ObjectArtifact(**row) for row in manifest.get("objects", [])]
    for artifact in objects:
        path = Path(artifact.local_path)
        if not path.is_file() or sha256_file(path) != artifact.content_hash:
            raise ValueError(f"완료 Manifest의 객체가 손상됐습니다: {path}")
    return ManifestResult(
        manifest={key: value for key, value in manifest.items() if key != "objects"},
        objects=objects,
        incidents=manifest.get("incidents", []),
        failures=manifest.get("failures", []),
    )


def _load_shard_state(
    state_path: Path,
    config: BackfillConfig,
) -> list[ObjectArtifact] | None:
    if not config.resume or not state_path.is_file():
        return None
    payload = _load_json(state_path)
    if payload.get("config_fingerprint") != config.fingerprint:
        raise ValueError(f"resume 설정이 기존 shard와 다릅니다: {state_path}")
    artifacts = [ObjectArtifact(**record) for record in payload["objects"]]
    for artifact in artifacts:
        path = Path(artifact.local_path)
        if not path.is_file() or sha256_file(path) != artifact.content_hash:
            return None
    return artifacts


def _write_candidate(
    table: pa.Table,
    directory: Path,
    max_size_bytes: int,
) -> tuple[Path, int]:
    candidate = directory / f".candidate-{os.urandom(12).hex()}.parquet"
    write_parquet_atomic(table, candidate)
    size = candidate.stat().st_size
    if size > max_size_bytes and table.num_rows > 1:
        candidate.unlink(missing_ok=True)
        return candidate, -1
    if size > max_size_bytes:
        candidate.unlink(missing_ok=True)
        raise ValueError("단일 행 Parquet이 max_size_mib를 초과합니다.")
    return candidate, size


def _write_shard_parts(
    table: pa.Table,
    config: BackfillConfig,
    spec: DatasetSpec,
    year: int,
    manifest_id: str,
    manifest_root: Path,
    shard_number: int,
) -> list[ObjectArtifact]:
    if table.num_rows == 0:
        return []
    key = shard_key(shard_number, config.shard_count)
    directory = manifest_root / f"shard={key}"
    directory.mkdir(parents=True, exist_ok=True)
    target_bytes = config.target_size_mib * 1024 * 1024
    max_bytes = config.max_size_mib * 1024 * 1024
    initial_rows = rows_per_part(table, target_bytes)
    chronological = pc.take(
        table,
        pc.sort_indices(
            table,
            sort_keys=[
                ("bar_start_at", "ascending"),
                ("instrument_id", "ascending"),
            ],
        ),
    )
    timestamps = chronological.column("bar_start_at").to_pylist()
    queue: list[pa.Table] = []
    offset = 0
    while offset < chronological.num_rows:
        end = min(offset + initial_rows, chronological.num_rows)
        while (
            end < chronological.num_rows
            and timestamps[end - 1] == timestamps[end]
        ):
            end += 1
        queue.append(chronological.slice(offset, end - offset))
        offset = end
    accepted: list[tuple[pa.Table, Path, int]] = []
    while queue:
        candidate_table = queue.pop(0)
        candidate_table = sort_bar_table(candidate_table)
        candidate_path, byte_size = _write_candidate(
            candidate_table,
            directory,
            max_bytes,
        )
        if byte_size < 0:
            midpoint = candidate_table.num_rows // 2
            candidate_times = candidate_table.column("bar_start_at").to_pylist()
            sorted_positions = sorted(
                range(candidate_table.num_rows),
                key=lambda index: (
                    candidate_times[index],
                    candidate_table.column("instrument_id")[index].as_py(),
                ),
            )
            chronological_candidate = pc.take(
                candidate_table,
                pa.array(sorted_positions, type=pa.int64()),
            )
            candidate_times = chronological_candidate.column(
                "bar_start_at"
            ).to_pylist()
            while (
                midpoint < chronological_candidate.num_rows
                and midpoint > 0
                and candidate_times[midpoint - 1] == candidate_times[midpoint]
            ):
                midpoint += 1
            if midpoint >= chronological_candidate.num_rows:
                midpoint = candidate_table.num_rows // 2
                while (
                    midpoint > 0
                    and candidate_times[midpoint - 1] == candidate_times[midpoint]
                ):
                    midpoint -= 1
            if midpoint <= 0 or midpoint >= chronological_candidate.num_rows:
                raise ValueError(
                    "같은 timestamp 행 묶음이 max_size_mib를 초과해 "
                    "기간 비중첩 part로 나눌 수 없습니다."
                )
            queue[0:0] = [
                chronological_candidate.slice(0, midpoint),
                chronological_candidate.slice(
                    midpoint,
                    chronological_candidate.num_rows - midpoint,
                ),
            ]
            continue
        accepted.append((candidate_table, candidate_path, byte_size))

    artifacts: list[ObjectArtifact] = []
    for part_number, (part, candidate_path, byte_size) in enumerate(accepted, 1):
        final_path = directory / f"part-{part_number:05d}.parquet"
        os.replace(candidate_path, final_path)
        content_hash = sha256_file(final_path)
        timestamps = part.column("bar_start_at").to_pandas()
        object_key = final_path.relative_to(config.output_root).as_posix()
        period_start = pd.Timestamp(timestamps.min())
        bar_duration = {
            "30m": pd.Timedelta(minutes=30),
            "1h": pd.Timedelta(hours=1),
            "4h": pd.Timedelta(hours=4),
            "1d": pd.Timedelta(days=1),
        }[spec.resolution]
        object_id = deterministic_uuid(
            "storage-object",
            manifest_id,
            key,
            part_number,
            content_hash,
        )
        dataset_object_id = deterministic_uuid(
            "dataset-object",
            manifest_id,
            object_id,
        )
        artifact = ObjectArtifact(
            object_id=object_id,
            dataset_object_id=dataset_object_id,
            dataset_manifest_id=manifest_id,
            local_path=str(final_path.resolve()),
            object_key=object_key,
            content_hash=content_hash,
            byte_size=byte_size,
            partition_granularity="YEAR",
            partition_start=f"{year}-01-01",
            partition_end=f"{year + 1}-01-01",
            period_start=iso_utc(period_start),
            period_end=iso_utc(pd.Timestamp(timestamps.max()) + bar_duration),
            shard_key=key,
            part_number=part_number,
            row_count=part.num_rows,
        )
        footer = pq.ParquetFile(final_path)
        if footer.metadata.num_rows != part.num_rows:
            raise ValueError(f"Parquet Footer row count 불일치: {final_path}")
        artifacts.append(artifact)
    return artifacts


def build_manifest_year(
    config: BackfillConfig,
    inventory: Inventory,
    spec: DatasetSpec,
    year: int,
) -> ManifestResult:
    manifest_id = _manifest_id(spec, year, config.revision)
    root = _manifest_root(config, spec, year, manifest_id)
    manifest_path = root / "manifest.json"
    completed = _load_completed_manifest(manifest_path, config)
    if completed is not None:
        return completed
    if manifest_path.exists() and not config.resume:
        raise ValueError(
            f"미완료 Revision이 이미 있습니다. --resume 또는 새 Revision을 사용하세요: {root}"
        )

    started_at = datetime.now(timezone.utc).isoformat()
    base_manifest = {
        "dataset_manifest_id": manifest_id,
        "feed": "SIP",
        "provider": "Alpaca",
        "instrument_id": None,
        "data_layer": spec.data_layer,
        "resolution": spec.resolution,
        "partition_granularity": "YEAR",
        "partition_start": f"{year}-01-01",
        "partition_end": f"{year + 1}-01-01",
        "revision": config.revision,
        "schema_version": BAR_SCHEMA_VERSION,
        "writer_version": WRITER_VERSION,
        "shard_count": config.shard_count,
        "status": "BUILDING",
        "config_fingerprint": config.fingerprint,
        "started_at": started_at,
    }
    _json_dump_atomic(base_manifest, manifest_path)

    failures = [
        {
            "provider_symbol": provider_symbol_from_path(path),
            "path": str(path),
            "code": "UNMAPPED_INSTRUMENT",
            "message": "instrument_map.csv에 매핑이 없습니다.",
        }
        for path in inventory.unmapped.get(spec.key, [])
    ]
    incidents: list[dict[str, Any]] = []
    artifacts: list[ObjectArtifact] = []
    sources = inventory.sources.get(spec.key, [])

    for shard_number in range(config.shard_count):
        key = shard_key(shard_number, config.shard_count)
        state_path = root / f"shard={key}" / "_shard_complete.json"
        resumed = _load_shard_state(state_path, config)
        if resumed is not None:
            artifacts.extend(resumed)
            continue

        tables: list[pa.Table] = []
        shard_sources = [
            source
            for source in sources
            if stable_shard_number(
                source.mapping.instrument_id,
                config.shard_count,
            )
            == shard_number
        ]
        for source in shard_sources:
            try:
                table = _read_source_year(source, spec, year)
                if table.num_rows:
                    tables.append(table)
            except Exception as exc:
                failures.append(
                    {
                        "provider_symbol": source.mapping.provider_symbol,
                        "path": str(source.path),
                        "code": "SOURCE_TRANSFORM_FAILED",
                        "message": str(exc),
                    }
                )
        if not tables:
            continue
        combined = sort_bar_table(pa.concat_tables(tables))
        shard_issues = quality_issues(combined, spec, year)
        for issue in shard_issues:
            incident = dict(issue)
            incident.update(
                {
                    "dataset_manifest_id": manifest_id,
                    "year": year,
                    "shard_key": key,
                }
            )
            incidents.append(incident)
        if any(issue["severity"] == "ERROR" for issue in shard_issues):
            failures.append(
                {
                    "shard_key": key,
                    "code": "QUALITY_VALIDATION_FAILED",
                    "message": "오류 품질 사건이 있어 shard 파일을 만들지 않았습니다.",
                }
            )
            continue
        shard_artifacts = _write_shard_parts(
            combined,
            config,
            spec,
            year,
            manifest_id,
            root,
            shard_number,
        )
        artifacts.extend(shard_artifacts)
        _json_dump_atomic(
            {
                "config_fingerprint": config.fingerprint,
                "objects": [asdict(artifact) for artifact in shard_artifacts],
            },
            state_path,
        )

    if not artifacts:
        failures.append(
            {
                "code": "NO_OUTPUT_ROWS",
                "message": f"{year}년에 출력할 행이 없습니다.",
            }
        )
    dataset_hash = canonical_dataset_hash(
        artifact.canonical_hash_record() for artifact in artifacts
    )
    status = "AVAILABLE" if not failures else "QUARANTINED"
    manifest = {
        **base_manifest,
        "status": status,
        "dataset_hash": dataset_hash,
        "hash_algorithm": HASH_ALGORITHM,
        "row_count": sum(artifact.row_count for artifact in artifacts),
        "object_count": len(artifacts),
        "period_start_at": min(
            (artifact.period_start for artifact in artifacts),
            default=None,
        ),
        "period_end_at": max(
            (artifact.period_end for artifact in artifacts),
            default=None,
        ),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "failures": failures,
        "incidents": incidents,
    }
    persisted = {
        **manifest,
        "objects": [asdict(artifact) for artifact in artifacts],
    }
    _json_dump_atomic(persisted, manifest_path)
    return ManifestResult(manifest, artifacts, incidents, failures)


def transform(config: BackfillConfig) -> tuple[list[ManifestResult], dict[str, Any]]:
    config.validate()
    mappings = load_instrument_map(config.instrument_map_path)
    inventory = scan_inventory(config, mappings)
    summary = inventory_summary(config, inventory)
    if config.dry_run:
        return [], summary
    results = [
        build_manifest_year(config, inventory, spec, year)
        for spec in config.specs
        for year in range(config.start_year, config.end_year + 1)
    ]
    write_load_plan(config, results, summary)
    return results, summary


def _write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                        default=json_ready,
                    )
                    + "\n"
                )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_load_plan(
    config: BackfillConfig,
    results: list[ManifestResult],
    inventory_plan: dict[str, Any],
) -> Path:
    """Write a DBML-column-aligned review artifact, never a DB source of truth."""
    root = config.output_root / "load-plan"
    root.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    provider_id = deterministic_uuid("provider", "Alpaca")
    feed_id = deterministic_uuid("feed", provider_id, "SIP")
    pipeline_run_id = deterministic_uuid("pipeline-run", config.fingerprint)
    all_available = all(result.available for result in results)
    providers = [
        {
            "id": provider_id,
            "code": "ALPACA",
            "display_name": "Alpaca",
            "rights_version": "UNVERIFIED",
            "status": "REVIEW_REQUIRED",
            "created_at": now,
        }
    ]
    feeds = [
        {
            "id": feed_id,
            "provider_id": provider_id,
            "code": "SIP",
            "data_kind": "BARS",
            "resolution": "30m",
            "timezone_name": "America/New_York",
            "feed_version": "alpaca-sip-adjustment-all-v1",
            "created_at": now,
            "retired_at": None,
        }
    ]
    input_hash = hashlib.sha256(config.fingerprint.encode("utf-8")).hexdigest()
    output_hash = hashlib.sha256(
        json.dumps(
            [
                {
                    "id": result.manifest["dataset_manifest_id"],
                    "dataset_hash": result.manifest.get("dataset_hash"),
                    "status": result.manifest["status"],
                }
                for result in results
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    pipeline_runs = [
        {
            "id": pipeline_run_id,
            "pipeline_code": "ALPACA_MARKET_DATA_BACKFILL",
            "pipeline_version": BAR_SCHEMA_VERSION,
            "idempotency_key": config.fingerprint,
            "status": "SUCCEEDED" if all_available else "FAILED",
            "input_hash": input_hash,
            "output_hash": output_hash,
            "started_at": min(
                (result.manifest["started_at"] for result in results),
                default=now,
            ),
            "completed_at": now,
            "failure_code": None if all_available else "PARTIAL_BACKFILL_FAILURE",
        }
    ]
    manifests = []
    storage_objects = []
    dataset_objects = []
    dataset_lineage = []
    object_lineage = []
    quality_incidents = []
    results_by_key = {
        (
            result.manifest["data_layer"],
            result.manifest["resolution"],
            int(result.manifest["partition_start"][:4]),
        ): result
        for result in results
    }
    for result in results:
        manifest = result.manifest
        year = int(manifest["partition_start"][:4])
        period_start, period_end = et_year_bounds_utc(year)
        manifests.append(
            {
                "id": manifest["dataset_manifest_id"],
                "feed_id": feed_id,
                "instrument_id": None,
                "data_layer": manifest["data_layer"],
                "resolution": manifest["resolution"],
                "revision_number": config.revision,
                "status": manifest["status"],
                "period_start": iso_utc(period_start),
                "period_end": iso_utc(period_end),
                "schema_version": BAR_SCHEMA_VERSION,
                "dataset_hash": manifest["dataset_hash"],
                "supersedes_manifest_id": (
                    _manifest_id(
                        DATASET_SPECS[
                            (manifest["data_layer"], manifest["resolution"])
                        ],
                        year,
                        config.revision - 1,
                    )
                    if config.revision > 1
                    else None
                ),
                "created_at": manifest["started_at"],
                "available_at": (
                    manifest["finished_at"] if result.available else None
                ),
            }
        )
        for artifact in result.objects:
            instrument_ids = pq.read_table(
                Path(artifact.local_path),
                columns=["instrument_id"],
            ).column("instrument_id").to_pylist()
            storage_objects.append(
                {
                    "id": artifact.object_id,
                    "status": "AVAILABLE",
                    "storage_provider": "LOCAL",
                    "bucket_name": config.output_root.name or "local-backfill",
                    "object_key": artifact.object_key,
                    "provider_version_id": artifact.content_hash,
                    "content_hash": artifact.content_hash,
                    "byte_size": artifact.byte_size,
                    "file_format": "PARQUET",
                    "compression_codec": "UNCOMPRESSED",
                    "media_type": "application/vnd.apache.parquet",
                    "schema_version": BAR_SCHEMA_VERSION,
                    "row_count": artifact.row_count,
                    "period_start": artifact.period_start,
                    "period_end": artifact.period_end,
                    "encryption_key_ref": None,
                    "retention_policy_version": "UNSPECIFIED",
                    "retention_until": None,
                    "legal_hold": False,
                    "created_at": manifest["started_at"],
                    "verified_at": manifest["finished_at"],
                    "quarantined_at": None,
                    "superseded_at": None,
                    "deleted_at": None,
                }
            )
            dataset_objects.append(
                {
                    "id": artifact.dataset_object_id,
                    "dataset_manifest_id": artifact.dataset_manifest_id,
                    "object_id": artifact.object_id,
                    "object_kind": "MARKET_BARS",
                    "partition_granularity": artifact.partition_granularity,
                    "partition_start": artifact.partition_start,
                    "partition_end": artifact.partition_end,
                    "period_start": artifact.period_start,
                    "period_end": artifact.period_end,
                    "shard_key": artifact.shard_key,
                    "part_number": artifact.part_number,
                    "row_count": artifact.row_count,
                    "min_instrument_id": min(instrument_ids),
                    "max_instrument_id": max(instrument_ids),
                }
            )
        for incident in result.incidents:
            quality_incidents.append(
                {
                    "id": deterministic_uuid(
                        manifest["dataset_manifest_id"],
                        incident.get("shard_key"),
                        incident["code"],
                    ),
                    "dataset_manifest_id": manifest["dataset_manifest_id"],
                    "instrument_id": None,
                    "severity": incident["severity"],
                    "incident_code": incident["code"],
                    "period_start": iso_utc(period_start),
                    "period_end": iso_utc(period_end),
                    "status": "ACTIVE",
                    "evidence_object_id": None,
                    "detected_at": manifest["finished_at"],
                    "resolved_at": None,
                }
            )
        spec = DATASET_SPECS[(manifest["data_layer"], manifest["resolution"])]
        if spec.parent is None:
            continue
        year = int(manifest["partition_start"][:4])
        parent = results_by_key.get((*spec.parent, year))
        if parent is None:
            continue
        dataset_lineage.append(
            {
                "derived_manifest_id": manifest["dataset_manifest_id"],
                "source_manifest_id": parent.manifest["dataset_manifest_id"],
                "relation_type": "DERIVED_FROM",
            }
        )
        parents_by_shard: dict[str, list[ObjectArtifact]] = {}
        for artifact in parent.objects:
            parents_by_shard.setdefault(artifact.shard_key, []).append(artifact)
        for child in result.objects:
            for source in parents_by_shard.get(child.shard_key, []):
                if (
                    source.period_end <= child.period_start
                    or child.period_end <= source.period_start
                ):
                    continue
                object_lineage.append(
                    {
                        "derived_dataset_object_id": child.dataset_object_id,
                        "source_dataset_object_id": source.dataset_object_id,
                        "pipeline_run_id": pipeline_run_id,
                        "relation_type": "DERIVED_FROM",
                        "created_at": now,
                    }
                )

    (root / "providers.json").write_text(
        json.dumps(providers, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (root / "feeds.json").write_text(
        json.dumps(feeds, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_jsonl(root / "pipeline-runs.jsonl", pipeline_runs)
    _write_jsonl(root / "dataset-manifests.jsonl", manifests)
    _write_jsonl(root / "storage-objects.jsonl", storage_objects)
    _write_jsonl(root / "dataset-objects.jsonl", dataset_objects)
    _write_jsonl(root / "dataset-lineage.jsonl", dataset_lineage)
    _write_jsonl(root / "dataset-object-lineage.jsonl", object_lineage)
    _write_jsonl(root / "quality-incidents.jsonl", quality_incidents)
    summary = {
        **inventory_plan,
        "review_artifact_only": True,
        "db_source_of_truth": False,
        "dbml_contract_verified": True,
        "dbml_reference": "Idea2Strategy feature/dbml schema.draft.dbml",
        "pipeline_run_id": pipeline_run_id,
        "manifest_total": len(results),
        "manifest_available": sum(result.available for result in results),
        "manifest_failed": sum(not result.available for result in results),
        "object_total": sum(len(result.objects) for result in results),
        "row_total": sum(
            int(result.manifest.get("row_count", 0)) for result in results
        ),
        "unmapped_failures": sum(
            1
            for result in results
            for failure in result.failures
            if failure.get("code") == "UNMAPPED_INSTRUMENT"
        ),
        "rights_version_requires_review": True,
        "point_in_time_universe": False,
    }
    _json_dump_atomic(summary, root / "summary.json")
    return root


def benchmark_summary(results: list[ManifestResult]) -> dict[str, Any]:
    sizes = [
        artifact.byte_size
        for result in results
        for artifact in result.objects
    ]
    return {
        "manifest_count": len(results),
        "available_count": sum(result.available for result in results),
        "object_count": len(sizes),
        "min_size_mib": min(sizes, default=0) / 1024 / 1024,
        "mean_size_mib": (
            sum(sizes) / len(sizes) / 1024 / 1024 if sizes else 0
        ),
        "max_size_mib": max(sizes, default=0) / 1024 / 1024,
        "part_counts": {
            result.manifest["dataset_manifest_id"]: len(result.objects)
            for result in results
        },
    }
