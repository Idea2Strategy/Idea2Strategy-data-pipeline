"""Catalog validation, export, upload, and benchmark operations."""

from __future__ import annotations

import json
import os
import re
import shutil
import time
import tracemalloc
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .catalog import TABLE_FILES, LocalCatalog
from .contracts import DATASET_CONTRACTS, canonical_dataset_hash
from .processing import quality_issues
from .storage import LocalObjectStore, S3ObjectStore


def _contract_for_manifest(
    manifest: dict[str, Any],
    feeds: dict[str, dict[str, Any]],
):
    feed_code = feeds[manifest["feed_id"]]["code"]
    price_type = "raw" if "RAW" in feed_code else "adjusted"
    key = (price_type, manifest["data_layer"], manifest["resolution"])
    return DATASET_CONTRACTS.get(key)


def validate_catalog(
    catalog: LocalCatalog,
    object_store: LocalObjectStore,
    *,
    write_report: bool = True,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    storage = {
        row["id"]: row for row in catalog.records("storage.objects")
    }
    feeds = {
        row["id"]: row for row in catalog.records("market_data.feeds")
    }
    relations = catalog.records("market_data.dataset_objects")
    by_manifest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for relation in relations:
        by_manifest[relation["dataset_manifest_id"]].append(relation)
    manifests = catalog.records("market_data.dataset_manifests")
    for manifest in manifests:
        contract = _contract_for_manifest(manifest, feeds)
        if contract is None:
            errors.append(
                {
                    "code": "UNKNOWN_DATASET_CONTRACT",
                    "manifest_id": manifest["id"],
                }
            )
            continue
        canonical_objects = []
        periods: dict[str, list[tuple[str, str]]] = defaultdict(list)
        manifest_errors_before = len(errors)
        for relation in by_manifest.get(manifest["id"], []):
            record = storage.get(relation["object_id"])
            if record is None:
                errors.append(
                    {
                        "code": "MISSING_STORAGE_OBJECT",
                        "dataset_object_id": relation["id"],
                    }
                )
                continue
            verification = object_store.verify(
                record["object_key"],
                record["content_hash"],
            )
            if not verification.ok:
                errors.append(
                    {
                        "code": "CONTENT_HASH_MISMATCH",
                        "object_id": record["id"],
                        "message": verification.message,
                    }
                )
                continue
            path = object_store.path_for(record["object_key"])
            try:
                parquet = pq.ParquetFile(path)
                table = parquet.read()
            except Exception as exc:
                errors.append(
                    {
                        "code": "PARQUET_FOOTER_INVALID",
                        "object_id": record["id"],
                        "message": str(exc),
                    }
                )
                continue
            if parquet.metadata.num_rows != relation["row_count"]:
                errors.append(
                    {
                        "code": "ROW_COUNT_MISMATCH",
                        "object_id": record["id"],
                    }
                )
            for issue in quality_issues(
                table,
                contract,
                partition_start=date.fromisoformat(relation["partition_start"]),
                partition_end=date.fromisoformat(relation["partition_end"]),
            ):
                target = errors if issue["severity"] == "ERROR" else warnings
                target.append(
                    {
                        **issue,
                        "object_id": record["id"],
                        "manifest_id": manifest["id"],
                    }
                )
            canonical_objects.append(
                {
                    "content_hash": record["content_hash"],
                    "object_kind": relation["object_kind"],
                    "partition_granularity": relation["partition_granularity"],
                    "partition_start": relation["partition_start"],
                    "partition_end": relation["partition_end"],
                    "period_start": relation["period_start"],
                    "period_end": relation["period_end"],
                    "shard_key": relation["shard_key"],
                    "part_number": relation["part_number"],
                    "row_count": relation["row_count"],
                    "schema_version": record["schema_version"],
                }
            )
            periods[relation["shard_key"]].append(
                (relation["period_start"], relation["period_end"])
            )
        for shard, values in periods.items():
            ordered = sorted(values)
            for previous, current in zip(ordered, ordered[1:]):
                if current[0] < previous[1]:
                    errors.append(
                        {
                            "code": "OVERLAPPING_ACTIVE_OBJECTS",
                            "manifest_id": manifest["id"],
                            "shard_key": shard,
                            "left": previous,
                            "right": current,
                        }
                    )
        digest = canonical_dataset_hash(canonical_objects)
        if digest != manifest["dataset_hash"]:
            errors.append(
                {
                    "code": "DATASET_HASH_MISMATCH",
                    "manifest_id": manifest["id"],
                    "expected": manifest["dataset_hash"],
                    "actual": digest,
                }
            )
        manifest_has_errors = len(errors) > manifest_errors_before
        if manifest["status"] == "AVAILABLE" and manifest_has_errors:
            errors.append(
                {
                    "code": "AVAILABLE_MANIFEST_HAS_ERRORS",
                    "manifest_id": manifest["id"],
                }
            )
    report = {
        "status": "PASSED" if not errors else "FAILED",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "manifest_count": len(manifests),
        "object_count": len(storage),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }
    if write_report:
        path = catalog.root / "validation-report.json"
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return report


def parse_dbml_columns(path: Path) -> dict[str, set[str]]:
    if not path.is_file():
        raise FileNotFoundError(f"DBML 파일이 없습니다: {path}")
    text = path.read_text(encoding="utf-8")
    tables: dict[str, set[str]] = {}
    pattern = re.compile(
        r"Table\s+([A-Za-z0-9_.]+)\s*\{(.*?)^\}",
        re.MULTILINE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        table, body = match.groups()
        columns = set()
        for raw in body.splitlines():
            line = raw.strip()
            if not line or line.startswith(("//", "Note:", "Indexes", "}")):
                continue
            name = line.split()[0].strip('"')
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                columns.add(name)
        tables[table] = columns
    return tables


def export_db_plan(
    catalog: LocalCatalog,
    destination: Path,
    *,
    dbml_path: Path | None = None,
) -> dict[str, Any]:
    destination = destination.expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    dbml = parse_dbml_columns(dbml_path) if dbml_path else {}
    errors = []
    counts = {}
    for table, filename in TABLE_FILES.items():
        rows = catalog.records(table)
        counts[table] = len(rows)
        if dbml:
            allowed = dbml.get(table)
            if allowed is None:
                errors.append(f"DBML에 테이블이 없습니다: {table}")
            else:
                for index, row in enumerate(rows, 1):
                    extras = set(row).difference(allowed)
                    if extras:
                        errors.append(
                            f"{filename} {index}행 비-DBML 열: {sorted(extras)}"
                        )
        path = destination / filename
        path.write_text(
            "".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                for row in rows
            ),
            encoding="utf-8",
        )
    summary = {
        "status": "PASSED" if not errors else "FAILED",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "table_counts": counts,
        "dbml_contract_errors": errors,
        "rights_version_requires_review": True,
        "pipeline_run_outputs_gap": True,
    }
    (destination / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def apply_catalog_to_postgres(
    catalog: LocalCatalog,
    object_store: LocalObjectStore,
    *,
    dbml_path: Path,
    execute: bool = False,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Validate the DBML contract and optionally apply it in one transaction."""
    tables = parse_dbml_columns(dbml_path)
    errors = []
    operations: list[tuple[str, dict[str, Any]]] = []
    ordered_tables = (
        "market_data.providers",
        "market_data.feeds",
        "market_data.pipeline_runs",
        "market_data.dataset_manifests",
        "storage.objects",
        "market_data.dataset_objects",
        "market_data.dataset_lineage",
        "market_data.dataset_object_lineage",
        "market_data.quality_incidents",
    )
    for table in ordered_tables:
        allowed = tables.get(table)
        if allowed is None:
            errors.append(f"DBML에 테이블이 없습니다: {table}")
            continue
        for row in catalog.records(table):
            extras = set(row).difference(allowed)
            if extras:
                errors.append(f"{table} 비-DBML 열: {sorted(extras)}")
            operations.append((table, row))
    manifests = catalog.records("market_data.dataset_manifests")
    by_hash: dict[str, list[str]] = defaultdict(list)
    for row in manifests:
        by_hash[row["dataset_hash"]].append(row["id"])
    duplicate_hashes = {
        digest: ids for digest, ids in by_hash.items() if len(ids) > 1
    }
    if duplicate_hashes:
        errors.append(
            "dataset_manifests.dataset_hash 전역 UNIQUE 충돌 가능성이 있습니다."
        )
    report = {
        "status": "FAILED" if errors else "PASSED",
        "mode": "execute" if execute else "dry-run",
        "operation_count": len(operations),
        "contract_errors": errors,
        "duplicate_dataset_hashes": duplicate_hashes,
    }
    if errors or not execute:
        return report
    providers = catalog.records("market_data.providers")
    if any(
        row.get("rights_version") in {None, "", "UNVERIFIED"}
        or row.get("status") == "REVIEW_REQUIRED"
        for row in providers
    ):
        raise RuntimeError(
            "Alpaca 권리 버전이 승인되지 않아 PostgreSQL 반영을 차단합니다."
        )
    if not database_url:
        raise ValueError("--execute에는 DATABASE_URL이 필요합니다.")
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL 반영에는 optional dependency psycopg가 필요합니다."
        ) from exc
    instrument_ids = set()
    for record in catalog.records("storage.objects"):
        path = object_store.path_for(record["object_key"])
        instrument_ids.update(
            pq.read_table(path, columns=["instrument_id"])
            .column("instrument_id")
            .to_pylist()
        )
    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                if instrument_ids:
                    cursor.execute(
                        "SELECT id::text FROM market_data.instruments "
                        "WHERE id = ANY(%s::uuid[])",
                        (list(instrument_ids),),
                    )
                    known = {row[0] for row in cursor.fetchall()}
                    missing = instrument_ids.difference(known)
                    if missing:
                        raise RuntimeError(
                            f"DB에 없는 instrument_id: {sorted(missing)}"
                        )
                for table, row in operations:
                    schema, name = table.split(".", 1)
                    columns = list(row)
                    conflict = (
                        sql.SQL("ON CONFLICT DO NOTHING")
                        if "id" not in row
                        else sql.SQL(
                            "ON CONFLICT (id) DO UPDATE SET {}"
                        ).format(
                            sql.SQL(",").join(
                                sql.SQL("{}=EXCLUDED.{}").format(
                                    sql.Identifier(column),
                                    sql.Identifier(column),
                                )
                                for column in columns
                                if column != "id"
                            )
                        )
                    )
                    statement = sql.SQL(
                        "INSERT INTO {}.{} ({}) VALUES ({}) {}"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(name),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                        sql.SQL(",").join(
                            sql.Placeholder() for _ in columns
                        ),
                        conflict,
                    )
                    cursor.execute(
                        statement,
                        [row[column] for column in columns],
                    )
    return {**report, "status": "APPLIED"}


def upload_catalog_objects(
    catalog: LocalCatalog,
    local_store: LocalObjectStore,
    remote_store: S3ObjectStore,
    destination_catalog_root: Path,
    *,
    dry_run: bool = True,
    resume: bool = False,
) -> dict[str, Any]:
    operations = []
    uploaded = []
    for record in catalog.records("storage.objects"):
        local_path = local_store.path_for(record["object_key"])
        if not local_path.is_file():
            raise FileNotFoundError(f"로컬 객체가 없습니다: {local_path}")
        operations.append(
            {
                "object_id": record["id"],
                "object_key": record["object_key"],
                "byte_size": record["byte_size"],
                "content_hash": record["content_hash"],
            }
        )
        if dry_run:
            continue
        if resume and remote_store.verify(
            record["object_key"],
            record["content_hash"],
        ).ok:
            receipt = remote_store.put(local_path, record["object_key"])
        else:
            receipt = remote_store.put(local_path, record["object_key"])
        uploaded.append(
            {
                **record,
                "storage_provider": receipt.storage_provider,
                "bucket_name": receipt.bucket_name,
                "object_key": receipt.object_key,
                "provider_version_id": receipt.provider_version_id,
            }
        )
    if dry_run:
        return {
            "status": "DRY_RUN",
            "operation_count": len(operations),
            "operations": operations,
        }
    destination = LocalCatalog(destination_catalog_root)
    for table in TABLE_FILES:
        if table == "storage.objects":
            for record in uploaded:
                destination.upsert(table, record)
        elif table in {
            "market_data.dataset_lineage",
            "market_data.dataset_object_lineage",
        }:
            for record in catalog.records(table):
                destination.append_unique(
                    table,
                    record,
                    tuple(record),
                )
        else:
            for record in catalog.records(table):
                destination.upsert(table, record)
    return {
        "status": "UPLOADED",
        "object_count": len(uploaded),
        "catalog_root": str(destination.root),
    }


def benchmark_catalog(
    catalog: LocalCatalog,
    store: LocalObjectStore,
    *,
    year: int | None = None,
    price_type: str | None = None,
    layer: str | None = None,
    resolution: str | None = None,
) -> dict[str, Any]:
    available = [
        row
        for row in catalog.records("market_data.dataset_manifests")
        if row["status"] == "AVAILABLE"
    ]
    feeds = {
        row["id"]: row for row in catalog.records("market_data.feeds")
    }
    filtered = []
    for manifest in available:
        feed = feeds.get(manifest["feed_id"], {})
        candidate_price_type = (
            "raw" if "RAW" in str(feed.get("code", "")) else "adjusted"
        )
        if year is not None and not str(manifest["period_start"]).startswith(
            str(year)
        ):
            continue
        if price_type is not None and candidate_price_type != price_type:
            continue
        if layer is not None and manifest["data_layer"] != layer:
            continue
        if resolution is not None and manifest["resolution"] != resolution:
            continue
        filtered.append(manifest)
    if not filtered:
        return {
            "status": "NO_DATA",
            "production_ready": False,
            "message": "benchmark할 AVAILABLE Manifest가 없습니다.",
        }
    relations = catalog.records("market_data.dataset_objects")
    storage = {
        row["id"]: row for row in catalog.records("storage.objects")
    }
    latest = max(filtered, key=lambda row: row["created_at"])
    objects = [
        row for row in relations if row["dataset_manifest_id"] == latest["id"]
    ]
    paths = [store.path_for(storage[row["object_id"]]["object_key"]) for row in objects]
    sizes = [path.stat().st_size for path in paths]
    tracemalloc.start()
    started = time.perf_counter()
    dataset = ds.dataset([str(path) for path in paths], format="parquet")
    table = dataset.to_table()
    full_read_seconds = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    one_instrument_seconds = None
    one_month_seconds = None
    if table.num_rows:
        instrument = table.column("instrument_id")[0].as_py()
        session_date = table.column("session_date_et")[0].as_py()
        month_start = session_date.replace(day=1)
        month_end = (
            date(month_start.year + 1, 1, 1)
            if month_start.month == 12
            else date(month_start.year, month_start.month + 1, 1)
        )
        started = time.perf_counter()
        dataset.to_table(
            filter=(
                (ds.field("instrument_id") == instrument)
                & (ds.field("session_date_et") >= month_start)
                & (ds.field("session_date_et") < month_end)
            )
        )
        one_month_seconds = time.perf_counter() - started
        started = time.perf_counter()
        dataset.to_table(filter=ds.field("instrument_id") == instrument)
        one_instrument_seconds = time.perf_counter() - started
    granularities = defaultdict(int)
    for relation in objects:
        granularities[relation["partition_granularity"]] += 1
    runs = [
        row
        for row in catalog.records("market_data.pipeline_runs")
        if row.get("completed_at") and row.get("started_at")
    ]
    latest_duration = None
    if runs:
        latest_run = max(runs, key=lambda row: row["completed_at"])
        latest_duration = (
            datetime.fromisoformat(
                str(latest_run["completed_at"]).replace("Z", "+00:00")
            )
            - datetime.fromisoformat(
                str(latest_run["started_at"]).replace("Z", "+00:00")
            )
        ).total_seconds()
    matching_snapshots = [
        row
        for row in catalog.records("market_data.dataset_manifests")
        if row["feed_id"] == latest["feed_id"]
        and row["data_layer"] == latest["data_layer"]
        and row["resolution"] == latest["resolution"]
        and str(row["period_start"])[:4] == str(latest["period_start"])[:4]
    ]
    granularity_read_seconds: dict[str, float] = {}
    for granularity in ("DAY", "YEAR"):
        snapshot = next(
            (
                manifest
                for manifest in sorted(
                    matching_snapshots,
                    key=lambda row: row["revision_number"],
                    reverse=True,
                )
                if any(
                    relation["partition_granularity"] == granularity
                    for relation in relations
                    if relation["dataset_manifest_id"] == manifest["id"]
                )
            ),
            None,
        )
        if snapshot is None:
            continue
        snapshot_objects = [
            relation
            for relation in relations
            if relation["dataset_manifest_id"] == snapshot["id"]
        ]
        snapshot_paths = [
            store.path_for(storage[row["object_id"]]["object_key"])
            for row in snapshot_objects
        ]
        started = time.perf_counter()
        ds.dataset(
            [str(path) for path in snapshot_paths],
            format="parquet",
        ).count_rows()
        granularity_read_seconds[granularity] = time.perf_counter() - started
    return {
        "status": "MEASURED",
        "production_ready": False,
        "manifest_id": latest["id"],
        "input_rows": sum(row["row_count"] for row in objects),
        "output_rows": table.num_rows,
        "object_count": len(paths),
        "min_size_mib": min(sizes, default=0) / 1024 / 1024,
        "mean_size_mib": (
            sum(sizes) / len(sizes) / 1024 / 1024 if sizes else 0
        ),
        "max_size_mib": max(sizes, default=0) / 1024 / 1024,
        "part_counts_by_granularity": dict(granularities),
        "full_manifest_read_seconds": full_read_seconds,
        "one_instrument_read_seconds": one_instrument_seconds,
        "one_instrument_one_month_seconds": one_month_seconds,
        "peak_python_memory_mib": peak / 1024 / 1024,
        "latest_pipeline_duration_seconds": latest_duration,
        "granularity_count_read_seconds": granularity_read_seconds,
        "day_vs_year_comparison_available": {
            "DAY",
            "YEAR",
        }.issubset(granularity_read_seconds),
    }
