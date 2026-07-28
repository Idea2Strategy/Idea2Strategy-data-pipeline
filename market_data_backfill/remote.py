"""Explicit, opt-in S3 upload and PostgreSQL load-plan application."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .core import sha256_file
from .validation import read_jsonl


TABLE_PLAN_FILES = (
    ("market_data.providers", "providers.json"),
    ("market_data.feeds", "feeds.json"),
    ("market_data.pipeline_runs", "pipeline-runs.jsonl"),
    ("market_data.dataset_manifests", "dataset-manifests.jsonl"),
    ("storage.objects", "storage-objects.jsonl"),
    ("market_data.dataset_objects", "dataset-objects.jsonl"),
    ("market_data.dataset_lineage", "dataset-lineage.jsonl"),
    ("market_data.dataset_object_lineage", "dataset-object-lineage.jsonl"),
    ("market_data.quality_incidents", "quality-incidents.jsonl"),
)


def upload_objects(
    output_root: Path,
    bucket: str,
    *,
    prefix: str = "",
    endpoint_url: str | None = None,
    execute: bool = False,
    resume: bool = False,
) -> dict[str, Any]:
    plan_path = output_root / "load-plan" / "storage-objects.jsonl"
    records = read_jsonl(plan_path)
    if not records:
        raise ValueError(f"업로드할 storage object 계획이 없습니다: {plan_path}")
    state_path = output_root / "upload-state.json"
    known_completed: dict[str, dict[str, Any]] = {}
    if state_path.is_file():
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        known_completed = payload.get("completed", {})
    completed = dict(known_completed) if resume else {}
    operations = []
    for record in records:
        path = output_root / record["object_key"]
        previous = known_completed.get(record["id"])
        if not path.is_file() and previous:
            path = Path(previous["local_path"])
        if not path.is_file():
            raise FileNotFoundError(f"로컬 객체가 없습니다: {path}")
        actual_hash = sha256_file(path)
        if actual_hash != record["content_hash"]:
            raise ValueError(f"업로드 전 SHA-256 불일치: {path}")
        key = (
            record["object_key"]
            if record.get("storage_provider") == "S3_COMPATIBLE"
            else "/".join(
                part.strip("/")
                for part in (prefix, record["object_key"])
                if part.strip("/")
            )
        )
        operations.append(
            {
                "object_id": record["id"],
                "local_path": str(path),
                "bucket": bucket,
                "key": key,
                "content_hash": actual_hash,
                "byte_size": path.stat().st_size,
            }
        )
    if not execute:
        return {
            "mode": "dry-run",
            "operation_count": len(operations),
            "operations": operations,
        }

    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "실제 업로드에는 optional dependency boto3가 필요합니다."
        ) from exc
    client = boto3.client("s3", endpoint_url=endpoint_url)
    for operation in operations:
        object_id = operation["object_id"]
        if object_id in completed:
            continue
        client.upload_file(
            operation["local_path"],
            bucket,
            operation["key"],
            ExtraArgs={
                "Metadata": {
                    "sha256": operation["content_hash"],
                }
            },
        )
        head = client.head_object(Bucket=bucket, Key=operation["key"])
        remote_hash = head.get("Metadata", {}).get("sha256")
        if (
            int(head["ContentLength"]) != operation["byte_size"]
            or remote_hash != operation["content_hash"]
        ):
            raise RuntimeError(
                "업로드 검증 실패. DB 반영 전에 객체를 확인해야 합니다: "
                f"s3://{bucket}/{operation['key']}"
            )
        completed[object_id] = operation
        completed[object_id]["provider_version_id"] = (
            head.get("VersionId")
            or str(head.get("ETag", "")).strip('"')
            or operation["content_hash"]
        )
        completed[object_id]["verified_at"] = datetime.now(
            timezone.utc
        ).isoformat()
        state_path.write_text(
            json.dumps(
                {"completed": completed},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    uploaded_records = []
    for record in records:
        uploaded = completed[record["id"]]
        uploaded_records.append(
            {
                **record,
                "status": "AVAILABLE",
                "storage_provider": "S3_COMPATIBLE",
                "bucket_name": uploaded["bucket"],
                "object_key": uploaded["key"],
                "provider_version_id": uploaded["provider_version_id"],
                "verified_at": uploaded["verified_at"],
            }
        )
    temporary_plan = plan_path.with_name(f".{plan_path.name}.{os.getpid()}.tmp")
    try:
        temporary_plan.write_text(
            "".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                for row in uploaded_records
            ),
            encoding="utf-8",
        )
        os.replace(temporary_plan, plan_path)
    finally:
        temporary_plan.unlink(missing_ok=True)
    return {
        "mode": "execute",
        "operation_count": len(operations),
        "completed_count": len(completed),
        "state_path": str(state_path),
    }


def parse_dbml_columns(path: Path) -> dict[str, set[str]]:
    if not path.is_file():
        raise FileNotFoundError(f"DBML 정본이 없습니다: {path}")
    text = path.read_text(encoding="utf-8")
    tables: dict[str, set[str]] = {}
    for match in re.finditer(
        r"Table\s+([A-Za-z0-9_.]+)\s*\{(.*?)\}",
        text,
        flags=re.DOTALL,
    ):
        table_name, body = match.groups()
        columns: set[str] = set()
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith(("//", "Note:", "Indexes", "}")):
                continue
            column = line.split()[0].strip('"')
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
                columns.add(column)
        tables[table_name] = columns
    return tables


def _load_plan_records(plan_root: Path, filename: str) -> list[dict[str, Any]]:
    path = plan_root / filename
    if filename.endswith(".jsonl"):
        return read_jsonl(path)
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else [payload]


def apply_database_plan(
    output_root: Path,
    dbml_path: Path,
    *,
    execute: bool = False,
) -> dict[str, Any]:
    plan_root = output_root / "load-plan"
    tables = parse_dbml_columns(dbml_path)
    operations: list[tuple[str, dict[str, Any]]] = []
    contract_errors: list[str] = []
    for table_name, filename in TABLE_PLAN_FILES:
        records = _load_plan_records(plan_root, filename)
        dbml_columns = tables.get(table_name)
        if dbml_columns is None:
            contract_errors.append(f"DBML에 테이블이 없습니다: {table_name}")
            continue
        for index, record in enumerate(records, 1):
            extras = set(record).difference(dbml_columns)
            if extras:
                contract_errors.append(
                    f"{filename} {index}행의 비-DBML 열: {sorted(extras)}"
                )
            operations.append((table_name, record))
    if contract_errors:
        raise ValueError(
            "DBML 계약 검증에 실패해 DB를 변경하지 않습니다.\n"
            + "\n".join(contract_errors)
        )
    manifest_records = _load_plan_records(
        plan_root,
        "dataset-manifests.jsonl",
    )
    manifests_by_hash: dict[str, list[str]] = {}
    for record in manifest_records:
        manifests_by_hash.setdefault(record["dataset_hash"], []).append(record["id"])
    duplicate_dataset_hashes = {
        digest: manifest_ids
        for digest, manifest_ids in manifests_by_hash.items()
        if len(manifest_ids) > 1
    }
    summary = {
        "mode": "execute" if execute else "dry-run",
        "operation_order": [name for name, _ in TABLE_PLAN_FILES],
        "operation_count": len(operations),
        "transaction_scope": "single PostgreSQL metadata transaction",
        "object_upload_transactional": False,
        "upload_compensation": (
            "업로드된 불변 객체는 DB 실패 시 삭제하지 않고 orphan으로 남겨 "
            "동일 적재 계획 재시도로 연결합니다."
        ),
        "constraint_risks": {
            "duplicate_dataset_hashes": duplicate_dataset_hashes,
        },
    }
    if not execute:
        return summary
    if duplicate_dataset_hashes:
        raise RuntimeError(
            "DBML의 dataset_hash UNIQUE 제약과 충돌하는 의미상 다른 "
            "Manifest가 있습니다. DBML 보완 검토 전에는 DB를 변경하지 않습니다."
        )
    providers = _load_plan_records(plan_root, "providers.json")
    if any(row.get("rights_version") == "UNVERIFIED" for row in providers):
        raise RuntimeError(
            "시장 데이터 권리 버전이 UNVERIFIED입니다. 승인된 권리 증적을 "
            "적재 계획에 반영하기 전에는 DB를 변경하지 않습니다."
        )
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL 환경변수가 필요합니다.")
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise RuntimeError(
            "실제 DB 반영에는 optional dependency psycopg가 필요합니다."
        ) from exc

    records_by_table = {
        table_name: _load_plan_records(plan_root, filename)
        for table_name, filename in TABLE_PLAN_FILES
    }
    instrument_ids: set[str] = set()
    upload_state_path = output_root / "upload-state.json"
    uploaded_state = (
        json.loads(upload_state_path.read_text(encoding="utf-8")).get(
            "completed", {}
        )
        if upload_state_path.is_file()
        else {}
    )
    for storage in records_by_table["storage.objects"]:
        local_path = output_root / storage["object_key"]
        if not local_path.is_file() and storage["id"] in uploaded_state:
            local_path = Path(uploaded_state[storage["id"]]["local_path"])
        if local_path.is_file():
            instrument_ids.update(
                pq.read_table(local_path, columns=["instrument_id"])
                .column("instrument_id")
                .to_pylist()
            )

    def insert_record(cursor: Any, table_name: str, record: dict[str, Any]) -> None:
        schema_name, relation_name = table_name.split(".", 1)
        columns = list(record)
        statement = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )
        cursor.execute(statement, [record[column] for column in columns])

    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                for table_name in ("market_data.providers", "market_data.feeds"):
                    for record in records_by_table[table_name]:
                        insert_record(cursor, table_name, record)

                if instrument_ids:
                    cursor.execute(
                        "SELECT id::text FROM market_data.instruments "
                        "WHERE id = ANY(%s::uuid[])",
                        (sorted(instrument_ids),),
                    )
                    found = {row[0] for row in cursor.fetchall()}
                    missing = sorted(instrument_ids.difference(found))
                    if missing:
                        raise RuntimeError(
                            "DB에 없는 instrument_id가 있어 적재를 중단합니다: "
                            + ", ".join(missing[:20])
                        )

                for record in records_by_table["market_data.pipeline_runs"]:
                    running = {
                        **record,
                        "status": "RUNNING",
                        "output_hash": None,
                        "completed_at": None,
                        "failure_code": None,
                    }
                    insert_record(
                        cursor,
                        "market_data.pipeline_runs",
                        running,
                    )
                for record in records_by_table[
                    "market_data.dataset_manifests"
                ]:
                    building = {
                        **record,
                        "status": "BUILDING",
                        "available_at": None,
                    }
                    insert_record(
                        cursor,
                        "market_data.dataset_manifests",
                        building,
                    )
                for record in records_by_table["storage.objects"]:
                    staged = {
                        **record,
                        "status": "STAGED",
                        "verified_at": None,
                    }
                    insert_record(cursor, "storage.objects", staged)
                    local_path = output_root / record["object_key"]
                    if not local_path.is_file() and record["id"] in uploaded_state:
                        local_path = Path(
                            uploaded_state[record["id"]]["local_path"]
                        )
                    if (
                        not local_path.is_file()
                        or sha256_file(local_path) != record["content_hash"]
                    ):
                        raise RuntimeError(
                            f"DB 반영 전 객체 검증 실패: {local_path}"
                        )
                    cursor.execute(
                        "UPDATE storage.objects SET status = 'AVAILABLE', "
                        "verified_at = %s WHERE id = %s",
                        (
                            record.get("verified_at")
                            or datetime.now(timezone.utc).isoformat(),
                            record["id"],
                        ),
                    )

                for table_name in (
                    "market_data.dataset_objects",
                    "market_data.dataset_lineage",
                    "market_data.dataset_object_lineage",
                    "market_data.quality_incidents",
                ):
                    for record in records_by_table[table_name]:
                        insert_record(cursor, table_name, record)

                for record in records_by_table[
                    "market_data.dataset_manifests"
                ]:
                    cursor.execute(
                        "UPDATE market_data.dataset_manifests "
                        "SET status = %s, dataset_hash = %s, "
                        "available_at = %s WHERE id = %s",
                        (
                            record["status"],
                            record["dataset_hash"],
                            record["available_at"],
                            record["id"],
                        ),
                    )
                for record in records_by_table["market_data.pipeline_runs"]:
                    cursor.execute(
                        "UPDATE market_data.pipeline_runs "
                        "SET status = %s, output_hash = %s, "
                        "completed_at = %s, failure_code = %s WHERE id = %s",
                        (
                            record["status"],
                            record["output_hash"],
                            record["completed_at"],
                            record["failure_code"],
                            record["id"],
                        ),
                    )
    return summary
