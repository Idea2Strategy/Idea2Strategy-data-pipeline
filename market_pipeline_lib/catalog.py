"""DBML-shaped local and PostgreSQL catalog boundaries."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable


TABLE_FILES = {
    "market_data.providers": "providers.jsonl",
    "market_data.feeds": "feeds.jsonl",
    "market_data.pipeline_runs": "pipeline-runs.jsonl",
    "market_data.dataset_manifests": "dataset-manifests.jsonl",
    "storage.objects": "storage-objects.jsonl",
    "market_data.dataset_objects": "dataset-objects.jsonl",
    "market_data.dataset_lineage": "dataset-lineage.jsonl",
    "market_data.dataset_object_lineage": "dataset-object-lineage.jsonl",
    "market_data.quality_incidents": "quality-incidents.jsonl",
}

ID_TABLES = {
    name
    for name in TABLE_FILES
    if name
    not in {
        "market_data.dataset_lineage",
        "market_data.dataset_object_lineage",
    }
}


@runtime_checkable
class MarketDataCatalog(Protocol):
    def begin_pipeline_run(self, record: dict[str, Any]) -> None: ...

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None: ...

    def stage_object(
        self,
        storage_record: dict[str, Any],
        dataset_object_record: dict[str, Any],
    ) -> None: ...

    def publish_manifest(self, record: dict[str, Any]) -> None: ...

    def record_dataset_lineage(self, record: dict[str, Any]) -> None: ...

    def record_object_lineage(self, record: dict[str, Any]) -> None: ...

    def record_quality_incident(self, record: dict[str, Any]) -> None: ...

    def records(self, table: str) -> list[dict[str, Any]]: ...


class LocalCatalog:
    """Atomic JSONL catalog whose exported fields mirror DBML columns."""

    def __init__(self, root: Path, *, create: bool = True) -> None:
        self.root = root.expanduser().resolve()
        if create:
            self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, table: str) -> Path:
        try:
            return self.root / TABLE_FILES[table]
        except KeyError as exc:
            raise ValueError(f"지원하지 않는 catalog table: {table}") from exc

    def records(self, table: str) -> list[dict[str, Any]]:
        path = self._path(table)
        if not path.is_file():
            return []
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if table not in ID_TABLES:
            return rows
        latest: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for row in rows:
            key = str(row["id"])
            if key not in latest:
                order.append(key)
            latest[key] = row
        return [latest[key] for key in order]

    def _write(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        path = self._path(table)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(
                "".join(
                    json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                    for row in rows
                ),
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    def upsert(self, table: str, record: dict[str, Any]) -> None:
        if table not in ID_TABLES:
            raise ValueError(f"upsert는 id table에만 사용합니다: {table}")
        rows = self.records(table)
        by_id = {str(row["id"]): row for row in rows}
        by_id[str(record["id"])] = dict(record)
        self._write(table, by_id.values())

    def append_unique(
        self,
        table: str,
        record: dict[str, Any],
        key_fields: tuple[str, ...],
    ) -> None:
        rows = self.records(table)
        key = tuple(record.get(field) for field in key_fields)
        if any(tuple(row.get(field) for field in key_fields) == key for row in rows):
            return
        rows.append(dict(record))
        self._write(table, rows)

    def begin_pipeline_run(self, record: dict[str, Any]) -> None:
        self.upsert("market_data.pipeline_runs", record)

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None:
        runs = {
            row["id"]: row
            for row in self.records("market_data.pipeline_runs")
        }
        if pipeline_run_id not in runs:
            raise KeyError(f"pipeline run이 없습니다: {pipeline_run_id}")
        record = runs[pipeline_run_id]
        record.update(
            {
                "status": status,
                "output_hash": output_hash,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "failure_code": failure_code,
            }
        )
        self.upsert("market_data.pipeline_runs", record)

    def stage_object(
        self,
        storage_record: dict[str, Any],
        dataset_object_record: dict[str, Any],
    ) -> None:
        self.upsert("storage.objects", storage_record)
        self.upsert("market_data.dataset_objects", dataset_object_record)

    def publish_manifest(self, record: dict[str, Any]) -> None:
        self.upsert("market_data.dataset_manifests", record)

    def record_dataset_lineage(self, record: dict[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_lineage",
            record,
            ("derived_manifest_id", "source_manifest_id", "relation_type"),
        )

    def record_object_lineage(self, record: dict[str, Any]) -> None:
        self.append_unique(
            "market_data.dataset_object_lineage",
            record,
            (
                "derived_dataset_object_id",
                "source_dataset_object_id",
                "relation_type",
            ),
        )

    def record_quality_incident(self, record: dict[str, Any]) -> None:
        self.upsert("market_data.quality_incidents", record)

    def latest_available_manifest(
        self,
        *,
        feed_id: str,
        data_layer: str,
        resolution: str,
        year: int,
    ) -> dict[str, Any] | None:
        matches = [
            row
            for row in self.records("market_data.dataset_manifests")
            if row["feed_id"] == feed_id
            and row["data_layer"] == data_layer
            and row["resolution"] == resolution
            and row["status"] == "AVAILABLE"
            and str(row["period_start"]).startswith(str(year))
        ]
        return max(matches, key=lambda row: row["revision_number"], default=None)

    def objects_for_manifest(
        self,
        manifest_id: str,
    ) -> list[dict[str, Any]]:
        storage = {
            row["id"]: row for row in self.records("storage.objects")
        }
        output = []
        for relation in self.records("market_data.dataset_objects"):
            if relation["dataset_manifest_id"] != manifest_id:
                continue
            output.append({**relation, "storage": storage[relation["object_id"]]})
        return output

    def write_summary(self, payload: dict[str, Any]) -> Path:
        path = self.root / "summary.json"
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def record_pipeline_output(
        self,
        *,
        pipeline_run_id: str,
        dataset_manifest_id: str,
        dataset_object_id: str,
    ) -> None:
        """Keep provenance locally until DBML adds pipeline_run_outputs."""
        path = self.root / "pipeline-run-outputs.local.jsonl"
        rows = []
        if path.is_file():
            rows = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        record = {
            "pipeline_run_id": pipeline_run_id,
            "dataset_manifest_id": dataset_manifest_id,
            "dataset_object_id": dataset_object_id,
        }
        if record not in rows:
            rows.append(record)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(
                "".join(
                    json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                    for row in rows
                ),
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


class PostgresCatalog:
    """Optional DBML-shaped sink implementing the same catalog boundary."""

    def __init__(self, connection: object) -> None:
        self.connection = connection

    @classmethod
    def connect(cls, database_url: str) -> "PostgresCatalog":
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "PostgresCatalog에는 optional dependency psycopg가 필요합니다."
            ) from exc
        return cls(psycopg.connect(database_url))

    @staticmethod
    def _qualified(table: str) -> tuple[str, str]:
        schema, name = table.split(".", 1)
        return schema, name

    def _upsert(self, table: str, record: dict[str, Any]) -> None:
        try:
            from psycopg import sql
        except ImportError as exc:
            raise RuntimeError("psycopg가 필요합니다.") from exc
        schema, name = self._qualified(table)
        columns = list(record)
        statement = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES ({}) "
            "ON CONFLICT (id) DO UPDATE SET {}"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(name),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
            sql.SQL(",").join(
                sql.SQL("{}=EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in columns
                if column != "id"
            ),
        )
        with self.connection.cursor() as cursor:
            cursor.execute(statement, [record[column] for column in columns])

    def begin_pipeline_run(self, record: dict[str, Any]) -> None:
        self._upsert("market_data.pipeline_runs", record)

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE market_data.pipeline_runs
                SET status=%s, output_hash=%s, completed_at=%s, failure_code=%s
                WHERE id=%s
                """,
                (
                    status,
                    output_hash,
                    datetime.now(timezone.utc),
                    failure_code,
                    pipeline_run_id,
                ),
            )

    def stage_object(
        self,
        storage_record: dict[str, Any],
        dataset_object_record: dict[str, Any],
    ) -> None:
        self._upsert("storage.objects", storage_record)
        self._upsert("market_data.dataset_objects", dataset_object_record)

    def publish_manifest(self, record: dict[str, Any]) -> None:
        self._upsert("market_data.dataset_manifests", record)

    def _insert_relation(self, table: str, record: dict[str, Any]) -> None:
        try:
            from psycopg import sql
        except ImportError as exc:
            raise RuntimeError("psycopg가 필요합니다.") from exc
        schema, name = self._qualified(table)
        columns = list(record)
        statement = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(name),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(sql.Placeholder() for _ in columns),
        )
        with self.connection.cursor() as cursor:
            cursor.execute(statement, [record[column] for column in columns])

    def record_dataset_lineage(self, record: dict[str, Any]) -> None:
        self._insert_relation("market_data.dataset_lineage", record)

    def record_object_lineage(self, record: dict[str, Any]) -> None:
        self._insert_relation("market_data.dataset_object_lineage", record)

    def record_quality_incident(self, record: dict[str, Any]) -> None:
        self._upsert("market_data.quality_incidents", record)

    def records(self, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError("PostgresCatalog 조회는 소비자별 query 계층에서 수행합니다.")
