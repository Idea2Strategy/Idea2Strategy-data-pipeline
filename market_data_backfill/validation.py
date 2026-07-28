"""Validate local Parquet objects and their manifest/load-plan metadata."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .core import (
    DATASET_SPECS,
    canonical_dataset_hash,
    quality_issues,
    sha256_file,
)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def validate_output(output_root: Path) -> dict[str, Any]:
    plan_root = output_root / "load-plan"
    manifests = read_jsonl(plan_root / "dataset-manifests.jsonl")
    storage_objects = read_jsonl(plan_root / "storage-objects.jsonl")
    dataset_objects = read_jsonl(plan_root / "dataset-objects.jsonl")
    summary_path = plan_root / "summary.json"
    summary = (
        json.loads(summary_path.read_text(encoding="utf-8"))
        if summary_path.is_file()
        else {}
    )
    upload_state_path = output_root / "upload-state.json"
    upload_state = (
        json.loads(upload_state_path.read_text(encoding="utf-8")).get(
            "completed", {}
        )
        if upload_state_path.is_file()
        else {}
    )
    if not manifests:
        raise ValueError(f"검증할 Manifest 적재 계획이 없습니다: {plan_root}")

    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    storage_by_id = {
        row["id"]: row for row in storage_objects
    }
    manifest_by_id = {
        row["id"]: row for row in manifests
    }
    objects_by_manifest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    periods_by_manifest_shard: dict[
        tuple[str, str], list[tuple[str, str, str]]
    ] = defaultdict(list)

    for relation in dataset_objects:
        manifest_id = relation["dataset_manifest_id"]
        objects_by_manifest[manifest_id].append(relation)
        storage = storage_by_id.get(relation["object_id"])
        if storage is None:
            errors.append(
                {
                    "code": "MISSING_STORAGE_OBJECT",
                    "object_id": relation["object_id"],
                }
            )
            continue
        path = output_root / storage["object_key"]
        if not path.is_file() and storage["id"] in upload_state:
            path = Path(upload_state[storage["id"]]["local_path"])
        if not path.is_file():
            errors.append(
                {
                    "code": "MISSING_PARQUET_FILE",
                    "path": str(path),
                }
            )
            continue
        actual_hash = sha256_file(path)
        if actual_hash != storage["content_hash"]:
            errors.append(
                {
                    "code": "CONTENT_HASH_MISMATCH",
                    "path": str(path),
                    "expected": storage["content_hash"],
                    "actual": actual_hash,
                }
            )
            continue
        try:
            parquet_file = pq.ParquetFile(path)
            table = parquet_file.read()
        except Exception as exc:
            errors.append(
                {
                    "code": "PARQUET_FOOTER_INVALID",
                    "path": str(path),
                    "message": str(exc),
                }
            )
            continue
        if parquet_file.metadata.num_rows != relation["row_count"]:
            errors.append(
                {
                    "code": "OBJECT_ROW_COUNT_MISMATCH",
                    "path": str(path),
                    "expected": relation["row_count"],
                    "actual": parquet_file.metadata.num_rows,
                }
            )
        manifest = manifest_by_id.get(manifest_id)
        if manifest is None:
            errors.append(
                {
                    "code": "MISSING_DATASET_MANIFEST",
                    "dataset_manifest_id": manifest_id,
                }
            )
            continue
        spec = DATASET_SPECS.get(
            (manifest["data_layer"], manifest["resolution"])
        )
        if spec is None:
            errors.append(
                {
                    "code": "UNSUPPORTED_DATASET",
                    "dataset_manifest_id": manifest_id,
                }
            )
            continue
        year = int(manifest["period_start"][:4])
        for issue in quality_issues(table, spec, year):
            record = {
                **issue,
                "dataset_manifest_id": manifest_id,
                "path": str(path),
            }
            (errors if issue["severity"] == "ERROR" else warnings).append(record)
        del parquet_file
        periods_by_manifest_shard[
            (manifest_id, relation["shard_key"])
        ].append(
            (
                relation["period_start"],
                relation["period_end"],
                str(path),
            )
        )

    for (manifest_id, shard), periods in periods_by_manifest_shard.items():
        ordered = sorted(periods)
        for previous, current in zip(ordered, ordered[1:]):
            if current[0] < previous[1]:
                errors.append(
                    {
                        "code": "OVERLAPPING_OBJECT_PERIOD",
                        "dataset_manifest_id": manifest_id,
                        "shard_key": shard,
                        "left_path": previous[2],
                        "right_path": current[2],
                    }
                )

    for manifest in manifests:
        manifest_id = manifest["id"]
        relations = objects_by_manifest.get(manifest_id, [])
        canonical_objects = []
        for relation in relations:
            storage = storage_by_id.get(relation["object_id"], {})
            canonical_objects.append(
                {
                    "content_hash": storage.get("content_hash"),
                    "partition_granularity": relation.get(
                        "partition_granularity"
                    ),
                    "partition_start": relation.get("partition_start"),
                    "partition_end": relation.get("partition_end"),
                    "period_start": relation.get("period_start"),
                    "period_end": relation.get("period_end"),
                    "shard_key": relation.get("shard_key"),
                    "part_number": relation.get("part_number"),
                    "row_count": relation.get("row_count"),
                    "schema_version": manifest.get("schema_version"),
                }
            )
        actual_hash = canonical_dataset_hash(canonical_objects)
        if actual_hash != manifest.get("dataset_hash"):
            errors.append(
                {
                    "code": "DATASET_HASH_MISMATCH",
                    "dataset_manifest_id": manifest_id,
                    "expected": manifest.get("dataset_hash"),
                    "actual": actual_hash,
                }
            )
        manifest_errors = [
            error
            for error in errors
            if error.get("dataset_manifest_id") == manifest_id
        ]
        if manifest.get("status") == "AVAILABLE" and manifest_errors:
            errors.append(
                {
                    "code": "AVAILABLE_MANIFEST_HAS_ERRORS",
                    "dataset_manifest_id": manifest_id,
                }
            )

    actual_total_rows = sum(int(row["row_count"]) for row in dataset_objects)
    if "row_total" in summary and actual_total_rows != int(summary["row_total"]):
        errors.append(
            {
                "code": "LOAD_PLAN_ROW_COUNT_MISMATCH",
                "expected": summary["row_total"],
                "actual": actual_total_rows,
            }
        )

    report = {
        "status": "PASSED" if not errors else "FAILED",
        "manifest_count": len(manifests),
        "object_count": len(dataset_objects),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "missing_bars_are_not_filled": True,
    }
    report_path = output_root / "validation-report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report
