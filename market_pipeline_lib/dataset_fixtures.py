"""Executable validation for committed COM-D1 dataset object fixtures."""

from __future__ import annotations

import json
from copy import deepcopy
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .compatibility import ContractValidationError, validate_dataset_manifest
from .contracts import sha256_file


class DatasetFixtureError(ValueError):
    """Raised when a committed dataset fixture is incomplete or inconsistent."""


def _timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise DatasetFixtureError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.utcoffset() is None:
        raise DatasetFixtureError(f"{label} must include a timezone")
    return parsed


def _fixture_root(fixture_path: Path, document: Mapping[str, Any]) -> Path:
    value = document.get("fixture_root", ".")
    if not isinstance(value, str) or not value:
        raise DatasetFixtureError("fixture_root must be a non-empty string")
    return (fixture_path.parent / value).resolve()


def _load_document(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetFixtureError(f"cannot read fixture: {path}") from exc
    if not isinstance(document, dict):
        raise DatasetFixtureError("fixture document must be an object")
    return document


def _apply_failure_fixture(
    fixture_path: Path,
    failure: Mapping[str, Any],
) -> tuple[dict[str, Any], Path]:
    base_value = failure.get("base_fixture")
    if not isinstance(base_value, str) or not base_value:
        raise DatasetFixtureError("base_fixture must be a non-empty string")
    base_path = (fixture_path.parent / base_value).resolve()
    document = deepcopy(_load_document(base_path))
    overrides = failure.get("object_overrides")
    if not isinstance(overrides, list) or not overrides:
        raise DatasetFixtureError("object_overrides must not be empty")
    by_name = {manifest.get("name"): manifest for manifest in document["manifests"]}
    for index, override in enumerate(overrides):
        if not isinstance(override, Mapping):
            raise DatasetFixtureError(f"object_overrides[{index}] must be an object")
        manifest = by_name.get(override.get("manifest"))
        object_index = override.get("object_index")
        object_path = override.get("object_path")
        if manifest is None or not isinstance(object_index, int):
            raise DatasetFixtureError(f"object_overrides[{index}] target is invalid")
        try:
            item = manifest["objects"][object_index]
        except (IndexError, KeyError, TypeError) as exc:
            raise DatasetFixtureError(f"object_overrides[{index}] target is invalid") from exc
        item["object_path"] = object_path
    return document, base_path


def _object_path(root: Path, value: object, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise DatasetFixtureError(f"{label}.object_path must be a non-empty string")
    path = (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise DatasetFixtureError(f"{label}.object_path escapes fixture_root") from exc
    return path


def _validate_object(
    root: Path,
    manifest: Mapping[str, Any],
    item: Mapping[str, Any],
    label: str,
) -> None:
    data_layer = manifest["data_layer"]
    object_key = item.get("object_key")
    if not isinstance(object_key, str) or f"layer={data_layer}" not in object_key:
        raise DatasetFixtureError(f"{label}.object_key does not bind data_layer")

    path = _object_path(root, item.get("object_path"), label)
    if not path.is_file():
        raise DatasetFixtureError(f"{label} object missing: {path.name}")

    expected_hash = item["content_hash"]
    actual_hash = sha256_file(path)
    if actual_hash != expected_hash:
        raise DatasetFixtureError(
            f"{label} sha256 mismatch: expected {expected_hash}, got {actual_hash}"
        )

    try:
        table = pq.read_table(path, columns=["instrument_id", "bar_start_at"])
    except Exception as exc:
        raise DatasetFixtureError(f"{label} is not readable Parquet") from exc
    if table.num_rows != item["row_count"]:
        raise DatasetFixtureError(f"{label}.row_count does not match Parquet")

    instruments = sorted(set(table.column("instrument_id").to_pylist()))
    declared_instruments = item.get("instrument_ids")
    if instruments != declared_instruments:
        raise DatasetFixtureError(f"{label}.instrument_ids do not match Parquet")

    timestamps = table.column("bar_start_at").to_pylist()
    if not timestamps:
        raise DatasetFixtureError(f"{label} Parquet must not be empty")
    if min(timestamps) != _timestamp(item["period_start"], f"{label}.period_start"):
        raise DatasetFixtureError(f"{label}.period_start does not match Parquet")
    if max(timestamps) != _timestamp(item["period_end"], f"{label}.period_end"):
        raise DatasetFixtureError(f"{label}.period_end does not match Parquet")


def validate_dataset_fixture(fixture_path: Path) -> dict[str, Any]:
    """Validate fixture manifests against their committed immutable objects."""

    fixture_path = fixture_path.resolve()
    document = _load_document(fixture_path)
    source_path = fixture_path
    if "base_fixture" in document:
        document, source_path = _apply_failure_fixture(fixture_path, document)

    manifests = document.get("manifests")
    lineage = document.get("lineage")
    if not isinstance(manifests, list) or not manifests:
        raise DatasetFixtureError("manifests must not be empty")
    if not isinstance(lineage, list):
        raise DatasetFixtureError("lineage must be a list")

    root = _fixture_root(source_path, document)
    by_id: dict[str, Mapping[str, Any]] = {}
    layers: set[str] = set()
    for index, manifest in enumerate(manifests):
        label = f"manifests[{index}]"
        if not isinstance(manifest, Mapping):
            raise DatasetFixtureError(f"{label} must be an object")
        try:
            validate_dataset_manifest(manifest)
        except ContractValidationError as exc:
            raise DatasetFixtureError(f"{label}: {exc}") from exc

        data_layer = manifest.get("data_layer")
        if data_layer not in {"RAW", "ADJUSTED"}:
            raise DatasetFixtureError(f"{label}.data_layer is unsupported")
        layers.add(data_layer)
        manifest_id = manifest["manifest_id"]
        if manifest_id in by_id:
            raise DatasetFixtureError(f"{label}.manifest_id is duplicated")
        by_id[manifest_id] = manifest

        objects = manifest["objects"]
        for object_index, item in enumerate(objects):
            if not isinstance(item, Mapping):
                raise DatasetFixtureError(f"{label}.objects[{object_index}] must be an object")
            _validate_object(
                root,
                manifest,
                item,
                f"{label}.objects[{object_index}]",
            )

        manifest_instruments = sorted(
            {instrument for item in objects for instrument in item["instrument_ids"]}
        )
        if manifest.get("instrument_ids") != manifest_instruments:
            raise DatasetFixtureError(f"{label}.instrument_ids do not match objects")
        if _timestamp(manifest["period_start"], f"{label}.period_start") != min(
            _timestamp(item["period_start"], f"{label}.objects.period_start")
            for item in objects
        ):
            raise DatasetFixtureError(f"{label}.period_start does not match objects")
        if _timestamp(manifest["period_end"], f"{label}.period_end") != max(
            _timestamp(item["period_end"], f"{label}.objects.period_end")
            for item in objects
        ):
            raise DatasetFixtureError(f"{label}.period_end does not match objects")

    if layers != {"RAW", "ADJUSTED"}:
        raise DatasetFixtureError("fixture must contain RAW and ADJUSTED manifests")

    lineage_keys: set[tuple[str, str, str]] = set()
    for index, relation in enumerate(lineage):
        if not isinstance(relation, Mapping):
            raise DatasetFixtureError(f"lineage[{index}] must be an object")
        key = (
            str(relation.get("derived_manifest_id", "")),
            str(relation.get("source_manifest_id", "")),
            str(relation.get("relation_type", "")),
        )
        if key[0] not in by_id or key[1] not in by_id or not key[2]:
            raise DatasetFixtureError(f"lineage[{index}] references an unknown manifest")
        lineage_keys.add(key)

    for index, manifest in enumerate(manifests):
        superseded_id = manifest.get("supersedes_manifest_id")
        if superseded_id is None:
            continue
        previous = by_id.get(superseded_id)
        if previous is None:
            raise DatasetFixtureError(f"manifests[{index}] supersedes an unknown manifest")
        if (
            previous["dataset_id"] != manifest["dataset_id"]
            or previous["data_layer"] != manifest["data_layer"]
            or previous["revision"] >= manifest["revision"]
        ):
            raise DatasetFixtureError(f"manifests[{index}] has invalid supersede ordering")
        expected_lineage = (manifest["manifest_id"], superseded_id, "SUPERSEDES")
        if expected_lineage not in lineage_keys:
            raise DatasetFixtureError(f"manifests[{index}] supersede lineage is missing")

    return document
