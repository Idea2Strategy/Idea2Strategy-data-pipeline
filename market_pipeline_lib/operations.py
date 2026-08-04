"""Catalog validation, export, upload, and benchmark operations.

`apply_catalog_to_postgres` used to be a second, independent raw-psycopg writer that
duplicated everything `PostgresCatalog` does.  It now validates and then replays the
local catalog through that one SQLAlchemy Core adapter, so there is a single place where
this repository writes `market_data`.
"""

from __future__ import annotations

import json
import os
import re
import time
import tracemalloc
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .catalog import (
    TABLE_FILES,
    LocalCatalog,
    MarketDataCatalog,
    PostgresCatalog,
    StorageObjectsPolicy,
)
from .contracts import DATASET_CONTRACTS, canonical_dataset_hash
from .processing import quality_issues
from .quality import (
    QualityIncident,
    content_hash_mismatch_incident,
    incident_from_issue,
    record_quality_incidents,
)
from .storage import LocalObjectStore, S3ObjectStore

# --------------------------------------------------------------------------------------
# Rights attestation
# --------------------------------------------------------------------------------------

#: `engine._ensure_provider_metadata` writes these because the pipeline itself cannot
#: know the licence position; they mean "not yet attested", never "attested as open".
PLACEHOLDER_RIGHTS_VERSIONS = frozenset({"", "UNVERIFIED"})
REVIEW_REQUIRED_STATUS = "REVIEW_REQUIRED"

#: Path to a JSON array of attestation objects.  The environment variable exists so the
#: existing `market-pipeline apply-db` CLI can satisfy the gate without a code change.
RIGHTS_ATTESTATIONS_ENV = "MARKET_DATA_RIGHTS_ATTESTATIONS"

_ATTESTATION_FIELDS = (
    "provider_code",
    "rights_version",
    "status",
    "approved_by",
    "approved_at",
    "evidence_uri",
)


class RightsVersionNotAttested(RuntimeError):
    """A provider's licence rights have not been attested, so the apply is refused.

    Distinct from a contract error: nothing is wrong with the data.  What is missing is
    external approval evidence, which `db/schema.dbml` requires for
    `market_data.providers.rights_version` and which no pipeline run can produce.
    """


@dataclass(frozen=True)
class RightsAttestation:
    """External approval evidence for one provider's licence rights.

    This is the *only* way `rights_version` reaches PostgreSQL as anything other than a
    placeholder.  Every field is required: an attestation without an approver, a date
    and a pointer to the evidence is not evidence.
    """

    provider_code: str
    rights_version: str
    status: str
    approved_by: str
    approved_at: str
    evidence_uri: str

    def __post_init__(self) -> None:
        for field in _ATTESTATION_FIELDS:
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"rights attestation field {field!r} must be a non-empty string")
        if self.rights_version in PLACEHOLDER_RIGHTS_VERSIONS:
            raise ValueError(
                f"{self.rights_version!r} is a placeholder, not an attested rights "
                "version; an attestation cannot restate the thing it is meant to resolve"
            )
        if self.status == REVIEW_REQUIRED_STATUS:
            raise ValueError(f"an attestation cannot carry status {REVIEW_REQUIRED_STATUS}")
        try:
            datetime.fromisoformat(self.approved_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"approved_at={self.approved_at!r} is not an ISO-8601 timestamp") from exc

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> RightsAttestation:
        missing = [field for field in _ATTESTATION_FIELDS if field not in payload]
        if missing:
            raise ValueError(f"rights attestation is missing {missing}")
        return cls(**{field: payload[field] for field in _ATTESTATION_FIELDS})


def load_rights_attestations(source: Path | None = None) -> dict[str, RightsAttestation]:
    """Read attestations from `source`, or from `$MARKET_DATA_RIGHTS_ATTESTATIONS`.

    Returns an empty mapping when neither is set.  An empty mapping is not permission:
    `apply_catalog_to_postgres` still refuses every provider that needs one.
    """

    path = source or (Path(os.environ[RIGHTS_ATTESTATIONS_ENV]) if os.environ.get(RIGHTS_ATTESTATIONS_ENV) else None)
    if path is None:
        return {}
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"rights attestation 파일이 없습니다: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON array of attestation objects")
    attestations = [RightsAttestation.from_mapping(item) for item in payload]
    by_code: dict[str, RightsAttestation] = {}
    for attestation in attestations:
        if attestation.provider_code in by_code:
            raise ValueError(f"duplicate rights attestation for provider {attestation.provider_code}")
        by_code[attestation.provider_code] = attestation
    return by_code


def rights_review_state(
    providers: list[dict[str, Any]],
    attestations: dict[str, RightsAttestation],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Split providers into applied rows, attested codes, and codes still blocked.

    A provider whose `rights_version` is already a real value and whose status is not
    `REVIEW_REQUIRED` needs nothing; a provider carrying the placeholder is rewritten
    from its attestation, or reported as blocked.  The placeholder is never written.
    """

    applied: list[dict[str, Any]] = []
    attested: list[str] = []
    blocked: list[str] = []
    for row in providers:
        code = str(row.get("code", ""))
        needs_review = (
            row.get("rights_version") is None
            or str(row.get("rights_version")) in PLACEHOLDER_RIGHTS_VERSIONS
            or row.get("status") == REVIEW_REQUIRED_STATUS
        )
        if not needs_review:
            applied.append(dict(row))
            continue
        attestation = attestations.get(code)
        if attestation is None:
            blocked.append(code)
            continue
        applied.append({**row, "rights_version": attestation.rights_version, "status": attestation.status})
        attested.append(code)
    return applied, sorted(attested), sorted(blocked)


def _contract_for_manifest(
    manifest: dict[str, Any],
    feeds: dict[str, dict[str, Any]],
):
    feed_code = feeds[manifest["feed_id"]]["code"]
    price_type = "raw" if "RAW" in feed_code else "adjusted"
    key = (price_type, manifest["data_layer"], manifest["resolution"])
    return DATASET_CONTRACTS.get(key)


def scoped_validation_incidents(
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    *,
    detected_at: datetime,
) -> list[tuple[str | None, QualityIncident]]:
    """Turn validation findings into scoped `market_data.quality_incidents` rows.

    Every finding this function accepts already carries the shard, the partition
    boundaries and, where the data knows one, the instrument -- `validate_catalog`
    attaches them from the `dataset_objects` row the finding came from.  Findings
    without that scope are *not* silently widened to the manifest: they are dropped
    here and stay in the report, because a manifest-wide incident with no reason is
    exactly the useless scope card D10 exists to remove.

    `CONTENT_HASH_MISMATCH` is built through `quality.content_hash_mismatch_incident`,
    which pins the failing object into `evidence_object_id`.  Previously this finding
    was skipped entirely -- it carried no `manifest_id`, so the guard dropped it and a
    checksum failure never reached the table at all.
    """

    incidents: list[tuple[str | None, QualityIncident]] = []
    for severity, findings in (("ERROR", errors), ("WARNING", warnings)):
        for finding in findings:
            manifest_id = finding.get("manifest_id")
            if finding["code"] == "CONTENT_HASH_MISMATCH":
                incidents.append(
                    (
                        manifest_id,
                        content_hash_mismatch_incident(
                            object_id=finding["object_id"],
                            object_key=finding["object_key"],
                            expected_content_hash=finding["expected_content_hash"],
                            actual_content_hash=finding["actual_content_hash"],
                            shard_key=finding["shard_key"],
                            partition_start=date.fromisoformat(finding["partition_start"]),
                            partition_end=date.fromisoformat(finding["partition_end"]),
                            period_start=datetime.fromisoformat(
                                str(finding["period_start"]).replace("Z", "+00:00")
                            ),
                            period_end=datetime.fromisoformat(
                                str(finding["period_end"]).replace("Z", "+00:00")
                            ),
                            detected_at=detected_at,
                        ),
                    )
                )
                continue
            if not finding.get("shard_key") or not finding.get("partition_start"):
                continue
            incidents.append(
                (
                    manifest_id,
                    incident_from_issue(
                        {**finding, "severity": finding.get("severity", severity)},
                        detected_at=detected_at,
                        evidence_object_id=finding.get("object_id"),
                    ),
                )
            )
    return incidents


def validate_catalog(
    catalog: MarketDataCatalog,
    object_store: LocalObjectStore,
    *,
    write_report: bool = True,
) -> dict[str, Any]:
    """Re-verify every published object and record what is wrong in the catalog.

    `catalog` is the shared `MarketDataCatalog` contract, not `LocalCatalog`.  The
    annotation used to name one implementation, which is the same coupling as an
    `isinstance` gate: D07/D08 could not validate a dataset that lived in PostgreSQL.

    Findings are *recorded*, not only reported.  A `CONTENT_HASH_MISMATCH` that exists
    solely in `validation-report.json` is invisible to every consumer of
    `market_data.quality_incidents`, which is the D10 defect spec section 1 names.
    """

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
            scope = {
                "shard_key": relation["shard_key"],
                "partition_start": relation["partition_start"],
                "partition_end": relation["partition_end"],
                "object_id": record["id"],
                "manifest_id": manifest["id"],
            }
            verification = object_store.verify(
                record["object_key"],
                record["content_hash"],
            )
            if not verification.ok:
                errors.append(
                    {
                        **scope,
                        "code": "CONTENT_HASH_MISMATCH",
                        "message": verification.message,
                        "object_key": record["object_key"],
                        "expected_content_hash": record["content_hash"],
                        "actual_content_hash": verification.content_hash,
                        "period_start": relation["period_start"],
                        "period_end": relation["period_end"],
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
                        **scope,
                        "code": "PARQUET_FOOTER_INVALID",
                        "message": str(exc),
                    }
                )
                continue
            if parquet.metadata.num_rows != relation["row_count"]:
                errors.append(
                    {
                        **scope,
                        "code": "ROW_COUNT_MISMATCH",
                        "message": (
                            f"Parquet footer {parquet.metadata.num_rows}행, "
                            f"dataset_objects {relation['row_count']}행"
                        ),
                    }
                )
            for issue in quality_issues(
                table,
                contract,
                partition_start=date.fromisoformat(relation["partition_start"]),
                partition_end=date.fromisoformat(relation["partition_end"]),
            ):
                target = errors if issue["severity"] == "ERROR" else warnings
                # `scope` first: the issue's own instrument/period/breadth win, and the
                # shard and partition it was found in are added, never overwritten.
                target.append({**scope, **issue})
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
    detected_at = datetime.now(timezone.utc)
    by_manifest: dict[str | None, list[QualityIncident]] = defaultdict(list)
    for manifest_id, incident in scoped_validation_incidents(errors, warnings, detected_at=detected_at):
        by_manifest[manifest_id].append(incident)
    recorded = sum(
        record_quality_incidents(catalog, group, dataset_manifest_id=manifest_id)
        for manifest_id, group in by_manifest.items()
    )

    report = {
        "status": "PASSED" if not errors else "FAILED",
        "checked_at": detected_at.isoformat(),
        "manifest_count": len(manifests),
        "object_count": len(storage),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "recorded_incident_count": recorded,
        "quality_incidents": [
            incident.to_report_entry(dataset_manifest_id=manifest_id)
            for manifest_id, group in by_manifest.items()
            for incident in group
        ],
    }
    if write_report:
        path = catalog.artifact_root / "validation-report.json"
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
    catalog: MarketDataCatalog,
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


#: Load order for the apply path: parents before children, so the deferrable foreign
#: keys in `V1__initial_schema.sql` are satisfied without deferring anything.
APPLY_TABLE_ORDER = (
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


def _referenced_instrument_ids(
    catalog: MarketDataCatalog,
    object_store: LocalObjectStore,
) -> set[str]:
    """Every `instrument_id` appearing in the catalog's Parquet objects."""

    instrument_ids: set[str] = set()
    for record in catalog.records("storage.objects"):
        path = object_store.path_for(record["object_key"])
        instrument_ids.update(
            pq.read_table(path, columns=["instrument_id"]).column("instrument_id").to_pylist()
        )
    return instrument_ids


def apply_catalog_to_postgres(
    catalog: LocalCatalog,
    object_store: LocalObjectStore,
    *,
    dbml_path: Path,
    execute: bool = False,
    database_url: str | None = None,
    rights_attestations: dict[str, RightsAttestation] | None = None,
    rights_attestations_path: Path | None = None,
    storage_objects: StorageObjectsPolicy = StorageObjectsPolicy.WRITE_D_OWNED,
) -> dict[str, Any]:
    """Validate the DBML contract and optionally apply it in one transaction.

    The apply itself goes through `PostgresCatalog`, the single SQLAlchemy Core writer;
    this function keeps only what it uniquely contributes -- the DBML column contract
    check, the global `dataset_hash` uniqueness check, the instrument existence check,
    and the rights gate.

    Rights
    ------
    `market_data.providers.rights_version` requires external approval evidence, and the
    pipeline stamps the placeholder ``UNVERIFIED`` because it cannot produce that
    evidence itself.  Rejecting the placeholder without any way to replace it made
    ``--execute`` unreachable for every plan this repository generates.  The gate is now
    a policy with a satisfiable input: supply `RightsAttestation`s here, or point
    ``$MARKET_DATA_RIGHTS_ATTESTATIONS`` at a JSON file.  Attested providers are written
    with their attested version; unattested ones block the apply by name.  The
    placeholder itself is never written.
    """

    tables = parse_dbml_columns(dbml_path)
    errors: list[str] = []
    operations: list[tuple[str, dict[str, Any]]] = []
    for table in APPLY_TABLE_ORDER:
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
    duplicate_hashes = {digest: ids for digest, ids in by_hash.items() if len(ids) > 1}
    if duplicate_hashes:
        errors.append("dataset_manifests.dataset_hash 전역 UNIQUE 충돌 가능성이 있습니다.")

    attestations = rights_attestations if rights_attestations is not None else load_rights_attestations(
        rights_attestations_path
    )
    resolved_providers, attested, blocked = rights_review_state(
        catalog.records("market_data.providers"), attestations
    )

    report: dict[str, Any] = {
        "status": "FAILED" if errors else "PASSED",
        "mode": "execute" if execute else "dry-run",
        "operation_count": len(operations),
        "contract_errors": errors,
        "duplicate_dataset_hashes": duplicate_hashes,
        "rights_attested_providers": attested,
        "rights_review_required_providers": blocked,
        "rights_attestations_env": RIGHTS_ATTESTATIONS_ENV,
    }
    if errors or not execute:
        return report

    if blocked:
        raise RightsVersionNotAttested(
            f"provider(s) {blocked} carry a placeholder rights_version. Supply a "
            f"RightsAttestation for each, or point ${RIGHTS_ATTESTATIONS_ENV} at a JSON "
            "array of attestations, before applying to PostgreSQL."
        )
    if not database_url:
        raise ValueError("--execute에는 DATABASE_URL이 필요합니다.")

    by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table, row in operations:
        by_table[table].append(row)
    by_table["market_data.providers"] = resolved_providers

    instrument_ids = _referenced_instrument_ids(catalog, object_store)
    target = PostgresCatalog.connect(
        database_url,
        artifact_root=catalog.artifact_root,
        storage_objects=storage_objects,
    )
    try:
        target.verify_schema()
        with target.transaction() as unit:
            if instrument_ids:
                _assert_instruments_exist(unit, instrument_ids)
            for table in APPLY_TABLE_ORDER:
                rows = by_table.get(table, [])
                for row in rows:
                    if table in {"market_data.dataset_lineage", "market_data.dataset_object_lineage"}:
                        unit.append_unique(table, row, tuple(row))
                    else:
                        unit.upsert(table, row)
    finally:
        target.close()
    return {**report, "status": "APPLIED", "applied_table_counts": {k: len(v) for k, v in by_table.items()}}


def _assert_instruments_exist(catalog: PostgresCatalog, instrument_ids: set[str]) -> None:
    """Refuse to apply objects that reference instruments the database does not have."""

    known = catalog.existing_ids("market_data.instruments", instrument_ids)
    missing = instrument_ids.difference(known)
    if missing:
        raise RuntimeError(f"DB에 없는 instrument_id: {sorted(missing)}")


def upload_catalog_objects(
    catalog: MarketDataCatalog,
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
    catalog: MarketDataCatalog,
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
