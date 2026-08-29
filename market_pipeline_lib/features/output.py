"""Immutable publication of one computed historical feature series.

This is the provider implementation for the isolated, unapproved
``feature-series.parquet.v1`` proposal.  It is opt-in: callers must inject the
publisher into :class:`FeatureMaterializer`, and the root contract must be approved
before that integration is treated as releasable.
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..contracts import deterministic_uuid
from ..storage import ObjectReceipt, ObjectStore
from .hashing import canonical_sha256, iso_utc
from .tables import FeatureCatalog

if TYPE_CHECKING:
    from .materialization import MaterializationRequest, MaterializationResult

FEATURE_SERIES_SCHEMA_VERSION = "feature-series.parquet.v1"
FEATURE_SERIES_SCHEMA = pa.schema(
    [
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value", pa.decimal128(38, 8), nullable=False),
    ]
)
FEATURE_OBJECT_LINEAGE_RELATION = "FEATURE_MATERIALIZED_FROM"


def _instant(value: object) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None:
        raise ValueError("feature manifest period must be a timezone-aware datetime")
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class PreparedFeatureOutput:
    receipt: ObjectReceipt
    storage: dict[str, Any]
    relation: dict[str, Any]
    building_manifest: dict[str, Any]
    available_manifest: dict[str, Any]


class FeatureOutputPublisher:
    """Write and register one exact, versioned feature-series object."""

    def __init__(
        self,
        catalog: FeatureCatalog,
        object_store: ObjectStore,
        *,
        staging_root: Path,
    ) -> None:
        self.catalog = catalog
        self.object_store = object_store
        self.staging_root = staging_root.expanduser().resolve()

    @staticmethod
    def _table(result: MaterializationResult) -> pa.Table:
        timestamps = [item.bar_start_at for item in result.values]
        if any(moment.tzinfo is None for moment in timestamps):
            raise ValueError("feature values must use timezone-aware bar_start_at")
        if any(current <= previous for previous, current in zip(timestamps, timestamps[1:], strict=False)):
            raise ValueError("feature values must be strictly increasing and unique by bar_start_at")
        return pa.Table.from_arrays(
            [
                pa.array(timestamps, type=FEATURE_SERIES_SCHEMA.field("bar_start_at").type),
                pa.array(
                    [item.value for item in result.values],
                    type=FEATURE_SERIES_SCHEMA.field("value").type,
                ),
            ],
            schema=FEATURE_SERIES_SCHEMA,
        )

    def prepare(
        self,
        request: MaterializationRequest,
        result: MaterializationResult,
    ) -> PreparedFeatureOutput:
        """Render, upload, and read back the exact returned object version."""

        available_scope = [
            row
            for row in self.catalog.records("market_data.dataset_manifests")
            if row.get("status") == "AVAILABLE"
            and str(row.get("feed_id")) == str(request.output_feed_id)
            and str(row.get("instrument_id")) == request.instrument_id
            and row.get("data_layer") == "DERIVED"
            and row.get("resolution") == request.definition.resolution
            and _instant(row["period_start"]).year
            == request.period_start.astimezone(UTC).year
        ]
        if any(
            _instant(row["period_start"]) != request.period_start.astimezone(UTC)
            or _instant(row["period_end"]) != request.period_end.astimezone(UTC)
            for row in available_scope
        ):
            raise ValueError(
                "an AVAILABLE feature output already owns this feed/instrument/resolution/year "
                "with a different period; publish a canonical period instead of superseding it"
            )

        table = self._table(result)
        work = self.staging_root / result.materialization_id
        path = work / "feature-series.parquet"
        work.mkdir(parents=True, exist_ok=True)
        receipt: ObjectReceipt | None = None
        try:
            pq.write_table(
                table,
                path,
                compression="zstd",
                use_dictionary=False,
                write_statistics=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=False,
                store_schema=True,
            )
            object_key = (
                f"features/schema={FEATURE_SERIES_SCHEMA_VERSION}/"
                f"definition={request.definition.id}/instrument={request.instrument_id}/"
                f"materialization={result.materialization_id}/"
                f"manifest_id={request.output_dataset_manifest_id}/feature-series.parquet"
            )
            receipt = self.object_store.put(path, object_key)
            with self.object_store.open_version(
                receipt.object_key, receipt.provider_version_id
            ) as stream:
                with tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024) as seekable:
                    shutil.copyfileobj(stream, seekable, length=1024 * 1024)
                    seekable.seek(0)
                    decoded = pq.read_table(seekable)
            if decoded.schema != FEATURE_SERIES_SCHEMA:
                raise ValueError(
                    f"published feature schema mismatch: {decoded.schema} != {FEATURE_SERIES_SCHEMA}"
                )
            if not result.verify_decoded_values(decoded):
                raise ValueError("decoded feature values do not reproduce result_hash")

            now = iso_utc(result.published_at)
            object_id = str(
                deterministic_uuid(
                    "storage-object",
                    receipt.storage_provider,
                    receipt.bucket_name,
                    receipt.object_key,
                    receipt.provider_version_id,
                )
            )
            relation_id = str(
                deterministic_uuid("dataset-object", request.output_dataset_manifest_id, object_id)
            )
            start_date = result.values[0].bar_start_at.date()
            end_date = max(start_date + timedelta(days=1), request.period_end.date() + timedelta(days=1))
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
                "compression_codec": "ZSTD",
                "media_type": "application/vnd.apache.parquet",
                "schema_version": FEATURE_SERIES_SCHEMA_VERSION,
                "row_count": result.row_count,
                "period_start": iso_utc(result.values[0].bar_start_at),
                "period_end": iso_utc(request.period_end),
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
                "dataset_manifest_id": request.output_dataset_manifest_id,
                "object_id": object_id,
                "object_kind": "FEATURE_SERIES",
                "partition_granularity": "DAY",
                "partition_start": start_date.isoformat(),
                "partition_end": end_date.isoformat(),
                "period_start": iso_utc(result.values[0].bar_start_at),
                "period_end": iso_utc(request.period_end),
                "shard_key": request.instrument_id,
                "part_number": 1,
                "row_count": result.row_count,
                "min_instrument_id": request.instrument_id,
                "max_instrument_id": request.instrument_id,
            }
            dataset_hash = canonical_sha256(
                {
                    "content_hash": receipt.content_hash,
                    "feature_definition_id": request.definition.id,
                    "instrument_id": request.instrument_id,
                    "materialization_id": result.materialization_id,
                    "object_kind": "FEATURE_SERIES",
                    "result_hash": result.result_hash,
                    "row_count": result.row_count,
                    "schema_version": FEATURE_SERIES_SCHEMA_VERSION,
                }
            )
            previous = max(
                available_scope,
                key=lambda row: int(row["revision_number"]),
                default=None,
            )
            common = {
                "id": request.output_dataset_manifest_id,
                "feed_id": request.output_feed_id,
                "instrument_id": request.instrument_id,
                "data_layer": "DERIVED",
                "resolution": request.definition.resolution,
                "revision_number": request.output_revision_number,
                "period_start": iso_utc(request.period_start),
                "period_end": iso_utc(request.period_end),
                "schema_version": FEATURE_SERIES_SCHEMA_VERSION,
                "supersedes_manifest_id": None if previous is None else previous["id"],
                "created_at": now,
            }
            building = {
                **common,
                "status": "BUILDING",
                "dataset_hash": canonical_sha256(
                    {"manifest_id": request.output_dataset_manifest_id, "status": "BUILDING"}
                ),
                "available_at": None,
            }
            available = {
                **common,
                "status": "AVAILABLE",
                "dataset_hash": dataset_hash,
                "available_at": now,
            }
            return PreparedFeatureOutput(receipt, storage, relation, building, available)
        except BaseException:
            if receipt is not None:
                self.object_store.delete(receipt)
            raise
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def register(
        self,
        request: MaterializationRequest,
        prepared: PreparedFeatureOutput,
    ) -> None:
        self.catalog.publish_manifest(prepared.building_manifest)
        self.catalog.stage_object(prepared.storage, prepared.relation)  # type: ignore[attr-defined]
        for source in sorted(request.sources, key=lambda item: item.dataset_object_id):
            self.catalog.record_object_lineage(  # type: ignore[attr-defined]
                {
                    "derived_dataset_object_id": prepared.relation["id"],
                    "source_dataset_object_id": source.dataset_object_id,
                    "pipeline_run_id": request.pipeline_run_id,
                    "relation_type": FEATURE_OBJECT_LINEAGE_RELATION,
                    "created_at": prepared.available_manifest["available_at"],
                }
            )
        for manifest_id in sorted({item.dataset_manifest_id for item in request.sources}):
            self.catalog.record_dataset_lineage(
                {
                    "derived_manifest_id": request.output_dataset_manifest_id,
                    "source_manifest_id": manifest_id,
                    "relation_type": FEATURE_OBJECT_LINEAGE_RELATION,
                }
            )
        supersedes = prepared.available_manifest.get("supersedes_manifest_id")
        if supersedes is not None:
            previous_rows = self.catalog.records(
                "market_data.dataset_manifests", where={"id": str(supersedes)}
            )
            if len(previous_rows) != 1 or previous_rows[0].get("status") != "AVAILABLE":
                raise ValueError("feature output supersession target is not uniquely AVAILABLE")
            self.catalog.publish_manifest({**previous_rows[0], "status": "SUPERSEDED"})
        self.catalog.publish_manifest(prepared.available_manifest)

    def cleanup(self, prepared: PreparedFeatureOutput) -> None:
        self.object_store.delete(prepared.receipt)
