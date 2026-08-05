"""Computing a feature over a dataset partition (`market_data.feature_materializations`).

A materialization is the answer to "what did *this* definition produce over *these*
inputs for *this* instrument and period", and its identity says all four things:

``input_dataset_set_hash``
    A fingerprint of the source objects the computation read.  This is the same value
    the COM06 backtest request calls ``input_bundle_fingerprint``: the producer and the
    consumer are naming one fact, and `input_bundle_fingerprint` below is the single
    place it is computed.

``result_hash``
    A fingerprint of the quantized output values together with the four identity facts.
    Recomputing the same definition over the same inputs produces the same bytes and
    therefore the same hash -- that is what makes the result *pinnable* rather than
    merely repeatable.

``feature_materialization_version``
    The short string a downstream consumer pins; see `hashing`.  It exists only for a
    ``SUCCEEDED`` row.

Failure is recorded, not swallowed.  A period with too little history to satisfy the
definition's warm-up is a real event in a market data pipeline, and it lands as a
``FAILED`` row with no `result_hash` before the exception propagates, so the gap is
visible in the catalog rather than only in a log line.

Canonical constraints honoured here
-----------------------------------
* ``uq_feature_materializations_definition_instrument_inputs_period`` -- the row id is
  derived from exactly those five values, so re-running is an idempotent overwrite of
  the same row rather than a duplicate.
* ``feature_materializations.pipeline_run_id`` is ``UNIQUE`` -- one pipeline run
  produces at most one materialization.  Checked before the write so both catalogs
  behave the same, and translated from the database's own violation as well.
* ``feature_materialization_success_complete`` -- ``status = 'SUCCEEDED'`` requires
  `output_dataset_manifest_id`, `result_hash` **and** `available_at`.  This is a CHECK
  in the applied DDL (`V1__initial_schema.sql:1084`) that the SQLAlchemy metadata does
  not restate, and it is why `output_dataset_manifest_id` is a required field of
  `MaterializationRequest` rather than an optional extra: values computed into nowhere
  are not a successful materialization, they are a leak.
* ``feature_materialization_period_order`` -- ``period_end > period_start``.
* ``feature_definitions.element_catalog_version_id`` really is a foreign key to
  `strategy.element_catalog_versions` in the applied DDL, so a definition cannot be
  published against a catalog version that does not exist.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from ..contracts import deterministic_uuid
from .calculators import BarPoint, FeatureValue, render
from .definitions import FeatureDefinition, FeatureDefinitionRegistry
from .errors import (
    FeatureDefinitionNotPublished,
    InvalidBarSeries,
    MaterializationConflict,
)
from .hashing import canonical_json, canonical_sha256, is_sha256_hex, iso_utc, materialization_version
from .output import FeatureOutputPublisher, PreparedFeatureOutput
from .tables import FEATURE_MATERIALIZATIONS, FeatureCatalog

__all__ = [
    "MATERIALIZATION_RESULT_SCHEMA_VERSION",
    "FeatureMaterializer",
    "MaterializationRequest",
    "MaterializationResult",
    "SourceObject",
    "input_bundle_fingerprint",
]


#: Bump when a field enters or leaves the hashed result payload.
MATERIALIZATION_RESULT_SCHEMA_VERSION = 1
INPUT_BUNDLE_SCHEMA_VERSION = 1

_UUID_PURPOSE = "feature-materialization"

#: `market_data.dataset_lineage.relation_type` for "this feature dataset was computed
#: from that market dataset".
FEATURE_LINEAGE_RELATION = "FEATURE_MATERIALIZED_FROM"


def _require_uuid(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a UUID string, got {type(value).__name__}")
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise ValueError(f"{label}={value!r} is not a UUID") from exc


@dataclass(frozen=True)
class SourceObject:
    """One `market_data.dataset_objects` row a materialization read."""

    dataset_object_id: str
    dataset_manifest_id: str
    content_hash: str
    partition_start: str
    partition_end: str
    row_count: int

    def __post_init__(self) -> None:
        _require_uuid(self.dataset_object_id, "dataset_object_id")
        _require_uuid(self.dataset_manifest_id, "dataset_manifest_id")
        if not is_sha256_hex(self.content_hash):
            raise ValueError(f"content_hash must be 64 lowercase hex characters, got {self.content_hash!r}")
        if not isinstance(self.row_count, int) or isinstance(self.row_count, bool) or self.row_count < 0:
            raise ValueError(f"row_count must be a non-negative integer, got {self.row_count!r}")
        if self.partition_end <= self.partition_start:
            raise ValueError(
                f"partition_end {self.partition_end!r} must be after partition_start {self.partition_start!r}"
            )

    def fingerprint_entry(self) -> dict[str, Any]:
        return {
            "content_hash": self.content_hash,
            "dataset_manifest_id": self.dataset_manifest_id,
            "dataset_object_id": self.dataset_object_id,
            "partition_end": self.partition_end,
            "partition_start": self.partition_start,
            "row_count": self.row_count,
        }


def input_bundle_fingerprint(sources: Iterable[SourceObject]) -> str:
    """The COM06 `input_bundle_fingerprint` / `input_dataset_set_hash`.

    Order-independent by construction: the entries are sorted by their own canonical
    rendering, so the sequence a planner happened to enumerate partitions in is not part
    of the identity of the bundle.  An empty bundle is refused -- "computed over
    nothing" is a bug, and a fingerprint for it would be a hash every empty run shares.
    """

    entries = [item.fingerprint_entry() for item in sources]
    if not entries:
        raise ValueError("an input bundle must contain at least one source object")
    identifiers = [entry["dataset_object_id"] for entry in entries]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("an input bundle must not list the same dataset object twice")
    entries.sort(key=canonical_json)
    return canonical_sha256(
        {"bundle_schema_version": INPUT_BUNDLE_SCHEMA_VERSION, "objects": entries}
    )


@dataclass(frozen=True)
class MaterializationRequest:
    """Everything one materialization needs, validated before anything is written."""

    definition: FeatureDefinition
    instrument_id: str
    pipeline_run_id: str
    sources: tuple[SourceObject, ...]
    bars: tuple[BarPoint, ...]
    period_start: datetime
    period_end: datetime
    source_watermark: str
    #: Required: `feature_materialization_success_complete` refuses a SUCCEEDED row
    #: without one, so the caller publishes the output dataset first.
    output_dataset_manifest_id: str
    #: Required only for immutable feature-series publication.  It is explicit because
    #: the canonical model has no feature-definition-to-feed mapping to infer safely.
    output_feed_id: str | None = None
    output_revision_number: int | None = None

    def validate(self) -> None:
        _require_uuid(self.instrument_id, "instrument_id")
        _require_uuid(self.pipeline_run_id, "pipeline_run_id")
        if self.output_dataset_manifest_id is None:
            raise ValueError(
                "output_dataset_manifest_id is required: the canonical CHECK "
                "feature_materialization_success_complete refuses a SUCCEEDED "
                "materialization that does not name the dataset its values were written "
                "to. Publish the output manifest first, then materialize into it."
            )
        _require_uuid(self.output_dataset_manifest_id, "output_dataset_manifest_id")
        if not isinstance(self.source_watermark, str) or not self.source_watermark.strip():
            raise ValueError("source_watermark must be a non-empty string")
        if len(self.source_watermark) > 300:
            raise ValueError("source_watermark exceeds the canonical 300 characters")
        if self.period_end <= self.period_start:
            raise InvalidBarSeries(
                f"period_end {self.period_end!r} must be after period_start {self.period_start!r}"
            )
        self._validate_bars()

    def _validate_bars(self) -> None:
        if not self.bars:
            raise InvalidBarSeries("a materialization needs at least one bar")
        # Timezone first: comparing a naive to an aware datetime raises TypeError, which
        # would surface as an unrelated failure instead of naming the real problem.
        for index, bar in enumerate(self.bars):
            if bar.bar_start_at.tzinfo is None:
                raise InvalidBarSeries(
                    f"bars[{index}].bar_start_at is naive; this pipeline works in ET and UTC "
                    "at once, so a timestamp without a zone is ambiguous"
                )
            if not isinstance(bar.close, Decimal):
                raise InvalidBarSeries(
                    f"bars[{index}].close must be a Decimal, got {type(bar.close).__name__}; "
                    "binary floats do not reproduce a result hash"
                )
        for index in range(1, len(self.bars)):
            previous = self.bars[index - 1].bar_start_at
            current = self.bars[index].bar_start_at
            if current <= previous:
                raise InvalidBarSeries(
                    f"bars must be strictly increasing in bar_start_at: bars[{index}] "
                    f"({current.isoformat()}) does not follow bars[{index - 1}] ({previous.isoformat()})"
                )
        for index, bar in enumerate(self.bars):
            if not (self.period_start <= bar.bar_start_at < self.period_end):
                raise InvalidBarSeries(
                    f"bars[{index}] at {bar.bar_start_at.isoformat()} is outside "
                    f"[{self.period_start.isoformat()}, {self.period_end.isoformat()})"
                )

    def validate_output_publication(self) -> None:
        if self.output_feed_id is None:
            raise ValueError("output_feed_id is required for feature-series publication")
        _require_uuid(self.output_feed_id, "output_feed_id")
        if (
            not isinstance(self.output_revision_number, int)
            or isinstance(self.output_revision_number, bool)
            or self.output_revision_number < 1
        ):
            raise ValueError("output_revision_number must be a positive integer")

    @property
    def input_dataset_set_hash(self) -> str:
        return input_bundle_fingerprint(self.sources)

    @property
    def materialization_id(self) -> str:
        """UUID5 over exactly the canonical uniqueness key."""

        return str(
            deterministic_uuid(
                _UUID_PURPOSE,
                self.definition.definition_hash,
                self.instrument_id,
                self.input_dataset_set_hash,
                iso_utc(self.period_start),
                iso_utc(self.period_end),
            )
        )


@dataclass(frozen=True)
class MaterializationResult:
    """The outcome of one materialization, and the version a consumer pins."""

    materialization_id: str
    definition_hash: str
    instrument_id: str
    pipeline_run_id: str
    input_dataset_set_hash: str
    period_start: datetime
    period_end: datetime
    values: tuple[FeatureValue, ...]
    result_hash: str
    status: str = field(default="SUCCEEDED")
    published_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    output_content_hash: str | None = None
    output_provider_version_id: str | None = None

    @property
    def row_count(self) -> int:
        return len(self.values)

    @property
    def feature_materialization_version(self) -> str:
        if self.status != "SUCCEEDED":
            raise ValueError(
                f"materialization {self.materialization_id} is {self.status}; only a SUCCEEDED "
                "materialization has a version a consumer may pin"
            )
        return materialization_version(
            definition_hash=self.definition_hash,
            input_dataset_set_hash=self.input_dataset_set_hash,
            result_hash=self.result_hash,
        )

    def serialized_rows(self) -> str:
        """The exact bytes the value rows contribute to `result_hash`."""

        return canonical_json(_rows_payload(self.values))

    def verify_decoded_values(self, table: Any) -> bool:
        """Recompute ``result_hash`` from decoded Parquet rows, not original memory."""

        try:
            timestamps = table.column("bar_start_at").to_pylist()
            values = table.column("value").to_pylist()
            decoded = tuple(
                FeatureValue(bar_start_at=moment, value=value)
                for moment, value in zip(timestamps, values, strict=True)
            )
        except (KeyError, TypeError, ValueError):
            return False
        return (
            _result_hash(
                definition_hash=self.definition_hash,
                instrument_id=self.instrument_id,
                input_dataset_set_hash=self.input_dataset_set_hash,
                period_start=self.period_start,
                period_end=self.period_end,
                values=decoded,
            )
            == self.result_hash
        )


def _rows_payload(values: Sequence[FeatureValue]) -> list[dict[str, str]]:
    # `render`, not `str`: see the note on negative and scientific-notation zeros there.
    return [{"at": iso_utc(item.bar_start_at), "value": render(item.value)} for item in values]


def _result_hash(
    *,
    definition_hash: str,
    instrument_id: str,
    input_dataset_set_hash: str,
    period_start: datetime,
    period_end: datetime,
    values: Sequence[FeatureValue],
) -> str:
    return canonical_sha256(
        {
            "definition_hash": definition_hash,
            "input_dataset_set_hash": input_dataset_set_hash,
            "instrument_id": instrument_id,
            "period_end": iso_utc(period_end),
            "period_start": iso_utc(period_start),
            "result_schema_version": MATERIALIZATION_RESULT_SCHEMA_VERSION,
            "rows": _rows_payload(values),
        }
    )


class FeatureMaterializer:
    """Runs one materialization and records it in the catalog."""

    def __init__(
        self,
        catalog: FeatureCatalog,
        registry: FeatureDefinitionRegistry,
        *,
        output_publisher: FeatureOutputPublisher | None = None,
    ) -> None:
        self._catalog = catalog
        self._registry = registry
        self._output_publisher = output_publisher

    def materialize(self, request: MaterializationRequest) -> MaterializationResult:
        """Compute, persist and return one materialization.

        Order matters and is deliberate:

        1. validate the request -- a malformed request never becomes catalog state;
        2. check the definition is published -- a materialization citing an
           unpublished definition would have no readable provenance;
        3. check the run is free -- so both catalogs report the canonical uniqueness
           rule the same way;
        4. write ``RUNNING``, compute, then write ``SUCCEEDED`` or ``FAILED``.
        """

        request.validate()
        if self._output_publisher is not None:
            request.validate_output_publication()
        if not self._registry.is_published(request.definition):
            raise FeatureDefinitionNotPublished(
                f"feature definition {request.definition.feature_definition_version} "
                f"(hash {request.definition.definition_hash}) is not published; publish it "
                "before materializing, so the result has a definition to point at"
            )
        materialization_id = request.materialization_id
        self._assert_run_is_free(materialization_id, request)

        previous_success = next(
            (
                row
                for row in self._catalog.records(FEATURE_MATERIALIZATIONS)
                if str(row.get("id")) == materialization_id and row.get("status") == "SUCCEEDED"
            ),
            None,
        )
        if previous_success is None:
            self._write(request, materialization_id, status="RUNNING", result_hash=None)
        try:
            values = request.definition.calculator().compute(
                request.bars, request.definition.normalized_parameters
            )
        except Exception:
            # Including `InsufficientHistory`, which is a data outcome rather than a
            # programming error: the FAILED row is how the gap becomes visible.
            self._write(request, materialization_id, status="FAILED", result_hash=None)
            raise

        digest = _result_hash(
            definition_hash=request.definition.definition_hash,
            instrument_id=request.instrument_id,
            input_dataset_set_hash=request.input_dataset_set_hash,
            period_start=request.period_start,
            period_end=request.period_end,
            values=values,
        )
        result = MaterializationResult(
            materialization_id=materialization_id,
            definition_hash=request.definition.definition_hash,
            instrument_id=request.instrument_id,
            pipeline_run_id=request.pipeline_run_id,
            input_dataset_set_hash=request.input_dataset_set_hash,
            period_start=request.period_start,
            period_end=request.period_end,
            values=values,
            result_hash=digest,
            status="SUCCEEDED",
        )
        if previous_success is not None and previous_success.get("result_hash") != digest:
            raise MaterializationConflict(
                f"successful materialization {materialization_id} recomputed a different result_hash"
            )
        if self._output_publisher is None:
            if previous_success is None:
                self._write(request, materialization_id, status="SUCCEEDED", result_hash=digest)
                self._record_lineage(request)
            return result

        prepared = None
        try:
            prepared = self._output_publisher.prepare(request, result)
            if previous_success is not None:
                self._assert_existing_publication(request, prepared)
                return replace(
                    result,
                    output_content_hash=prepared.receipt.content_hash,
                    output_provider_version_id=prepared.receipt.provider_version_id,
                )
            with self._catalog.transaction():
                self._output_publisher.register(request, prepared)
                self._write(request, materialization_id, status="SUCCEEDED", result_hash=digest)
        except Exception:
            if prepared is not None:
                self._output_publisher.cleanup(prepared)
            if previous_success is None:
                self._write(request, materialization_id, status="FAILED", result_hash=None)
            raise
        return replace(
            result,
            output_content_hash=prepared.receipt.content_hash,
            output_provider_version_id=prepared.receipt.provider_version_id,
        )

    def _assert_existing_publication(
        self, request: MaterializationRequest, prepared: PreparedFeatureOutput
    ) -> None:
        manifests = [
            row
            for row in self._catalog.records("market_data.dataset_manifests")
            if row.get("id") == request.output_dataset_manifest_id
            and row.get("status") == "AVAILABLE"
        ]
        objects = [
            row
            for row in self._catalog.records("storage.objects")
            if row.get("object_key") == prepared.receipt.object_key
            and row.get("provider_version_id") == prepared.receipt.provider_version_id
            and row.get("content_hash") == prepared.receipt.content_hash
        ]
        relations = [
            row
            for row in self._catalog.records("market_data.dataset_objects")
            if row.get("dataset_manifest_id") == request.output_dataset_manifest_id
            and row.get("object_id") == prepared.storage["id"]
        ]
        if len(manifests) != 1 or len(objects) != 1 or len(relations) != 1:
            raise MaterializationConflict(
                f"successful materialization {request.materialization_id} has incomplete version-pinned output"
            )

    # -- internals ---------------------------------------------------------------------

    def _assert_run_is_free(self, materialization_id: str, request: MaterializationRequest) -> None:
        """Enforce the two canonical UNIQUE columns before writing.

        Checked in application code as well as by the database so `LocalCatalog` and
        `PostgresCatalog` refuse the same things -- the whole point of the shared
        catalog contract is that a pipeline that works on one works on the other.
        """

        for row in self._catalog.records(FEATURE_MATERIALIZATIONS):
            if str(row.get("id")) == materialization_id:
                continue
            if str(row.get("pipeline_run_id")) == request.pipeline_run_id:
                raise MaterializationConflict(
                    f"pipeline run {request.pipeline_run_id} already carries materialization "
                    f"{row.get('id')}; feature_materializations.pipeline_run_id is UNIQUE, so "
                    "each materialization needs its own run"
                )
            if row.get("output_dataset_manifest_id") == request.output_dataset_manifest_id:
                raise MaterializationConflict(
                    f"output manifest {request.output_dataset_manifest_id} already belongs to "
                    f"materialization {row.get('id')}; "
                    "uq_feature_materializations_output_manifest is UNIQUE, so each "
                    "materialization writes its own output dataset"
                )

    def _write(
        self,
        request: MaterializationRequest,
        materialization_id: str,
        *,
        status: str,
        result_hash: str | None,
    ) -> None:
        now = datetime.now(UTC)
        record: dict[str, Any] = {
            "id": materialization_id,
            "feature_definition_id": request.definition.id,
            "instrument_id": request.instrument_id,
            "pipeline_run_id": request.pipeline_run_id,
            "input_dataset_set_hash": request.input_dataset_set_hash,
            "period_start": iso_utc(request.period_start),
            "period_end": iso_utc(request.period_end),
            "source_watermark": request.source_watermark,
            "output_dataset_manifest_id": (
                request.output_dataset_manifest_id if status == "SUCCEEDED" else None
            ),
            "result_hash": result_hash,
            "status": status,
            "available_at": iso_utc(now) if status == "SUCCEEDED" else None,
            "created_at": iso_utc(now),
        }
        try:
            self._catalog.upsert(FEATURE_MATERIALIZATIONS, record)
        except IntegrityError as exc:  # pragma: no cover - the pre-check normally wins
            raise MaterializationConflict(
                f"writing materialization {materialization_id} violated a canonical "
                f"uniqueness rule on {FEATURE_MATERIALIZATIONS}: {exc.orig}"
            ) from exc

    def _record_lineage(self, request: MaterializationRequest) -> None:
        """Point the output dataset at every manifest it was computed from."""

        derived = request.output_dataset_manifest_id
        for manifest_id in sorted({item.dataset_manifest_id for item in request.sources}):
            self._catalog.record_dataset_lineage(
                {
                    "derived_manifest_id": derived,
                    "source_manifest_id": manifest_id,
                    "relation_type": FEATURE_LINEAGE_RELATION,
                }
            )


def materialization_record_version(row: Mapping[str, Any], definition_hash: str) -> str:
    """The version string for a stored `feature_materializations` row."""

    if str(row.get("status")) != "SUCCEEDED":
        raise ValueError(
            f"materialization {row.get('id')} is {row.get('status')}; only a SUCCEEDED row has a "
            "version a consumer may pin"
        )
    return materialization_version(
        definition_hash=definition_hash,
        input_dataset_set_hash=str(row["input_dataset_set_hash"]),
        result_hash=str(row["result_hash"]),
    )
