"""One contract suite, run against every `MarketDataCatalog` implementation.

`engine.py` carries fourteen `isinstance(self.catalog, LocalCatalog)` gates because
`PostgresCatalog` was never behaviourally equal to `LocalCatalog`.  Deleting those
gates is only safe if the two implementations are interchangeable, so the guarantee is
written here once and parameterised, rather than asserted twice and allowed to drift.

Every test in this module runs against both catalogs.  The PostgreSQL parameter is
marked `integration` and skips without Docker, so `-m "not integration"` still exercises
the whole contract against `LocalCatalog`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from market_pipeline_lib.catalog import (
    CatalogCapability,
    LocalCatalog,
    MarketDataCatalog,
)
from market_pipeline_lib.contracts import canonical_dataset_hash
from market_pipeline_lib.db.engine import check_statement
from market_pipeline_lib.db.errors import (
    DuplicateAvailableManifest,
    RuntimeDdlForbidden,
    SchemaWriteForbidden,
    UnknownCatalogColumn,
    UnknownCatalogTable,
    UnsupportedCatalogCapability,
)

pytestmark = pytest.mark.usefixtures("_catalog_isolation")


# --------------------------------------------------------------------------------------
# Canonical row builders.  Deterministic literals, never values derived from production
# code, so a test cannot pass by re-implementing the thing it checks.
# --------------------------------------------------------------------------------------

PROVIDER_ID = "11111111-1111-4111-8111-111111111111"
FEED_ID = "22222222-2222-4222-8222-222222222222"
RUN_ID = "33333333-3333-4333-8333-333333333333"
MANIFEST_A = "44444444-4444-4444-8444-444444444444"
MANIFEST_B = "55555555-5555-4555-8555-555555555555"
OBJECT_A = "66666666-6666-4666-8666-666666666666"
OBJECT_B = "77777777-7777-4777-8777-777777777777"
RELATION_A = "88888888-8888-4888-8888-888888888888"
RELATION_B = "99999999-9999-4999-8999-999999999999"
INCIDENT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
INSTRUMENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
ACTION_A = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
ACTION_B = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64

#: `canonical_dataset_hash` over the two objects `_stage_two_objects` writes, as read
#: back through `objects_for_manifest`.  A literal, so a catalog cannot pass this by
#: agreeing with itself; see `test_carried_forward_objects_hash_to_one_pinned_digest`.
PINNED_CARRY_FORWARD_HASH = "eb4615a9bec56f69e2756f88d6687f30cfdd008559206eba298e56237be074db"


def provider_row() -> dict[str, Any]:
    return {
        "id": PROVIDER_ID,
        "code": "CONTRACT_TEST",
        "display_name": "Contract Test",
        "rights_version": "1.0.0",
        "status": "ACTIVE",
        "created_at": "2026-01-02T00:00:00Z",
    }


def feed_row() -> dict[str, Any]:
    return {
        "id": FEED_ID,
        "provider_id": PROVIDER_ID,
        "code": "CONTRACT_TEST_30M",
        "data_kind": "BARS",
        "resolution": "30m",
        "timezone_name": "America/New_York",
        "feed_version": "contract-v1",
        "created_at": "2026-01-02T00:00:00Z",
        "retired_at": None,
    }


def instrument_row() -> dict[str, Any]:
    """`corporate_actions.instrument_id` is a foreign key, so this has to exist first."""

    return {
        "id": INSTRUMENT_ID,
        "asset_type": "STOCK",
        "primary_exchange_mic": "XNAS",
        "currency_code": "USD",
        "provider_reference": None,
        "listed_at": None,
        "delisted_at": None,
        "created_at": "2026-01-02T00:00:00Z",
    }


def run_row(run_id: str = RUN_ID, *, status: str = "RUNNING") -> dict[str, Any]:
    return {
        "id": run_id,
        "pipeline_code": "CONTRACT_TEST",
        "pipeline_version": "market-bars-v2",
        "idempotency_key": f"contract-test:{run_id}",
        "status": status,
        "input_hash": HASH_A,
        "output_hash": None,
        "started_at": "2026-01-02T00:00:00Z",
        "completed_at": None,
        "failure_code": None,
    }


def manifest_row(
    manifest_id: str,
    *,
    revision: int,
    status: str,
    dataset_hash: str,
    supersedes: str | None = None,
    period_start: str = "2026-01-01T05:00:00Z",
    period_end: str = "2027-01-01T05:00:00Z",
) -> dict[str, Any]:
    return {
        "id": manifest_id,
        "feed_id": FEED_ID,
        "instrument_id": None,
        "data_layer": "RAW",
        "resolution": "30m",
        "revision_number": revision,
        "status": status,
        "period_start": period_start,
        "period_end": period_end,
        "schema_version": "market-bars-v2",
        "dataset_hash": dataset_hash,
        "supersedes_manifest_id": supersedes,
        "created_at": "2026-01-02T00:00:00Z",
        "available_at": "2026-01-02T00:00:00Z" if status == "AVAILABLE" else None,
    }


def storage_row(object_id: str, *, content_hash: str, byte_size: int = 4096) -> dict[str, Any]:
    return {
        "id": object_id,
        "status": "AVAILABLE",
        "storage_provider": "LOCAL",
        "bucket_name": "contract-test",
        "object_key": f"market-data/contract/{object_id}.parquet",
        "provider_version_id": "v1",
        "content_hash": content_hash,
        "byte_size": byte_size,
        "file_format": "PARQUET",
        "compression_codec": "UNCOMPRESSED",
        "media_type": "application/vnd.apache.parquet",
        "schema_version": "market-bars-v2",
        "row_count": 10,
        "period_start": "2026-01-05T14:30:00Z",
        "period_end": "2026-01-05T21:00:00Z",
        "encryption_key_ref": None,
        "retention_policy_version": "UNSPECIFIED",
        "retention_until": None,
        "legal_hold": False,
        "created_at": "2026-01-02T00:00:00Z",
        "verified_at": "2026-01-02T00:00:00Z",
        "quarantined_at": None,
        "superseded_at": None,
        "deleted_at": None,
    }


def relation_row(
    relation_id: str,
    *,
    manifest_id: str,
    object_id: str,
    partition_start: str = "2026-01-05",
    partition_end: str = "2026-01-06",
    part_number: int = 1,
) -> dict[str, Any]:
    return {
        "id": relation_id,
        "dataset_manifest_id": manifest_id,
        "object_id": object_id,
        "object_kind": "MARKET_BARS",
        "partition_granularity": "DAY",
        "partition_start": partition_start,
        "partition_end": partition_end,
        "period_start": "2026-01-05T14:30:00Z",
        "period_end": "2026-01-05T21:00:00Z",
        "shard_key": "s00-of-04",
        "part_number": part_number,
        "row_count": 10,
        "min_instrument_id": None,
        "max_instrument_id": None,
    }


def incident_row(manifest_id: str) -> dict[str, Any]:
    return {
        "id": INCIDENT_ID,
        "dataset_manifest_id": manifest_id,
        "instrument_id": None,
        "severity": "ERROR",
        "incident_code": "CONTENT_HASH_MISMATCH",
        "period_start": "2026-01-05T14:30:00Z",
        "period_end": "2026-01-05T21:00:00Z",
        "status": "ACTIVE",
        "evidence_object_id": None,
        "detected_at": "2026-01-06T00:00:00Z",
        "resolved_at": None,
    }


def seed_reference_data(catalog: MarketDataCatalog) -> None:
    """Provider, feed and pipeline run: the foreign keys everything else needs."""

    catalog.upsert("market_data.providers", provider_row())
    catalog.upsert("market_data.feeds", feed_row())
    catalog.begin_pipeline_run(run_row())


# --------------------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------------------


def test_records_of_an_untouched_table_is_empty(catalog: MarketDataCatalog) -> None:
    assert catalog.records("market_data.dataset_manifests") == []


def test_records_rejects_a_table_outside_the_catalog_contract(catalog: MarketDataCatalog) -> None:
    with pytest.raises(UnknownCatalogTable):
        catalog.records("market_data.not_a_table")


def test_upsert_rejects_a_column_that_is_not_in_the_canonical_schema(
    catalog: MarketDataCatalog,
) -> None:
    with pytest.raises(UnknownCatalogColumn):
        catalog.upsert("market_data.providers", {**provider_row(), "invented_column": 1})


def test_round_trip_preserves_every_canonical_value(catalog: MarketDataCatalog) -> None:
    """The exact literal written must come back, not a re-serialised approximation."""

    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )

    stored = {row["id"]: row for row in catalog.records("storage.objects")}[OBJECT_A]
    assert stored == storage_row(OBJECT_A, content_hash=HASH_B)

    relation = {row["id"]: row for row in catalog.records("market_data.dataset_objects")}[RELATION_A]
    assert relation == relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A)


def test_bigint_columns_round_trip_at_the_postgres_limit(catalog: MarketDataCatalog) -> None:
    """`byte_size` and `row_count` are bigint; a 2^53 value must survive intact.

    market_data owns no `numeric` column, so bigint is where this pipeline can actually
    lose precision -- a JSON/float round trip would silently corrupt these two.
    """

    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    huge = 9_007_199_254_740_993  # 2**53 + 1: not representable as a float64
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B, byte_size=huge),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )

    stored = {row["id"]: row for row in catalog.records("storage.objects")}[OBJECT_A]
    assert stored["byte_size"] == huge
    assert isinstance(stored["byte_size"], int)


def test_timestamps_are_normalised_to_one_canonical_rendering(catalog: MarketDataCatalog) -> None:
    """`+00:00` in, `Z` out -- identically on both catalogs.

    `engine.publish_dataset` recomputes `dataset_hash` from carried-forward relation
    rows, so two catalogs that render the same instant differently would produce two
    different manifest hashes for the same data.
    """

    seed_reference_data(catalog)
    catalog.publish_manifest(
        manifest_row(
            MANIFEST_A,
            revision=1,
            status="BUILDING",
            dataset_hash=HASH_A,
            period_start="2026-01-01T05:00:00+00:00",
        )
    )

    stored = catalog.records("market_data.dataset_manifests")[0]
    assert stored["period_start"] == "2026-01-01T05:00:00Z"


def test_objects_for_manifest_joins_storage_rows(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )

    joined = catalog.objects_for_manifest(MANIFEST_A)

    assert len(joined) == 1
    assert joined[0]["id"] == RELATION_A
    assert joined[0]["storage"]["object_key"] == f"market-data/contract/{OBJECT_A}.parquet"
    assert joined[0]["storage"]["content_hash"] == HASH_B


def test_objects_for_manifest_of_an_unknown_manifest_is_empty(catalog: MarketDataCatalog) -> None:
    assert catalog.objects_for_manifest(MANIFEST_A) == []


def _stage_two_objects(catalog: MarketDataCatalog) -> None:
    """Two objects staged in *descending* id order, so insertion order is not id order."""

    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.stage_object(
        storage_row(OBJECT_B, content_hash=HASH_C),
        relation_row(
            RELATION_B,
            manifest_id=MANIFEST_A,
            object_id=OBJECT_B,
            partition_start="2026-01-06",
            partition_end="2026-01-07",
            part_number=2,
        ),
    )
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )


def test_objects_for_manifest_is_ordered_by_id_on_every_catalog(catalog: MarketDataCatalog) -> None:
    """Carry-forward reads this list; a different order per catalog is a different dataset.

    `engine.publish_dataset` carries the previous manifest's objects forward and
    recomputes the manifest hash and the observed period from them, so the two
    implementations have to agree on the sequence, not merely on the set.
    """

    _stage_two_objects(catalog)

    joined = catalog.objects_for_manifest(MANIFEST_A)

    assert [item["id"] for item in joined] == [RELATION_A, RELATION_B]
    assert [item["storage"]["id"] for item in joined] == [OBJECT_A, OBJECT_B]


def test_carried_forward_objects_hash_to_one_pinned_digest(catalog: MarketDataCatalog) -> None:
    """The manifest identity computed from a carried-forward read is fixed.

    The expected value is a literal, not a second call to `canonical_dataset_hash`: a
    catalog that rendered a timestamp, a bigint or a date differently would produce a
    different manifest hash for the same data and this pins which rendering is correct.
    """

    _stage_two_objects(catalog)

    canonical = [
        {
            "content_hash": item["storage"]["content_hash"],
            "object_kind": item["object_kind"],
            "partition_granularity": item["partition_granularity"],
            "partition_start": item["partition_start"],
            "partition_end": item["partition_end"],
            "period_start": item["period_start"],
            "period_end": item["period_end"],
            "shard_key": item["shard_key"],
            "part_number": item["part_number"],
            "row_count": item["row_count"],
            "schema_version": item["storage"]["schema_version"],
        }
        for item in catalog.objects_for_manifest(MANIFEST_A)
    ]

    assert canonical_dataset_hash(canonical) == PINNED_CARRY_FORWARD_HASH


def test_latest_available_manifest_picks_the_highest_revision(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    with catalog.transaction():
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="SUPERSEDED", dataset_hash=HASH_A))
        catalog.publish_manifest(manifest_row(MANIFEST_B, revision=2, status="AVAILABLE", dataset_hash=HASH_B))

    latest = catalog.latest_available_manifest(
        feed_id=FEED_ID,
        data_layer="RAW",
        resolution="30m",
        year=2026,
    )

    assert latest is not None
    assert latest["id"] == MANIFEST_B
    assert latest["revision_number"] == 2


def test_latest_available_manifest_ignores_other_years(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(
        manifest_row(
            MANIFEST_A,
            revision=1,
            status="AVAILABLE",
            dataset_hash=HASH_A,
            period_start="2025-01-01T05:00:00Z",
            period_end="2026-01-01T05:00:00Z",
        )
    )

    assert (
        catalog.latest_available_manifest(
            feed_id=FEED_ID,
            data_layer="RAW",
            resolution="30m",
            year=2026,
        )
        is None
    )


def test_latest_available_manifest_ignores_non_available_status(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="QUARANTINED", dataset_hash=HASH_A))

    assert (
        catalog.latest_available_manifest(
            feed_id=FEED_ID,
            data_layer="RAW",
            resolution="30m",
            year=2026,
        )
        is None
    )


# --------------------------------------------------------------------------------------
# Writes and idempotency
# --------------------------------------------------------------------------------------


def test_publish_manifest_updates_an_existing_revision_in_place(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_B))

    rows = catalog.records("market_data.dataset_manifests")

    assert len(rows) == 1
    assert rows[0]["status"] == "AVAILABLE"
    assert rows[0]["dataset_hash"] == HASH_B


def test_finish_pipeline_run_records_the_terminal_state(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.finish_pipeline_run(RUN_ID, status="SUCCEEDED", output_hash=HASH_C)

    run = catalog.records("market_data.pipeline_runs")[0]

    assert run["status"] == "SUCCEEDED"
    assert run["output_hash"] == HASH_C
    assert run["completed_at"] is not None
    assert run["failure_code"] is None


def test_finish_pipeline_run_rejects_an_unknown_run(catalog: MarketDataCatalog) -> None:
    with pytest.raises(KeyError):
        catalog.finish_pipeline_run(RUN_ID, status="SUCCEEDED", output_hash=HASH_C)


def test_reused_run_is_discoverable_for_idempotency(catalog: MarketDataCatalog) -> None:
    """`engine._run_record` reuses a SUCCEEDED run with the same deterministic id."""

    seed_reference_data(catalog)
    catalog.finish_pipeline_run(RUN_ID, status="SUCCEEDED", output_hash=HASH_C)

    found = next(row for row in catalog.records("market_data.pipeline_runs") if row["id"] == RUN_ID)

    assert found["status"] == "SUCCEEDED"


def test_pipeline_run_looks_one_run_up_by_id(catalog: MarketDataCatalog) -> None:
    """`engine._run_record` asks for exactly one run, not for the whole table.

    This is the read the ``isinstance(self.catalog, LocalCatalog)`` gate in
    `_run_record` used to skip: with a non-local catalog the reuse check never ran, so
    a completed run was silently re-executed and its objects rewritten.
    """

    seed_reference_data(catalog)
    catalog.finish_pipeline_run(RUN_ID, status="SUCCEEDED", output_hash=HASH_C)

    found = catalog.pipeline_run(RUN_ID)

    assert found is not None
    assert found["id"] == RUN_ID
    assert found["status"] == "SUCCEEDED"
    assert found["output_hash"] == HASH_C
    assert found["idempotency_key"] == f"contract-test:{RUN_ID}"


def test_pipeline_run_of_an_unknown_id_is_none(catalog: MarketDataCatalog) -> None:
    assert catalog.pipeline_run(RUN_ID) is None


def test_reference_upserts_are_idempotent(catalog: MarketDataCatalog) -> None:
    """`engine._ensure_provider_metadata` runs on every construction, including reruns."""

    for _ in range(3):
        catalog.upsert("market_data.providers", provider_row())
        catalog.upsert("market_data.feeds", feed_row())

    assert catalog.records("market_data.providers") == [provider_row()]
    assert catalog.records("market_data.feeds") == [feed_row()]


def test_dataset_lineage_is_recorded_once_per_triple(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))
    catalog.publish_manifest(manifest_row(MANIFEST_B, revision=2, status="BUILDING", dataset_hash=HASH_B))
    record = {
        "derived_manifest_id": MANIFEST_B,
        "source_manifest_id": MANIFEST_A,
        "relation_type": "RESAMPLED_FROM",
    }

    catalog.record_dataset_lineage(record)
    catalog.record_dataset_lineage(record)

    assert catalog.records("market_data.dataset_lineage") == [record]


def test_object_lineage_records_compacted_from(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )
    catalog.stage_object(
        storage_row(OBJECT_B, content_hash=HASH_C),
        relation_row(
            RELATION_B,
            manifest_id=MANIFEST_A,
            object_id=OBJECT_B,
            partition_start="2026-01-06",
            partition_end="2026-01-07",
            part_number=2,
        ),
    )
    record = {
        "derived_dataset_object_id": RELATION_B,
        "source_dataset_object_id": RELATION_A,
        "pipeline_run_id": RUN_ID,
        "relation_type": "COMPACTED_FROM",
        "created_at": "2026-01-07T00:00:00Z",
    }

    catalog.record_object_lineage(record)
    catalog.record_object_lineage(record)

    assert catalog.records("market_data.dataset_object_lineage") == [record]


def test_quality_incident_is_stored_with_its_impact_scope(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))

    catalog.record_quality_incident(incident_row(MANIFEST_A))

    assert catalog.records("market_data.quality_incidents") == [incident_row(MANIFEST_A)]


def test_write_summary_produces_an_operator_artifact(catalog: MarketDataCatalog) -> None:
    path = catalog.write_summary({"status": "OK", "manifest_count": 1})

    assert path.is_file()
    assert '"manifest_count": 1' in path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------------------
# Capabilities: the one place the two catalogs are allowed to differ, declared explicitly
# --------------------------------------------------------------------------------------


def test_pipeline_output_capability_declaration_matches_behaviour(
    catalog: MarketDataCatalog,
) -> None:
    """A catalog that says it cannot record run outputs must refuse loudly.

    `market_data.pipeline_run_outputs` does not exist in the canonical DBML.  The
    difference between the two catalogs is therefore real; what this asserts is that it
    is *declared*, so no implementation can silently drop provenance.
    """

    seed_reference_data(catalog)
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
    catalog.stage_object(
        storage_row(OBJECT_A, content_hash=HASH_B),
        relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
    )
    supported = catalog.supports(CatalogCapability.PIPELINE_RUN_OUTPUTS)

    if supported:
        assert catalog.unsupported_reason(CatalogCapability.PIPELINE_RUN_OUTPUTS) is None
        catalog.record_pipeline_output(
            pipeline_run_id=RUN_ID,
            dataset_manifest_id=MANIFEST_A,
            dataset_object_id=RELATION_A,
        )
        assert catalog.pipeline_outputs() == [
            {
                "pipeline_run_id": RUN_ID,
                "dataset_manifest_id": MANIFEST_A,
                "dataset_object_id": RELATION_A,
            }
        ]
    else:
        reason = catalog.unsupported_reason(CatalogCapability.PIPELINE_RUN_OUTPUTS)
        assert reason is not None and "pipeline_run_outputs" in reason
        with pytest.raises(UnsupportedCatalogCapability):
            catalog.record_pipeline_output(
                pipeline_run_id=RUN_ID,
                dataset_manifest_id=MANIFEST_A,
                dataset_object_id=RELATION_A,
            )


# --------------------------------------------------------------------------------------
# Filtered reads.  Both catalogs must select the same rows for the same predicate; the
# PostgreSQL one pushes it into SQL, and that is the only difference callers may observe.
# --------------------------------------------------------------------------------------


def _seed_two_manifests(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    with catalog.transaction():
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="SUPERSEDED", dataset_hash=HASH_A))
        catalog.publish_manifest(manifest_row(MANIFEST_B, revision=2, status="AVAILABLE", dataset_hash=HASH_B))


def test_records_filters_on_equality(catalog: MarketDataCatalog) -> None:
    _seed_two_manifests(catalog)

    found = catalog.records("market_data.dataset_manifests", where={"status": "AVAILABLE"})

    assert [row["id"] for row in found] == [MANIFEST_B]


def test_records_ands_every_predicate(catalog: MarketDataCatalog) -> None:
    _seed_two_manifests(catalog)

    assert catalog.records(
        "market_data.dataset_manifests",
        where={"status": "AVAILABLE", "revision_number": 1},
    ) == []
    matched = catalog.records(
        "market_data.dataset_manifests",
        where={"status": "AVAILABLE", "revision_number": 2, "feed_id": FEED_ID},
    )
    assert [row["id"] for row in matched] == [MANIFEST_B]


def test_records_filter_treats_none_as_is_null(catalog: MarketDataCatalog) -> None:
    """`instrument_id IS NULL` is how the multi-instrument datasets are found."""

    _seed_two_manifests(catalog)

    assert len(catalog.records("market_data.dataset_manifests", where={"instrument_id": None})) == 2
    assert catalog.records("market_data.dataset_manifests", where={"supersedes_manifest_id": MANIFEST_A}) == []


def test_records_filter_normalises_its_values_before_comparing(catalog: MarketDataCatalog) -> None:
    """`+00:00` in must match the row stored as `Z`, on both implementations."""

    _seed_two_manifests(catalog)

    found = catalog.records(
        "market_data.dataset_manifests",
        where={"period_start": "2026-01-01T05:00:00+00:00"},
    )

    assert sorted(row["id"] for row in found) == sorted([MANIFEST_A, MANIFEST_B])


def test_records_filter_rejects_a_column_outside_the_schema(catalog: MarketDataCatalog) -> None:
    with pytest.raises(UnknownCatalogColumn):
        catalog.records("market_data.dataset_manifests", where={"invented_column": 1})


def test_records_with_an_empty_filter_returns_everything(catalog: MarketDataCatalog) -> None:
    _seed_two_manifests(catalog)

    assert len(catalog.records("market_data.dataset_manifests", where={})) == 2


# --------------------------------------------------------------------------------------
# Natural-key identity
# --------------------------------------------------------------------------------------


def corporate_action_row(action_id: str, *, terms_hash: str, event_key: str = "SPLIT-2026-02-01") -> dict[str, Any]:
    return {
        "id": action_id,
        "instrument_id": INSTRUMENT_ID,
        "source_manifest_id": MANIFEST_A,
        "provider_event_key": event_key,
        "action_type": "SPLIT",
        "effective_at": "2026-02-01T05:00:00Z",
        "terms_document": {"ratio": "4:1"},
        "terms_hash": terms_hash,
        "supersedes_action_id": None,
        "created_at": "2026-01-20T00:00:00Z",
    }


def test_corporate_actions_merge_on_their_unique_index_not_on_id(catalog: MarketDataCatalog) -> None:
    """Two writers that generate different surrogate ids must still produce one row.

    `uq_corporate_actions_source_manifest_event` is the real identity.  Deduplicating by
    reading first and writing second is racy: both writers can read "absent".  Both
    catalogs therefore merge on the natural key, and the first-stored `id` survives so
    `supersedes_action_id` references stay valid.
    """

    seed_reference_data(catalog)
    catalog.upsert("market_data.instruments", instrument_row())
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))

    catalog.upsert("market_data.corporate_actions", corporate_action_row(ACTION_A, terms_hash=HASH_B))
    catalog.upsert("market_data.corporate_actions", corporate_action_row(ACTION_B, terms_hash=HASH_C))

    rows = catalog.records("market_data.corporate_actions")

    assert len(rows) == 1
    assert rows[0]["id"] == ACTION_A
    assert rows[0]["terms_hash"] == HASH_C


def test_a_different_provider_event_is_a_different_corporate_action(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    catalog.upsert("market_data.instruments", instrument_row())
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))

    catalog.upsert("market_data.corporate_actions", corporate_action_row(ACTION_A, terms_hash=HASH_B))
    catalog.upsert(
        "market_data.corporate_actions",
        corporate_action_row(ACTION_B, terms_hash=HASH_C, event_key="DIVIDEND-2026-03-01"),
    )

    rows = catalog.records("market_data.corporate_actions")

    assert sorted(row["id"] for row in rows) == sorted([ACTION_A, ACTION_B])


def test_corporate_actions_are_findable_by_a_filtered_read(catalog: MarketDataCatalog) -> None:
    """The lookup that replaces "scan the whole table into Python and filter there"."""

    seed_reference_data(catalog)
    catalog.upsert("market_data.instruments", instrument_row())
    catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))
    catalog.upsert("market_data.corporate_actions", corporate_action_row(ACTION_A, terms_hash=HASH_B))
    catalog.upsert(
        "market_data.corporate_actions",
        corporate_action_row(ACTION_B, terms_hash=HASH_C, event_key="DIVIDEND-2026-03-01"),
    )

    found = catalog.records(
        "market_data.corporate_actions",
        where={"source_manifest_id": MANIFEST_A, "provider_event_key": "DIVIDEND-2026-03-01"},
    )

    assert [row["id"] for row in found] == [ACTION_B]


# --------------------------------------------------------------------------------------
# The unit of work
# --------------------------------------------------------------------------------------


def test_transaction_commits_every_write_together(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)

    with catalog.transaction():
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
        catalog.stage_object(
            storage_row(OBJECT_A, content_hash=HASH_B),
            relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
        )
        catalog.record_quality_incident(incident_row(MANIFEST_A))
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_B))

    assert len(catalog.records("market_data.dataset_manifests")) == 1
    assert len(catalog.records("market_data.dataset_objects")) == 1
    assert len(catalog.records("market_data.quality_incidents")) == 1
    assert catalog.records("market_data.dataset_manifests")[0]["status"] == "AVAILABLE"


def test_transaction_rolls_back_every_write_on_failure(catalog: MarketDataCatalog) -> None:
    """A crash between the last two publishes must not leave a half-published dataset."""

    seed_reference_data(catalog)

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with catalog.transaction():
            catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A))
            catalog.stage_object(
                storage_row(OBJECT_A, content_hash=HASH_B),
                relation_row(RELATION_A, manifest_id=MANIFEST_A, object_id=OBJECT_A),
            )
            catalog.record_quality_incident(incident_row(MANIFEST_A))
            raise Boom("publish crashed before the manifest went AVAILABLE")

    assert catalog.records("market_data.dataset_manifests") == []
    assert catalog.records("market_data.dataset_objects") == []
    assert catalog.records("market_data.quality_incidents") == []
    assert catalog.records("storage.objects") == []


def test_transaction_refuses_to_commit_two_available_manifests_for_one_period(
    catalog: MarketDataCatalog,
) -> None:
    """The canonical DDL has no `uq_available_manifest_period`; the unit of work does.

    `engine.publish_dataset` publishes the new manifest AVAILABLE and only then marks
    the previous one SUPERSEDED.  A crash in between leaves two AVAILABLE manifests for
    the same feed/layer/resolution/year, which the DBML's uniqueness intent forbids.
    """

    seed_reference_data(catalog)
    with catalog.transaction():
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))

    with pytest.raises(DuplicateAvailableManifest):
        with catalog.transaction():
            catalog.publish_manifest(manifest_row(MANIFEST_B, revision=2, status="AVAILABLE", dataset_hash=HASH_B))

    remaining = catalog.records("market_data.dataset_manifests")
    assert [row["id"] for row in remaining] == [MANIFEST_A]


def test_transaction_allows_publish_then_supersede_in_one_unit_of_work(
    catalog: MarketDataCatalog,
) -> None:
    seed_reference_data(catalog)
    with catalog.transaction():
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="AVAILABLE", dataset_hash=HASH_A))

    with catalog.transaction():
        catalog.publish_manifest(
            manifest_row(MANIFEST_B, revision=2, status="AVAILABLE", dataset_hash=HASH_B, supersedes=MANIFEST_A)
        )
        catalog.publish_manifest(manifest_row(MANIFEST_A, revision=1, status="SUPERSEDED", dataset_hash=HASH_A))

    by_id = {row["id"]: row for row in catalog.records("market_data.dataset_manifests")}
    assert by_id[MANIFEST_A]["status"] == "SUPERSEDED"
    assert by_id[MANIFEST_B]["status"] == "AVAILABLE"
    assert by_id[MANIFEST_B]["supersedes_manifest_id"] == MANIFEST_A


def test_nested_transactions_are_rejected(catalog: MarketDataCatalog) -> None:
    with catalog.transaction():
        with pytest.raises(RuntimeError):
            with catalog.transaction():
                pass


# --------------------------------------------------------------------------------------
# Type sanity that does not need a database
# --------------------------------------------------------------------------------------


def test_local_catalog_satisfies_the_protocol(tmp_path: Any) -> None:
    assert isinstance(LocalCatalog(tmp_path / "catalog"), MarketDataCatalog)


# --------------------------------------------------------------------------------------
# The runtime guards, unit-tested without a database
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE market_data.invented (id uuid)",
        "ALTER TABLE market_data.dataset_manifests ADD COLUMN x int",
        "DROP TABLE market_data.dataset_objects",
        "TRUNCATE TABLE market_data.pipeline_runs",
        "GRANT SELECT ON market_data.feeds TO someone",
        "REFRESH MATERIALIZED VIEW market_data.anything",
        "  -- a leading comment\n  CREATE INDEX ix ON market_data.feeds (code)",
    ],
)
def test_the_runtime_refuses_every_form_of_ddl(statement: str) -> None:
    """Migration execution belongs to the central Flyway bundle, never to a runtime."""

    with pytest.raises(RuntimeDdlForbidden):
        check_statement(statement, frozenset({"market_data", "storage"}))


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO identity.accounts (id) VALUES (1)",
        "UPDATE backtest.runs SET status = 'COMPLETED'",
        'DELETE FROM "strategy"."strategies"',
    ],
)
def test_the_runtime_refuses_writes_to_schemas_this_repository_does_not_own(statement: str) -> None:
    with pytest.raises(SchemaWriteForbidden):
        check_statement(statement, frozenset({"market_data", "storage"}))


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM identity.accounts",
        "INSERT INTO market_data.feeds (id) VALUES ('x')",
        'UPDATE "storage"."objects" SET status = \'SUPERSEDED\'',
        "",
    ],
)
def test_reads_anywhere_and_writes_to_owned_schemas_are_allowed(statement: str) -> None:
    check_statement(statement, frozenset({"market_data", "storage"}))


def test_storage_writes_are_refused_when_the_ownership_side_is_read_only() -> None:
    """`StorageObjectsPolicy.READ_ONLY` has to reach the connection, not just the API."""

    with pytest.raises(SchemaWriteForbidden):
        check_statement("INSERT INTO storage.objects (id) VALUES ('x')", frozenset({"market_data"}))


def test_iso_normalisation_accepts_naive_datetimes_only_as_utc(catalog: MarketDataCatalog) -> None:
    """A naive timestamp is ambiguous; both catalogs must refuse it rather than guess."""

    seed_reference_data(catalog)
    with pytest.raises(ValueError):
        catalog.publish_manifest(
            manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A, period_start="2026-01-01")
        )


def test_datetime_objects_are_accepted_and_normalised(catalog: MarketDataCatalog) -> None:
    seed_reference_data(catalog)
    row = manifest_row(MANIFEST_A, revision=1, status="BUILDING", dataset_hash=HASH_A)
    row["created_at"] = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)

    catalog.publish_manifest(row)

    assert catalog.records("market_data.dataset_manifests")[0]["created_at"] == "2026-03-04T05:06:07Z"
