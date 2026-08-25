from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import boto3
import pytest
from sqlalchemy import Engine, text

from market_pipeline_lib.contracts import canonical_dataset_hash
from market_pipeline_lib.corporate_actions import (
    AdjustedDatasetRegenerator,
    ApprovalEvidenceVerifier,
    BackendRelayApprovalConsumer,
    CorporateActionReviewService,
)
from market_pipeline_lib.corporate_actions.adjustment import Bar
from market_pipeline_lib.corporate_actions.object_bars import (
    CatalogObjectBarReader,
    ImmutableObjectBarWriter,
)
from market_pipeline_lib.corporate_actions.postgres_evidence import (
    PostgresApprovalAuditDirectory,
    PostgresOperatorDirectory,
)
from market_pipeline_lib.storage import S3ObjectStore

pytestmark = pytest.mark.integration

PROVIDER = "11000000-0000-4000-8000-000000000001"
RAW_FEED = "12000000-0000-4000-8000-000000000001"
ADJUSTED_FEED = "12000000-0000-4000-8000-000000000002"
INSTRUMENT = "13000000-0000-4000-8000-000000000001"
RAW_MANIFEST = "14000000-0000-4000-8000-000000000001"
CANDIDATE = UUID("15000000-0000-4000-8000-000000000001")
ACTOR = UUID("16000000-0000-4000-8000-000000000001")
AUDIT = UUID("17000000-0000-4000-8000-000000000001")
PERMISSION = UUID("18000000-0000-4000-8000-000000000001")
DELIVERY = UUID("19000000-0000-4000-8000-000000000001")
CONTENT_HASH = "a" * 64
EVIDENCE_HASH = "b" * 64


def test_postgres_localstack_approval_publishes_queryable_registered_revision(
    postgres_catalog: object, admin_engine: Engine, tmp_path: Path
) -> None:
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("set LOCALSTACK_ENDPOINT_URL to run the corporate-action production E2E")
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    bucket = f"i2s-d15-{os.getpid()}"
    client.create_bucket(Bucket=bucket)
    store = S3ObjectStore(bucket, prefix="market", endpoint_url=endpoint)
    writer = ImmutableObjectBarWriter(object_store=store, staging_root=tmp_path / "staging")
    raw_written = writer.write_bars(
        (
            Bar(
                instrument_id=INSTRUMENT,
                bar_start_at=datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
                open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
                close=Decimal("100.5"), volume=1000, provider_symbol="AAPL",
                session_date_et=date(2026, 1, 2), trade_count=10, vwap=Decimal("100.25"),
            ),
        ),
        dataset_key="feed=raw/layer=RAW/resolution=30m/revision=1",
    )
    assert raw_written.storage_record is not None and raw_written.relation_record is not None
    catalog = postgres_catalog
    catalog.upsert("market_data.providers", {
        "id": PROVIDER, "code": "D15_E2E", "display_name": "D15 E2E",
        "rights_version": "1", "status": "ACTIVE", "created_at": "2026-01-01T00:00:00Z",
    })
    for feed_id, code in ((RAW_FEED, "D15_RAW_30M"), (ADJUSTED_FEED, "D15_ADJUSTED_30M")):
        catalog.upsert("market_data.feeds", {
            "id": feed_id, "provider_id": PROVIDER, "code": code, "data_kind": "BARS",
            "resolution": "30m", "timezone_name": "America/New_York", "feed_version": "1",
            "created_at": "2026-01-01T00:00:00Z", "retired_at": None,
        })
    catalog.upsert("market_data.instruments", {
        "id": INSTRUMENT, "asset_type": "STOCK", "primary_exchange_mic": "XNAS",
        "currency_code": "USD", "provider_reference": None, "listed_at": None,
        "delisted_at": None, "created_at": "2026-01-01T00:00:00Z",
    })
    raw_canonical = {
        **dict(raw_written.relation_record), "content_hash": raw_written.content_hash,
        "schema_version": raw_written.storage_record["schema_version"],
    }
    with catalog.transaction() as tx:
        tx.publish_manifest({
            "id": RAW_MANIFEST, "feed_id": RAW_FEED, "instrument_id": INSTRUMENT,
            "data_layer": "RAW", "resolution": "30m", "revision_number": 1,
            "status": "AVAILABLE", "period_start": "2026-01-01T00:00:00Z",
            "period_end": "2027-01-01T00:00:00Z", "schema_version": "market-bars-v2",
            "dataset_hash": canonical_dataset_hash([raw_canonical]), "supersedes_manifest_id": None,
            "created_at": "2026-01-01T00:00:00Z", "available_at": "2026-01-01T00:00:00Z",
        })
        tx.stage_object(
            {**dict(raw_written.storage_record), "id": "1a000000-0000-4000-8000-000000000001"},
            {
                **dict(raw_written.relation_record),
                "id": "1b000000-0000-4000-8000-000000000001",
                "dataset_manifest_id": RAW_MANIFEST,
                "object_id": "1a000000-0000-4000-8000-000000000001",
            },
        )
        tx.upsert("market_data.corporate_actions", {
            "id": str(CANDIDATE), "instrument_id": INSTRUMENT,
            "source_manifest_id": RAW_MANIFEST, "provider_event_key": "D15:E2E:SPLIT",
            "action_type": "STOCK_SPLIT", "effective_at": "2026-01-03T05:00:00Z",
            "terms_document": {
                "ticker": "AAPL", "event_type": "STOCK_SPLIT", "effective_date": "2026-01-03",
                "terms": {"from_shares": "1", "to_shares": "2"},
                "evidence": [{"content_sha256": EVIDENCE_HASH}],
                "review": {"state": "REVIEW_REQUIRED"}, "review_history": [],
            },
            "terms_hash": CONTENT_HASH, "supersedes_action_id": None,
            "created_at": "2026-01-01T00:00:00Z",
        })

    response_document = {
        "candidateId": str(CANDIDATE), "decision": "APPROVE",
        "decidedContentHash": CONTENT_HASH, "evidenceBindings": [EVIDENCE_HASH],
        "actorId": str(ACTOR), "auditId": str(AUDIT), "permissionId": str(PERMISSION),
        "requestSchemaVersion": "schema-v1", "decidedAt": "2026-08-04T00:00:00Z",
        "deliveryId": str(DELIVERY), "aggregateSequence": 1,
    }
    with admin_engine.begin() as connection:
        connection.execute(text("""
            insert into operations.operator_accounts
              (id, status, created_at)
            values (:id, 'ACTIVE', now())
        """), {"id": ACTOR})
        connection.execute(text("""
            insert into operations.audit_events
              (id, actor_type, actor_id, action_type, target_domain, target_id, reason_code,
               correlation_id, idempotency_key, before_hash, after_hash, occurred_at,
               request_hash, decision_status, response_status, response_code,
               request_document, response_document, before_document, after_document,
               evidence_document, evidence_hash)
            values (:id, 'OPERATOR', :actor, 'corporate_action_candidate.approve',
               'CORPORATE_ACTION', :target, 'CORPORATE_ACTION_DECISION_ACCEPTED', :id,
               'd15-e2e', :hash, :hash, now(), :hash, 'SUCCEEDED', 200,
               'CORPORATE_ACTION_DECISION_ACCEPTED', '{}'::jsonb, cast(:response as jsonb),
               '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, :hash)
        """), {
            "id": AUDIT, "actor": ACTOR, "target": CANDIDATE, "hash": "c" * 64,
            "response": json.dumps(response_document),
        })

    regenerator = AdjustedDatasetRegenerator(
        catalog=catalog,
        reader=CatalogObjectBarReader(catalog=catalog, object_store=store),
        writer=writer,
        require_feed_compatibility=True,
    )
    service = CorporateActionReviewService(
        catalog=catalog, regenerator=regenerator, raw_manifest_id=None,
        adjusted_feed_id=ADJUSTED_FEED,
        approval_verifier=ApprovalEvidenceVerifier(
            PostgresOperatorDirectory(catalog.engine),
            PostgresApprovalAuditDirectory(catalog.engine), PERMISSION, "schema-v1",
        ),
    )
    outcome = BackendRelayApprovalConsumer(service).apply(response_document)
    assert outcome == {"candidateId": str(CANDIDATE), "state": "APPROVED", "regenerated": True}
    adjusted = catalog.latest_available_manifest(
        feed_id=ADJUSTED_FEED, data_layer="ADJUSTED", resolution="30m", year=2026
    )
    assert adjusted is not None
    objects = catalog.objects_for_manifest(str(adjusted["id"]))
    assert len(objects) == 1
    canonical = [{
        **{key: objects[0][key] for key in (
            "object_kind", "partition_granularity", "partition_start", "partition_end",
            "period_start", "period_end", "shard_key", "part_number", "row_count",
        )},
        "content_hash": objects[0]["storage"]["content_hash"],
        "schema_version": objects[0]["storage"]["schema_version"],
    }]
    assert adjusted["dataset_hash"] == canonical_dataset_hash(canonical)
    assert store.verify(
        objects[0]["storage"]["object_key"], objects[0]["storage"]["content_hash"]
    ).ok

    # A second candidate in another source revision must derive from its own
    # canonical source, never the process-wide first manifest.
    raw_manifest_2 = "14000000-0000-4000-8000-000000000002"
    candidate_2 = UUID("15000000-0000-4000-8000-000000000002")
    audit_2 = UUID("17000000-0000-4000-8000-000000000002")
    delivery_2 = UUID("19000000-0000-4000-8000-000000000002")
    raw_written_2 = writer.write_bars(
        (
            Bar(
                instrument_id=INSTRUMENT,
                bar_start_at=datetime(2027, 1, 4, 14, 30, tzinfo=UTC),
                open=Decimal("200"), high=Decimal("201"), low=Decimal("199"),
                close=Decimal("200.5"), volume=2000, provider_symbol="AAPL",
                session_date_et=date(2027, 1, 4), trade_count=20, vwap=Decimal("200.25"),
            ),
        ),
        dataset_key="feed=raw/layer=RAW/resolution=30m/revision=2",
    )
    assert raw_written_2.storage_record is not None and raw_written_2.relation_record is not None
    raw_canonical_2 = {
        **dict(raw_written_2.relation_record), "content_hash": raw_written_2.content_hash,
        "schema_version": raw_written_2.storage_record["schema_version"],
    }
    with catalog.transaction() as tx:
        tx.publish_manifest({
            "id": raw_manifest_2, "feed_id": RAW_FEED, "instrument_id": INSTRUMENT,
            "data_layer": "RAW", "resolution": "30m", "revision_number": 2,
            "status": "AVAILABLE", "period_start": "2027-01-01T00:00:00Z",
            "period_end": "2028-01-01T00:00:00Z", "schema_version": "market-bars-v2",
            "dataset_hash": canonical_dataset_hash([raw_canonical_2]), "supersedes_manifest_id": None,
            "created_at": "2027-01-01T00:00:00Z", "available_at": "2027-01-01T00:00:00Z",
        })
        tx.stage_object(
            {**dict(raw_written_2.storage_record), "id": "1a000000-0000-4000-8000-000000000002"},
            {
                **dict(raw_written_2.relation_record),
                "id": "1b000000-0000-4000-8000-000000000002",
                "dataset_manifest_id": raw_manifest_2,
                "object_id": "1a000000-0000-4000-8000-000000000002",
            },
        )
        tx.upsert("market_data.corporate_actions", {
            "id": str(candidate_2), "instrument_id": INSTRUMENT,
            "source_manifest_id": raw_manifest_2, "provider_event_key": "D15:E2E:SPLIT:2",
            "action_type": "STOCK_SPLIT", "effective_at": "2027-01-05T05:00:00Z",
            "terms_document": {
                "ticker": "AAPL", "event_type": "STOCK_SPLIT", "effective_date": "2027-01-05",
                "terms": {"from_shares": "1", "to_shares": "2"},
                "evidence": [{"content_sha256": EVIDENCE_HASH}],
                "review": {"state": "REVIEW_REQUIRED"}, "review_history": [],
            },
            "terms_hash": CONTENT_HASH, "supersedes_action_id": None,
            "created_at": "2027-01-01T00:00:00Z",
        })
    response_2 = {
        **response_document,
        "candidateId": str(candidate_2), "auditId": str(audit_2),
        "deliveryId": str(delivery_2), "decidedAt": "2027-01-06T00:00:00Z",
    }
    with admin_engine.begin() as connection:
        connection.execute(text("""
            insert into operations.audit_events
              (id, actor_type, actor_id, action_type, target_domain, target_id, reason_code,
               correlation_id, idempotency_key, before_hash, after_hash, occurred_at,
               request_hash, decision_status, response_status, response_code,
               request_document, response_document, before_document, after_document,
               evidence_document, evidence_hash)
            values (:id, 'OPERATOR', :actor, 'corporate_action_candidate.approve',
               'CORPORATE_ACTION', :target, 'CORPORATE_ACTION_DECISION_ACCEPTED', :id,
               'd15-e2e-2', :hash, :hash, now(), :hash, 'SUCCEEDED', 200,
               'CORPORATE_ACTION_DECISION_ACCEPTED', '{}'::jsonb, cast(:response as jsonb),
               '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, :hash)
        """), {
            "id": audit_2, "actor": ACTOR, "target": candidate_2, "hash": "d" * 64,
            "response": json.dumps(response_2),
        })
    BackendRelayApprovalConsumer(service).apply(response_2)
    adjusted_2027 = catalog.latest_available_manifest(
        feed_id=ADJUSTED_FEED, data_layer="ADJUSTED", resolution="30m", year=2027
    )
    assert adjusted_2027 is not None
    sources_2027 = {
        str(item["source_manifest_id"])
        for item in catalog.records("market_data.dataset_lineage")
        if str(item["derived_manifest_id"]) == str(adjusted_2027["id"])
        and item["relation_type"] == "ADJUSTMENT_SOURCE"
    }
    assert sources_2027 == {raw_manifest_2}
