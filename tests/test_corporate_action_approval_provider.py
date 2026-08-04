from __future__ import annotations

import unittest
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

from market_pipeline_lib.corporate_actions.consumer import BackendRelayApprovalConsumer
from market_pipeline_lib.corporate_actions.decisions import (
    ApprovalRefusedError,
    ApprovalResult,
    DecisionType,
    ReviewState,
)
from market_pipeline_lib.corporate_actions.service import (
    ApprovalEvidenceVerifier,
    CorporateActionReviewService,
)

CANDIDATE = UUID("10000000-0000-4000-8000-000000000001")
PRIOR = UUID("10000000-0000-4000-8000-000000000002")
ACTOR = UUID("20000000-0000-4000-8000-000000000001")
AUDIT = UUID("30000000-0000-4000-8000-000000000001")
PERMISSION = UUID("20000000-0000-4000-8000-000000000012")
DELIVERY = UUID("40000000-0000-4000-8000-000000000001")
HASH = "a" * 64
EVIDENCE = "b" * 64


class Catalog:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.transaction_active = False

    def records(self, table: str) -> list[dict[str, object]]:
        assert table == "market_data.corporate_actions"
        return [dict(row) for row in self.rows]

    def upsert(self, table: str, record: dict[str, object]) -> None:
        assert table == "market_data.corporate_actions"
        for index, row in enumerate(self.rows):
            if str(row["id"]) == str(record["id"]):
                self.rows[index] = record
                return
        self.rows.append(record)

    @contextmanager
    def transaction(self):  # type: ignore[no-untyped-def]
        if self.transaction_active:
            raise RuntimeError("catalog transactions do not nest")
        snapshot = deepcopy(self.rows)
        self.transaction_active = True
        try:
            yield self
        except Exception:
            self.rows = snapshot
            raise
        finally:
            self.transaction_active = False


class Operators:
    def __init__(self, active: bool = True) -> None:
        self.active = active

    def is_active(self, operator_id: UUID) -> bool:
        return self.active and operator_id == ACTOR


class Audits:
    def __init__(self, matches: bool = True) -> None:
        self.valid = matches

    def matches(self, result: ApprovalResult) -> bool:
        return self.valid and result.audit_id == AUDIT


class Regenerator:
    def __init__(self) -> None:
        self.calls = 0
        self.fail = False
        self.catalog: Catalog | None = None

    def with_catalog(self, catalog: Catalog) -> Regenerator:
        self.catalog = catalog
        return self

    def regenerate(self, **_: object) -> object:
        assert self.catalog is not None
        with self.catalog.transaction():
            return self.regenerate_in_transaction()

    def regenerate_in_transaction(self, **_: object) -> object:
        assert self.catalog is not None and self.catalog.transaction_active
        if self.fail:
            raise RuntimeError("injected regeneration failure")
        self.calls += 1
        return SimpleNamespace(created=True, revision_number=self.calls)


def row(candidate: UUID = CANDIDATE, *, state: str = "REVIEW_REQUIRED") -> dict[str, object]:
    return {
        "id": str(candidate),
        "instrument_id": "instrument-1",
        "action_type": "STOCK_SPLIT",
        "effective_at": "2026-08-15T00:00:00Z",
        "terms_hash": HASH,
        "terms_document": {
            "terms": {"from_shares": "1", "to_shares": "2"},
            "evidence": [{"content_sha256": EVIDENCE}],
            "review": {"state": state},
            "review_history": [],
        },
    }


def result(
    *,
    decision: DecisionType = DecisionType.APPROVE,
    candidate: UUID = CANDIDATE,
    delivery: UUID = DELIVERY,
    sequence: int = 1,
    supersedes: UUID | None = None,
    content_hash: str = HASH,
) -> ApprovalResult:
    return ApprovalResult(
        candidate_id=candidate,
        decision=decision,
        decided_content_hash=content_hash,
        evidence_bindings=(EVIDENCE,),
        actor_id=ACTOR,
        audit_id=AUDIT,
        permission_id=PERMISSION,
        request_schema_version="schema-v1",
        decided_at=datetime(2026, 8, 4, tzinfo=UTC),
        delivery_id=delivery,
        aggregate_sequence=sequence,
        supersedes_candidate_id=supersedes,
    )


class CorporateActionProviderResultTest(unittest.TestCase):
    def service(
        self,
        rows: list[dict[str, object]],
        *,
        active: bool = True,
    ) -> tuple[CorporateActionReviewService, Catalog, Regenerator]:
        catalog = Catalog(rows)
        regenerator = Regenerator()
        verifier = ApprovalEvidenceVerifier(Operators(active), Audits(), PERMISSION, "schema-v1")
        service = CorporateActionReviewService(
            catalog=catalog,
            regenerator=regenerator,
            raw_manifest_id="raw",
            adjusted_feed_id="adjusted",
            approval_verifier=verifier,
        )
        return service, catalog, regenerator

    def test_parser_refuses_a_missing_proof_field(self) -> None:
        with self.assertRaisesRegex(ApprovalRefusedError, "missing"):
            ApprovalResult.from_mapping({"candidateId": str(CANDIDATE)})

    def test_backend_relay_json_reaches_the_verified_consumer(self) -> None:
        service, _, regenerator = self.service([row()])
        consumer = BackendRelayApprovalConsumer(service)
        relay_payload = {
            "candidateId": str(CANDIDATE),
            "decision": "APPROVE",
            "decidedContentHash": HASH,
            "evidenceBindings": [EVIDENCE],
            "actorId": str(ACTOR),
            "auditId": str(AUDIT),
            "permissionId": str(PERMISSION),
            "requestSchemaVersion": "schema-v1",
            "decidedAt": "2026-08-04T00:00:00Z",
            "deliveryId": str(DELIVERY),
            "aggregateSequence": 1,
        }

        response = consumer.apply(relay_payload)

        self.assertEqual(response["state"], "APPROVED")
        self.assertTrue(response["regenerated"])
        self.assertEqual(regenerator.calls, 1)

    def test_unwired_provider_fails_closed(self) -> None:
        service = CorporateActionReviewService(
            catalog=Catalog([row()]), regenerator=Regenerator(),
            raw_manifest_id="raw", adjusted_feed_id="adjusted",
        )
        with self.assertRaisesRegex(ApprovalRefusedError, "unwired"):
            service.apply_approval_result(result())

    def test_content_hash_and_active_operator_are_verified(self) -> None:
        service, _, regenerator = self.service([row()])
        with self.assertRaises(ApprovalRefusedError) as stale:
            service.apply_approval_result(result(content_hash="c" * 64))
        self.assertEqual(stale.exception.code, "STALE_CONTENT_HASH")
        self.assertEqual(regenerator.calls, 0)

        inactive, _, _ = self.service([row()], active=False)
        with self.assertRaises(ApprovalRefusedError) as actor:
            inactive.apply_approval_result(result())
        self.assertEqual(actor.exception.code, "INACTIVE_OPERATOR")

    def test_permanent_refusal_redelivery_records_one_durable_fact(self) -> None:
        service, catalog, regenerator = self.service([row()])
        stale = result(content_hash="c" * 64)

        for _ in range(2):
            with self.assertRaises(ApprovalRefusedError) as refused:
                service.apply_approval_result(stale)
            self.assertEqual(refused.exception.code, "STALE_CONTENT_HASH")

        history = catalog.rows[0]["terms_document"]["review_history"]  # type: ignore[index]
        refusals = [entry for entry in history if entry["state"] == "REFUSED"]
        self.assertEqual(len(refusals), 1)
        self.assertEqual(refusals[0]["reason_code"], "STALE_CONTENT_HASH")
        self.assertEqual(regenerator.calls, 0)

    def test_duplicate_delivery_does_not_regenerate_twice(self) -> None:
        service, catalog, regenerator = self.service([row()])
        first = service.apply_approval_result(result())
        second = service.apply_approval_result(result())
        self.assertEqual(first.state, ReviewState.APPROVED)
        self.assertEqual(second.state, ReviewState.APPROVED)
        self.assertEqual(regenerator.calls, 1)
        self.assertEqual(len(catalog.rows[0]["terms_document"]["review_history"]), 1)  # type: ignore[index]

    def test_duplicate_identity_covers_the_entire_protected_envelope(self) -> None:
        service, catalog, regenerator = self.service([row()])
        accepted = result()
        service.apply_approval_result(accepted)

        tampered = replace(
            accepted,
            rationale="changed after the delivery was accepted",
        )
        with self.assertRaises(ApprovalRefusedError) as conflict:
            service.apply_approval_result(tampered)

        self.assertEqual(conflict.exception.code, "DELIVERY_ID_CONFLICT")
        self.assertEqual(regenerator.calls, 1)
        history = catalog.rows[0]["terms_document"]["review_history"]  # type: ignore[index]
        self.assertEqual([entry["state"] for entry in history], ["APPROVED", "REFUSED"])
        self.assertNotEqual(history[0]["envelope_hash"], history[1]["envelope_hash"])

    def test_withdrawal_is_an_event_and_regenerates_forward(self) -> None:
        service, catalog, regenerator = self.service([row()])
        service.apply_approval_result(result())
        withdrawal = result(
            decision=DecisionType.WITHDRAW,
            delivery=UUID("40000000-0000-4000-8000-000000000002"),
            sequence=2,
        )
        outcome = service.apply_approval_result(withdrawal)
        self.assertEqual(outcome.state, ReviewState.WITHDRAWN)
        self.assertEqual(regenerator.calls, 2)
        history = catalog.rows[0]["terms_document"]["review_history"]  # type: ignore[index]
        self.assertEqual([entry["decision"] for entry in history], ["APPROVE", "WITHDRAW"])

    def test_supersede_requires_the_current_prior_candidate(self) -> None:
        service, catalog, regenerator = self.service([row(PRIOR, state="APPROVED"), row()])
        with self.assertRaises(ApprovalRefusedError) as unnamed:
            service.apply_approval_result(result())
        self.assertEqual(unnamed.exception.code, "UNNAMED_SUPERSEDE_CONFLICT")
        self.assertEqual(regenerator.calls, 0)

        named = result(
            delivery=UUID("40000000-0000-4000-8000-000000000003"),
            supersedes=PRIOR,
        )
        outcome = service.apply_approval_result(named)
        self.assertEqual(outcome.state, ReviewState.APPROVED)
        states = {
            str(item["id"]): item["terms_document"]["review"]["state"]  # type: ignore[index]
            for item in catalog.rows
        }
        self.assertEqual(states[str(PRIOR)], "SUPERSEDED")
        self.assertEqual(states[str(CANDIDATE)], "APPROVED")
        self.assertEqual(regenerator.calls, 1)

    def test_supersede_rolls_back_both_transitions_when_regeneration_fails(self) -> None:
        service, catalog, regenerator = self.service([row(PRIOR, state="APPROVED"), row()])
        regenerator.fail = True
        named = result(supersedes=PRIOR)

        with self.assertRaisesRegex(RuntimeError, "injected"):
            service.apply_approval_result(named)

        states = {
            str(item["id"]): item["terms_document"]["review"]["state"]  # type: ignore[index]
            for item in catalog.rows
        }
        self.assertEqual(states[str(PRIOR)], "APPROVED")
        self.assertEqual(states[str(CANDIDATE)], "REVIEW_REQUIRED")


if __name__ == "__main__":
    unittest.main()
