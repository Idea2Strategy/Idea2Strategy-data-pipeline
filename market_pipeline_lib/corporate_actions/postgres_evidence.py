"""Read-only PostgreSQL evidence adapters for relayed approval decisions."""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from .decisions import ApprovalResult

__all__ = ["PostgresApprovalAuditDirectory", "PostgresOperatorDirectory"]


class PostgresOperatorDirectory:
    """Resolve only the ACTIVE/disabled status needed by the approval gate."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def is_active(self, operator_id: UUID) -> bool:
        statement = text("""
            select exists (
                select 1 from operations.operator_accounts
                where id = :operator_id and status = 'ACTIVE' and disabled_at is null
            )
        """)
        with self._engine.connect() as connection:
            return bool(connection.execute(statement, {"operator_id": operator_id}).scalar_one())


class PostgresApprovalAuditDirectory:
    """Bind an approval envelope to the immutable backend audit fact.

    The query projects only fields required by the canonical verifier. It never
    selects request, before/after, or evidence documents and performs no write.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def matches(self, result: ApprovalResult) -> bool:
        statement = text("""
            select
                actor_id::text as actor_id,
                target_id::text as target_id,
                response_document ->> 'candidateId' as candidate_id,
                response_document ->> 'decision' as decision,
                response_document ->> 'decidedContentHash' as decided_content_hash,
                response_document -> 'evidenceBindings' as evidence_bindings,
                response_document ->> 'permissionId' as permission_id,
                response_document ->> 'requestSchemaVersion' as request_schema_version,
                response_document ->> 'decidedAt' as decided_at,
                response_document ->> 'deliveryId' as delivery_id,
                response_document ->> 'aggregateSequence' as aggregate_sequence,
                response_document ->> 'supersedesCandidateId' as supersedes_candidate_id,
                coalesce(response_document ->> 'rationale', '') as rationale
            from operations.audit_events
            where id = :audit_id
              and actor_type = 'OPERATOR'
              and action_type = 'corporate_action_candidate.approve'
              and target_domain = 'CORPORATE_ACTION'
              and decision_status = 'SUCCEEDED'
              and response_status between 200 and 299
              and response_code = 'CORPORATE_ACTION_DECISION_ACCEPTED'
        """)
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"audit_id": result.audit_id}).mappings().one_or_none()
        if row is None:
            return False
        bindings: Any = row["evidence_bindings"]
        if isinstance(bindings, str):
            try:
                bindings = json.loads(bindings)
            except json.JSONDecodeError:
                return False
        expected = {
            "actor_id": str(result.actor_id),
            "target_id": str(result.candidate_id),
            "candidate_id": str(result.candidate_id),
            "decision": result.decision.value,
            "decided_content_hash": result.decided_content_hash,
            "evidence_bindings": list(result.evidence_bindings),
            "permission_id": str(result.permission_id),
            "request_schema_version": result.request_schema_version,
            "decided_at": result.decided_at.isoformat().replace("+00:00", "Z"),
            "delivery_id": str(result.delivery_id),
            "aggregate_sequence": str(result.aggregate_sequence),
            "supersedes_candidate_id": (
                None if result.supersedes_candidate_id is None else str(result.supersedes_candidate_id)
            ),
            "rationale": result.rationale,
        }
        actual = {key: row[key] for key in expected}
        actual["evidence_bindings"] = bindings
        return actual == expected
