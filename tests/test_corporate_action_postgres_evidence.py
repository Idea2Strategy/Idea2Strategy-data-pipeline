from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from market_pipeline_lib.corporate_actions.postgres_evidence import (
    PostgresApprovalAuditDirectory,
    PostgresOperatorDirectory,
)
from tests.test_corporate_action_approval_provider import result


class _Rows:
    def __init__(self, row: Mapping[str, Any] | None) -> None:
        self._row = row

    def mappings(self) -> _Rows:
        return self

    def one_or_none(self) -> Mapping[str, Any] | None:
        return self._row

    def scalar_one(self) -> Any:
        return self._row


class _Connection:
    def __init__(self, value: Any, calls: list[tuple[str, Mapping[str, Any]]]) -> None:
        self._value = value
        self._calls = calls

    def execute(self, statement: Any, params: Mapping[str, Any]) -> _Rows:
        self._calls.append((str(statement), params))
        return _Rows(self._value)


class _Engine:
    def __init__(self, value: Any) -> None:
        self.value = value
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    @contextmanager
    def connect(self) -> Any:
        yield _Connection(self.value, self.calls)


def _audit_row() -> dict[str, Any]:
    approval = result()
    return {
        "actor_id": str(approval.actor_id),
        "target_id": str(approval.candidate_id),
        "candidate_id": str(approval.candidate_id),
        "decision": approval.decision.value,
        "decided_content_hash": approval.decided_content_hash,
        "evidence_bindings": json.dumps(list(approval.evidence_bindings)),
        "permission_id": str(approval.permission_id),
        "request_schema_version": approval.request_schema_version,
        "decided_at": approval.decided_at.isoformat().replace("+00:00", "Z"),
        "delivery_id": str(approval.delivery_id),
        "aggregate_sequence": str(approval.aggregate_sequence),
        "supersedes_candidate_id": None,
        "rationale": approval.rationale,
    }


def test_operator_adapter_uses_one_read_only_status_projection() -> None:
    engine = _Engine(True)
    assert PostgresOperatorDirectory(engine).is_active(result().actor_id) is True  # type: ignore[arg-type]
    sql, params = engine.calls[0]
    assert "select exists" in sql.lower()
    assert "status = 'ACTIVE'" in sql
    assert "disabled_at is null" in sql
    assert not any(token in sql.lower() for token in ("insert ", "update ", "delete "))
    assert params == {"operator_id": result().actor_id}


def test_audit_adapter_matches_every_protected_envelope_field_without_document_reads() -> None:
    engine = _Engine(_audit_row())
    assert PostgresApprovalAuditDirectory(engine).matches(result()) is True  # type: ignore[arg-type]
    sql, _ = engine.calls[0]
    assert "response_document ->>" in sql
    assert "request_document" not in sql
    assert "evidence_document" not in sql
    assert "before_document" not in sql
    assert "after_document" not in sql
    assert not any(token in sql.lower() for token in ("insert ", "update ", "delete "))


def test_audit_adapter_refuses_one_changed_protected_field() -> None:
    row = _audit_row()
    row["aggregate_sequence"] = "2"
    engine = _Engine(row)
    assert PostgresApprovalAuditDirectory(engine).matches(result()) is False  # type: ignore[arg-type]
