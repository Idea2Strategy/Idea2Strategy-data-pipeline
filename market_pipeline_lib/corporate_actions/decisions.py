"""Administrator review decisions on researched corporate actions.

A researched action is inert.  It becomes capable of changing a dataset only
when a named administrator approves it, on the record, with a rationale.  These
types are the vocabulary for that step; the transitions themselves are applied
by :class:`market_pipeline_lib.corporate_actions.service.CorporateActionReviewService`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

__all__ = [
    "AdminDecision",
    "ApprovalResult",
    "ApprovalRefusedError",
    "ConflictingDecisionError",
    "DecisionType",
    "ReviewState",
    "UnknownCandidateError",
]


class DecisionType(StrEnum):
    """What an administrator decided."""

    APPROVE = "APPROVE"
    REJECT = "REJECT"
    WITHDRAW = "WITHDRAW"


class ReviewState(StrEnum):
    """Where a researched action sits in the review workflow."""

    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    WITHDRAWN = "WITHDRAWN"
    SUPERSEDED = "SUPERSEDED"

    @classmethod
    def for_decision(cls, decision: DecisionType) -> ReviewState:
        if decision is DecisionType.APPROVE:
            return cls.APPROVED
        if decision is DecisionType.WITHDRAW:
            return cls.WITHDRAWN
        return cls.REJECTED


class UnknownCandidateError(LookupError):
    """No recorded corporate action carries the decided candidate id."""


class ConflictingDecisionError(RuntimeError):
    """An already-decided action was decided the other way.

    Reversing an approval is a real operation, but it is not a re-decision: the
    adjusted dataset built from the approval already exists and downstream
    consumers may have read it.  Silently flipping the state would leave that
    dataset in place while the catalog claimed the action was rejected, so this
    refuses and leaves the reversal to an explicit superseding workflow.
    """


class ApprovalRefusedError(RuntimeError):
    """A provider result failed a canonical fail-closed rule."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ApprovalResult:
    """The authenticated backend-relay result consumed by the pipeline."""

    candidate_id: UUID
    decision: DecisionType
    decided_content_hash: str
    evidence_bindings: tuple[str, ...]
    actor_id: UUID
    audit_id: UUID
    permission_id: UUID
    request_schema_version: str
    decided_at: datetime
    delivery_id: UUID
    aggregate_sequence: int
    supersedes_candidate_id: UUID | None = None
    rationale: str = ""

    def __post_init__(self) -> None:
        if self.decision not in {DecisionType.APPROVE, DecisionType.WITHDRAW}:
            raise ApprovalRefusedError("DECISION_UNSUPPORTED", "only APPROVE and WITHDRAW are provider decisions")
        if not _sha256(self.decided_content_hash):
            raise ApprovalRefusedError("DECIDED_CONTENT_HASH_INVALID", "decidedContentHash must be lowercase SHA-256")
        if not self.evidence_bindings or any(not _sha256(item) for item in self.evidence_bindings):
            raise ApprovalRefusedError(
                "EVIDENCE_BINDING_INVALID",
                "evidenceBindings must contain lowercase SHA-256 hashes",
            )
        if not self.request_schema_version.strip():
            raise ApprovalRefusedError("SCHEMA_VERSION_MISSING", "requestSchemaVersion is required")
        if self.decided_at.tzinfo is None or self.decided_at.utcoffset() != timedelta(0):
            raise ApprovalRefusedError("DECIDED_AT_INVALID", "decidedAt must be timezone-aware UTC")
        if self.aggregate_sequence < 1:
            raise ApprovalRefusedError("AGGREGATE_SEQUENCE_INVALID", "aggregateSequence must be positive")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ApprovalResult:
        required = {
            "candidateId", "decision", "decidedContentHash", "evidenceBindings",
            "actorId", "auditId", "permissionId", "requestSchemaVersion",
            "decidedAt", "deliveryId", "aggregateSequence",
        }
        missing = sorted(required - set(value))
        if missing:
            raise ApprovalRefusedError("APPROVAL_FIELD_MISSING", f"missing approval field(s): {missing}")
        try:
            evidence = value["evidenceBindings"]
            if not isinstance(evidence, list):
                raise TypeError("evidenceBindings")
            supersedes = value.get("supersedesCandidateId")
            return cls(
                candidate_id=UUID(str(value["candidateId"])),
                decision=DecisionType(str(value["decision"])),
                decided_content_hash=str(value["decidedContentHash"]),
                evidence_bindings=tuple(str(item) for item in evidence),
                actor_id=UUID(str(value["actorId"])),
                audit_id=UUID(str(value["auditId"])),
                permission_id=UUID(str(value["permissionId"])),
                request_schema_version=str(value["requestSchemaVersion"]),
                decided_at=datetime.fromisoformat(str(value["decidedAt"]).replace("Z", "+00:00")),
                delivery_id=UUID(str(value["deliveryId"])),
                aggregate_sequence=int(value["aggregateSequence"]),
                supersedes_candidate_id=None if supersedes in (None, "") else UUID(str(supersedes)),
                rationale=str(value.get("rationale", "")),
            )
        except ApprovalRefusedError:
            raise
        except (TypeError, ValueError, KeyError) as error:
            raise ApprovalRefusedError("APPROVAL_ENVELOPE_INVALID", "approval envelope is malformed") from error


def _sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


@dataclass(frozen=True)
class AdminDecision:
    """One administrator's decision about one researched candidate."""

    candidate_id: str
    decision: DecisionType
    decided_by: str
    decided_at: datetime
    rationale: str

    def __post_init__(self) -> None:
        if not self.candidate_id.strip():
            raise ValueError("candidate_id is required")
        if not isinstance(self.decision, DecisionType):
            raise ValueError("decision must be a DecisionType")
        if not self.decided_by.strip():
            raise ValueError("decided_by is required: decisions are attributable")
        if not self.rationale.strip():
            raise ValueError("rationale is required: decisions are justified on the record")
        if self.decided_at.tzinfo is None or self.decided_at.utcoffset() != timedelta(0):
            raise ValueError("decided_at must be timezone-aware UTC")

    @property
    def target_state(self) -> ReviewState:
        return ReviewState.for_decision(self.decision)
