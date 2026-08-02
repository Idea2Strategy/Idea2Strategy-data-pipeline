"""Administrator review decisions on researched corporate actions.

A researched action is inert.  It becomes capable of changing a dataset only
when a named administrator approves it, on the record, with a rationale.  These
types are the vocabulary for that step; the transitions themselves are applied
by :class:`market_pipeline_lib.corporate_actions.service.CorporateActionReviewService`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

__all__ = [
    "AdminDecision",
    "ConflictingDecisionError",
    "DecisionType",
    "ReviewState",
    "UnknownCandidateError",
]


class DecisionType(StrEnum):
    """What an administrator decided."""

    APPROVE = "APPROVE"
    REJECT = "REJECT"


class ReviewState(StrEnum):
    """Where a researched action sits in the review workflow."""

    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    @classmethod
    def for_decision(cls, decision: DecisionType) -> ReviewState:
        return cls.APPROVED if decision is DecisionType.APPROVE else cls.REJECTED


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
