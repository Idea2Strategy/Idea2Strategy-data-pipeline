"""Applying an administrator's decision to a researched corporate action.

The whole point of the review gate is that research alone changes nothing.  This
service is the only place a researched action can start affecting data, and it
does so on exactly one transition: `REVIEW_REQUIRED -> APPROVED`.  A rejected
action and an undecided one are indistinguishable from the dataset's point of
view -- neither contributes a factor, neither triggers a regeneration.

Review state lives in the `terms_document` of the canonical
`market_data.corporate_actions` row, alongside an append-only `review_history`.
There is no `corporate_action_reviews` table in `db/schema.dbml` and this track
does not author DDL, so the decision is recorded on the row it decides.
`terms_hash` deliberately does not cover the review keys, so approving an action
does not make the next research run think the terms changed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..corporate_action_research import (
    CORPORATE_ACTIONS_TABLE,
    ResearchCandidate,
    corporate_action_record,
    parse_terms,
    research_digest,
)
from .adjustment import ApprovedAction
from .decisions import (
    AdminDecision,
    ConflictingDecisionError,
    DecisionType,
    ReviewState,
    UnknownCandidateError,
)
from .regeneration import AdjustedDatasetRegenerator, RegenerationResult

__all__ = ["CorporateActionReviewService", "DecisionOutcome"]


@dataclass(frozen=True)
class DecisionOutcome:
    """What a decision did."""

    candidate_id: str
    state: ReviewState
    #: Present only when the decision was an approval; `None` for a rejection.
    regeneration: RegenerationResult | None


class RegeneratorNotConfiguredError(RuntimeError):
    """An approval needs to rebuild data, and no regenerator is wired in."""


class CorporateActionReviewService:
    """Records candidates, applies decisions, and regenerates on approval."""

    def __init__(
        self,
        *,
        catalog: Any,
        regenerator: AdjustedDatasetRegenerator | None,
        raw_manifest_id: str,
        adjusted_feed_id: str,
    ) -> None:
        self._catalog = catalog
        self._regenerator = regenerator
        self._raw_manifest_id = raw_manifest_id
        self._adjusted_feed_id = adjusted_feed_id

    # -- recording ---------------------------------------------------------------
    def record_candidate(
        self,
        candidate: ResearchCandidate,
        *,
        instrument_id: str,
        source_manifest_id: str,
    ) -> bool:
        """Persist a researched candidate in `REVIEW_REQUIRED`. Idempotent."""
        record = corporate_action_record(
            candidate,
            instrument_id=instrument_id,
            source_manifest_id=source_manifest_id,
        )
        for row in self._rows():
            if str(row["id"]) == str(record["id"]):
                return False
        self._catalog.upsert(CORPORATE_ACTIONS_TABLE, record)
        return True

    # -- reading -----------------------------------------------------------------
    def _rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = self._catalog.records(CORPORATE_ACTIONS_TABLE)
        return rows

    @staticmethod
    def _state(row: Mapping[str, Any]) -> ReviewState:
        return ReviewState(row["terms_document"]["review"]["state"])

    def approved_actions(self) -> tuple[ApprovedAction, ...]:
        """Every approved action, ordered by effective instant.

        A `REVIEW_REQUIRED` or `REJECTED` row is simply not here, which is how
        "a rejected or pending candidate has no effect" is enforced: the
        regenerator is only ever handed this set.
        """
        actions: list[ApprovedAction] = []
        for row in self._rows():
            if self._state(row) is not ReviewState.APPROVED:
                continue
            document = row["terms_document"]
            actions.append(
                ApprovedAction(
                    action_type=str(row["action_type"]),
                    effective_at=_parse_utc(str(row["effective_at"])),
                    terms=parse_terms(str(row["action_type"]), document["terms"]),
                )
            )
        return tuple(sorted(actions, key=lambda action: action.effective_at))

    # -- deciding ----------------------------------------------------------------
    def apply_decision(self, decision: AdminDecision) -> DecisionOutcome:
        row = self._row_for(decision.candidate_id)
        current = self._state(row)
        target = decision.target_state

        if current is not ReviewState.REVIEW_REQUIRED and current is not target:
            raise ConflictingDecisionError(
                f"candidate {decision.candidate_id} is already {current.value}; "
                f"deciding it {target.value} would contradict a decision that has "
                "already taken effect"
            )

        if decision.decision is DecisionType.APPROVE and self._regenerator is None:
            raise RegeneratorNotConfiguredError(
                "approving a corporate action regenerates the adjusted dataset, but no "
                "regenerator is configured. Refusing rather than recording an approval "
                "whose data effect would silently not happen."
            )

        if current is ReviewState.REVIEW_REQUIRED:
            self._transition(row, decision, target)

        if target is ReviewState.REJECTED:
            return DecisionOutcome(
                candidate_id=decision.candidate_id, state=target, regeneration=None
            )

        assert self._regenerator is not None  # guarded above
        regeneration = self._regenerator.regenerate(
            raw_manifest_id=self._raw_manifest_id,
            adjusted_feed_id=self._adjusted_feed_id,
            approved_actions=self.approved_actions(),
            now=decision.decided_at,
        )
        return DecisionOutcome(
            candidate_id=decision.candidate_id, state=target, regeneration=regeneration
        )

    def _transition(
        self,
        row: Mapping[str, Any],
        decision: AdminDecision,
        target: ReviewState,
    ) -> None:
        document = dict(row["terms_document"])
        decided_at = decision.decided_at.astimezone(decision.decided_at.tzinfo)
        entry = {
            "state": target.value,
            "decided_by": decision.decided_by.strip(),
            "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
            "rationale": decision.rationale.strip(),
        }
        document["review"] = entry
        document["review_history"] = [*document.get("review_history", []), entry]
        self._catalog.upsert(
            CORPORATE_ACTIONS_TABLE,
            {
                **dict(row),
                "terms_document": document,
                # Recomputed, but by construction unchanged: the digest covers the
                # economic substance only, never the review block.
                "terms_hash": research_digest(document),
            },
        )

    def _row_for(self, candidate_id: str) -> dict[str, Any]:
        for row in self._rows():
            if str(row["terms_document"].get("candidate_id")) == candidate_id:
                return row
        raise UnknownCandidateError(
            f"no recorded corporate action carries candidate {candidate_id!r}"
        )


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
