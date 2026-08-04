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
from typing import Any, Protocol
from uuid import UUID

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
    ApprovalRefusedError,
    ApprovalResult,
    ConflictingDecisionError,
    DecisionType,
    ReviewState,
    UnknownCandidateError,
)
from .regeneration import AdjustedDatasetRegenerator, RegenerationResult

__all__ = [
    "ApprovalEvidenceVerifier",
    "CorporateActionReviewService",
    "DecisionOutcome",
]


class ActiveOperatorDirectory(Protocol):
    def is_active(self, operator_id: UUID) -> bool: ...


class ApprovalAuditDirectory(Protocol):
    def matches(self, result: ApprovalResult) -> bool: ...


@dataclass(frozen=True)
class ApprovalEvidenceVerifier:
    """Verifies backend-owned proof without interpreting transport headers."""

    operators: ActiveOperatorDirectory
    audits: ApprovalAuditDirectory
    permission_id: UUID
    request_schema_version: str

    def verify(self, result: ApprovalResult) -> None:
        if result.permission_id != self.permission_id:
            raise ApprovalRefusedError("PERMISSION_MISMATCH", "approval permissionId is not registered")
        if result.request_schema_version != self.request_schema_version:
            raise ApprovalRefusedError("UNKNOWN_SCHEMA_VERSION", "approval requestSchemaVersion is unknown")
        if not self.operators.is_active(result.actor_id):
            raise ApprovalRefusedError("INACTIVE_OPERATOR", "approval actor is not an ACTIVE operator")
        if not self.audits.matches(result):
            raise ApprovalRefusedError("AUDIT_BINDING_MISMATCH", "backend audit evidence does not bind this result")


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
        approval_verifier: ApprovalEvidenceVerifier | None = None,
    ) -> None:
        self._catalog = catalog
        self._regenerator = regenerator
        self._raw_manifest_id = raw_manifest_id
        self._adjusted_feed_id = adjusted_feed_id
        self._approval_verifier = approval_verifier

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

    def apply_approval_result(self, result: ApprovalResult) -> DecisionOutcome:
        """Apply an authenticated relay result and persist its durable audit facts."""
        try:
            if self._approval_verifier is None:
                raise ApprovalRefusedError(
                    "APPROVAL_PROVIDER_UNWIRED",
                    "approval provider is unwired; refusing instead of reporting zero approvals",
                )
            self._approval_verifier.verify(result)
            if self._regenerator is None:
                raise RegeneratorNotConfiguredError(
                    "approval effects require an adjusted-dataset regenerator"
                )
            bind_catalog = getattr(self._regenerator, "with_catalog", None)
            transaction = getattr(self._catalog, "transaction", None)
            if not callable(bind_catalog) or not callable(transaction):
                raise ApprovalRefusedError(
                    "APPROVAL_TRANSACTION_UNAVAILABLE",
                    "candidate transitions and regeneration require one catalog transaction",
                )
            with transaction() as catalog:
                scoped = CorporateActionReviewService(
                    catalog=catalog,
                    regenerator=bind_catalog(catalog),
                    raw_manifest_id=self._raw_manifest_id,
                    adjusted_feed_id=self._adjusted_feed_id,
                    approval_verifier=self._approval_verifier,
                )
                return scoped._apply_verified_result(result)
        except ApprovalRefusedError as error:
            self._record_refusal(result, error.code)
            raise

    def _apply_verified_result(self, result: ApprovalResult) -> DecisionOutcome:
        try:
            row = self._row_for_record_id(result.candidate_id)
        except UnknownCandidateError as error:
            raise ApprovalRefusedError("UNKNOWN_CANDIDATE", str(error)) from error

        prior = list(row["terms_document"].get("review_history", []))
        delivery = str(result.delivery_id)
        for entry in prior:
            if entry.get("delivery_id") == delivery and entry.get("state") != "REFUSED":
                if entry.get("envelope_hash") == result.envelope_hash:
                    return DecisionOutcome(str(result.candidate_id), self._state(row), None)
                raise ApprovalRefusedError(
                    "DELIVERY_ID_CONFLICT",
                    "deliveryId was reused with a different protected envelope",
                )

        latest_sequence = max(
            (
                int(entry.get("aggregate_sequence", 0))
                for entry in prior
                if entry.get("state") != "REFUSED"
            ),
            default=0,
        )
        if result.aggregate_sequence <= latest_sequence:
            self._refuse(row, result, "REVERSED_AGGREGATE_SEQUENCE")
        if str(row["terms_hash"]) != result.decided_content_hash:
            self._refuse(row, result, "STALE_CONTENT_HASH")

        evidence = {
            str(item.get("content_sha256"))
            for item in row["terms_document"].get("evidence", [])
            if item.get("content_sha256")
        }
        if not set(result.evidence_bindings).issubset(evidence):
            self._refuse(row, result, "UNBOUND_EVIDENCE")
        assert self._regenerator is not None

        target_state: ReviewState
        if result.decision is DecisionType.WITHDRAW:
            if self._state(row) is not ReviewState.APPROVED:
                self._refuse(row, result, "WITHDRAWAL_STATE_CONFLICT")
            self._provider_transition(row, result, ReviewState.WITHDRAWN)
            target_state = ReviewState.WITHDRAWN
        else:
            current = self._state(row)
            if current is not ReviewState.REVIEW_REQUIRED:
                self._refuse(row, result, "CONFLICTING_REDECISION")
            conflicts = self._approved_for_same_subject(row)
            if conflicts:
                if len(conflicts) != 1 or result.supersedes_candidate_id != UUID(str(conflicts[0]["id"])):
                    self._refuse(row, result, "UNNAMED_SUPERSEDE_CONFLICT")
                self._provider_transition(
                    conflicts[0],
                    result,
                    ReviewState.SUPERSEDED,
                    affected_candidate=result.supersedes_candidate_id,
                )
            elif result.supersedes_candidate_id is not None:
                self._refuse(row, result, "SUPERSEDED_CANDIDATE_NOT_APPROVED")
            self._provider_transition(row, result, ReviewState.APPROVED)
            target_state = ReviewState.APPROVED

        regeneration = self._regenerator.regenerate(
            raw_manifest_id=self._raw_manifest_id,
            adjusted_feed_id=self._adjusted_feed_id,
            approved_actions=self.approved_actions(),
            now=result.decided_at,
        )
        return DecisionOutcome(str(result.candidate_id), target_state, regeneration)

    def _provider_transition(
        self,
        row: Mapping[str, Any],
        result: ApprovalResult,
        target: ReviewState,
        *,
        affected_candidate: UUID | None = None,
    ) -> None:
        document = dict(row["terms_document"])
        entry = {
            "state": target.value,
            "decision": result.decision.value,
            "candidate_id": str(affected_candidate or result.candidate_id),
            "decided_content_hash": result.decided_content_hash,
            "evidence_bindings": list(result.evidence_bindings),
            "actor_id": str(result.actor_id),
            "audit_id": str(result.audit_id),
            "permission_id": str(result.permission_id),
            "request_schema_version": result.request_schema_version,
            "decided_at": result.decided_at.isoformat().replace("+00:00", "Z"),
            "delivery_id": str(result.delivery_id),
            "envelope_hash": result.envelope_hash,
            "aggregate_sequence": result.aggregate_sequence,
            "supersedes_candidate_id": (
                None if result.supersedes_candidate_id is None else str(result.supersedes_candidate_id)
            ),
            "rationale": result.rationale.strip(),
        }
        document["review"] = entry
        document["review_history"] = [*document.get("review_history", []), entry]
        self._catalog.upsert(CORPORATE_ACTIONS_TABLE, {**dict(row), "terms_document": document})

    def _refuse(self, row: Mapping[str, Any], result: ApprovalResult, code: str) -> None:
        raise ApprovalRefusedError(code, f"corporate-action approval refused: {code}")

    def _record_refusal(self, result: ApprovalResult, code: str) -> None:
        transaction = getattr(self._catalog, "transaction", None)
        if not callable(transaction):
            return
        with transaction() as catalog:
            rows = catalog.records(CORPORATE_ACTIONS_TABLE)
            row = next((item for item in rows if str(item["id"]) == str(result.candidate_id)), None)
            if row is None:
                return
            document = dict(row["terms_document"])
            refused = {
                "state": "REFUSED",
                "reason_code": code,
                "candidate_id": str(result.candidate_id),
                "delivery_id": str(result.delivery_id),
                "envelope_hash": result.envelope_hash,
                "audit_id": str(result.audit_id),
                "aggregate_sequence": result.aggregate_sequence,
                "recorded_at": result.decided_at.isoformat().replace("+00:00", "Z"),
            }
            document["review_history"] = [*document.get("review_history", []), refused]
            catalog.upsert(CORPORATE_ACTIONS_TABLE, {**dict(row), "terms_document": document})

    def _approved_for_same_subject(self, row: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [
            other for other in self._rows()
            if str(other["id"]) != str(row["id"])
            and self._state(other) is ReviewState.APPROVED
            and str(other["instrument_id"]) == str(row["instrument_id"])
            and str(other["action_type"]) == str(row["action_type"])
            and str(other["effective_at"]) == str(row["effective_at"])
        ]

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

    def _row_for_record_id(self, candidate_id: UUID) -> dict[str, Any]:
        for row in self._rows():
            if str(row["id"]) == str(candidate_id):
                return row
        raise UnknownCandidateError(f"no corporate-action row has canonical id {candidate_id}")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
