"""Backend outbox-relay consumer for corporate-action approval results."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .decisions import ApprovalResult
from .service import CorporateActionReviewService


class BackendRelayApprovalConsumer:
    """Parse the canonical relay payload and apply it through the verified service."""

    def __init__(self, service: CorporateActionReviewService) -> None:
        self._service = service

    def apply(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        result = ApprovalResult.from_mapping(payload)
        outcome = self._service.apply_approval_result(result)
        return {
            "candidateId": outcome.candidate_id,
            "state": outcome.state.value,
            "regenerated": outcome.regeneration is not None,
        }
