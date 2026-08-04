"""Backend outbox-relay consumer for corporate-action approval results."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .decisions import ApprovalResult
from .service import CorporateActionReviewService


class BackendRelayApprovalConsumer:
    """Parse the canonical relay payload and apply it through the verified service."""

    def __init__(self, service: CorporateActionReviewService, *, catalog: Any | None = None) -> None:
        self._service = service
        self._catalog = catalog

    def apply(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        result = ApprovalResult.from_mapping(payload)
        outcome = self._service.apply_approval_result(result)
        return {
            "candidateId": outcome.candidate_id,
            "state": outcome.state.value,
            "regenerated": outcome.regeneration is not None,
        }

    def prepare(self) -> None:
        verify = getattr(self._catalog, "verify_schema", None)
        if callable(verify):
            verify()

    def request_stop(self, reason: str) -> None:
        close = getattr(self._catalog, "close", None)
        if callable(close):
            close()
