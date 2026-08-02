"""`lightweight-validation` Lambda.

Cheap, no-I/O contract validation of a single pipeline document, so a malformed
manifest is caught before the worker spends minutes on it.

Event::

    {
      "validationId": "val-0001",          # required, the idempotency key
      "documentType": "dataset-manifest",  # required, one of DOCUMENT_TYPES
      "document":     { ... }              # required, the document to validate
    }

Both document types are validated **today**:

`dataset-manifest`
    by the canonical `market_pipeline_lib.compatibility.validate_dataset_manifest`.

`feature-snapshot`
    by `market_pipeline_lib.features.FeatureSnapshotValidator` (D13).  It is stateless
    -- no database, no object store -- which is what makes it appropriate for a
    "lightweight" Lambda, and it cross-checks the document's
    `feature_materialization_version` against the three hashes in the same document, so
    a version string copied from a different batch is caught here rather than by the
    backtest engine hours later.

A contract violation is a legitimate outcome and is returned as ``decision: REJECTED``
with the reason: retrying it would produce the same answer, so failing the invocation
would only burn redeliveries.  A malformed *event*, an unwired port, or an
infrastructure fault raises, so the invocation is recorded as failed and the message is
retried or dead-lettered rather than silently accepted.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from apps.common.events import (
    reject_unknown_fields,
    require_enum,
    require_field,
    require_identifier,
    require_mapping,
)
from apps.common.idempotency import IdempotencyStore, InMemoryIdempotencyStore
from lambdas.common import STATUS_DUPLICATE, ResultCache, lambda_result
from market_pipeline_lib.features import FeatureSnapshotValidator

LOGGER = logging.getLogger("lambdas.lightweight_validation")

HANDLER_NAME = "lightweight-validation"
EVENT_FIELDS: tuple[str, ...] = ("validationId", "documentType", "document")
DOCUMENT_TYPES: tuple[str, ...] = ("dataset-manifest", "feature-snapshot")
STATUS_VALIDATED = "VALIDATED"

DECISION_ACCEPTED = "ACCEPTED"
DECISION_REJECTED = "REJECTED"


@runtime_checkable
class FeatureValidationPort(Protocol):
    """Port for D13 feature-snapshot validation.

    Satisfied by `market_pipeline_lib.features.FeatureSnapshotValidator`, which is the
    default.  It stays a port so a deployment that wants the *stateful* check as well --
    "and does this batch actually exist in `feature_snapshot_batches`" -- can substitute
    an adapter that reaches the catalog, without this module growing a database
    dependency it does not need for the cheap path.
    """

    def validate(self, document: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return a decision document for one feature snapshot."""


class LightweightValidationHandler:
    """Validates one document and reports an explicit decision."""

    def __init__(
        self,
        *,
        feature_port: FeatureValidationPort | None = None,
        idempotency_store: IdempotencyStore | None = None,
        result_cache: ResultCache | None = None,
    ) -> None:
        self._feature_port: FeatureValidationPort = feature_port or FeatureSnapshotValidator()
        self._idempotency: IdempotencyStore = idempotency_store or InMemoryIdempotencyStore()
        self._results = result_cache or ResultCache()

    def handle(self, event: Any, context: Any = None) -> dict[str, Any]:
        document_event = require_mapping(event, "lightweight-validation event")
        reject_unknown_fields(document_event, EVENT_FIELDS, "lightweight-validation event")
        validation_id = require_identifier(
            document_event, "validationId", "lightweight-validation event"
        )
        document_type = require_enum(
            document_event, "documentType", DOCUMENT_TYPES, "lightweight-validation event"
        )
        document = require_mapping(
            require_field(document_event, "document", "lightweight-validation event"),
            "lightweight-validation event.document",
        )

        if not self._idempotency.claim(validation_id):
            remembered = self._results.recall(validation_id) or {
                "reason": "validationId already decided by this container"
            }
            LOGGER.info(
                "lightweight_validation.duplicate", extra={"validation_id": validation_id}
            )
            return lambda_result(
                handler=HANDLER_NAME,
                status=STATUS_DUPLICATE,
                idempotency_key=validation_id,
                context=context,
                result=remembered,
            )

        try:
            if document_type == "dataset-manifest":
                result = self._validate_dataset_manifest(document)
            else:
                result = dict(self._feature_port.validate(document))
        except Exception:
            # Release the claim: an unwired port or an infrastructure fault must
            # not make the retry look like a duplicate.
            self._idempotency.forget(validation_id)
            LOGGER.error(
                "lightweight_validation.failed",
                extra={"validation_id": validation_id, "document_type": document_type},
                exc_info=True,
            )
            raise

        self._results.remember(validation_id, result)
        LOGGER.info(
            "lightweight_validation.decided",
            extra={
                "validation_id": validation_id,
                "document_type": document_type,
                "decision": result.get("decision"),
            },
        )
        return lambda_result(
            handler=HANDLER_NAME,
            status=STATUS_VALIDATED,
            idempotency_key=validation_id,
            context=context,
            result=result,
        )

    @staticmethod
    def _validate_dataset_manifest(document: Mapping[str, Any]) -> dict[str, Any]:
        from market_pipeline_lib.compatibility import (
            ContractValidationError,
            validate_dataset_manifest,
        )

        try:
            validate_dataset_manifest(document)
        except ContractValidationError as error:
            return {
                "documentType": "dataset-manifest",
                "decision": DECISION_REJECTED,
                "manifestId": document.get("manifest_id"),
                "violation": str(error),
            }
        return {
            "documentType": "dataset-manifest",
            "decision": DECISION_ACCEPTED,
            "manifestId": document.get("manifest_id"),
            "datasetId": document.get("dataset_id"),
            "revision": document.get("revision"),
            "datasetHash": document.get("dataset_hash"),
        }


#: Warm-container singleton so redelivery to the same container is idempotent.
_DEFAULT_HANDLER = LightweightValidationHandler()


def handler(event: Any, context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point."""

    return _DEFAULT_HANDLER.handle(event, context)
