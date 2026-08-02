"""Stateless validation of a `feature-snapshot` document.

This is what the `lightweight-validation` Lambda calls: no database, no object store,
no network -- only whether the document in front of it is internally coherent.  It is
worth having as a cheap gate precisely because the expensive checks are expensive.

The interesting check is the cross-check.  COM06 types `feature_materialization_version`
as a non-empty string, so a schema validator alone accepts
``"feature-materialization-v1"`` -- a label that pins nothing, which is how the two
sides of that contract drifted in the first place.  Here the version string is parsed
and then verified against the three hashes in the same document, so a version copied
from a different batch is caught rather than carried forward.

A document that fails is a **decision**, not an exception: "this snapshot is not
usable" is a valid answer to "is this snapshot usable", and retrying it would produce
the same answer.  Only a caller error (a non-mapping) raises.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .hashing import is_sha256_hex, parse_feature_materialization_version

__all__ = [
    "DECISION_ACCEPTED",
    "DECISION_REJECTED",
    "REQUIRED_FIELDS",
    "FeatureSnapshotValidator",
]


DECISION_ACCEPTED = "ACCEPTED"
DECISION_REJECTED = "REJECTED"

#: A snapshot is only consumable when it is sealed, so `status` is required and the
#: only accepted value is SUCCEEDED.
CONSUMABLE_STATUS = "SUCCEEDED"

#: Mirrors `feature_snapshot_batch_success_complete`: a sealed batch carries all of
#: these, so a document that omits one is not describing a sealed batch.
REQUIRED_FIELDS: tuple[str, ...] = (
    "feature_materialization_version",
    "feature_set_hash",
    "input_market_set_hash",
    "batch_hash",
    "period_start",
    "period_end",
    "row_count",
    "snapshot_object_id",
    "status",
)

_HASH_FIELDS: tuple[str, ...] = ("feature_set_hash", "input_market_set_hash", "batch_hash")


class FeatureSnapshotValidator:
    """Decides whether one feature-snapshot document may be consumed."""

    def validate(self, document: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(document, Mapping):
            raise TypeError(f"a feature-snapshot document must be a mapping, got {type(document).__name__}")
        violation = self._violation(document)
        decision: dict[str, Any] = {
            "documentType": "feature-snapshot",
            "decision": DECISION_REJECTED if violation else DECISION_ACCEPTED,
            "featureMaterializationVersion": document.get("feature_materialization_version"),
            "batchHash": document.get("batch_hash"),
        }
        if violation:
            decision["violation"] = violation
        else:
            decision["featureSetHash"] = document["feature_set_hash"]
            decision["inputMarketSetHash"] = document["input_market_set_hash"]
            decision["rowCount"] = document.get("row_count")
        return decision

    # -- checks ------------------------------------------------------------------------

    def _violation(self, document: Mapping[str, Any]) -> str | None:
        missing = [field for field in REQUIRED_FIELDS if field not in document]
        if missing:
            return f"missing required field(s): {missing}"

        for field in _HASH_FIELDS:
            if not is_sha256_hex(document[field]):
                return f"{field} must be 64 lowercase hex characters, got {document[field]!r}"

        status = document["status"]
        if status != CONSUMABLE_STATUS:
            return (
                f"status is {status!r}; only a {CONSUMABLE_STATUS} snapshot batch is a "
                "consistent point-in-time set, a partially materialized one is not consumable"
            )

        period = self._period_violation(document)
        if period is not None:
            return period

        row_count = document["row_count"]
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 1:
            # `feature_snapshot_batch_row_count_positive`: an empty snapshot is a data
            # gap, not a snapshot.
            return f"row_count must be a positive integer, got {row_count!r}"

        member_count = document.get("member_count")
        if member_count is not None:
            if isinstance(member_count, bool) or not isinstance(member_count, int) or member_count < 1:
                return f"member_count must be a positive integer, got {member_count!r}"

        try:
            parsed = parse_feature_materialization_version(document["feature_materialization_version"])
        except ValueError as error:
            return str(error)
        if not parsed.is_batch:
            return (
                "feature_materialization_version has scope "
                f"{parsed.scope!r}; a feature-snapshot document must pin a batch (scope 'b')"
            )
        if not parsed.matches(
            identity=document["feature_set_hash"],
            inputs=document["input_market_set_hash"],
            result=document["batch_hash"],
        ):
            return (
                "feature_materialization_version "
                f"{document['feature_materialization_version']!r} was not rendered from this "
                "document's feature_set_hash / input_market_set_hash / batch_hash"
            )
        return None

    @staticmethod
    def _period_violation(document: Mapping[str, Any]) -> str | None:
        start = document["period_start"]
        end = document["period_end"]
        for label, value in (("period_start", start), ("period_end", end)):
            if not isinstance(value, str) or not value.endswith("Z"):
                return f"{label} must be an ISO-8601 UTC timestamp ending in Z, got {value!r}"
        if end <= start:
            # Both are canonical `...Z` renderings, so lexicographic order is chronological.
            return f"period_end {end!r} must be after period_start {start!r}"
        return None
