"""`corporate-action-decision` -- apply one administrator decision.

Exit codes (spec 1: a CLI must never report success it did not achieve):

===  ==========================================================================
0    the decision was applied; an approval also regenerated the adjusted dataset
1    the decision could not be applied (unknown candidate, conflicting decision,
     missing manifest, or no object-store wiring for an approval)
2    the arguments themselves were unusable
===  ==========================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from ..catalog import LocalCatalog
from .decisions import AdminDecision, DecisionType
from .regeneration import AdjustedBarWriter, AdjustedDatasetRegenerator, RawBarReader
from .service import CorporateActionReviewService

__all__ = ["main"]

LOGGER = logging.getLogger("market_pipeline_lib.corporate_actions.cli")

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_BAD_ARGUMENTS = 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corporate-action-decision",
        description="Apply an administrator's decision to a researched corporate action.",
    )
    parser.add_argument("--catalog-root", required=True, help="LocalCatalog root directory")
    parser.add_argument("--candidate-id", required=True, help="researched candidate id")
    parser.add_argument(
        "--decision",
        required=True,
        choices=[item.value for item in DecisionType],
        help="APPROVE or REJECT",
    )
    parser.add_argument("--decided-by", required=True, help="the administrator's identity")
    parser.add_argument("--decided-at", required=True, help="UTC ISO-8601 decision instant")
    parser.add_argument("--rationale", required=True, help="why the decision was made")
    parser.add_argument(
        "--raw-manifest-id", required=True, help="raw revision an approval rebuilds from"
    )
    parser.add_argument(
        "--adjusted-feed-id", required=True, help="feed the adjusted dataset belongs to"
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    reader: RawBarReader | None = None,
    writer: AdjustedBarWriter | None = None,
) -> int:
    """Apply one decision. Returns the process exit code; never raises."""
    try:
        arguments = _parser().parse_args(argv)
    except SystemExit as exc:  # argparse already reported the problem
        return EXIT_BAD_ARGUMENTS if exc.code else EXIT_OK

    decision_type = DecisionType(arguments.decision)

    try:
        decided_at = datetime.fromisoformat(arguments.decided_at.replace("Z", "+00:00"))
    except ValueError:
        LOGGER.error(
            "corporate_action_decision.bad_timestamp value=%r", arguments.decided_at
        )
        return EXIT_BAD_ARGUMENTS

    try:
        decision = AdminDecision(
            candidate_id=arguments.candidate_id,
            decision=decision_type,
            decided_by=arguments.decided_by,
            decided_at=decided_at,
            rationale=arguments.rationale,
        )
    except ValueError as exc:
        LOGGER.error("corporate_action_decision.invalid_decision error=%s", exc)
        return EXIT_BAD_ARGUMENTS

    catalog = LocalCatalog(Path(arguments.catalog_root))
    regenerator = (
        AdjustedDatasetRegenerator(catalog=catalog, reader=reader, writer=writer)
        if reader is not None and writer is not None
        else None
    )
    service = CorporateActionReviewService(
        catalog=catalog,
        regenerator=regenerator,
        raw_manifest_id=arguments.raw_manifest_id,
        adjusted_feed_id=arguments.adjusted_feed_id,
    )

    try:
        outcome = service.apply_decision(decision)
    except (LookupError, ValueError, RuntimeError) as exc:
        LOGGER.error(
            "corporate_action_decision.refused candidate=%s error=%s: %s",
            decision.candidate_id,
            type(exc).__name__,
            exc,
        )
        return EXIT_REFUSED

    summary = {
        "candidateId": outcome.candidate_id,
        "state": outcome.state.value,
        "decidedBy": decision.decided_by,
        "regeneratedManifestId": (
            None if outcome.regeneration is None else outcome.regeneration.manifest_id
        ),
        "revisionNumber": (
            None if outcome.regeneration is None else outcome.regeneration.revision_number
        ),
        "newRevisionPublished": (
            False if outcome.regeneration is None else outcome.regeneration.created
        ),
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
