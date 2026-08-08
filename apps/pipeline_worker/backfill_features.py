"""Prints the commands a historical feature backfill needs, or sends them.

Run it against the operating environment's catalog; it reads the database to decide what is
missing and, by default, only prints the plan.

    python -m apps.pipeline_worker.backfill_features --database-url "$PIPELINE_WORKER_DATABASE_URL"
    python -m apps.pipeline_worker.backfill_features --database-url ... --send --queue-url ...

The plan is printed before anything is sent because the decision it encodes is worth
reading: which instruments have no bars at the resolution their strategies select, whether
any span had to be split, and how much of the work is already done. ``--send`` is a
separate, explicit step, and it refuses a plan whose series would come out with holes
unless ``--allow-holes`` says that is understood.

Credentials come from the environment; none are read from or written to a file here.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_pipeline_lib.features.backfill import BackfillPlan, plan_feature_backfill
from market_pipeline_lib.features.definitions import (
    PRODUCTION_RSI_14_RESOLUTIONS,
    production_rsi_14_definition,
)
from market_pipeline_lib.features.tables import FeatureCatalog


def _instant(text: str) -> datetime:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            f"{text!r} has no timezone; this pipeline works in ET and UTC, so an instant "
            "must say which it is"
        )
    return parsed.astimezone(UTC)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill-features",
        description="Plan (and optionally send) the MATERIALIZE_FEATURE_OUTPUT commands "
                    "needed so every strategy clock has its indicator series.",
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="PostgreSQL URL of the catalog to read. Nothing is written to it.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("."),
        help="Local artifact root the catalog requires; unused for reads.",
    )
    parser.add_argument(
        "--resolution",
        action="append",
        choices=list(PRODUCTION_RSI_14_RESOLUTIONS),
        help="Limit to these strategy clocks. Repeatable. Default: all four.",
    )
    parser.add_argument(
        "--instrument",
        action="append",
        help="Limit to these instrument ids. Repeatable. Default: every covered instrument.",
    )
    parser.add_argument("--from", dest="period_start", type=_instant, help="Clamp the span's start.")
    parser.add_argument("--through", dest="period_end", type=_instant, help="Clamp the span's end.")
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually enqueue the planned commands. Without this, only the plan is printed.",
    )
    parser.add_argument("--queue-url", help="SQS queue URL. Required with --send.")
    parser.add_argument(
        "--allow-holes",
        action="store_true",
        help="Permit sending a plan whose spans had to be split, accepting the warm-up "
             "gap at each seam.",
    )
    return parser


def _summary(plan: BackfillPlan) -> dict[str, Any]:
    return {
        "commands": len(plan.commands),
        "alreadyMaterialized": len(plan.satisfied),
        "warnings": [
            {
                "code": item.code,
                "instrumentId": item.instrument_id,
                "resolution": item.resolution,
                "detail": item.detail,
            }
            for item in plan.warnings
        ],
        "hasHoles": plan.has_holes,
    }


def run(
    arguments: argparse.Namespace,
    *,
    catalog: FeatureCatalog,
    send: Callable[[str, str], None] | None = None,
) -> int:
    """Plan against ``catalog`` and, when asked, hand each message to ``send``.

    The catalog and the transport are parameters rather than things this builds, so every
    decision below -- what to plan, whether to refuse, what to enqueue -- is exercised by
    tests without a database or a queue. :func:`main` supplies the real ones.
    """
    resolutions = arguments.resolution or list(PRODUCTION_RSI_14_RESOLUTIONS)
    plan = plan_feature_backfill(
        catalog,
        [production_rsi_14_definition(item) for item in resolutions],
        instrument_ids=arguments.instrument,
        period_start=arguments.period_start,
        period_end=arguments.period_end,
    )

    print(json.dumps({"plan": _summary(plan), "messages": plan.messages()}, indent=2))

    if not arguments.send:
        return 0
    if plan.has_holes and not arguments.allow_holes:
        print(
            "refusing to send: at least one span had to be split, so the series would have "
            "a warm-up gap at each seam. Re-run with --allow-holes if that is intended.",
            file=sys.stderr,
        )
        return 1
    if not plan.commands:
        print("nothing to send", file=sys.stderr)
        return 0
    if send is None:  # pragma: no cover - main always supplies one
        raise ValueError("sending requires a transport")

    for command in plan.commands:
        send(arguments.queue_url, json.dumps(command.message()))
    print(f"sent {len(plan.commands)} commands", file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.send and not arguments.queue_url:
        print("--send requires --queue-url", file=sys.stderr)
        return 2
    return run(
        arguments,
        catalog=_catalog(arguments),
        send=_sqs_sender() if arguments.send else None,
    )


def _catalog(arguments: argparse.Namespace) -> FeatureCatalog:  # pragma: no cover - needs a database
    # Imported here so planning stays importable without the database extras installed.
    from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy

    return PostgresCatalog.connect(
        arguments.database_url,
        artifact_root=arguments.artifact_root,
        storage_objects=StorageObjectsPolicy.READ_ONLY,
    )


def _sqs_sender() -> Callable[[str, str], None]:  # pragma: no cover - needs a queue
    import boto3

    queue = boto3.client("sqs")

    def send(queue_url: str, body: str) -> None:
        queue.send_message(QueueUrl=queue_url, MessageBody=body)

    return send


if __name__ == "__main__":  # pragma: no cover - console entry point
    raise SystemExit(main())
