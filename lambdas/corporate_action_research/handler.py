"""`corporate-action-research` Lambda (D14).

Runs one scheduled research slot: for each requested ticker it asks the research
adapter for evidence, turns each finding into an immutable
:class:`~market_pipeline_lib.corporate_action_research.ResearchCandidate`, and appends
it to the review store.  Candidate identity is a content hash, so the same finding
discovered again is recognised and not duplicated.

The domain -- terms, evidence, claims, candidate identity, the review store -- lives
entirely in `market_pipeline_lib.corporate_action_research`.  This module is the AWS
edge: event validation, configuration, idempotency, and turning outcomes into a result
envelope.  It deliberately re-exports the domain's `ResearchFinding` and
`CorporateActionResearchPort` rather than declaring lookalikes, so an adapter written
against the domain plugs in here unchanged.

Event::

    {
      "researchRunId":   "car-2026-08-02T06",     # required, the idempotency key
      "slotScheduledAt": "2026-08-02T06:00:00Z",  # required, UTC ISO-8601
      "tickers":         ["AAPL", "MSFT"]         # required, non-empty, unique
    }

Partial failure
---------------
Tickers are researched independently and one ticker's failure does not abandon the
rest: every ticker is attempted, successful candidates are appended as they are found,
and the slot then fails with :class:`PartialResearchSlotError` naming exactly which
tickers failed and why.  Failing rather than returning a cheerful summary is the point
-- "3 of 5 tickers researched" reported as success is a missed corporate action, and a
missed split silently corrupts every adjusted price after it.  Retrying is safe: the
candidate store deduplicates by candidate identity, so the tickers that did succeed are
not written twice.

An unwired adapter fails immediately, because "0 candidates found" is indistinguishable
from a genuinely quiet slot.

Environment:
    CORPORATE_ACTION_CANDIDATE_STORE  required (unless a path is injected).
        Path of the append-only JSONL review store.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from apps.common.errors import (
    ConfigurationError,
    MalformedEventError,
    PipelineAppError,
    PortNotConfiguredError,
)
from apps.common.events import (
    reject_unknown_fields,
    require_identifier,
    require_mapping,
    require_sequence,
    require_utc_timestamp,
)
from apps.common.idempotency import IdempotencyStore, InMemoryIdempotencyStore
from lambdas.common import STATUS_DUPLICATE, ResultCache, lambda_result
from market_pipeline_lib.corporate_action_research import (
    CandidateStore,
    CorporateActionResearchPort,
    ResearchCandidate,
    ResearchFinding,
    ResearchScheduleExecutor,
)

LOGGER = logging.getLogger("lambdas.corporate_action_research")

HANDLER_NAME = "corporate-action-research"
EVENT_FIELDS: tuple[str, ...] = ("researchRunId", "slotScheduledAt", "tickers")
STATUS_RESEARCHED = "RESEARCHED"
CANDIDATE_STORE_VARIABLE = "CORPORATE_ACTION_CANDIDATE_STORE"
MAX_TICKERS_PER_SLOT = 500

#: Re-exported so an adapter imports one name whichever side it is written against.
__all__ = [
    "CorporateActionResearchHandler",
    "CorporateActionResearchPort",
    "PartialResearchSlotError",
    "ResearchFinding",
    "UnconfiguredResearchPort",
    "handler",
]


class PartialResearchSlotError(PipelineAppError):
    """Some tickers in a scheduled slot could not be researched.

    Carries the per-ticker failures so the invocation record names what went wrong
    rather than only that something did.
    """

    code = "PARTIAL_RESEARCH_SLOT"

    def __init__(self, failures: Mapping[str, str], *, recorded: int, attempted: int) -> None:
        detail = "; ".join(f"{ticker}: {reason}" for ticker, reason in sorted(failures.items()))
        super().__init__(
            f"{len(failures)} of {attempted} tickers failed research ({recorded} candidates "
            f"recorded before the failure): {detail}"
        )
        self.failures = dict(failures)
        self.recorded = recorded
        self.attempted = attempted


class UnconfiguredResearchPort:
    """Default adapter: refuses, loudly, naming the stage that supplies a real one.

    The domain has its own `UnconfiguredResearchPort` raising `ResearchAdapterError`;
    this one raises `PortNotConfiguredError` so an unwired *deployment* is reported in
    the same vocabulary as every other unwired port in this bundle.
    """

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        raise PortNotConfiguredError(
            "corporate-action-research has no CorporateActionResearchPort adapter. The "
            "D14 evidence-collection adapter is wired in DP7; "
            "market_pipeline_lib.corporate_action_research.AiResearchAdapter is one. "
            "Returning an empty finding list would be indistinguishable from a genuinely "
            "quiet slot, so this Lambda fails instead. Inject a "
            "CorporateActionResearchPort into CorporateActionResearchHandler to enable it."
        )


class CorporateActionResearchHandler:
    """Executes one scheduled research slot."""

    def __init__(
        self,
        *,
        research_port: CorporateActionResearchPort | None = None,
        schedule_executor: ResearchScheduleExecutor | None = None,
        candidate_store_path: Path | None = None,
        idempotency_store: IdempotencyStore | None = None,
        result_cache: ResultCache | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        if schedule_executor is not None and research_port is not None:
            raise ConfigurationError(
                "pass either a schedule_executor (it owns its own research port) or a "
                "research_port, not both: two ports would make it ambiguous which one "
                "actually ran the slot"
            )
        self._executor = schedule_executor
        self._port: CorporateActionResearchPort = research_port or UnconfiguredResearchPort()
        self._store_path = candidate_store_path
        self._environment = environment
        self._idempotency: IdempotencyStore = idempotency_store or InMemoryIdempotencyStore()
        self._results = result_cache or ResultCache()

    def handle(self, event: Any, context: Any = None) -> dict[str, Any]:
        document = require_mapping(event, "corporate-action-research event")
        reject_unknown_fields(document, EVENT_FIELDS, "corporate-action-research event")
        run_id = require_identifier(document, "researchRunId", "corporate-action-research event")
        scheduled_at = require_utc_timestamp(
            document, "slotScheduledAt", "corporate-action-research event"
        )
        tickers = self._parse_tickers(document)
        if self._executor is not None:
            return self._handle_with_executor(run_id, tickers, scheduled_at, context)
        store_path = self._resolve_store_path()

        if not self._idempotency.claim(run_id):
            remembered = self._results.recall(run_id) or {
                "reason": "researchRunId already executed by this container"
            }
            LOGGER.info("corporate_action_research.duplicate", extra={"research_run_id": run_id})
            return lambda_result(
                handler=HANDLER_NAME,
                status=STATUS_DUPLICATE,
                idempotency_key=run_id,
                context=context,
                result=remembered,
            )

        try:
            outcome = self._run_slot(CandidateStore(store_path), tickers, scheduled_at)
        except BaseException:
            # Release the claim so the redelivery is a real attempt.  Candidates already
            # appended stay appended; the store deduplicates them on the retry.
            self._idempotency.forget(run_id)
            LOGGER.error(
                "corporate_action_research.failed",
                extra={"research_run_id": run_id, "tickers": len(tickers)},
                exc_info=True,
            )
            raise

        result = {
            "slotScheduledAt": scheduled_at.isoformat().replace("+00:00", "Z"),
            "tickersResearched": len(tickers),
            "candidatesRecorded": outcome["recorded"],
            "candidatesAlreadyKnown": outcome["already_known"],
            "candidatesPerTicker": outcome["per_ticker"],
            "candidateStore": str(store_path),
            "workflowState": "REVIEW_REQUIRED",
        }
        self._results.remember(run_id, result)
        LOGGER.info("corporate_action_research.completed", extra={"research_run_id": run_id, **result})
        return lambda_result(
            handler=HANDLER_NAME,
            status=STATUS_RESEARCHED,
            idempotency_key=run_id,
            context=context,
            result=result,
        )

    # -- slot execution ---------------------------------------------------
    def _handle_with_executor(
        self,
        run_id: str,
        tickers: tuple[str, ...],
        scheduled_at: datetime,
        context: Any,
    ) -> dict[str, Any]:
        """Delegate the whole slot to DP-e's `ResearchScheduleExecutor`.

        The executor owns slot resolution, canonical `corporate_actions` persistence and
        `pipeline_runs` idempotency, so this path adds nothing but event parsing and the
        result envelope.  It is the preferred wiring wherever a catalog is available;
        the candidate-store path below exists for a deployment that has no database and
        can only collect candidates for review.
        """

        if not self._idempotency.claim(run_id):
            remembered = self._results.recall(run_id) or {
                "reason": "researchRunId already executed by this container"
            }
            return lambda_result(
                handler=HANDLER_NAME,
                status=STATUS_DUPLICATE,
                idempotency_key=run_id,
                context=context,
                result=remembered,
            )
        try:
            report = self._executor.run_due_slot(tickers, now=scheduled_at)  # type: ignore[union-attr]
        except BaseException:
            self._idempotency.forget(run_id)
            LOGGER.error(
                "corporate_action_research.failed",
                extra={"research_run_id": run_id, "tickers": len(tickers)},
                exc_info=True,
            )
            raise
        result = {
            "slotId": report.slot_id,
            "slotScheduledAt": scheduled_at.isoformat().replace("+00:00", "Z"),
            "tickersResearched": report.tickers_researched,
            "candidatesRecorded": report.candidates_recorded,
            "candidatesAlreadyKnown": report.candidates_already_known,
            "actionsPersisted": report.actions_persisted,
            "actionsAlreadyPresent": report.actions_already_present,
            "skippedAsDuplicateSlot": report.skipped_as_duplicate_slot,
            "workflowState": "REVIEW_REQUIRED",
        }
        self._results.remember(run_id, result)
        LOGGER.info(
            "corporate_action_research.completed", extra={"research_run_id": run_id, **result}
        )
        return lambda_result(
            handler=HANDLER_NAME,
            status=STATUS_DUPLICATE if report.skipped_as_duplicate_slot else STATUS_RESEARCHED,
            idempotency_key=run_id,
            context=context,
            result=result,
        )

    def _run_slot(
        self,
        store: CandidateStore,
        tickers: tuple[str, ...],
        scheduled_at: datetime,
    ) -> dict[str, Any]:
        """Attempt every ticker; raise naming the ones that failed."""

        recorded = 0
        already_known = 0
        per_ticker: dict[str, int] = {}
        failures: dict[str, str] = {}
        for ticker in tickers:
            try:
                found = 0
                for finding in self._port.research(ticker, scheduled_at):
                    candidate = ResearchCandidate.from_finding(
                        finding, ticker=ticker, researched_at=scheduled_at
                    )
                    if store.append(candidate):
                        recorded += 1
                        found += 1
                    else:
                        already_known += 1
                per_ticker[ticker] = found
            except PortNotConfiguredError:
                # Not a per-ticker problem: the deployment is unwired, and every
                # remaining ticker would fail the same way.
                raise
            except Exception as error:
                failures[ticker] = f"{type(error).__name__}: {error}"
                LOGGER.warning(
                    "corporate_action_research.ticker_failed",
                    extra={"ticker": ticker, "reason": failures[ticker]},
                    exc_info=True,
                )
        if failures:
            raise PartialResearchSlotError(failures, recorded=recorded, attempted=len(tickers))
        return {"recorded": recorded, "already_known": already_known, "per_ticker": per_ticker}

    # -- helpers ----------------------------------------------------------
    @staticmethod
    def _parse_tickers(document: Mapping[str, Any]) -> tuple[str, ...]:
        raw = require_sequence(document, "tickers", "corporate-action-research event")
        if not raw:
            raise MalformedEventError("corporate-action-research event.tickers must not be empty")
        if len(raw) > MAX_TICKERS_PER_SLOT:
            raise MalformedEventError(
                f"corporate-action-research event.tickers exceeds {MAX_TICKERS_PER_SLOT} entries"
            )
        tickers: list[str] = []
        for index, value in enumerate(raw):
            if not isinstance(value, str) or not value.strip():
                raise MalformedEventError(
                    f"corporate-action-research event.tickers[{index}] must be a non-empty string"
                )
            normalized = value.strip().upper()
            if not normalized.replace(".", "").replace("-", "").isalnum():
                raise MalformedEventError(
                    f"corporate-action-research event.tickers[{index}] is not a market symbol: "
                    f"{value!r}"
                )
            if normalized in tickers:
                raise MalformedEventError(
                    f"corporate-action-research event.tickers repeats {normalized!r}"
                )
            tickers.append(normalized)
        return tuple(tickers)

    def _resolve_store_path(self) -> Path:
        if self._store_path is not None:
            return self._store_path
        values: Mapping[str, str] = (
            os.environ if self._environment is None else self._environment
        )
        configured = values.get(CANDIDATE_STORE_VARIABLE)
        if configured is None or not configured.strip():
            raise ConfigurationError.missing(
                [CANDIDATE_STORE_VARIABLE],
                hint="path of the append-only corporate-action review store (JSONL)",
            )
        return Path(configured.strip())


#: Warm-container singleton so redelivery to the same container is idempotent.
_DEFAULT_HANDLER = CorporateActionResearchHandler()


def handler(event: Any, context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point."""

    return _DEFAULT_HANDLER.handle(event, context)
