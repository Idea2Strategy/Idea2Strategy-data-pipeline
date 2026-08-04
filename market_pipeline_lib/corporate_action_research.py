"""D14 -- corporate-action research: adapter, schedule executor, persistence.

What the audit found here was a dataclass validator on `origin/develop` with no
research adapter, no schedule executor, and nothing ever reaching the canonical
`market_data.corporate_actions` table.  The validator itself was sound, so it is
kept; the three missing halves are added around it:

* :class:`CorporateActionResearchPort` -- the adapter seam.  Evidence collection
  is behind an interface, so tests drive a deterministic fake and no test ever
  performs network I/O.  :class:`AiResearchAdapter` is the production shape: a
  language model is injected as a plain callable, and its output is parsed
  strictly rather than trusted.
* :class:`ResearchScheduleExecutor` -- runs one due slot, is idempotent across
  re-runs at both the slot level (the canonical `market_data.pipeline_runs`
  ledger) and the row level (the canonical unique index on
  `(source_manifest_id, provider_event_key)`).
* :func:`corporate_action_record` -- the canonical row, whose `terms_document`
  carries the full provenance: every piece of evidence, every individual claim
  with the source that substantiates it, and a confidence for each.

Nothing in this module approves anything.  A researched action is written in the
`REVIEW_REQUIRED` state and has no effect on any dataset until an administrator
decides on it; that is :mod:`market_pipeline_lib.corporate_actions`.

Persistence is SQLAlchemy Core only, reached through the narrow
:class:`ResearchCatalog` protocol that both `LocalCatalog` and `PostgresCatalog`
already satisfy.  This module authors no DDL.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlparse

from .contracts import ET, deterministic_uuid

__all__ = [
    "AiResearchAdapter",
    "CandidateStore",
    "CashDividendTerms",
    "CatalogInstrumentResolver",
    "CatalogSourceManifestResolver",
    "Claim",
    "ConflictingResearchError",
    "CorporateActionResearchPort",
    "CorporateActionTerms",
    "Evidence",
    "EvidenceFetcher",
    "InstrumentResolver",
    "ResearchAdapterError",
    "ResearchCandidate",
    "ResearchCatalog",
    "ResearchFinding",
    "ResearchModel",
    "ResearchRunReport",
    "ResearchScheduleExecutor",
    "ResearchSlot",
    "SourceCitation",
    "SourceManifestResolver",
    "SplitTerms",
    "TERMS_BY_EVENT_TYPE",
    "TwiceDailySchedule",
    "UnconfiguredEvidenceFetcher",
    "UnconfiguredResearchPort",
    "UnknownInstrumentError",
    "UnknownSourceManifestError",
    "build_research_prompt",
    "corporate_action_record",
    "parse_terms",
]

LOGGER = logging.getLogger(__name__)

CORPORATE_ACTIONS_TABLE = "market_data.corporate_actions"
PIPELINE_RUNS_TABLE = "market_data.pipeline_runs"
INSTRUMENT_SYMBOLS_TABLE = "market_data.instrument_symbols"

#: Identifies this executor in the canonical pipeline-run ledger.
PIPELINE_CODE = "corporate-action-research"
PIPELINE_VERSION = "d14.1.0"
IDEMPOTENCY_PREFIX = "corporate-action-research"

#: Workflow states a researched action can be in.  Research only ever writes the
#: first one; the other two are written by the admin decision path in D15.
REVIEW_REQUIRED = "REVIEW_REQUIRED"

#: Confidences are compared and rendered at four decimal places so that a
#: threshold test and a stored document can never disagree about a boundary.
CONFIDENCE_EXPONENT = Decimal("0.0001")

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]*$")
_EVENT_TYPE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


def _require_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _require_public_uri(value: str, field_name: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must be a public HTTP(S) URI")


# ======================================================================================
# Terms
# ======================================================================================
class CorporateActionTerms(ABC):
    """The economic substance of a corporate action.

    Terms are required, not optional: an action without them cannot be applied
    to a price series, so a candidate that lacks them would be an unactionable
    row that only looks like progress.
    """

    #: The canonical `market_data.corporate_actions.action_type` value.
    event_type: str

    @abstractmethod
    def claim_fields(self) -> dict[str, str]:
        """Canonical string form of every field an evidence claim must cover."""

    def to_document(self) -> dict[str, str]:
        return {"event_type": self.event_type, **self.claim_fields()}


@dataclass(frozen=True)
class SplitTerms(CorporateActionTerms):
    """`to_shares` new shares are issued for every `from_shares` old shares."""

    from_shares: int
    to_shares: int

    event_type = "STOCK_SPLIT"

    def __post_init__(self) -> None:
        for name in ("from_shares", "to_shares"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"split {name} must be an integer")
            if value <= 0:
                raise ValueError(f"split {name} must be positive")
        if self.from_shares == self.to_shares:
            raise ValueError("a 1-for-1 split is a no-op and is not a corporate action")

    def claim_fields(self) -> dict[str, str]:
        return {"from_shares": str(self.from_shares), "to_shares": str(self.to_shares)}


@dataclass(frozen=True)
class CashDividendTerms(CorporateActionTerms):
    """A cash distribution of `amount` per share, in `currency`."""

    amount: Decimal
    currency: str

    event_type = "CASH_DIVIDEND"

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise ValueError("dividend amount must be a Decimal")
        if self.amount <= 0:
            raise ValueError("dividend amount must be positive")
        if not _CURRENCY_PATTERN.fullmatch(self.currency):
            raise ValueError("dividend currency must be an ISO 4217 alpha-3 code")

    def claim_fields(self) -> dict[str, str]:
        return {"amount": str(self.amount), "currency": self.currency}


#: Every corporate-action type this pipeline can research *and* adjust for.  An
#: event type absent from here is refused rather than stored unactionable.
TERMS_BY_EVENT_TYPE: dict[str, type[CorporateActionTerms]] = {
    SplitTerms.event_type: SplitTerms,
    CashDividendTerms.event_type: CashDividendTerms,
}


def parse_terms(event_type: str, payload: Mapping[str, Any]) -> CorporateActionTerms:
    """Build terms from an untrusted mapping, refusing anything unsupported."""
    terms_type = TERMS_BY_EVENT_TYPE.get(event_type)
    if terms_type is None:
        raise ValueError(
            f"unsupported event_type {event_type!r}; supported: {sorted(TERMS_BY_EVENT_TYPE)}"
        )
    if terms_type is SplitTerms:
        try:
            return SplitTerms(
                from_shares=int(payload["from_shares"]),
                to_shares=int(payload["to_shares"]),
            )
        except KeyError as exc:
            raise ValueError(f"split terms are missing {exc.args[0]!r}") from exc
        except (TypeError, ValueError) as exc:
            raise ValueError(f"split terms are not integral: {exc}") from exc
    try:
        return CashDividendTerms(
            amount=Decimal(str(payload["amount"])),
            currency=str(payload["currency"]),
        )
    except KeyError as exc:
        raise ValueError(f"dividend terms are missing {exc.args[0]!r}") from exc
    except InvalidOperation as exc:
        raise ValueError("dividend amount is not a decimal number") from exc


# ======================================================================================
# Evidence and claims
# ======================================================================================
@dataclass(frozen=True)
class SourceCitation:
    """A source a research model *claims* substantiates a finding.

    This is the whole of what a model is trusted to assert about a source: where
    it is and what it is called.  It deliberately carries no integrity values --
    see :class:`Evidence` for why.
    """

    source_uri: str
    source_title: str

    def __post_init__(self) -> None:
        _require_public_uri(self.source_uri, "citation source_uri")
        if not self.source_title.strip():
            raise ValueError("citation source_title is required")


@runtime_checkable
class EvidenceFetcher(Protocol):
    """Retrieves a cited source so its integrity values can be *derived*.

    The seam exists so this module performs no network I/O and its tests drive a
    deterministic fake, matching :class:`CorporateActionResearchPort`.  The
    concrete HTTP implementation belongs to the deployment wiring, not here.
    """

    def fetch(self, source_uri: str) -> tuple[bytes, datetime]:
        """Return the exact response bytes and the UTC instant they arrived."""


class UnconfiguredEvidenceFetcher:
    """Default fetcher: refuses rather than letting unverified evidence through."""

    def fetch(self, source_uri: str) -> tuple[bytes, datetime]:
        raise ResearchAdapterError(
            "no EvidenceFetcher is configured, so a cited source cannot be retrieved "
            "and its content hash cannot be derived. Storing a candidate whose evidence "
            "was never fetched would record provenance that nothing can re-derive, so "
            "this refuses instead. Inject an EvidenceFetcher."
        )


@dataclass(frozen=True)
class Evidence:
    """A reproducible pointer to a public research source.

    `content_sha256` and `retrieved_at` are **derived by this pipeline from bytes
    it actually received** -- never asserted by a research model.  A model cannot
    hash a document it did not fetch, so a model-supplied hash is fabricated:
    syntactically valid, unfalsifiable, and indistinguishable from a real one.
    That would make the provenance record look verifiable while substantiating
    nothing, and an administrator approving the action would be approving a hash
    no one can re-derive.  Construct via :meth:`from_fetched`.
    """

    source_uri: str
    source_title: str
    content_sha256: str
    retrieved_at: datetime

    @classmethod
    def from_fetched(
        cls,
        citation: SourceCitation,
        *,
        content: bytes,
        retrieved_at: datetime,
    ) -> Evidence:
        """Derive evidence from a citation and the bytes actually retrieved for it."""
        if not isinstance(content, bytes):
            raise ValueError("evidence content must be the retrieved bytes")
        if not content:
            raise ValueError(
                f"{citation.source_uri} returned no content; a source that cannot be "
                "read does not substantiate a claim"
            )
        return cls(
            source_uri=citation.source_uri,
            source_title=citation.source_title,
            content_sha256=hashlib.sha256(content).hexdigest(),
            retrieved_at=retrieved_at,
        )

    def __post_init__(self) -> None:
        _require_public_uri(self.source_uri, "evidence source_uri")
        if not self.source_title.strip():
            raise ValueError("evidence source_title is required")
        if not _SHA256_PATTERN.fullmatch(self.content_sha256):
            raise ValueError("evidence content_sha256 must be 64 lowercase hex characters")
        _require_utc(self.retrieved_at, "evidence retrieved_at")

    def to_record(self) -> dict[str, str]:
        return {
            "source_uri": self.source_uri,
            "source_title": self.source_title.strip(),
            "content_sha256": self.content_sha256,
            "retrieved_at": _format_utc(self.retrieved_at),
        }


@dataclass(frozen=True)
class Claim:
    """One asserted fact, the source that substantiates it, and how sure we are.

    This is the unit that makes "the source of each claim" true rather than
    decorative: a candidate is rejected unless *every* material field carries a
    claim, and every claim cites evidence the candidate actually holds.
    """

    field: str
    value: str
    source_uri: str
    confidence: Decimal

    def __post_init__(self) -> None:
        if not self.field.strip():
            raise ValueError("claim field is required")
        if not self.value.strip():
            raise ValueError("claim value is required")
        _require_public_uri(self.source_uri, "claim source_uri")
        if not isinstance(self.confidence, Decimal):
            raise ValueError("claim confidence must be a Decimal")
        if not Decimal(0) < self.confidence <= Decimal(1):
            raise ValueError("claim confidence must be in (0, 1]")

    @property
    def quantized_confidence(self) -> Decimal:
        return self.confidence.quantize(CONFIDENCE_EXPONENT)

    def to_record(self) -> dict[str, str]:
        return {
            "field": self.field,
            "value": self.value,
            "source_uri": self.source_uri,
            "confidence": str(self.quantized_confidence),
        }


# ======================================================================================
# Candidate
# ======================================================================================
@dataclass(frozen=True)
class ResearchCandidate:
    """An unapproved corporate-action proposal awaiting administrator review."""

    candidate_id: str
    ticker: str
    event_type: str
    proposed_date: date
    terms: CorporateActionTerms
    evidence: tuple[Evidence, ...]
    claims: tuple[Claim, ...]
    researched_at: datetime

    def __post_init__(self) -> None:
        if not _TICKER_PATTERN.fullmatch(self.ticker):
            raise ValueError("ticker must be a normalized market symbol")
        if not _EVENT_TYPE_PATTERN.fullmatch(self.event_type):
            raise ValueError("event_type must be a normalized identifier")
        if type(self.proposed_date) is not date:
            raise ValueError("proposed_date must be a date")
        _require_utc(self.researched_at, "researched_at")
        if not self.evidence:
            raise ValueError("at least one evidence source is required")
        if len(set(self.evidence)) != len(self.evidence):
            raise ValueError("duplicate evidence is not allowed")
        if self.terms.event_type != self.event_type:
            raise ValueError(
                f"event_type {self.event_type!r} disagrees with the terms, which describe "
                f"{self.terms.event_type!r}"
            )
        self._validate_claims()
        expected_id = self._identity(self.to_identity_record())
        if self.candidate_id != expected_id:
            raise ValueError("candidate_id does not match the candidate identity")

    def _validate_claims(self) -> None:
        if not self.claims:
            raise ValueError("at least one evidence claim is required")
        carried = {item.source_uri for item in self.evidence}
        for claim in self.claims:
            if claim.source_uri not in carried:
                raise ValueError(
                    f"claim {claim.field!r} cites {claim.source_uri!r}, which is not among "
                    "the evidence this candidate carries"
                )
        expected = self._material_fields()
        claimed = {claim.field: claim.value for claim in self.claims}
        missing = sorted(set(expected) - set(claimed))
        if missing:
            raise ValueError(f"unclaimed material field(s): {missing}")
        for field, value in expected.items():
            if claimed[field] != value:
                raise ValueError(
                    f"claim {field!r}={claimed[field]!r} disagrees with the candidate "
                    f"value {value!r}"
                )

    def _material_fields(self) -> dict[str, str]:
        return {
            "event_type": self.event_type,
            "effective_date": self.proposed_date.isoformat(),
            **self.terms.claim_fields(),
        }

    @property
    def confidence(self) -> Decimal:
        """The weakest link: a candidate is only as sure as its least-sure claim."""
        return min(claim.quantized_confidence for claim in self.claims)

    @classmethod
    def create(
        cls,
        *,
        ticker: str,
        event_type: str,
        proposed_date: date,
        terms: CorporateActionTerms,
        evidence: Iterable[Evidence],
        claims: Iterable[Claim],
        researched_at: datetime,
    ) -> ResearchCandidate:
        normalized_ticker = ticker.strip().upper()
        normalized_event_type = event_type.strip().upper()
        if not _TICKER_PATTERN.fullmatch(normalized_ticker):
            raise ValueError("ticker must be a non-empty normalized market symbol")
        if not _EVENT_TYPE_PATTERN.fullmatch(normalized_event_type):
            raise ValueError("event_type must be a non-empty normalized identifier")
        if type(proposed_date) is not date:
            raise ValueError("proposed_date must be a date")
        _require_utc(researched_at, "researched_at")

        normalized_evidence = tuple(
            sorted(
                evidence,
                key=lambda item: (item.source_uri, item.content_sha256, item.retrieved_at),
            )
        )
        if not normalized_evidence:
            raise ValueError("at least one evidence source is required")
        normalized_claims = tuple(
            sorted(claims, key=lambda item: (item.field, item.source_uri, item.value))
        )
        draft = object.__new__(cls)
        object.__setattr__(draft, "candidate_id", "")
        object.__setattr__(draft, "ticker", normalized_ticker)
        object.__setattr__(draft, "event_type", normalized_event_type)
        object.__setattr__(draft, "proposed_date", proposed_date)
        object.__setattr__(draft, "terms", terms)
        object.__setattr__(draft, "evidence", normalized_evidence)
        object.__setattr__(draft, "claims", normalized_claims)
        object.__setattr__(draft, "researched_at", researched_at)
        candidate_id = cls._identity(draft.to_identity_record())
        return cls(
            candidate_id=candidate_id,
            ticker=normalized_ticker,
            event_type=normalized_event_type,
            proposed_date=proposed_date,
            terms=terms,
            evidence=normalized_evidence,
            claims=normalized_claims,
            researched_at=researched_at,
        )

    @classmethod
    def from_finding(
        cls,
        finding: ResearchFinding,
        *,
        ticker: str,
        researched_at: datetime,
    ) -> ResearchCandidate:
        """Adapter output -> reviewable candidate.  The seam DP-d's Lambda uses."""
        return cls.create(
            ticker=ticker,
            event_type=finding.event_type,
            proposed_date=finding.proposed_date,
            terms=finding.terms,
            evidence=finding.evidence,
            claims=finding.claims,
            researched_at=researched_at,
        )

    def to_identity_record(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "event_type": self.event_type,
            "proposed_date": self.proposed_date.isoformat(),
            "terms": self.terms.to_document(),
            "researched_at": _format_utc(self.researched_at),
            "evidence": [item.to_record() for item in self.evidence],
            "claims": [item.to_record() for item in self.claims],
        }

    @staticmethod
    def _identity(identity_payload: dict[str, Any]) -> str:
        return _sha256(identity_payload)

    def to_record(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "ticker": self.ticker,
            "event_type": self.event_type,
            "proposed_date": self.proposed_date.isoformat(),
            "terms": self.terms.to_document(),
            "researched_at": _format_utc(self.researched_at),
            "workflow_state": REVIEW_REQUIRED,
            "confidence": str(self.confidence),
            "evidence": [item.to_record() for item in self.evidence],
            "claims": [item.to_record() for item in self.claims],
        }


# ======================================================================================
# Research adapter
# ======================================================================================
class ResearchAdapterError(RuntimeError):
    """The research adapter produced something that cannot be trusted."""


@dataclass(frozen=True)
class ResearchFinding:
    """One proposed corporate action with the evidence and claims behind it."""

    event_type: str
    proposed_date: date
    terms: CorporateActionTerms
    evidence: tuple[Evidence, ...]
    claims: tuple[Claim, ...]

    @property
    def confidence(self) -> Decimal:
        return min(claim.quantized_confidence for claim in self.claims)


@runtime_checkable
class CorporateActionResearchPort(Protocol):
    """The seam every evidence-collection adapter implements."""

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        """Return every corporate-action finding for `ticker` at this slot."""


class UnconfiguredResearchPort:
    """Default adapter: refuses loudly rather than reporting a quiet slot."""

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        raise ResearchAdapterError(
            "no CorporateActionResearchPort adapter is configured. Returning an empty "
            "finding list would be indistinguishable from a genuinely quiet slot, so "
            "this refuses instead. Inject an adapter (for example AiResearchAdapter)."
        )


#: The research model seam: a prompt in, a raw JSON document out.  Keeping it a
#: plain callable is what lets every test drive it without network access.
ResearchModel = Callable[[str], str]


_PROMPT_TEMPLATE = """\
You are a corporate-actions research assistant for a US equities market-data pipeline.

Ticker: {ticker}
Research slot (UTC): {slot}

Report every announced corporate action for this ticker whose effective date is in
the future as of the research slot. Supported event_type values are exactly:
CASH_DIVIDEND, STOCK_SPLIT.

Answer with a single JSON document and nothing else:

{{"findings": [{{"event_type": "STOCK_SPLIT",
                "effective_date": "YYYY-MM-DD",
                "terms": {{"from_shares": 1, "to_shares": 2}},
                "evidence": [{{"source_uri": "https://...",
                              "source_title": "..."}}],
                "claims": [{{"field": "event_type", "value": "STOCK_SPLIT",
                            "source_uri": "https://...", "confidence": "0.95"}}]}}]}}

Cite only sources you actually consulted, by their real URL. Do not report a
content hash or a retrieval time for any source: those are derived from the
retrieved bytes by the pipeline, not by you.

CASH_DIVIDEND terms are {{"amount": "0.00", "currency": "USD"}}.
Every material field (event_type, effective_date, and each terms field) needs its own
claim, and every claim must cite a source_uri that also appears in evidence.
If there is nothing to report, answer {{"findings": []}}.
"""


def build_research_prompt(ticker: str, scheduled_at: datetime) -> str:
    """The exact prompt the adapter sends.  Deterministic, so it can be pinned."""
    _require_utc(scheduled_at, "scheduled_at")
    return _PROMPT_TEMPLATE.format(ticker=ticker, slot=_format_utc(scheduled_at))


class AiResearchAdapter:
    """Turns a language model's answer into validated, sourced findings.

    The model is injected, so this class performs no I/O of its own and its
    tests need no network.  Everything the model returns is treated as
    untrusted: the structure is parsed strictly, the claim graph is checked
    against the evidence, and anything below `min_confidence` is discarded with
    a warning rather than quietly promoted into a reviewable candidate.
    """

    def __init__(
        self,
        model: ResearchModel,
        *,
        min_confidence: Decimal,
        evidence_fetcher: EvidenceFetcher | None = None,
    ) -> None:
        if not callable(model):
            raise TypeError("model must be callable")
        if not isinstance(min_confidence, Decimal):
            raise TypeError("min_confidence must be a Decimal")
        if not Decimal(0) < min_confidence <= Decimal(1):
            raise ValueError("min_confidence must be in (0, 1]")
        self._model = model
        self._min_confidence = min_confidence.quantize(CONFIDENCE_EXPONENT)
        # Defaults to refusing rather than to trusting: an adapter with no fetcher
        # cannot derive a content hash, and must not invent one.
        self._evidence_fetcher: EvidenceFetcher = (
            evidence_fetcher if evidence_fetcher is not None else UnconfiguredEvidenceFetcher()
        )

    @property
    def min_confidence(self) -> Decimal:
        return self._min_confidence

    def research(self, ticker: str, scheduled_at: datetime) -> tuple[ResearchFinding, ...]:
        raw = self._model(build_research_prompt(ticker, scheduled_at))
        document = self._parse(raw)
        accepted: list[ResearchFinding] = []
        # One fetch per distinct URI for the whole pass: two findings citing the
        # same issuer page must not hash it twice, or a page that changed between
        # fetches would yield two different hashes for one source.
        fetched: dict[str, Evidence] = {}
        for index, entry in enumerate(document):
            finding = self._finding(entry, index, fetched)
            if finding.confidence < self._min_confidence:
                LOGGER.warning(
                    "corporate_action_research.finding_below_threshold "
                    "ticker=%s event_type=%s effective_date=%s confidence=%s minimum=%s",
                    ticker,
                    finding.event_type,
                    finding.proposed_date.isoformat(),
                    finding.confidence,
                    self._min_confidence,
                )
                continue
            accepted.append(finding)
        return tuple(accepted)

    def _parse(self, raw: str) -> list[Mapping[str, Any]]:
        try:
            document = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ResearchAdapterError(
                "the research model did not return a JSON document"
            ) from exc
        if not isinstance(document, Mapping):
            raise ResearchAdapterError("the research response must be a JSON object")
        findings = document.get("findings")
        if not isinstance(findings, list):
            raise ResearchAdapterError("the research response has no 'findings' array")
        for entry in findings:
            if not isinstance(entry, Mapping):
                raise ResearchAdapterError("every finding must be a JSON object")
        return list(findings)

    def _finding(
        self, entry: Mapping[str, Any], index: int, fetched: dict[str, Evidence]
    ) -> ResearchFinding:
        where = f"findings[{index}]"
        try:
            event_type = str(entry["event_type"]).strip().upper()
            if event_type not in TERMS_BY_EVENT_TYPE:
                raise ValueError(
                    f"unsupported event_type {event_type!r}; supported: "
                    f"{sorted(TERMS_BY_EVENT_TYPE)}"
                )
            effective_date = date.fromisoformat(str(entry["effective_date"]))
            terms_payload = entry["terms"]
            if not isinstance(terms_payload, Mapping):
                raise ValueError("terms must be a JSON object")
            terms = parse_terms(event_type, terms_payload)
            evidence = self._evidence(entry.get("evidence"), fetched)
            claims = self._claims(entry.get("claims"))
        except KeyError as exc:
            raise ResearchAdapterError(f"{where} is missing {exc.args[0]!r}") from exc
        except (TypeError, ValueError) as exc:
            raise ResearchAdapterError(f"{where} is invalid: {exc}") from exc

        finding = ResearchFinding(
            event_type=event_type,
            proposed_date=effective_date,
            terms=terms,
            evidence=evidence,
            claims=claims,
        )
        # Reuse the candidate's claim-graph invariants so the adapter cannot emit a
        # finding the candidate would later refuse.
        try:
            ResearchCandidate.from_finding(
                finding,
                ticker="VALIDATE",
                researched_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
            )
        except ValueError as exc:
            raise ResearchAdapterError(f"{where} is not self-consistent: {exc}") from exc
        return finding

    def _evidence(self, payload: Any, fetched: dict[str, Evidence]) -> tuple[Evidence, ...]:
        """Parse citations, then *derive* each evidence item from retrieved bytes.

        A cited source that cannot be retrieved fails the finding rather than
        being stored with an unverifiable hash -- the same fail-closed choice
        `UnconfiguredResearchPort` makes for a whole slot.

        `fetched` is the per-pass fetch cache owned by :meth:`research`.
        """
        if not isinstance(payload, list) or not payload:
            raise ValueError("at least one evidence entry is required")
        items: list[Evidence] = []
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise ValueError("every evidence entry must be a JSON object")
            if "content_sha256" in entry or "retrieved_at" in entry:
                raise ValueError(
                    "evidence must not carry 'content_sha256' or 'retrieved_at': a model "
                    "cannot hash a document it did not fetch, so these are derived here "
                    "from the retrieved bytes"
                )
            citation = SourceCitation(
                source_uri=str(entry["source_uri"]),
                source_title=str(entry["source_title"]),
            )
            if citation.source_uri in fetched:
                items.append(fetched[citation.source_uri])
                continue
            try:
                content, retrieved_at = self._evidence_fetcher.fetch(citation.source_uri)
            except ResearchAdapterError:
                raise
            except Exception as exc:  # noqa: BLE001 - any transport failure is fatal here
                raise ValueError(
                    f"cited source {citation.source_uri} could not be retrieved: {exc}"
                ) from exc
            _require_utc(retrieved_at, "evidence retrieved_at")
            evidence = Evidence.from_fetched(
                citation, content=content, retrieved_at=retrieved_at
            )
            fetched[citation.source_uri] = evidence
            items.append(evidence)
        return tuple(items)

    @staticmethod
    def _claims(payload: Any) -> tuple[Claim, ...]:
        if not isinstance(payload, list) or not payload:
            raise ValueError("at least one claim is required")
        items: list[Claim] = []
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise ValueError("every claim must be a JSON object")
            try:
                confidence = Decimal(str(entry["confidence"]))
            except InvalidOperation as exc:
                raise ValueError("claim confidence is not a decimal number") from exc
            items.append(
                Claim(
                    field=str(entry["field"]),
                    value=str(entry["value"]),
                    source_uri=str(entry["source_uri"]),
                    confidence=confidence,
                )
            )
        return tuple(items)


# ======================================================================================
# Schedule
# ======================================================================================
@dataclass(frozen=True)
class ResearchSlot:
    scheduled_at: datetime

    @property
    def slot_id(self) -> str:
        return _format_utc(self.scheduled_at)


@dataclass(frozen=True)
class TwiceDailySchedule:
    """Configured UTC schedule semantics without owning an external scheduler."""

    slots: tuple[time, time]

    def __post_init__(self) -> None:
        if len(self.slots) != 2:
            raise ValueError("a research schedule requires exactly two daily slots")
        if self.slots[0] == self.slots[1]:
            raise ValueError("the two daily slots must be distinct")
        for slot in self.slots:
            if slot.tzinfo is None or slot.utcoffset() != timedelta(0):
                raise ValueError("daily research slots must use UTC")
        object.__setattr__(self, "slots", tuple(sorted(self.slots)))

    def latest_due_slot(self, now: datetime) -> ResearchSlot:
        _require_utc(now, "now")
        utc_now = now.astimezone(timezone.utc)
        candidates = [
            datetime.combine(day, slot, tzinfo=timezone.utc)
            for day in (utc_now.date() - timedelta(days=1), utc_now.date())
            for slot in self.slots
        ]
        due = max(candidate for candidate in candidates if candidate <= utc_now)
        return ResearchSlot(scheduled_at=due)


class CandidateStore:
    """Append-only JSONL store with deterministic in-file deduplication."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, candidate: ResearchCandidate) -> bool:
        existing_ids = self._existing_ids()
        if candidate.candidate_id in existing_ids:
            return False

        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = _canonical_json(candidate.to_record())
        with self.path.open("a", encoding="utf-8", newline="\n") as output:
            output.write(serialized)
            output.write("\n")
        return True

    def _existing_ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        identifiers: set[str] = set()
        with self.path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                record = json.loads(line)
                candidate_id = record.get("candidate_id")
                if not isinstance(candidate_id, str):
                    raise ValueError(f"candidate store line {line_number} has no candidate_id")
                identifiers.add(candidate_id)
        return identifiers


# ======================================================================================
# Canonical persistence
# ======================================================================================
def effective_at_for(proposed_date: date) -> datetime:
    """Exchange-local midnight on the effective date, as UTC.

    Corporate actions take effect at the open of the effective session, so the
    boundary that separates "needs adjusting" from "already reflects the event"
    is the start of that ET day -- not UTC midnight, which lands mid-session on
    the previous day.
    """
    return datetime.combine(proposed_date, time.min, tzinfo=ET).astimezone(timezone.utc)


def provider_event_key(ticker: str, event_type: str, proposed_date: date) -> str:
    """The natural key of a researched action, stable across re-discovery."""
    key = f"RESEARCH:{ticker}:{event_type}:{proposed_date.isoformat()}"
    if len(key) > 160:  # pragma: no cover - guards the canonical varchar(160)
        raise ValueError(f"provider_event_key exceeds 160 characters: {key!r}")
    return key


#: The keys of `terms_document` that constitute the *economic substance* of an
#: action -- what it is, to whom, when, and on what terms.  `terms_hash` covers
#: exactly these and nothing else, by allowlist rather than by exclusion.
#:
#: Everything left out is provenance or workflow: `evidence`, `claims`,
#: `confidence` and `researched_at` describe how and when we came to believe the
#: action exists, and `review`/`review_history` describe what an administrator
#: decided about it.  None of those change what the action *is*, so re-finding
#: the same split in a later slot, corroborating it with a second source, or
#: approving it must all leave `terms_hash` untouched.  Only a genuine
#: disagreement about the substance produces a different hash -- which is what
#: makes it safe for that to be treated as a conflict.
_SUBSTANCE_KEYS = ("ticker", "event_type", "effective_date", "terms")


def research_digest(document: Mapping[str, Any]) -> str:
    """`terms_hash`: the substance of the action, independent of how it was found."""
    missing = [key for key in _SUBSTANCE_KEYS if key not in document]
    if missing:
        raise ValueError(f"terms_document is missing substance key(s): {missing}")
    return _sha256({key: document[key] for key in _SUBSTANCE_KEYS})


def corporate_action_record(
    candidate: ResearchCandidate,
    *,
    instrument_id: str,
    source_manifest_id: str,
) -> dict[str, Any]:
    """One canonical `market_data.corporate_actions` row, provenance included."""
    key = provider_event_key(candidate.ticker, candidate.event_type, candidate.proposed_date)
    document: dict[str, Any] = {
        "candidate_id": candidate.candidate_id,
        "ticker": candidate.ticker,
        "event_type": candidate.event_type,
        "effective_date": candidate.proposed_date.isoformat(),
        "terms": candidate.terms.to_document(),
        "confidence": str(candidate.confidence),
        "researched_at": _format_utc(candidate.researched_at),
        "evidence": [item.to_record() for item in candidate.evidence],
        "claims": [item.to_record() for item in candidate.claims],
        "review": {"state": REVIEW_REQUIRED, "decided_by": None, "decided_at": None, "rationale": None},
        "review_history": [],
    }
    return {
        "id": deterministic_uuid(CORPORATE_ACTIONS_TABLE, source_manifest_id, key),
        "instrument_id": instrument_id,
        "source_manifest_id": source_manifest_id,
        "provider_event_key": key,
        "action_type": candidate.event_type,
        "effective_at": _format_utc(effective_at_for(candidate.proposed_date)),
        "terms_document": document,
        "terms_hash": research_digest(document),
        "supersedes_action_id": None,
        "created_at": _format_utc(candidate.researched_at),
    }


# ======================================================================================
# Resolvers
# ======================================================================================
class UnknownInstrumentError(LookupError):
    """The researched ticker maps to no instrument in the canonical catalog."""


class UnknownSourceManifestError(LookupError):
    """No AVAILABLE source manifest exists to attribute the research to."""


@runtime_checkable
class ResearchCatalog(Protocol):
    """The narrow slice of `MarketDataCatalog` this module needs.

    Both `LocalCatalog` and the SQLAlchemy Core `PostgresCatalog` satisfy it
    structurally, so nothing here depends on which one is wired in.
    """

    def records(self, table: str) -> list[dict[str, Any]]: ...

    def upsert(self, table: str, record: Mapping[str, Any]) -> None: ...

    def begin_pipeline_run(self, record: Mapping[str, Any]) -> None: ...

    def finish_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        status: str,
        output_hash: str | None,
        failure_code: str | None = None,
    ) -> None: ...

    def latest_available_manifest(
        self, *, feed_id: str, data_layer: str, resolution: str, year: int
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class InstrumentResolver(Protocol):
    def resolve(self, ticker: str, on: date) -> str: ...


@runtime_checkable
class SourceManifestResolver(Protocol):
    def resolve(self, instrument_id: str, on: date) -> str: ...


def _as_date(value: Any) -> date:
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text).date()


class CatalogInstrumentResolver:
    """Ticker -> instrument via the canonical `instrument_symbols` history."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self._catalog = catalog

    def resolve(self, ticker: str, on: date) -> str:
        symbol = ticker.strip().upper()
        for row in self._catalog.records(INSTRUMENT_SYMBOLS_TABLE):
            if str(row["symbol"]).upper() != symbol:
                continue
            if _as_date(row["effective_from"]) > on:
                continue
            effective_to = row.get("effective_to")
            if effective_to is not None and _as_date(effective_to) <= on:
                continue
            return str(row["instrument_id"])
        raise UnknownInstrumentError(
            f"no instrument is mapped to {symbol!r} on {on.isoformat()}; research cannot "
            "be attributed to an instrument and is not recorded"
        )


class CatalogSourceManifestResolver:
    """The AVAILABLE raw manifest a finding is attributed to."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        *,
        feed_id: str,
        resolution: str,
        data_layer: str = "RAW",
    ) -> None:
        self._catalog = catalog
        self._feed_id = feed_id
        self._resolution = resolution
        self._data_layer = data_layer

    def resolve(self, instrument_id: str, on: date) -> str:
        manifest = self._catalog.latest_available_manifest(
            feed_id=self._feed_id,
            data_layer=self._data_layer,
            resolution=self._resolution,
            year=on.year,
        )
        if manifest is None:
            raise UnknownSourceManifestError(
                f"no AVAILABLE {self._data_layer} manifest for feed {self._feed_id} in "
                f"{on.year}; corporate_actions.source_manifest_id cannot be satisfied"
            )
        return str(manifest["id"])


# ======================================================================================
# Schedule executor
# ======================================================================================
class ConflictingResearchError(RuntimeError):
    """The same action was re-researched with materially different terms."""


@dataclass(frozen=True)
class ResearchRunReport:
    slot_id: str
    tickers_researched: int
    candidates_recorded: int
    candidates_already_known: int
    actions_persisted: int
    actions_already_present: int
    skipped_as_duplicate_slot: bool


class ResearchScheduleExecutor:
    """Runs one due research slot and records the result canonically.

    Idempotency is enforced twice, deliberately:

    * **slot level** -- a SUCCEEDED `market_data.pipeline_runs` row keyed by the
      slot's idempotency key short-circuits a repeat run of the same slot.  A
      FAILED run does not, so a failure is retryable.
    * **row level** -- a finding is written only if
      `(source_manifest_id, provider_event_key)` is absent, which is exactly the
      canonical unique index.  Re-discovery in a later slot is therefore free,
      while a re-discovery whose *terms* changed raises rather than being
      dropped, because silently keeping the stale terms would adjust prices with
      numbers we know to be superseded.
    """

    def __init__(
        self,
        *,
        schedule: TwiceDailySchedule,
        port: CorporateActionResearchPort,
        catalog: ResearchCatalog,
        instrument_resolver: InstrumentResolver,
        manifest_resolver: SourceManifestResolver,
        candidate_store: CandidateStore | None = None,
    ) -> None:
        self._schedule = schedule
        self._port = port
        self._catalog = catalog
        self._instruments = instrument_resolver
        self._manifests = manifest_resolver
        self._store = candidate_store

    def run_due_slot(self, tickers: Sequence[str], *, now: datetime) -> ResearchRunReport:
        normalized = tuple(dict.fromkeys(ticker.strip().upper() for ticker in tickers))
        if not normalized or any(not ticker for ticker in normalized):
            raise ValueError("at least one ticker is required to run a research slot")

        slot = self._schedule.latest_due_slot(now)
        idempotency_key = f"{IDEMPOTENCY_PREFIX}:{slot.slot_id}"
        if self._slot_already_succeeded(idempotency_key):
            LOGGER.info("corporate_action_research.slot_already_done slot=%s", slot.slot_id)
            return ResearchRunReport(
                slot_id=slot.slot_id,
                tickers_researched=0,
                candidates_recorded=0,
                candidates_already_known=0,
                actions_persisted=0,
                actions_already_present=0,
                skipped_as_duplicate_slot=True,
            )

        run_id = deterministic_uuid(PIPELINE_RUNS_TABLE, idempotency_key)
        self._catalog.begin_pipeline_run(
            {
                "id": run_id,
                "pipeline_code": PIPELINE_CODE,
                "pipeline_version": PIPELINE_VERSION,
                "idempotency_key": idempotency_key,
                "status": "RUNNING",
                "input_hash": _sha256({"slot": slot.slot_id, "tickers": list(normalized)}),
                "output_hash": None,
                "started_at": _format_utc(now),
                "completed_at": None,
                "failure_code": None,
            }
        )
        try:
            report = self._research(normalized, slot)
        except Exception as exc:
            self._catalog.finish_pipeline_run(
                run_id,
                status="FAILED",
                output_hash=None,
                failure_code=type(exc).__name__[:80],
            )
            LOGGER.error(
                "corporate_action_research.slot_failed slot=%s", slot.slot_id, exc_info=True
            )
            raise
        self._catalog.finish_pipeline_run(
            run_id,
            status="SUCCEEDED",
            output_hash=_sha256(
                {
                    "slot": report.slot_id,
                    "persisted": report.actions_persisted,
                    "already_present": report.actions_already_present,
                }
            ),
        )
        return report

    def _slot_already_succeeded(self, idempotency_key: str) -> bool:
        return any(
            row.get("idempotency_key") == idempotency_key and row.get("status") == "SUCCEEDED"
            for row in self._catalog.records(PIPELINE_RUNS_TABLE)
        )

    def _research(self, tickers: Sequence[str], slot: ResearchSlot) -> ResearchRunReport:
        recorded = already_known = persisted = already_present = 0
        for ticker in tickers:
            for finding in self._port.research(ticker, slot.scheduled_at):
                candidate = ResearchCandidate.from_finding(
                    finding, ticker=ticker, researched_at=slot.scheduled_at
                )
                instrument_id = self._instruments.resolve(ticker, candidate.proposed_date)
                source_manifest_id = self._manifests.resolve(
                    instrument_id, candidate.proposed_date
                )
                record = corporate_action_record(
                    candidate,
                    instrument_id=instrument_id,
                    source_manifest_id=source_manifest_id,
                )
                if self._persist(record):
                    persisted += 1
                else:
                    already_present += 1
                if self._store is not None:
                    if self._store.append(candidate):
                        recorded += 1
                    else:
                        already_known += 1
        return ResearchRunReport(
            slot_id=slot.slot_id,
            tickers_researched=len(tickers),
            candidates_recorded=recorded,
            candidates_already_known=already_known,
            actions_persisted=persisted,
            actions_already_present=already_present,
            skipped_as_duplicate_slot=False,
        )

    def _persist(self, record: Mapping[str, Any]) -> bool:
        existing = self._existing_action(
            str(record["source_manifest_id"]), str(record["provider_event_key"])
        )
        if existing is not None:
            if existing.get("terms_hash") != record["terms_hash"]:
                raise ConflictingResearchError(
                    f"{record['provider_event_key']} was already recorded with different "
                    f"terms (stored {existing.get('terms_hash')}, researched "
                    f"{record['terms_hash']}). Resolve the disagreement in review rather "
                    "than letting either version win silently."
                )
            return False
        self._catalog.upsert(CORPORATE_ACTIONS_TABLE, record)
        return True

    def _existing_action(
        self, source_manifest_id: str, event_key: str
    ) -> dict[str, Any] | None:
        for row in self._catalog.records(CORPORATE_ACTIONS_TABLE):
            if (
                str(row.get("source_manifest_id")) == source_manifest_id
                and str(row.get("provider_event_key")) == event_key
            ):
                return row
        return None
