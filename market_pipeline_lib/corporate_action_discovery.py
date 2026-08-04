"""Production D14 discovery for the fixed ETF universe.

Alpaca is the required structured source. Issuer pages are a best-effort
availability cross-check: a broken issuer page records a quality incident but
never turns a successful Alpaca zero-result into a provider failure.
"""

from __future__ import annotations

import csv
import ipaddress
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin, urlparse

import httpx

from .alpaca import AlpacaCorporateActionPage, AlpacaCorporateActionsClient
from .contracts import deterministic_uuid
from .corporate_action_research import (
    CashDividendTerms,
    Claim,
    CorporateActionTerms,
    Evidence,
    EvidenceFetcher,
    ResearchAdapterError,
    ResearchFinding,
    SourceCitation,
    SplitTerms,
)

QUALITY_INCIDENTS_TABLE = "market_data.quality_incidents"


@dataclass(frozen=True)
class EtfUniverseEntry:
    ticker: str
    issuer: str
    source_url: str


def load_etf_universe(path: Path, *, expected_count: int = 27) -> tuple[EtfUniverseEntry, ...]:
    """Load the reviewed, enabled v1 universe and fail on silent scope drift."""

    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = tuple(csv.DictReader(stream))
    entries = tuple(
        EtfUniverseEntry(
            ticker=str(row.get("ticker", "")).strip().upper(),
            issuer=str(row.get("issuer", "")).strip(),
            source_url=str(row.get("source_url", "")).strip(),
        )
        for row in rows
        if str(row.get("enabled", "")).strip().lower() == "true"
    )
    if len(entries) != expected_count:
        raise ValueError(
            f"the reviewed ETF universe must contain exactly {expected_count} enabled rows; "
            f"found {len(entries)}"
        )
    if len({entry.ticker for entry in entries}) != len(entries):
        raise ValueError("the ETF universe contains duplicate tickers")
    if any(not entry.ticker or not entry.issuer or not entry.source_url for entry in entries):
        raise ValueError("every ETF universe row needs ticker, issuer and source_url")
    return entries


class HttpEvidenceFetcher(EvidenceFetcher):
    """Read public evidence bytes with bounded size and no credential surface."""

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str],
        http_client: httpx.Client | None = None,
        now: Callable[[], datetime] | None = None,
        max_bytes: int = 5 * 1024 * 1024,
    ) -> None:
        self._allowed_hosts = frozenset(host.lower() for host in allowed_hosts)
        if not self._allowed_hosts:
            raise ValueError("at least one evidence host must be allowed")
        self._client = http_client or httpx.Client(timeout=20.0)
        self._owns_client = http_client is None
        self._now = now or (lambda: datetime.now(UTC))
        self._max_bytes = max_bytes

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch(self, source_uri: str) -> tuple[bytes, datetime]:
        self._validate_uri(source_uri)
        current_uri = source_uri
        try:
            for _ in range(6):
                response = self._client.get(
                    current_uri,
                    headers={"Accept": "text/html,application/pdf"},
                    follow_redirects=False,
                )
                if not response.is_redirect:
                    response.raise_for_status()
                    break
                location = response.headers.get("location")
                if not location:
                    raise ResearchAdapterError("evidence redirect omitted Location")
                current_uri = urljoin(current_uri, location)
                self._validate_uri(current_uri)
            else:
                raise ResearchAdapterError("evidence source exceeded the redirect limit")
        except httpx.HTTPError as exc:
            raise ResearchAdapterError(f"evidence source could not be retrieved: {type(exc).__name__}") from exc
        self._validate_uri(str(response.url))
        content = response.content
        if not content:
            raise ResearchAdapterError("evidence source returned zero bytes")
        if len(content) > self._max_bytes:
            raise ResearchAdapterError("evidence source exceeded the configured byte limit")
        retrieved_at = self._now()
        if retrieved_at.tzinfo is None or retrieved_at.utcoffset() != timedelta(0):
            raise ResearchAdapterError("evidence retrieval clock must return UTC")
        return content, retrieved_at

    def _validate_uri(self, source_uri: str) -> None:
        parsed = urlparse(source_uri)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "https" or host not in self._allowed_hosts:
            raise ResearchAdapterError("evidence URI is not an allowed HTTPS issuer source")
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            if not address.is_global:
                raise ResearchAdapterError("evidence URI must not target a private address")


class QualityIncidentRecorder(Protocol):
    def record_quality_incident(self, record: Mapping[str, Any]) -> None: ...


@dataclass(frozen=True)
class DiscoveryWindow:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class SourceCoverage:
    source: str
    state: str
    events: int


@dataclass(frozen=True)
class DiscoveryReport:
    window: DiscoveryWindow
    coverage: tuple[SourceCoverage, ...]


class CorporateActionDiscoveryPort:
    """Old per-ticker port shape backed by one source×window discovery pass."""

    def __init__(
        self,
        *,
        universe: Sequence[EtfUniverseEntry],
        alpaca: AlpacaCorporateActionsClient,
        issuer_fetcher: EvidenceFetcher | None = None,
        incident_recorder: QualityIncidentRecorder | None = None,
        lookback: timedelta = timedelta(days=3),
        slot_interval: timedelta = timedelta(hours=12),
    ) -> None:
        self._universe = tuple(universe)
        if not self._universe:
            raise ValueError("the corporate-action universe must not be empty")
        self._tickers = frozenset(entry.ticker for entry in self._universe)
        self._alpaca = alpaca
        self._issuer_fetcher = issuer_fetcher
        self._incidents = incident_recorder
        self._lookback = lookback
        self._slot_interval = slot_interval
        self._cache: dict[datetime, dict[str, tuple[ResearchFinding, ...]]] = {}
        self.last_report: DiscoveryReport | None = None

    def research(self, ticker: str, scheduled_at: datetime) -> Sequence[ResearchFinding]:
        symbol = ticker.strip().upper()
        if symbol not in self._tickers:
            raise ResearchAdapterError(f"{symbol!r} is outside the reviewed ETF universe")
        if scheduled_at.tzinfo is None or scheduled_at.utcoffset() != timedelta(0):
            raise ResearchAdapterError("scheduled_at must be UTC")
        if scheduled_at not in self._cache:
            self._cache[scheduled_at] = self._discover(scheduled_at)
        return self._cache[scheduled_at][symbol]

    def _discover(self, scheduled_at: datetime) -> dict[str, tuple[ResearchFinding, ...]]:
        window = DiscoveryWindow(
            start=scheduled_at - self._slot_interval - self._lookback,
            end=scheduled_at,
        )
        pages = tuple(
            self._alpaca.iter_corporate_action_pages(
                tuple(entry.ticker for entry in self._universe),
                window.start.date(),
                window.end.date(),
            )
        )
        findings: dict[str, list[ResearchFinding]] = {
            entry.ticker: [] for entry in self._universe
        }
        seen: dict[tuple[str, str, date, str], ResearchFinding] = {}
        for page in pages:
            for finding, ticker in self._normalize_page(page):
                key = (
                    ticker,
                    finding.event_type,
                    finding.proposed_date,
                    finding.provider_source_event_id or "",
                )
                previous = seen.get(key)
                if previous is not None:
                    if previous.terms != finding.terms:
                        raise ResearchAdapterError(
                            f"Alpaca event {finding.provider_source_event_id!r} has conflicting terms"
                        )
                    continue
                seen[key] = finding
                findings[ticker].append(finding)
        coverage = [SourceCoverage("alpaca", "ZERO" if not seen else "SUCCEEDED", len(seen))]
        coverage.extend(self._probe_issuers(window))
        self.last_report = DiscoveryReport(window=window, coverage=tuple(coverage))
        return {ticker: tuple(values) for ticker, values in findings.items()}

    def _normalize_page(
        self, page: AlpacaCorporateActionPage
    ) -> Iterable[tuple[ResearchFinding, str]]:
        citation = SourceCitation(page.source_uri, "Alpaca Corporate Actions")
        evidence = Evidence.from_fetched(
            citation, content=page.raw_bytes, retrieved_at=page.retrieved_at
        )
        for source_type, entries in page.corporate_actions.items():
            for entry in entries:
                ticker = str(
                    entry.get("initiating_symbol")
                    or entry.get("symbol")
                    or entry.get("target_symbol")
                    or ""
                ).strip().upper()
                if ticker not in self._tickers:
                    continue
                source_id = str(entry.get("id") or entry.get("corporate_action_id") or "").strip()
                if not source_id:
                    raise ResearchAdapterError("Alpaca corporate action is missing its stable id")
                effective = self._required_date(entry)
                terms: CorporateActionTerms
                if source_type == "cash_dividends":
                    amount = Decimal(str(entry.get("cash") or entry.get("amount") or "0"))
                    terms = CashDividendTerms(
                        amount=amount,
                        currency=str(entry.get("currency") or "USD").upper(),
                    )
                    event_type = "CASH_DIVIDEND"
                elif source_type in {"stock_splits", "forward_splits", "reverse_splits"}:
                    terms = self._split_terms(entry)
                    event_type = "STOCK_SPLIT"
                else:
                    raise ResearchAdapterError(
                        f"unsupported Alpaca corporate-action collection {source_type!r}"
                    )
                claims = tuple(
                    Claim(field, value, page.source_uri, Decimal("1"))
                    for field, value in {
                        "event_type": event_type,
                        "effective_date": effective.isoformat(),
                        **terms.claim_fields(),
                    }.items()
                )
                yield (
                    ResearchFinding(
                        event_type=event_type,
                        proposed_date=effective,
                        terms=terms,
                        evidence=(evidence,),
                        claims=claims,
                        provider_source_event_id=source_id,
                    ),
                    ticker,
                )

    @staticmethod
    def _required_date(entry: Mapping[str, Any]) -> date:
        raw = entry.get("ex_date") or entry.get("effective_date")
        try:
            return date.fromisoformat(str(raw))
        except ValueError as exc:
            raise ResearchAdapterError("Alpaca corporate action has an invalid effective date") from exc

    @staticmethod
    def _split_terms(entry: Mapping[str, Any]) -> SplitTerms:
        try:
            old_rate = Fraction(Decimal(str(entry.get("old_rate") or entry["from_shares"])))
            new_rate = Fraction(Decimal(str(entry.get("new_rate") or entry["to_shares"])))
            ratio = new_rate / old_rate
        except (KeyError, ArithmeticError, ValueError) as exc:
            raise ResearchAdapterError("Alpaca split has invalid old/new rates") from exc
        return SplitTerms(from_shares=ratio.denominator, to_shares=ratio.numerator)

    def _probe_issuers(self, window: DiscoveryWindow) -> list[SourceCoverage]:
        if self._issuer_fetcher is None:
            return []
        by_issuer: dict[str, str] = {}
        for entry in self._universe:
            by_issuer.setdefault(entry.issuer, entry.source_url)
        coverage: list[SourceCoverage] = []
        for issuer, source_url in sorted(by_issuer.items()):
            try:
                self._issuer_fetcher.fetch(source_url)
            except Exception as exc:
                coverage.append(SourceCoverage(f"issuer:{issuer}", "FAILED", 0))
                self._record_issuer_failure(issuer, window, exc)
            else:
                coverage.append(SourceCoverage(f"issuer:{issuer}", "SUCCEEDED", 0))
        return coverage

    def _record_issuer_failure(
        self, issuer: str, window: DiscoveryWindow, error: Exception
    ) -> None:
        if self._incidents is None:
            return
        issuer_code = "".join(character if character.isalnum() else "_" for character in issuer.upper())
        incident_code = f"ISSUER_CALENDAR_FETCH_FAILED_{issuer_code}"[:80]
        detected = window.end.isoformat().replace("+00:00", "Z")
        self._incidents.record_quality_incident(
            {
                "id": deterministic_uuid(
                    QUALITY_INCIDENTS_TABLE,
                    issuer,
                    window.start.isoformat(),
                    window.end.isoformat(),
                    type(error).__name__,
                ),
                "dataset_manifest_id": None,
                "instrument_id": None,
                "severity": "WARNING",
                "incident_code": incident_code,
                "period_start": window.start.isoformat().replace("+00:00", "Z"),
                "period_end": detected,
                "status": "OPEN",
                "evidence_object_id": None,
                "detected_at": detected,
                "resolved_at": None,
            }
        )
