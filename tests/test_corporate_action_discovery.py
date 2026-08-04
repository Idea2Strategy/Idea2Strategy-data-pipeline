from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from market_pipeline_lib.alpaca import AlpacaCorporateActionPage
from market_pipeline_lib.corporate_action_discovery import (
    CorporateActionDiscoveryPort,
    EtfUniverseEntry,
    HttpEvidenceFetcher,
    load_etf_universe,
)
from market_pipeline_lib.corporate_action_research import ResearchAdapterError

NOW = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)


def action_page(*, actions: dict[str, list[dict[str, Any]]] | None = None) -> AlpacaCorporateActionPage:
    raw = b'{"corporate_actions":"recorded-response-bytes"}'
    return AlpacaCorporateActionPage(
        corporate_actions={key: tuple(value) for key, value in (actions or {}).items()},
        raw_bytes=raw,
        retrieved_at=NOW,
        source_uri="https://data.alpaca.markets/v1/corporate-actions?symbols=SPY%2CQQQ",
    )


class RecordingAlpaca:
    def __init__(self, pages: tuple[AlpacaCorporateActionPage, ...]) -> None:
        self.pages = pages
        self.calls: list[tuple[tuple[str, ...], date, date]] = []

    def iter_corporate_action_pages(
        self, symbols: tuple[str, ...], start: date, end: date
    ) -> tuple[AlpacaCorporateActionPage, ...]:
        self.calls.append((symbols, start, end))
        return self.pages


class ExplodingAlpaca:
    def iter_corporate_action_pages(self, *_: Any, **__: Any) -> Any:
        raise RuntimeError("required Alpaca source unavailable")


class IssuerFetcher:
    def __init__(self, failing: str | None = None) -> None:
        self.failing = failing
        self.calls: list[str] = []

    def fetch(self, source_uri: str) -> tuple[bytes, datetime]:
        self.calls.append(source_uri)
        if source_uri == self.failing:
            raise ResearchAdapterError("issuer page redesigned")
        return b"issuer-calendar", NOW


class IncidentRecorder:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def record_quality_incident(self, record: dict[str, Any]) -> None:
        self.rows.append(record)


UNIVERSE = (
    EtfUniverseEntry("SPY", "State Street", "https://issuer.example/ssga"),
    EtfUniverseEntry("QQQ", "Invesco", "https://issuer.example/invesco"),
)


def test_the_committed_v1_universe_is_exactly_27_enabled_etfs() -> None:
    entries = load_etf_universe(Path("ticker_info/etf_universe.csv"))

    assert len(entries) == 27
    # The issue draft said eight issuers, but the canonical CSV currently has
    # seven. Discovery follows the reviewed file instead of preserving that typo.
    assert len({entry.issuer for entry in entries}) == 7
    assert {"SPY", "QQQ", "SGOV", "BITO", "GLD", "USO"} <= {
        entry.ticker for entry in entries
    }


def test_one_source_window_is_shared_by_every_ticker_and_zero_is_explicit() -> None:
    alpaca = RecordingAlpaca((action_page(),))
    port = CorporateActionDiscoveryPort(universe=UNIVERSE, alpaca=alpaca)  # type: ignore[arg-type]

    assert port.research("SPY", NOW) == ()
    assert port.research("QQQ", NOW) == ()

    assert alpaca.calls == [(('SPY', 'QQQ'), date(2026, 8, 1), date(2026, 8, 4))]
    assert port.last_report is not None
    assert port.last_report.coverage[0].state == "ZERO"


def test_required_source_failure_is_not_reported_as_zero() -> None:
    port = CorporateActionDiscoveryPort(universe=UNIVERSE, alpaca=ExplodingAlpaca())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="required Alpaca"):
        port.research("SPY", NOW)

    assert port.last_report is None


def test_normalizes_and_deduplicates_alpaca_events_from_fetched_bytes() -> None:
    duplicate = {
        "id": "alpaca-div-spy-202608",
        "initiating_symbol": "SPY",
        "ex_date": "2026-08-14",
        "cash": "1.25",
        "currency": "USD",
    }
    page = action_page(actions={"cash_dividends": [duplicate, dict(duplicate)]})
    port = CorporateActionDiscoveryPort(
        universe=UNIVERSE, alpaca=RecordingAlpaca((page,))  # type: ignore[arg-type]
    )

    (finding,) = port.research("SPY", NOW)

    assert finding.event_type == "CASH_DIVIDEND"
    assert finding.provider_source_event_id == "alpaca-div-spy-202608"
    assert finding.proposed_date == date(2026, 8, 14)
    assert finding.evidence[0].content_sha256 == hashlib.sha256(page.raw_bytes).hexdigest()
    assert finding.evidence[0].retrieved_at == NOW


def test_issuer_calendar_failure_is_isolated_and_recorded_as_a_quality_incident() -> None:
    page = action_page(
        actions={
            "stock_splits": [
                {
                    "id": "alpaca-split-qqq",
                    "initiating_symbol": "QQQ",
                    "ex_date": "2026-08-15",
                    "old_rate": "1",
                    "new_rate": "2",
                }
            ]
        }
    )
    issuer = IssuerFetcher(failing="https://issuer.example/ssga")
    incidents = IncidentRecorder()
    port = CorporateActionDiscoveryPort(
        universe=UNIVERSE,
        alpaca=RecordingAlpaca((page,)),  # type: ignore[arg-type]
        issuer_fetcher=issuer,
        incident_recorder=incidents,
    )

    (finding,) = port.research("QQQ", NOW)

    assert finding.event_type == "STOCK_SPLIT"
    assert len(issuer.calls) == 2
    assert len(incidents.rows) == 1
    assert incidents.rows[0]["incident_code"] == "ISSUER_CALENDAR_FETCH_FAILED_STATE_STREET"
    assert {item.state for item in port.last_report.coverage[1:]} == {"FAILED", "SUCCEEDED"}  # type: ignore[union-attr]


def test_http_evidence_fetcher_returns_response_bytes_and_pipeline_retrieval_time() -> None:
    request_headers: list[httpx.Headers] = []

    def respond(request: httpx.Request) -> httpx.Response:
        request_headers.append(request.headers)
        return httpx.Response(200, content=b"issuer calendar bytes")

    fetcher = HttpEvidenceFetcher(
        allowed_hosts={"issuer.example"},
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
        now=lambda: NOW,
    )

    content, retrieved_at = fetcher.fetch("https://issuer.example/calendar")

    assert content == b"issuer calendar bytes"
    assert retrieved_at == NOW
    assert "authorization" not in request_headers[0]


def test_http_evidence_fetcher_refuses_unapproved_or_non_https_hosts() -> None:
    fetcher = HttpEvidenceFetcher(
        allowed_hosts={"issuer.example"},
        http_client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))),
    )

    with pytest.raises(ResearchAdapterError, match="allowed HTTPS"):
        fetcher.fetch("http://issuer.example/calendar")
    with pytest.raises(ResearchAdapterError, match="allowed HTTPS"):
        fetcher.fetch("https://127.0.0.1/calendar")


def test_http_evidence_fetcher_validates_redirect_before_following_it() -> None:
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    fetcher = HttpEvidenceFetcher(
        allowed_hosts={"issuer.example"},
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    )

    with pytest.raises(ResearchAdapterError, match="allowed HTTPS"):
        fetcher.fetch("https://issuer.example/calendar")

    assert requests == ["https://issuer.example/calendar"]
