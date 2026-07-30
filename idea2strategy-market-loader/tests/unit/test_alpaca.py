from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from market_loader.alpaca.client import AlpacaBarsClient
from market_loader.alpaca.pagination import iter_pages
from market_loader.config import AlpacaConfig


def _config(max_attempts: int = 3) -> AlpacaConfig:
    return AlpacaConfig(
        base_url="https://data.alpaca.markets",
        feed="sip",
        request_timeframe="30Min",
        chunk_days=180,
        symbols_per_request=50,
        page_limit=10_000,
        connect_timeout_seconds=1,
        read_timeout_seconds=1,
        max_attempts=max_attempts,
    )


def test_page_token_repeats_until_empty() -> None:
    tokens = iter(["one", "two", None])
    received = []

    def fetch(token: str | None) -> dict:
        received.append(token)
        return {"bars": {}, "next_page_token": next(tokens)}

    assert len(list(iter_pages(fetch))) == 3
    assert received == [None, "one", "two"]


def test_client_retries_429_and_preserves_required_request_contract() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "0.001"})
        return httpx.Response(200, json={"bars": {}, "next_page_token": None})

    client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://data.alpaca.markets"
    )
    delays = []
    alpaca = AlpacaBarsClient(
        _config(),
        "key",
        "secret",
        client=client,
        sleeper=delays.append,
    )
    pages = list(
        alpaca.iter_bar_pages(
            ["AAPL"],
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 2, 1, tzinfo=UTC),
            "raw",
        )
    )
    assert pages == [{"bars": {}, "next_page_token": None}]
    assert len(calls) == 2
    assert calls[-1].url.params["timeframe"] == "30Min"
    assert calls[-1].url.params["feed"] == "sip"
    assert calls[-1].headers["APCA-API-KEY-ID"] == "key"
    assert delays == [0.001]


def test_repeated_page_token_is_rejected() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"bars": {}, "next_page_token": "same"})

    client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url="https://data.alpaca.markets"
    )
    alpaca = AlpacaBarsClient(_config(), "key", "secret", client=client, sleeper=lambda _: None)
    with pytest.raises(Exception, match="repeated"):
        list(
            alpaca.iter_bar_pages(
                ["AAPL"],
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 2, 1, tzinfo=UTC),
                "raw",
            )
        )
