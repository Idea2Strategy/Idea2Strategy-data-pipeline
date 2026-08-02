"""Alpaca `next_page_token` traversal with a loop guard.

Kept separate from the HTTP client so the termination rule — including the
empty final page Alpaca emits when a range ends exactly on a page boundary —
can be reasoned about and tested on its own.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol

from .errors import PermanentAlpacaError

__all__ = ["PageFetcher", "iter_pages"]


class PageFetcher(Protocol):
    def __call__(self, token: str | None) -> dict[str, Any]: ...


def iter_pages(fetch_page: PageFetcher) -> Iterator[dict[str, Any]]:
    """Yield every page, following `next_page_token` until it is empty.

    A token the server has already handed out would loop forever, so a repeat
    is treated as a permanent provider fault rather than being followed.
    """
    token: str | None = None
    seen: set[str] = set()
    while True:
        page = fetch_page(token)
        yield page
        next_token = page.get("next_page_token")
        if next_token is None or next_token == "":
            return
        if not isinstance(next_token, str):
            raise PermanentAlpacaError(
                f"Alpaca next_page_token 타입이 잘못되었습니다: {type(next_token).__name__}"
            )
        if next_token in seen:
            raise PermanentAlpacaError("Alpaca next_page_token이 반복되었습니다.")
        seen.add(next_token)
        token = next_token
