from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol


class PageFetcher(Protocol):
    def __call__(self, token: str | None) -> dict[str, Any]: ...


def iter_pages(fetch_page: PageFetcher) -> Iterator[dict[str, Any]]:
    token: str | None = None
    seen: set[str] = set()
    while True:
        page = fetch_page(token)
        yield page
        next_token = page.get("next_page_token")
        if next_token is None or next_token == "":
            return
        if not isinstance(next_token, str) or next_token in seen:
            raise ValueError("invalid or repeated next_page_token")
        seen.add(next_token)
        token = next_token
