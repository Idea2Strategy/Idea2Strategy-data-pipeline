from __future__ import annotations

from datetime import date
from itertools import pairwise

from hypothesis import given
from hypothesis import strategies as st

from market_loader.model.partition import chunk_ranges, idempotency_key, shard_for


def test_chunk_ranges_are_start_inclusive_end_exclusive() -> None:
    chunks = list(chunk_ranges(date(2024, 1, 1), date(2025, 1, 1), 180))
    assert chunks[0].start == date(2024, 1, 1)
    assert chunks[-1].end == date(2025, 1, 1)
    assert all(left.end == right.start for left, right in pairwise(chunks))
    assert all((item.end - item.start).days <= 180 for item in chunks)


def test_idempotency_sorts_semantically_unordered_arrays() -> None:
    first = {"adjustments": ["raw", "all"], "resolutions": ["30m", "1d"], "x": 1}
    second = {"resolutions": ["1d", "30m"], "x": 1, "adjustments": ["all", "raw"]}
    assert idempotency_key(first) == idempotency_key(second)


@given(st.uuids(), st.integers(min_value=1, max_value=99))
def test_uuid_shard_is_deterministic(value: object, count: int) -> None:
    identifier = str(value)
    first = shard_for(identifier, count)
    assert first == shard_for(identifier, count)
    assert 0 <= first < count


def test_uuid_shard_snapshot() -> None:
    assert shard_for("11111111-1111-1111-1111-111111111111", 8) == 6
