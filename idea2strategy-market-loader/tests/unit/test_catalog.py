from __future__ import annotations

from pathlib import Path

import pytest

from market_loader.errors import InputError
from market_loader.model.catalog import read_universe, universe_hash


def test_read_universe_and_hash_are_order_independent(tmp_path: Path) -> None:
    header = (
        "provider_symbol,asset_type,primary_exchange_mic,effective_from,"
        "effective_to,support_status,instrument_id\n"
    )
    rows = [
        "AAPL,STOCK,XNAS,2016-01-01,,ACTIVE,11111111-1111-1111-1111-111111111111\n",
        "SPY,ETF,ARCX,2016-01-01,,ACTIVE,22222222-2222-2222-2222-222222222222\n",
    ]
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    first.write_text(header + "".join(rows), encoding="utf-8")
    second.write_text(header + "".join(reversed(rows)), encoding="utf-8")
    assert universe_hash(read_universe(first)) == universe_hash(read_universe(second))


def test_overlapping_symbol_periods_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "universe.csv"
    source.write_text(
        "provider_symbol,asset_type,primary_exchange_mic,effective_from,"
        "effective_to,support_status\n"
        "ABC,STOCK,XNAS,2020-01-01,2022-01-01,ACTIVE\n"
        "ABC,STOCK,XNAS,2021-01-01,,ACTIVE\n",
        encoding="utf-8",
    )
    with pytest.raises(InputError, match="overlapping"):
        read_universe(source)
