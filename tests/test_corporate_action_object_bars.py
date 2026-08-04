from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_pipeline_lib.corporate_actions.adjustment import Bar
from market_pipeline_lib.corporate_actions.object_bars import (
    CatalogObjectBarReader,
    ImmutableObjectBarWriter,
)
from market_pipeline_lib.storage import LocalObjectStore


class _Catalog:
    def __init__(self, objects: list[dict[str, Any]]) -> None:
        self._objects = objects

    def objects_for_manifest(self, manifest_id: str) -> list[dict[str, Any]]:
        assert manifest_id == "raw-manifest"
        return self._objects


def _bars() -> tuple[Bar, ...]:
    return (
        Bar(
            instrument_id="10000000-0000-4000-8000-000000000001",
            bar_start_at=datetime(2026, 1, 2, 14, 30, tzinfo=UTC),
            open=Decimal("100.00000000"), high=Decimal("101.00000000"),
            low=Decimal("99.00000000"), close=Decimal("100.50000000"), volume=1000,
            provider_symbol="AAPL", session_date_et=date(2026, 1, 2), trade_count=10,
            vwap=Decimal("100.25000000"),
        ),
    )


def test_writer_registers_immutable_object_and_reader_round_trips_canonical_bars(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    written = ImmutableObjectBarWriter(
        object_store=store, staging_root=tmp_path / "staging"
    ).write_bars(
        _bars(), dataset_key="feed=adjusted/layer=ADJUSTED/resolution=30m/revision=1"
    )

    assert written.storage_record is not None
    assert written.relation_record is not None
    assert written.storage_record["content_hash"] == written.content_hash
    assert written.storage_record["row_count"] == 1
    assert written.relation_record["part_number"] == 1
    catalog = _Catalog(
        [{**written.relation_record, "storage": written.storage_record}]
    )
    restored = CatalogObjectBarReader(catalog=catalog, object_store=store).read_bars("raw-manifest")
    assert restored == _bars()


def test_reader_refuses_an_object_whose_registered_hash_does_not_match(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    written = ImmutableObjectBarWriter(
        object_store=store, staging_root=tmp_path / "staging"
    ).write_bars(
        _bars(), dataset_key="feed=adjusted/layer=ADJUSTED/resolution=30m/revision=1"
    )
    assert written.storage_record is not None
    catalog = _Catalog(
        [{"storage": {**written.storage_record, "content_hash": "0" * 64}}]
    )
    try:
        CatalogObjectBarReader(catalog=catalog, object_store=store).read_bars("raw-manifest")
    except OSError as error:
        assert "verification failed" in str(error)
    else:
        raise AssertionError("tampered registration was accepted")
