"""Stable schemas, identifiers, partition rules, and object-key contracts."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal
from zoneinfo import ZoneInfo

import pyarrow as pa


PROVIDER_CODE = "ALPACA"
CALENDAR_NAME = "XNYS"
ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SCHEMA_VERSION = "market-bars-v2"
WRITER_VERSION = "pyarrow-25.0.0/parquet-2.6"
HASH_ALGORITHM = "sha256"
UUID_NAMESPACE = uuid.UUID("05a27d5a-75d8-4d57-bc9a-31cedf90d791")

PriceType = Literal["raw", "adjusted"]
DataLayer = Literal["RAW", "ADJUSTED", "DERIVED"]
#: `1m` exists because C publishes `BAR_1M` (`MarketEventType.java`) and D conforms
#: to what C already publishes.  Without it a one-minute bar has no dataset contract
#: to land in, so a realtime object could not carry a canonical `object_key` and
#: could therefore never enter `MarketPipelineEngine.compact`.
#:
#: This is a code-level literal, not canonical DDL: `market_data.dataset_manifests.
#: resolution` and `market_data.feeds.resolution` are `varchar(30)` in the central
#: `V1__initial_schema.sql`, with no CHECK constraint and no enum, so adding a
#: resolution needs no migration and touches no protected contract.
Resolution = Literal["1m", "30m", "1h", "4h", "1d"]
Granularity = Literal["DAY", "WEEK", "MONTH", "YEAR"]


@dataclass(frozen=True)
class DatasetContract:
    """Logical dataset contract kept separate by source price semantics."""

    price_type: PriceType
    data_layer: DataLayer
    resolution: Resolution
    feed_code: str
    source_resolution: Resolution = "30m"

    @property
    def key(self) -> tuple[str, str, str]:
        return self.price_type, self.data_layer, self.resolution

    @property
    def has_source_minutes(self) -> bool:
        return self.data_layer == "DERIVED"

    @property
    def logical_code(self) -> str:
        return (
            f"{self.feed_code}:{self.data_layer}:{self.resolution}"
        )


RAW_FEED = "ALPACA_SIP_RAW_30M"
ADJUSTED_FEED = "ALPACA_SIP_ADJUSTED_30M"

#: The feed C's realtime `BAR_1M` stream lands in.  It is a *separate* feed from
#: `ALPACA_SIP_RAW_30M`, not a second resolution of it: `market_data.feeds` carries
#: one `resolution` per row, and `market_data.stream_watermarks` is keyed by
#: `feed_id`, so sharing a feed would make the 30-minute backfill's watermark and
#: the 1-minute stream's watermark the same row and each would keep clobbering the
#: other's freshness.
RAW_1M_FEED = "ALPACA_SIP_RAW_1M"

#: `market_data.feeds` metadata per feed code: `(resolution, feed_version)`.
#: Stated per feed rather than derived, because the resolution of a feed row is a
#: published fact about that feed and the previous hardcoded `"30m"` would have
#: labelled the 1-minute feed as 30-minute.
FEED_METADATA: dict[str, tuple[str, str]] = {
    RAW_FEED: ("30m", "alpaca-sip-raw-v1"),
    ADJUSTED_FEED: ("30m", "alpaca-sip-adjustment-all-v1"),
    RAW_1M_FEED: ("1m", "alpaca-sip-raw-1m-v1"),
}


def _contracts() -> dict[tuple[str, str, str], DatasetContract]:
    values: list[DatasetContract] = [
        DatasetContract("raw", "RAW", "30m", RAW_FEED),
        DatasetContract("adjusted", "ADJUSTED", "30m", ADJUSTED_FEED),
        # C's native cadence.  RAW only: an ADJUSTED 1-minute dataset would have to
        # be produced by the corporate-action path, and C publishes no adjusted
        # prices, so declaring one would promise a dataset nothing fills.
        DatasetContract("raw", "RAW", "1m", RAW_1M_FEED),
    ]
    for price_type, feed_code in (
        ("raw", RAW_FEED),
        ("adjusted", ADJUSTED_FEED),
    ):
        for resolution in ("1h", "4h", "1d"):
            values.append(
                DatasetContract(
                    price_type,  # type: ignore[arg-type]
                    "DERIVED",
                    resolution,  # type: ignore[arg-type]
                    feed_code,
                )
            )
    return {value.key: value for value in values}


DATASET_CONTRACTS = _contracts()


def selected_contracts(
    price_type: str = "all",
    resolution: str = "all",
    layer: str = "all",
) -> tuple[DatasetContract, ...]:
    selected = []
    for contract in DATASET_CONTRACTS.values():
        if price_type != "all" and contract.price_type != price_type:
            continue
        if resolution != "all" and contract.resolution != resolution:
            continue
        if layer != "all" and contract.data_layer != layer:
            continue
        selected.append(contract)
    return tuple(
        sorted(
            selected,
            key=lambda value: (
                value.price_type,
                value.data_layer,
                value.resolution,
            ),
        )
    )


def bar_schema(derived: bool = False) -> pa.Schema:
    fields = [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("provider_symbol", pa.string(), nullable=False),
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("session_date_et", pa.date32(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("trade_count", pa.int64(), nullable=True),
        pa.field("vwap", pa.float64(), nullable=True),
    ]
    if derived:
        fields.append(pa.field("source_minutes", pa.int16(), nullable=False))
    return pa.schema(
        fields,
        metadata={
            b"schema_version": SCHEMA_VERSION.encode("ascii"),
            b"timestamp_semantics": b"bar_start_at_utc",
            b"partition_timezone": b"America/New_York",
            b"writer_version": WRITER_VERSION.encode("ascii"),
        },
    )


@dataclass(frozen=True)
class InstrumentMapping:
    """One row of the operator's instrument map.

    Everything after `instrument_id` is optional *here* because the collection path
    only needs the symbol and the id.  The reference-data registration path
    (`market_pipeline_lib.reference`) needs the full canonical identity and refuses
    a mapping that omits it -- the columns are carried verbatim as text so this
    module stays a plain reader and every validation lives in one place.
    """

    provider_symbol: str
    instrument_id: str
    provider_reference: str | None = None
    asset_type: str | None = None
    primary_exchange_mic: str | None = None
    currency_code: str | None = None
    listed_at: str | None = None
    delisted_at: str | None = None
    symbol_effective_from: str | None = None


def canonical_provider_symbol(value: str) -> str:
    return value.strip().upper().replace("/", ".")


def safe_provider_symbol(value: str) -> str:
    return canonical_provider_symbol(value).replace(".", "-")


def load_instrument_map(path: Path) -> dict[str, InstrumentMapping]:
    import csv

    if not path.is_file():
        raise FileNotFoundError(f"instrument map이 없습니다: {path}")
    mappings: dict[str, InstrumentMapping] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"provider_symbol", "instrument_id"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(
                "instrument map에는 provider_symbol,instrument_id 열이 필요합니다."
            )
        for line_number, row in enumerate(reader, 2):
            symbol = canonical_provider_symbol(row["provider_symbol"])
            if not symbol:
                raise ValueError(f"instrument map {line_number}행의 symbol이 비었습니다.")
            try:
                instrument_id = str(uuid.UUID(row["instrument_id"].strip()))
            except (AttributeError, ValueError) as exc:
                raise ValueError(
                    f"instrument map {line_number}행 UUID가 잘못되었습니다."
                ) from exc
            if symbol in mappings:
                raise ValueError(f"instrument map 중복 symbol: {symbol}")
            mappings[symbol] = InstrumentMapping(
                provider_symbol=symbol,
                instrument_id=instrument_id,
                provider_reference=(row.get("provider_reference") or None),
                asset_type=(row.get("asset_type") or None),
                primary_exchange_mic=(row.get("primary_exchange_mic") or None),
                currency_code=(row.get("currency_code") or None),
                listed_at=(row.get("listed_at") or None),
                delisted_at=(row.get("delisted_at") or None),
                symbol_effective_from=(row.get("symbol_effective_from") or None),
            )
    return mappings


def deterministic_uuid(*values: object) -> str:
    return str(
        uuid.uuid5(
            UUID_NAMESPACE,
            "|".join(str(value) for value in values),
        )
    )


def stable_shard_number(instrument_id: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count는 양수여야 합니다.")
    canonical = str(uuid.UUID(instrument_id))
    digest = hashlib.sha256(canonical.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % shard_count


def stable_shard_key(instrument_id: str, shard_count: int) -> str:
    number = stable_shard_number(instrument_id, shard_count)
    width = max(2, len(str(shard_count - 1)))
    return f"s{number:0{width}d}-of-{shard_count}"


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.astimezone(ET).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def partition_bounds(
    value: date | datetime | str,
    granularity: Granularity,
) -> tuple[date, date]:
    current = _as_date(value)
    if granularity == "DAY":
        start = current
        end = current + timedelta(days=1)
    elif granularity == "WEEK":
        start = current - timedelta(days=current.weekday())
        end = start + timedelta(days=7)
    elif granularity == "MONTH":
        start = current.replace(day=1)
        end = (
            date(start.year + 1, 1, 1)
            if start.month == 12
            else date(start.year, start.month + 1, 1)
        )
    elif granularity == "YEAR":
        start = date(current.year, 1, 1)
        end = date(current.year + 1, 1, 1)
    else:
        raise ValueError(f"지원하지 않는 granularity: {granularity}")
    return start, end


def partition_utc_bounds(
    value: date | datetime | str,
    granularity: Granularity,
) -> tuple[datetime, datetime]:
    start, end = partition_bounds(value, granularity)
    return (
        datetime.combine(start, datetime.min.time(), ET).astimezone(UTC),
        datetime.combine(end, datetime.min.time(), ET).astimezone(UTC),
    )


def logical_dataset_id(contract: DatasetContract, year: int) -> str:
    return deterministic_uuid(
        "dataset",
        PROVIDER_CODE,
        contract.feed_code,
        contract.data_layer,
        contract.resolution,
        year,
    )


def object_key(
    contract: DatasetContract,
    dataset_id: str,
    revision: int,
    granularity: Granularity,
    partition_start: date,
    partition_end: date,
    shard_key: str,
    part_number: int,
) -> str:
    if revision <= 0 or part_number <= 0:
        raise ValueError("revision과 part_number는 1 이상이어야 합니다.")
    return "/".join(
        (
            "market-data",
            f"provider={PROVIDER_CODE}",
            f"feed={contract.feed_code}",
            f"dataset={dataset_id}",
            f"revision={revision}",
            f"layer={contract.data_layer}",
            f"resolution={contract.resolution}",
            f"granularity={granularity}",
            f"partition_start={partition_start.isoformat()}",
            f"partition_end={partition_end.isoformat()}",
            f"shard={shard_key}",
            f"part-{part_number:05d}.parquet",
        )
    )


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_dataset_hash(objects: Iterable[dict[str, Any]]) -> str:
    keys = (
        "content_hash",
        "object_kind",
        "partition_granularity",
        "partition_start",
        "partition_end",
        "period_start",
        "period_end",
        "shard_key",
        "part_number",
        "row_count",
        "schema_version",
    )
    rows = [{key: item.get(key) for key in keys} for item in objects]
    rows.sort(
        key=lambda row: json.dumps(
            row,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    payload = json.dumps(
        rows,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timezone-aware datetime이 필요합니다.")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
