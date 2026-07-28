"""Deterministic schemas, identifiers, hashing, and validation primitives."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
BAR_SCHEMA_VERSION = "market-bars-v1"
HASH_ALGORITHM = "sha256"
WRITER_VERSION = "pyarrow-25.0.0/parquet-2.6"
ROW_GROUP_SIZE = 131_072
DATA_PAGE_SIZE = 1_048_576
UUID_NAMESPACE = uuid.UUID("4a9c4147-e856-5ba7-94de-7758271e23f6")


@dataclass(frozen=True)
class DatasetSpec:
    data_layer: str
    resolution: str
    source_relative_path: Path
    filename_marker: str
    has_source_bars: bool = False
    parent: tuple[str, str] | None = None

    @property
    def key(self) -> tuple[str, str]:
        return self.data_layer, self.resolution


DATASET_SPECS: dict[tuple[str, str], DatasetSpec] = {
    ("ADJUSTED", "30m"): DatasetSpec(
        "ADJUSTED",
        "30m",
        Path("sip_market_data/adjusted/parquet"),
        "_30min_sip_historical",
    ),
    ("DERIVED", "30m"): DatasetSpec(
        "DERIVED",
        "30m",
        Path("regular_sip_30min_market_data/adjusted/parquet"),
        "_30min_sip_historical",
        parent=("ADJUSTED", "30m"),
    ),
    ("DERIVED", "1h"): DatasetSpec(
        "DERIVED",
        "1h",
        Path("regular_sip_1hour_market_data/adjusted/parquet"),
        "_1hour_sip_historical",
        has_source_bars=True,
        parent=("DERIVED", "30m"),
    ),
    ("DERIVED", "4h"): DatasetSpec(
        "DERIVED",
        "4h",
        Path("regular_sip_4hour_market_data/adjusted/parquet"),
        "_4hour_sip_historical",
        has_source_bars=True,
        parent=("DERIVED", "30m"),
    ),
    ("DERIVED", "1d"): DatasetSpec(
        "DERIVED",
        "1d",
        Path("regular_sip_1day_market_data/adjusted/parquet"),
        "_1day_sip_historical",
        has_source_bars=True,
        parent=("DERIVED", "30m"),
    ),
}


def bar_schema(has_source_bars: bool = False) -> pa.Schema:
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
    if has_source_bars:
        fields.append(pa.field("source_bars", pa.int16(), nullable=False))
    return pa.schema(
        fields,
        metadata={
            b"schema_version": BAR_SCHEMA_VERSION.encode("ascii"),
            b"timestamp_semantics": b"bar_start_at_utc",
            b"partition_timezone": b"America/New_York",
            b"writer_version": WRITER_VERSION.encode("ascii"),
        },
    )


@dataclass(frozen=True)
class InstrumentMapping:
    provider_symbol: str
    instrument_id: str
    provider_reference: str | None = None
    asset_type: str | None = None
    primary_exchange_mic: str | None = None


def canonical_provider_symbol(value: str) -> str:
    """Normalize provider symbols in exactly one place.

    Alpaca uses a dot for class-share symbols. Slash-form symbols from the
    legacy ticker list are therefore normalized to dot form. Hyphens are not
    blindly changed because they can be valid provider-symbol characters.
    """
    return value.strip().upper().replace("/", ".")


def load_instrument_map(path: Path) -> dict[str, InstrumentMapping]:
    if not path.is_file():
        raise FileNotFoundError(f"instrument map이 없습니다: {path}")
    mappings: dict[str, InstrumentMapping] = {}
    seen_ids: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"provider_symbol", "instrument_id"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(
                "instrument_map.csv 필수 열이 없습니다: "
                + ", ".join(sorted(missing))
            )
        for line_number, row in enumerate(reader, 2):
            provider_symbol = canonical_provider_symbol(row["provider_symbol"])
            if not provider_symbol:
                raise ValueError(f"{line_number}행 provider_symbol이 비어 있습니다.")
            try:
                instrument_id = str(uuid.UUID(row["instrument_id"].strip()))
            except (AttributeError, ValueError) as exc:
                raise ValueError(
                    f"{line_number}행 instrument_id가 UUID가 아닙니다."
                ) from exc
            if provider_symbol in mappings:
                raise ValueError(f"중복 provider_symbol: {provider_symbol}")
            previous_symbol = seen_ids.get(instrument_id)
            if previous_symbol and previous_symbol != provider_symbol:
                # Symbol history is valid. The current map resolves both aliases
                # to the same immutable instrument and therefore the same shard.
                pass
            seen_ids[instrument_id] = provider_symbol
            mappings[provider_symbol] = InstrumentMapping(
                provider_symbol=provider_symbol,
                instrument_id=instrument_id,
                provider_reference=(row.get("provider_reference") or "").strip()
                or None,
                asset_type=(row.get("asset_type") or "").strip() or None,
                primary_exchange_mic=(
                    row.get("primary_exchange_mic") or ""
                ).strip()
                or None,
            )
    if not mappings:
        raise ValueError("instrument_map.csv에 매핑이 없습니다.")
    return mappings


def provider_symbol_from_path(path: Path) -> str:
    stem = path.stem
    markers = tuple(
        sorted(
            {spec.filename_marker for spec in DATASET_SPECS.values()},
            key=len,
            reverse=True,
        )
    )
    for marker in markers:
        if stem.endswith(marker):
            return canonical_provider_symbol(stem[: -len(marker)])
    return canonical_provider_symbol(stem)


def resolve_mapping(
    path: Path,
    mappings: dict[str, InstrumentMapping],
) -> InstrumentMapping | None:
    symbol = provider_symbol_from_path(path)
    direct = mappings.get(symbol)
    if direct is not None:
        return direct
    # Legacy filenames replaced the ticker-list slash with a hyphen. Only use
    # this fallback when the exact provider symbol did not match.
    if "-" in symbol:
        return mappings.get(symbol.replace("-", "."))
    return None


def stable_shard_number(instrument_id: str, shard_count: int) -> int:
    if shard_count <= 0:
        raise ValueError("shard_count는 양수여야 합니다.")
    canonical_id = str(uuid.UUID(instrument_id))
    digest = hashlib.sha256(canonical_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % shard_count


def shard_key(shard_number: int, shard_count: int) -> str:
    width = max(2, len(str(shard_count - 1)))
    return f"s{shard_number:0{width}d}-of-{shard_count}"


def et_year_bounds_utc(year: int) -> tuple[datetime, datetime]:
    start = datetime(year, 1, 1, tzinfo=ET).astimezone(UTC)
    end = datetime(year + 1, 1, 1, tzinfo=ET).astimezone(UTC)
    return start, end


def deterministic_uuid(*values: object) -> str:
    name = "|".join(str(value) for value in values)
    return str(uuid.uuid5(UUID_NAMESPACE, name))


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_dataset_hash(objects: Iterable[dict[str, Any]]) -> str:
    keys = (
        "content_hash",
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
    canonical_rows = [
        {key: record.get(key) for key in keys}
        for record in objects
    ]
    canonical_rows.sort(
        key=lambda row: json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
    )
    payload = json.dumps(
        canonical_rows,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _integer_series(
    values: pd.Series,
    column: str,
    *,
    nullable: bool,
) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if not nullable and numeric.isna().any():
        raise ValueError(f"{column}에 null 또는 숫자가 아닌 값이 있습니다.")
    non_null = numeric.dropna()
    if not np.isfinite(non_null.astype(float)).all():
        raise ValueError(f"{column}에 infinity가 있습니다.")
    if not np.equal(non_null, np.floor(non_null)).all():
        raise ValueError(f"{column}에 정수가 아닌 값이 있습니다.")
    return numeric.astype("Int64")


def normalize_legacy_frame(
    dataframe: pd.DataFrame,
    mapping: InstrumentMapping,
    spec: DatasetSpec,
    year: int,
) -> pa.Table:
    """Convert a legacy pandas-index file to the canonical explicit schema."""
    frame = dataframe.reset_index() if "timestamp" in dataframe.index.names else dataframe.copy()
    if "timestamp" not in frame.columns:
        if "bar_start_at" in frame.columns:
            frame = frame.rename(columns={"bar_start_at": "timestamp"})
        else:
            raise ValueError("입력에 timestamp 열 또는 인덱스가 없습니다.")
    required = {"open", "high", "low", "close", "volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError("필수 OHLCV 열이 없습니다: " + ", ".join(sorted(missing)))

    timestamps = pd.to_datetime(frame["timestamp"], errors="raise", utc=True)
    start, end = et_year_bounds_utc(year)
    mask = (timestamps >= start) & (timestamps < end)
    frame = frame.loc[mask].copy()
    timestamps = timestamps.loc[mask]
    if frame.empty:
        return pa.Table.from_pylist([], schema=bar_schema(spec.has_source_bars))

    output = pd.DataFrame()
    output["instrument_id"] = pd.Series(
        [mapping.instrument_id] * len(frame),
        dtype="string",
    )
    output["provider_symbol"] = pd.Series(
        [mapping.provider_symbol] * len(frame),
        dtype="string",
    )
    output["bar_start_at"] = timestamps.reset_index(drop=True)
    output["session_date_et"] = (
        timestamps.dt.tz_convert(ET).dt.date.reset_index(drop=True)
    )
    for column in ("open", "high", "low", "close"):
        output[column] = pd.to_numeric(
            frame[column], errors="coerce"
        ).astype("float64").reset_index(drop=True)
    output["volume"] = _integer_series(
        frame["volume"], "volume", nullable=False
    ).reset_index(drop=True)
    if "trade_count" in frame.columns:
        output["trade_count"] = _integer_series(
            frame["trade_count"], "trade_count", nullable=True
        ).reset_index(drop=True)
    else:
        output["trade_count"] = pd.Series([pd.NA] * len(frame), dtype="Int64")
    if "vwap" in frame.columns:
        output["vwap"] = pd.to_numeric(
            frame["vwap"], errors="coerce"
        ).astype("float64").reset_index(drop=True)
    else:
        output["vwap"] = pd.Series([math.nan] * len(frame), dtype="float64")
    if spec.has_source_bars:
        if "source_bars" in frame.columns:
            source_bars = _integer_series(
                frame["source_bars"],
                "source_bars",
                nullable=False,
            )
        elif "source_minutes" in frame.columns:
            source_minutes = _integer_series(
                frame["source_minutes"],
                "source_minutes",
                nullable=False,
            )
            if (source_minutes % 30 != 0).any():
                raise ValueError(
                    "source_minutes가 30분 원본 봉 개수로 정확히 변환되지 않습니다."
                )
            source_bars = source_minutes // 30
        else:
            raise ValueError(
                f"DERIVED {spec.resolution} 입력에 source_bars 또는 "
                "source_minutes가 없습니다."
            )
        output["source_bars"] = source_bars.astype("int16").reset_index(drop=True)

    table = pa.Table.from_pandas(
        output,
        schema=bar_schema(spec.has_source_bars),
        preserve_index=False,
        safe=True,
    )
    return table.replace_schema_metadata(bar_schema(spec.has_source_bars).metadata)


def aggregate_regular_30m(table: pa.Table, resolution: str) -> pa.Table:
    """Aggregate observed regular-session 30m bars without creating gaps."""
    if resolution not in {"1h", "4h", "1d"}:
        raise ValueError(f"지원하지 않는 집계 해상도입니다: {resolution}")
    if not table.schema.equals(bar_schema(False), check_metadata=True):
        raise ValueError("집계 입력은 공식 30m PyArrow 스키마여야 합니다.")
    if table.num_rows == 0:
        return pa.Table.from_pylist([], schema=bar_schema(True))

    frequency = {"1h": "1h", "4h": "4h", "1d": "24h"}[resolution]
    frame = table.to_pandas()
    frames: list[pd.DataFrame] = []
    for (_, _), session in frame.groupby(
        ["instrument_id", "session_date_et"],
        sort=True,
        dropna=False,
    ):
        session = session.sort_values("bar_start_at").set_index("bar_start_at")
        origin = session.index.min()
        resampler = session.resample(
            frequency,
            origin=origin,
            closed="left",
            label="left",
        )
        aggregated = resampler.agg(
            {
                "instrument_id": "first",
                "provider_symbol": "first",
                "session_date_et": "first",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "trade_count": lambda values: values.sum(min_count=1),
            }
        )
        source_bars = resampler["close"].count()
        aggregated = aggregated.loc[source_bars > 0].copy()
        aggregated["source_bars"] = source_bars.loc[aggregated.index].astype(
            "int16"
        )
        valid_vwap = session["vwap"].notna() & session["volume"].notna()
        weighted = (
            (session["vwap"] * session["volume"])
            .where(valid_vwap)
            .resample(
                frequency,
                origin=origin,
                closed="left",
                label="left",
            )
            .sum(min_count=1)
        )
        weight = (
            session["volume"]
            .where(valid_vwap)
            .resample(
                frequency,
                origin=origin,
                closed="left",
                label="left",
            )
            .sum(min_count=1)
        )
        aggregated["vwap"] = (
            weighted.loc[aggregated.index]
            / weight.loc[aggregated.index].replace(0, pd.NA)
        )
        frames.append(aggregated.reset_index())
    output = pd.concat(frames, ignore_index=True)
    aggregated_table = pa.Table.from_pandas(
        output,
        schema=bar_schema(True),
        preserve_index=False,
        safe=True,
    )
    aggregated_table = aggregated_table.replace_schema_metadata(
        bar_schema(True).metadata
    )
    return sort_bar_table(aggregated_table)


def sort_bar_table(table: pa.Table) -> pa.Table:
    if table.num_rows == 0:
        return table
    indices = pc.sort_indices(
        table,
        sort_keys=[
            ("instrument_id", "ascending"),
            ("bar_start_at", "ascending"),
        ],
    )
    return pc.take(table, indices)


def quality_issues(
    table: pa.Table,
    spec: DatasetSpec,
    year: int,
    *,
    calendar_name: str = "XNYS",
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    expected_schema = bar_schema(spec.has_source_bars)
    if not table.schema.equals(expected_schema, check_metadata=True):
        issues.append(
            {
                "severity": "ERROR",
                "code": "SCHEMA_MISMATCH",
                "message": f"expected={expected_schema}, actual={table.schema}",
            }
        )
        return issues
    if table.num_rows == 0:
        return issues

    frame = table.to_pandas()
    duplicated = frame.duplicated(["instrument_id", "bar_start_at"], keep=False)
    if duplicated.any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "DUPLICATE_BAR",
                "row_count": int(duplicated.sum()),
            }
        )
    start, end = et_year_bounds_utc(year)
    out_of_partition = (
        (frame["bar_start_at"] < start)
        | (frame["bar_start_at"] >= end)
        | (frame["session_date_et"].map(lambda value: value.year) != year)
    )
    if out_of_partition.any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "YEAR_BOUNDARY",
                "row_count": int(out_of_partition.sum()),
            }
        )

    prices = frame[["open", "high", "low", "close"]]
    invalid_price = (
        ~np.isfinite(prices.to_numpy(dtype=float)).all(axis=1)
        | (prices <= 0).any(axis=1)
    )
    invalid_ohlc = (
        frame["high"] < prices[["open", "close", "low"]].max(axis=1)
    ) | (
        frame["low"] > prices[["open", "close", "high"]].min(axis=1)
    )
    if invalid_price.any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "INVALID_PRICE",
                "row_count": int(invalid_price.sum()),
            }
        )
    if invalid_ohlc.any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "INVALID_OHLC",
                "row_count": int(invalid_ohlc.sum()),
            }
        )
    if (frame["volume"] < 0).any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "NEGATIVE_VOLUME",
                "row_count": int((frame["volume"] < 0).sum()),
            }
        )
    if frame["trade_count"].dropna().lt(0).any():
        issues.append(
            {
                "severity": "ERROR",
                "code": "NEGATIVE_TRADE_COUNT",
                "row_count": int(frame["trade_count"].dropna().lt(0).sum()),
            }
        )
    opens: dict[date, pd.Timestamp] = {}
    closes: dict[date, pd.Timestamp] = {}
    if spec.data_layer == "DERIVED":
        calendar = mcal.get_calendar(calendar_name)
        dates = sorted(set(frame["session_date_et"]))
        schedule = calendar.schedule(min(dates), max(dates), tz="UTC")
        opens = {
            index.date(): pd.Timestamp(row["market_open"])
            for index, row in schedule.iterrows()
        }
        closes = {
            index.date(): pd.Timestamp(row["market_close"])
            for index, row in schedule.iterrows()
        }
    if spec.has_source_bars:
        maximum = {"1h": 2, "4h": 8, "1d": 13}[spec.resolution]
        invalid_source = ~frame["source_bars"].between(1, maximum)
        if invalid_source.any():
            issues.append(
                {
                    "severity": "ERROR",
                    "code": "INVALID_SOURCE_BARS",
                    "row_count": int(invalid_source.sum()),
                }
            )
        duration = {
            "1h": pd.Timedelta(hours=1),
            "4h": pd.Timedelta(hours=4),
            "1d": pd.Timedelta(days=1),
        }[spec.resolution]
        expected_source_bars = pd.Series(
            [
                max(
                    0,
                    int(
                        (
                            min(timestamp + duration, closes[session_date])
                            - timestamp
                        )
                        / pd.Timedelta(minutes=30)
                    ),
                )
                if session_date in closes
                else 0
                for timestamp, session_date in zip(
                    frame["bar_start_at"],
                    frame["session_date_et"],
                )
            ],
            index=frame.index,
        )
        missing_source = frame["source_bars"] < expected_source_bars
        if missing_source.any():
            issues.append(
                {
                    "severity": "WARNING",
                    "code": "PARTIAL_SOURCE_BARS",
                    "row_count": int(missing_source.sum()),
                }
            )
        excess_source = frame["source_bars"] > expected_source_bars
        if excess_source.any():
            issues.append(
                {
                    "severity": "ERROR",
                    "code": "SOURCE_BARS_EXCEED_SESSION_BIN",
                    "row_count": int(excess_source.sum()),
                }
            )

    if spec.data_layer == "DERIVED":
        outside = pd.Series(
            [
                session_date not in opens
                or timestamp < opens.get(session_date, timestamp)
                or timestamp >= closes.get(session_date, timestamp)
                for timestamp, session_date in zip(
                    frame["bar_start_at"], frame["session_date_et"]
                )
            ]
        )
        if outside.any():
            issues.append(
                {
                    "severity": "ERROR",
                    "code": "OUTSIDE_XNYS_REGULAR_SESSION",
                    "row_count": int(outside.sum()),
                }
            )
        if spec.resolution == "30m":
            missing_count = 0
            for (_, session_date), session in frame.groupby(
                ["instrument_id", "session_date_et"]
            ):
                market_open = opens.get(session_date)
                market_close = closes.get(session_date)
                if market_open is None or market_close is None:
                    continue
                expected = pd.date_range(
                    market_open,
                    market_close,
                    freq="30min",
                    inclusive="left",
                )
                actual = pd.DatetimeIndex(session["bar_start_at"])
                missing_count += len(expected.difference(actual))
            if missing_count:
                issues.append(
                    {
                        "severity": "WARNING",
                        "code": "MISSING_REGULAR_BAR",
                        "row_count": missing_count,
                    }
                )
    return issues


def write_parquet_atomic(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            table,
            temporary_path,
            compression="NONE",
            version="2.6",
            data_page_version="2.0",
            use_dictionary=False,
            write_statistics=True,
            row_group_size=ROW_GROUP_SIZE,
            data_page_size=DATA_PAGE_SIZE,
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
            store_schema=True,
        )
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def rows_per_part(table: pa.Table, target_size_bytes: int) -> int:
    if table.num_rows == 0:
        return 0
    if target_size_bytes <= 0:
        raise ValueError("target_size_bytes는 양수여야 합니다.")
    bytes_per_row = max(1.0, table.nbytes / table.num_rows)
    return max(1, int(target_size_bytes / bytes_per_row))


def iso_utc(value: datetime | pd.Timestamp) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat()


def json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        return iso_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, np.generic):
        return value.item()
    return value
