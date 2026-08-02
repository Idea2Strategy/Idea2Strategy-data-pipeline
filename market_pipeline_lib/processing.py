"""Streaming-friendly normalization, derivation, validation, and Parquet writing."""

from __future__ import annotations

import math
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .contracts import (
    CALENDAR_NAME,
    ET,
    SCHEMA_VERSION,
    DatasetContract,
    InstrumentMapping,
    bar_schema,
)
from .fs_paths import short_temp_path
from .quality import (
    Finding,
    bar_span,
    detect_derived_session_issues,
    detect_duplicate_bars,
    detect_invalid_values,
    detect_missing_bars,
    detect_out_of_order_bars,
    detect_partition_boundary_violation,
    detect_price_outliers,
    detect_session_date_mismatch,
    detect_volume_anomalies,
    normalise_bar_frame,
    schema_mismatch_finding,
)


ROW_GROUP_SIZE = 131_072
DATA_PAGE_SIZE = 1_048_576
PARQUET_WRITE_OPTIONS = {
    "version": "2.6",
    "data_page_version": "2.0",
    "compression": "NONE",
    "use_dictionary": False,
    "row_group_size": ROW_GROUP_SIZE,
    "data_page_size": DATA_PAGE_SIZE,
    "coerce_timestamps": "us",
    "allow_truncated_timestamps": False,
    "write_statistics": True,
}


def _source_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.reset_index() if isinstance(frame.index, pd.MultiIndex) else frame.copy()
    if "timestamp" not in output.columns and output.index.name == "timestamp":
        output = output.reset_index()
    if "bar_start_at" in output.columns and "timestamp" not in output.columns:
        output = output.rename(columns={"bar_start_at": "timestamp"})
    if "symbol" not in output.columns:
        output["symbol"] = None
    return output


def normalize_provider_frame(
    frame: pd.DataFrame,
    mapping: InstrumentMapping,
) -> pa.Table:
    """Convert an Alpaca or legacy frame to the fixed provider schema."""
    if frame.empty:
        return pa.Table.from_pylist([], schema=bar_schema(False))
    source = _source_dataframe(frame)
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"공급자 30분봉 필수 열이 없습니다: {sorted(missing)}")
    timestamps = pd.to_datetime(source["timestamp"], utc=True, errors="raise")
    output = pd.DataFrame(
        {
            "instrument_id": mapping.instrument_id,
            "provider_symbol": mapping.provider_symbol,
            "bar_start_at": timestamps,
            "session_date_et": timestamps.dt.tz_convert(ET).dt.date,
            "open": pd.to_numeric(source["open"], errors="raise").astype("float64"),
            "high": pd.to_numeric(source["high"], errors="raise").astype("float64"),
            "low": pd.to_numeric(source["low"], errors="raise").astype("float64"),
            "close": pd.to_numeric(source["close"], errors="raise").astype("float64"),
            "volume": pd.to_numeric(source["volume"], errors="raise").astype("int64"),
        }
    )
    for column, dtype in (("trade_count", "Int64"), ("vwap", "Float64")):
        if column in source.columns:
            output[column] = pd.to_numeric(source[column], errors="coerce").astype(dtype)
        else:
            output[column] = pd.Series(pd.NA, index=output.index, dtype=dtype)
    output = output.drop_duplicates(
        subset=["instrument_id", "bar_start_at"],
        keep="last",
    ).sort_values(["instrument_id", "bar_start_at"], kind="mergesort")
    table = pa.Table.from_pandas(
        output,
        schema=bar_schema(False),
        preserve_index=False,
        safe=True,
    )
    return table.replace_schema_metadata(bar_schema(False).metadata)


def normalize_legacy_frame(
    frame: pd.DataFrame,
    mapping: InstrumentMapping,
    contract: DatasetContract,
) -> pa.Table:
    """Normalize a legacy per-symbol file without inventing missing values."""
    if contract.data_layer != "DERIVED":
        return normalize_provider_frame(frame, mapping)
    if frame.empty:
        return pa.Table.from_pylist([], schema=bar_schema(True))
    source = _source_dataframe(frame)
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"기존 파생봉 필수 열이 없습니다: {sorted(missing)}")
    timestamps = pd.to_datetime(source["timestamp"], utc=True, errors="raise")
    if "source_minutes" in source.columns:
        source_minutes = pd.to_numeric(
            source["source_minutes"], errors="raise"
        ).astype("int16")
    elif "source_bars" in source.columns:
        source_minutes = (
            pd.to_numeric(source["source_bars"], errors="raise") * 30
        ).astype("int16")
    else:
        raise ValueError("기존 파생봉에 source_minutes가 없습니다.")
    output = pd.DataFrame(
        {
            "instrument_id": mapping.instrument_id,
            "provider_symbol": mapping.provider_symbol,
            "bar_start_at": timestamps,
            "session_date_et": timestamps.dt.tz_convert(ET).dt.date,
            "open": pd.to_numeric(source["open"], errors="raise").astype("float64"),
            "high": pd.to_numeric(source["high"], errors="raise").astype("float64"),
            "low": pd.to_numeric(source["low"], errors="raise").astype("float64"),
            "close": pd.to_numeric(source["close"], errors="raise").astype("float64"),
            "volume": pd.to_numeric(source["volume"], errors="raise").astype("int64"),
            "source_minutes": source_minutes,
        }
    )
    for column, dtype in (("trade_count", "Int64"), ("vwap", "Float64")):
        if column in source.columns:
            output[column] = pd.to_numeric(source[column], errors="coerce").astype(dtype)
        else:
            output[column] = pd.Series(pd.NA, index=output.index, dtype=dtype)
    output = output[
        [
            "instrument_id",
            "provider_symbol",
            "bar_start_at",
            "session_date_et",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
            "source_minutes",
        ]
    ].drop_duplicates(
        subset=["instrument_id", "bar_start_at"],
        keep="last",
    )
    table = pa.Table.from_pandas(
        output.sort_values(["instrument_id", "bar_start_at"], kind="mergesort"),
        schema=bar_schema(True),
        preserve_index=False,
        safe=True,
    )
    return table.replace_schema_metadata(bar_schema(True).metadata)


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
    return table.take(indices)


def filter_table_period(
    table: pa.Table,
    start: datetime,
    end: datetime,
) -> pa.Table:
    timestamps = table.column("bar_start_at")
    mask = pc.and_(
        pc.greater_equal(timestamps, pa.scalar(start, timestamps.type)),
        pc.less(timestamps, pa.scalar(end, timestamps.type)),
    )
    return table.filter(mask)


def _schedule(
    start: date,
    end: date,
    calendar_name: str = CALENDAR_NAME,
) -> pd.DataFrame:
    return mcal.get_calendar(calendar_name).schedule(
        start_date=start,
        end_date=end,
        tz="UTC",
    )


def derive_regular_bars(
    provider_table: pa.Table,
    resolution: str,
    calendar_name: str = CALENDAR_NAME,
) -> pa.Table:
    """Filter to XNYS and aggregate observed 30m bars without gap filling."""
    if resolution not in {"1h", "4h", "1d"}:
        raise ValueError(f"지원하지 않는 파생 해상도: {resolution}")
    if provider_table.num_rows == 0:
        return pa.Table.from_pylist([], schema=bar_schema(True))
    source = provider_table.to_pandas()
    source["bar_start_at"] = pd.to_datetime(source["bar_start_at"], utc=True)
    start_date = source["session_date_et"].min()
    end_date = source["session_date_et"].max()
    schedule = _schedule(start_date, end_date, calendar_name)
    schedule_by_date = {
        pd.Timestamp(index).date(): (
            pd.Timestamp(row["market_open"]).tz_convert("UTC"),
            pd.Timestamp(row["market_close"]).tz_convert("UTC"),
        )
        for index, row in schedule.iterrows()
    }
    duration = {"1h": 60, "4h": 240, "1d": 24 * 60}[resolution]
    rows: list[dict[str, Any]] = []
    for (instrument_id, session_date), group in source.groupby(
        ["instrument_id", "session_date_et"],
        sort=True,
    ):
        bounds = schedule_by_date.get(session_date)
        if bounds is None:
            continue
        market_open, market_close = bounds
        regular = group[
            (group["bar_start_at"] >= market_open)
            & (group["bar_start_at"] < market_close)
        ].copy()
        if regular.empty:
            continue
        offset_minutes = (
            regular["bar_start_at"] - market_open
        ).dt.total_seconds().div(60)
        regular["_bin"] = 0 if resolution == "1d" else (offset_minutes // duration).astype(int)
        for _, bucket in regular.groupby("_bin", sort=True):
            bucket = bucket.sort_values("bar_start_at")
            volumes = bucket["volume"].astype("int64")
            valid_vwap = bucket["vwap"].notna()
            vwap = None
            if valid_vwap.any():
                weight = volumes[valid_vwap].sum()
                if weight > 0:
                    vwap = float(
                        (
                            bucket.loc[valid_vwap, "vwap"].astype(float)
                            * volumes[valid_vwap]
                        ).sum()
                        / weight
                    )
            trade_count = (
                int(bucket["trade_count"].sum())
                if bucket["trade_count"].notna().any()
                else None
            )
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "provider_symbol": bucket.iloc[0]["provider_symbol"],
                    "bar_start_at": bucket.iloc[0]["bar_start_at"],
                    "session_date_et": session_date,
                    "open": float(bucket.iloc[0]["open"]),
                    "high": float(bucket["high"].max()),
                    "low": float(bucket["low"].min()),
                    "close": float(bucket.iloc[-1]["close"]),
                    "volume": int(volumes.sum()),
                    "trade_count": trade_count,
                    "vwap": vwap,
                    "source_minutes": int(len(bucket) * 30),
                }
            )
    if not rows:
        return pa.Table.from_pylist([], schema=bar_schema(True))
    table = pa.Table.from_pylist(rows, schema=bar_schema(True))
    return sort_bar_table(table).replace_schema_metadata(bar_schema(True).metadata)


def quality_findings(
    table: pa.Table,
    contract: DatasetContract,
    *,
    partition_start: date | None = None,
    partition_end: date | None = None,
    calendar_name: str = CALENDAR_NAME,
) -> list[Finding]:
    """Every D10 check family, each finding carrying its own impact scope.

    Ordering (역순) is detected on the row order of ``table`` **as given**.  The
    caller must therefore validate *before* sorting: ``sort_bar_table`` repairs
    the defect and destroys the evidence in the same step.

    Missing bars (누락 bar) are derived from the session calendar for the base
    RAW/ADJUSTED layer.  The DERIVED layer keeps its ``source_minutes`` checks,
    which measure a different thing (how much 30m input each derived bar
    aggregated), and adding a second calendar expectation on top of unevenly
    spaced derived bars would double-report the same gap.
    """
    expected_schema = bar_schema(contract.has_source_minutes)
    if table.schema != expected_schema:
        return [schema_mismatch_finding(expected_schema, table.schema)]
    if table.num_rows == 0:
        return []
    frame = normalise_bar_frame(table.to_pandas())
    span = bar_span(contract.resolution)
    findings: list[Finding] = []
    findings.extend(detect_duplicate_bars(frame, span=span))
    findings.extend(detect_out_of_order_bars(frame, span=span))
    findings.extend(detect_invalid_values(frame, span=span))
    findings.extend(detect_price_outliers(frame, span=span))
    findings.extend(detect_volume_anomalies(frame, span=span))
    findings.extend(detect_session_date_mismatch(frame, span=span))
    if partition_start is not None and partition_end is not None:
        findings.extend(
            detect_partition_boundary_violation(frame, partition_start, partition_end)
        )
    if contract.data_layer == "DERIVED":
        findings.extend(
            detect_derived_session_issues(
                frame,
                resolution=contract.resolution,
                span=span,
                calendar_name=calendar_name,
            )
        )
    else:
        findings.extend(
            detect_missing_bars(frame, span=span, calendar_name=calendar_name)
        )
    return findings


def quality_issues(
    table: pa.Table,
    contract: DatasetContract,
    *,
    partition_start: date | None = None,
    partition_end: date | None = None,
    calendar_name: str = CALENDAR_NAME,
) -> list[dict[str, Any]]:
    """Flat, JSON-safe rendering of :func:`quality_findings`.

    ``severity``/``code``/``message`` keep their historical names so existing
    callers are unaffected; ``instrument_id``, ``scope_breadth``,
    ``period_start``, ``period_end``, ``affected_bar_count`` and
    ``policy_version`` are added so the impact scope survives the trip to
    ``engine.py``, which is what finally lands it in
    ``market_data.quality_incidents``.
    """
    return [
        finding.as_issue()
        for finding in quality_findings(
            table,
            contract,
            partition_start=partition_start,
            partition_end=partition_end,
            calendar_name=calendar_name,
        )
    ]


def write_parquet(table: pa.Table, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = short_temp_path(destination)
    try:
        pq.write_table(table, temporary, **PARQUET_WRITE_OPTIONS)
        metadata = pq.read_metadata(temporary)
        if metadata.num_rows != table.num_rows:
            raise OSError("Parquet Footer 행 수 검증에 실패했습니다.")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def scan_tables(
    paths: Iterable[Path],
    *,
    columns: list[str] | None = None,
    filter_expression: ds.Expression | None = None,
    batch_size: int = 65_536,
) -> Iterator[pa.Table]:
    files = [str(path) for path in paths]
    if not files:
        return
    dataset = ds.dataset(files, format="parquet")
    scanner = dataset.scanner(
        columns=columns,
        filter=filter_expression,
        batch_size=batch_size,
        use_threads=True,
    )
    for batch in scanner.to_batches():
        yield pa.Table.from_batches([batch])


def split_table_by_time(
    table: pa.Table,
    max_rows: int,
) -> list[pa.Table]:
    """Split on timestamp boundaries so parts never overlap in time."""
    if table.num_rows <= max_rows:
        return [sort_bar_table(table)]
    frame = table.select(["bar_start_at"]).to_pandas()
    counts = frame.groupby("bar_start_at", sort=True).size()
    parts: list[pa.Table] = []
    start = 0
    running = 0
    boundaries: list[int] = []
    for _, count in counts.items():
        if running and running + int(count) > max_rows:
            boundaries.append(start + running)
            start += running
            running = 0
        running += int(count)
    boundaries.append(table.num_rows)
    time_sorted = table.take(
        pc.sort_indices(
            table,
            sort_keys=[
                ("bar_start_at", "ascending"),
                ("instrument_id", "ascending"),
            ],
        )
    )
    lower = 0
    for upper in boundaries:
        parts.append(sort_bar_table(time_sorted.slice(lower, upper - lower)))
        lower = upper
    return parts


def estimate_rows_for_size(table: pa.Table, size_mib: int) -> int:
    if table.num_rows == 0:
        return 1
    bytes_per_row = max(1.0, table.nbytes / table.num_rows)
    return max(1, math.floor(size_mib * 1024 * 1024 / bytes_per_row))
