"""Alpaca SIP 30m adapter plus deprecated per-symbol long-term migration path.

The official pipeline publishes provider-native 30m bars through
``market_pipeline.py`` and its ObjectStore/Catalog boundaries.  The legacy
``update_symbol_data`` function remains only so existing local files and tests
can be migrated without silently deleting user data.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient

from data_collection.collect_sip_1min import (
    CollectionResult,
    InactiveSymbolCache,
    PRICE_COLUMNS,
    alpaca_symbol,
    deduplicate_bars,
    earliest_changed_timestamp,
    load_local_data,
    merge_frames,
    save_local_data,
    should_skip_inactive_symbol,
    trim_to_window,
)
from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    filter_regular_session,
    session_open_for_timestamp,
)
from data_filtering.resample_sip_5min import BAR_INTERVALS, resample_sip_bars


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HISTORY_YEARS = 10
END_DELAY_MINUTES = 15
BASE_TIMEFRAME_MINUTES = 30
DEFAULT_CHUNK_DAYS = 180
DEFAULT_REQUEST_DELAY_SECONDS = 0.35
OUTPUT_INTERVALS = ("1hour", "4hour", "1day")
OUTPUT_ROOTS = {
    interval: PROJECT_ROOT / f"regular_sip_{interval}_market_data"
    for interval in OUTPUT_INTERVALS
}


def rolling_window(
    end_time: datetime,
    years: int = HISTORY_YEARS,
) -> tuple[datetime, datetime]:
    """Return a UTC rolling window whose lower boundary starts at midnight."""
    if years <= 0:
        raise ValueError("years는 양수여야 합니다.")
    end = pd.Timestamp(end_time)
    if end.tzinfo is None:
        end = end.tz_localize("UTC")
    else:
        end = end.tz_convert("UTC")
    start = (end - pd.DateOffset(years=years)).normalize()
    return start.to_pydatetime(), end.to_pydatetime()


def output_path(
    symbol: str,
    interval: str,
    data_type: str,
    storage_format: str,
    output_roots: dict[str, Path] | None = None,
) -> Path:
    if interval not in OUTPUT_INTERVALS:
        raise ValueError(f"지원하지 않는 출력 봉 간격입니다: {interval}")
    roots = output_roots or OUTPUT_ROOTS
    safe_symbol = symbol.replace("/", "-")
    return (
        roots[interval]
        / data_type
        / storage_format
        / f"{safe_symbol}_{interval}_sip_historical.{storage_format}"
    )


def output_paths(
    symbol: str,
    data_type: str,
    storage_format: str,
    output_roots: dict[str, Path] | None = None,
) -> dict[str, Path]:
    return {
        interval: output_path(
            symbol,
            interval,
            data_type,
            storage_format,
            output_roots,
        )
        for interval in OUTPUT_INTERVALS
    }


def fetch_chunk(
    client: StockHistoricalDataClient,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    data_type: str,
    retries: int = 3,
) -> pd.DataFrame | None:
    """Fetch provider-native 30-minute SIP bars for one bounded interval."""
    adjustment = Adjustment.ALL if data_type == "adjusted" else Adjustment.RAW
    request = StockBarsRequest(
        symbol_or_symbols=[alpaca_symbol(symbol)],
        timeframe=TimeFrame(BASE_TIMEFRAME_MINUTES, TimeFrameUnit.Minute),
        start=start_time,
        end=end_time,
        adjustment=adjustment,
        feed=DataFeed.SIP,
    )
    for attempt in range(1, retries + 1):
        try:
            bars = client.get_stock_bars(request)
            dataframe = bars.df
            return dataframe if not dataframe.empty else pd.DataFrame()
        except Exception as exc:
            if attempt == retries:
                print(f"[{symbol}] 30분봉 요청 실패 ({start_time} ~ {end_time}): {exc}")
                return None
            wait_seconds = 2 ** (attempt - 1)
            print(f"[{symbol}] 요청 재시도 {attempt}/{retries - 1} ({wait_seconds}초 후): {exc}")
            time.sleep(wait_seconds)
    return None


def fetch_range(
    client: StockHistoricalDataClient,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    data_type: str,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    request_delay: float = DEFAULT_REQUEST_DELAY_SECONDS,
) -> pd.DataFrame | None:
    if chunk_days <= 0 or request_delay < 0:
        raise ValueError("chunk_days는 양수, request_delay는 0 이상이어야 합니다.")
    frames: list[pd.DataFrame] = []
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end_time)
        dataframe = fetch_chunk(
            client,
            symbol,
            chunk_start,
            chunk_end,
            data_type,
        )
        if dataframe is None:
            return None
        if not dataframe.empty:
            frames.append(dataframe)
        print(
            f"[{symbol}] 30분봉 {chunk_start.date()} ~ {chunk_end.date()}: "
            f"{len(dataframe):,}행"
        )
        chunk_start = chunk_end
        if request_delay > 0:
            time.sleep(request_delay)
    if not frames:
        return pd.DataFrame()
    return deduplicate_bars(pd.concat(frames))


def build_output_bars(
    source_bars: pd.DataFrame,
    calendar_name: str = DEFAULT_CALENDAR,
) -> dict[str, pd.DataFrame]:
    """Keep XNYS regular-session 30-minute bars and build requested outputs."""
    regular = filter_regular_session(source_bars, calendar_name)
    return {
        interval: resample_sip_bars(
            regular,
            interval,
            calendar_name,
            source_bar_minutes=BASE_TIMEFRAME_MINUTES,
        )
        for interval in OUTPUT_INTERVALS
    }


def _load_existing_outputs(
    paths: dict[str, Path],
    storage_format: str,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, pd.DataFrame]:
    existing: dict[str, pd.DataFrame] = {}
    for interval, path in paths.items():
        if not path.is_file():
            existing[interval] = pd.DataFrame()
            continue
        existing[interval] = trim_to_window(
            load_local_data(path, storage_format),
            start_time,
            end_time,
        )
    return existing


def _incremental_fetch_start(
    existing: dict[str, pd.DataFrame],
    start_time: datetime,
    calendar_name: str,
) -> datetime:
    if any(frame.empty for frame in existing.values()):
        return start_time
    last_timestamps = [
        pd.Timestamp(frame.index.get_level_values("timestamp").max())
        for frame in existing.values()
    ]
    return max(
        pd.Timestamp(start_time),
        session_open_for_timestamp(min(last_timestamps), calendar_name),
    ).to_pydatetime()


def update_symbol_data(
    client: StockHistoricalDataClient,
    symbol: str,
    data_type: str,
    storage_format: str,
    start_time: datetime,
    end_time: datetime,
    refresh_start: datetime | None = None,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    request_delay: float = DEFAULT_REQUEST_DELAY_SECONDS,
    asset_client: TradingClient | None = None,
    inactive_cache: InactiveSymbolCache | None = None,
    output_roots: dict[str, Path] | None = None,
    calendar_name: str = DEFAULT_CALENDAR,
) -> CollectionResult:
    """Legacy-only updater for old per-symbol 1h/4h/1d files."""
    paths = output_paths(
        symbol,
        data_type,
        storage_format,
        output_roots,
    )
    try:
        existing = _load_existing_outputs(
            paths,
            storage_format,
            start_time,
            end_time,
        )
    except (OSError, ValueError, ImportError) as exc:
        print(f"[{symbol}] 기존 장기 데이터 읽기 실패: {exc}")
        return CollectionResult(False)

    fetch_start = _incremental_fetch_start(
        existing,
        start_time,
        calendar_name,
    )
    nonempty = [frame for frame in existing.values() if not frame.empty]
    if nonempty:
        latest_bar = max(
            pd.Timestamp(frame.index.get_level_values("timestamp").max())
            for frame in nonempty
        ).to_pydatetime()
        if should_skip_inactive_symbol(
            asset_client,
            inactive_cache,
            symbol,
            latest_bar,
            end_time,
        ):
            return CollectionResult(True, inactive=True)
    if refresh_start is not None:
        fetch_start = max(start_time, min(fetch_start, refresh_start))

    fetched = fetch_range(
        client,
        symbol,
        fetch_start,
        end_time,
        data_type,
        chunk_days,
        request_delay,
    )
    if fetched is None:
        return CollectionResult(False)
    if fetched.empty and not nonempty:
        print(f"[{symbol}] 저장할 장기 데이터가 없습니다.")
        return CollectionResult(False)

    refreshed = build_output_bars(fetched, calendar_name) if not fetched.empty else {
        interval: frame.iloc[0:0].copy()
        for interval, frame in existing.items()
    }
    changed_values: list[pd.Timestamp] = []
    adjusted_revision = False
    for interval in OUTPUT_INTERVALS:
        overlap_changed = earliest_changed_timestamp(
            existing[interval],
            refreshed[interval],
        )
        if overlap_changed is not None:
            changed_values.append(overlap_changed)
        price_changed = earliest_changed_timestamp(
            existing[interval],
            refreshed[interval],
            PRICE_COLUMNS,
        )
        adjusted_revision |= data_type == "adjusted" and price_changed is not None

    if adjusted_revision and fetch_start > start_time:
        print(f"[{symbol}] 수정주가 이력 변경 감지 - 최근 10년 전체를 갱신합니다.")
        fetched = fetch_range(
            client,
            symbol,
            start_time,
            end_time,
            data_type,
            chunk_days,
            request_delay,
        )
        if fetched is None or fetched.empty:
            return CollectionResult(False)
        refreshed = build_output_bars(fetched, calendar_name)
        changed_values = [pd.Timestamp(start_time).tz_convert("UTC")]

    total_added = 0
    first_new_values: list[pd.Timestamp] = []
    for interval in OUTPUT_INTERVALS:
        combined = merge_frames(
            existing[interval],
            [refreshed[interval]],
            start_time,
            end_time,
        )
        if combined.empty:
            print(f"[{symbol}] {interval} 결과가 비어 있습니다.")
            return CollectionResult(False)
        added_index = combined.index.difference(existing[interval].index)
        total_added += len(added_index)
        if not added_index.empty:
            first = added_index[0]
            if isinstance(first, tuple):
                first = first[combined.index.names.index("timestamp")]
            first_new_values.append(pd.Timestamp(first).tz_convert("UTC"))
        save_local_data(combined, paths[interval], storage_format)
        print(f"[{symbol}] {interval} 저장: 총 {len(combined):,}행 -> {paths[interval]}")

    all_changes = [*changed_values, *first_new_values]
    changed_from = min(all_changes) if all_changes else None
    return CollectionResult(
        True,
        changed_from_utc=changed_from,
        adjustment_revision=adjusted_revision,
        added_rows=total_added,
    )


def run_immutable_collection(
    *,
    config,
    start_time: datetime,
    end_time: datetime,
    price_types: tuple[str, ...],
    resolutions: tuple[str, ...] = ("30m", "1h", "4h", "1d"),
    symbols: list[str] | None = None,
    incremental_sessions: list[date] | None = None,
    source=None,
):
    """Publish native 30m and derived objects using the official boundaries.

    ``source`` is injectable for sample tests. When omitted, credentials are
    read by the caller and an ``AlpacaBarSource`` must be supplied; this helper
    never writes credentials to state or reports.
    """
    from market_pipeline_lib.engine import MarketPipelineEngine

    engine = MarketPipelineEngine(config, source=source)
    if incremental_sessions is not None:
        return engine.incremental(
            sessions=incremental_sessions,
            price_types=price_types,
            resolutions=resolutions,
            symbols=symbols,
        )
    return engine.backfill(
        start=start_time,
        end=end_time,
        price_types=price_types,
        resolutions=resolutions,
        symbols=symbols,
    )
