"""Collect three years of one-minute US equity bars from Alpaca's SIP feed."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetStatus
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_collection.get_ticker import (
    get_historical_sp500_tickers,
    SP500UniverseError,
)


YEARS_TO_COLLECT = 3
TICKER_LOOKBACK_YEARS = 10
END_DELAY_MINUTES = 15
DEFAULT_CHUNK_DAYS = 7
DEFAULT_REQUEST_DELAY_SECONDS = 0.35
TICKER_FILE = PROJECT_ROOT / "ticker_info" / "sp500_tickers_10years.txt"
# The ten-year S&P 500 membership universe is always well above 500 symbols.
# Anything near or below this floor means the fetch degraded, and overwriting
# the ticker file with it would destroy the real universe.
MIN_EXPECTED_TICKERS = 400
INACTIVE_SYMBOL_CACHE_PATH = PROJECT_ROOT / "pipeline_state" / "inactive_symbols.json"
STALE_SYMBOL_DAYS = 30
INACTIVE_CACHE_TTL_DAYS = 30
PRICE_COLUMNS = ("open", "high", "low", "close", "vwap")


@dataclass(frozen=True)
class CollectionResult:
    """Outcome of one symbol update, including downstream invalidation metadata."""

    success: bool
    changed_from_utc: pd.Timestamp | None = None
    adjustment_revision: bool = False
    added_rows: int = 0
    inactive: bool = False
    empty: bool = False

    def __post_init__(self) -> None:
        if self.empty and self.success:
            raise ValueError(
                "0행 결과는 성공으로 표시할 수 없습니다. "
                "체크포인트가 해당 종목을 완료로 기록해 버립니다."
            )

    def __bool__(self) -> bool:
        return self.success

    @property
    def outcome(self) -> str:
        """Distinguish an empty collection from a real success or a failure."""
        if self.success:
            return "INACTIVE" if self.inactive else "SUCCESS"
        return "EMPTY" if self.empty else "FAILED"


class InactiveSymbolCache:
    """Persist confirmed inactive symbols and avoid duplicate checks in one run."""

    def __init__(self, path: Path = INACTIVE_SYMBOL_CACHE_PATH) -> None:
        self.path = path
        self.symbols = self._load()
        self.active_this_run: set[str] = set()

    def _load(self) -> dict[str, dict[str, str]]:
        if not self.path.is_file():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("version") != 1
                or not isinstance(payload.get("symbols"), dict)
            ):
                raise ValueError("unsupported cache format")
            return payload["symbols"]
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(f"[경고] 비활성 종목 캐시를 읽지 못해 새로 시작합니다: {exc}")
            return {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        payload = {"version": 1, "symbols": self.symbols}
        try:
            temporary_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary_path, self.path)
        finally:
            temporary_path.unlink(missing_ok=True)

    def is_fresh_inactive(
        self,
        symbol: str,
        last_bar: pd.Timestamp,
        checked_at: pd.Timestamp,
    ) -> bool:
        entry = self.symbols.get(symbol)
        if not isinstance(entry, dict) or entry.get("status") != "inactive":
            return False
        try:
            cached_last_bar = pd.Timestamp(entry["last_bar_utc"])
            cached_checked_at = pd.Timestamp(entry["checked_at_utc"])
        except (KeyError, TypeError, ValueError):
            self.symbols.pop(symbol, None)
            return False
        if cached_last_bar.tzinfo is None:
            cached_last_bar = cached_last_bar.tz_localize("UTC")
        else:
            cached_last_bar = cached_last_bar.tz_convert("UTC")
        if cached_checked_at.tzinfo is None:
            cached_checked_at = cached_checked_at.tz_localize("UTC")
        else:
            cached_checked_at = cached_checked_at.tz_convert("UTC")
        cache_age = checked_at - cached_checked_at
        return (
            last_bar <= cached_last_bar
            and pd.Timedelta(0)
            <= cache_age
            <= pd.Timedelta(days=INACTIVE_CACHE_TTL_DAYS)
        )

    def mark_inactive(
        self,
        symbol: str,
        last_bar: pd.Timestamp,
        checked_at: pd.Timestamp,
    ) -> None:
        self.active_this_run.discard(symbol)
        self.symbols[symbol] = {
            "status": "inactive",
            "last_bar_utc": last_bar.isoformat(),
            "checked_at_utc": checked_at.isoformat(),
        }
        try:
            self._save()
        except OSError as exc:
            print(f"[{symbol}] 비활성 종목 캐시 저장 실패(이번 실행에서는 적용): {exc}")

    def mark_active(self, symbol: str) -> None:
        self.active_this_run.add(symbol)
        if self.symbols.pop(symbol, None) is not None:
            try:
                self._save()
            except OSError as exc:
                print(f"[{symbol}] 비활성 종목 캐시 정리 실패: {exc}")


def should_skip_inactive_symbol(
    asset_client: TradingClient | None,
    inactive_cache: InactiveSymbolCache | None,
    symbol: str,
    last_timestamp: datetime,
    end_time: datetime,
) -> bool:
    """Return true only when a stale symbol is confirmed inactive by Alpaca."""
    if asset_client is None or inactive_cache is None:
        return False

    last_bar = pd.Timestamp(last_timestamp)
    checked_at = pd.Timestamp(end_time)
    if last_bar.tzinfo is None:
        last_bar = last_bar.tz_localize("UTC")
    else:
        last_bar = last_bar.tz_convert("UTC")
    if checked_at.tzinfo is None:
        checked_at = checked_at.tz_localize("UTC")
    else:
        checked_at = checked_at.tz_convert("UTC")

    if checked_at - last_bar < pd.Timedelta(days=STALE_SYMBOL_DAYS):
        return False
    if inactive_cache.is_fresh_inactive(symbol, last_bar, checked_at):
        print(
            f"[{symbol}] Alpaca 비활성 종목 캐시 확인 - "
            f"마지막 봉 {last_bar.isoformat()} 이후 조회 생략"
        )
        return True
    if symbol in inactive_cache.active_this_run:
        return False

    try:
        asset = asset_client.get_asset(alpaca_symbol(symbol))
        status = asset.get("status") if isinstance(asset, dict) else asset.status
        status_value = getattr(status, "value", status)
    except Exception as exc:
        print(f"[{symbol}] Alpaca 자산 상태 확인 실패 - 기존 수집 계속: {exc}")
        return False

    if status_value != AssetStatus.INACTIVE.value:
        inactive_cache.mark_active(symbol)
        return False

    inactive_cache.mark_inactive(symbol, last_bar, checked_at)
    print(
        f"[{symbol}] Alpaca 비활성 종목 확인 - "
        f"마지막 봉 {last_bar.isoformat()} 이후 조회 생략"
    )
    return True


def alpaca_symbol(symbol: str) -> str:
    """Convert the ticker-list class-share separator to Alpaca's notation."""
    return symbol.replace("/", ".")


def choose_data_type() -> str:
    choices = {
        "": "raw",
        "1": "raw",
        "raw": "raw",
        "2": "adjusted",
        "adjusted": "adjusted",
        "adjust": "adjusted",
    }

    print("=" * 54)
    print(" [선택 1] SIP 데이터 타입을 선택해 주세요.")
    print("  1. Raw 데이터 (기본값)")
    print("  2. Adjusted 데이터 (배당/분할 반영)")
    print("=" * 54)
    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""
        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(Raw) 또는 2(Adjusted)를 입력해 주세요.")


def choose_storage_format() -> str:
    choices = {
        "": "csv",
        "1": "csv",
        "csv": "csv",
        ".csv": "csv",
        "2": "parquet",
        "parquet": "parquet",
        ".parquet": "parquet",
    }

    print("\n" + "=" * 54)
    print(" [선택 2] 저장 형식을 선택해 주세요.")
    print("  1. CSV (기본값)")
    print("  2. Parquet")
    print("=" * 54)
    while True:
        try:
            choice = input("선택 [1/2]: ").strip().lower()
        except EOFError:
            choice = ""
        if choice in choices:
            return choices[choice]
        print("잘못된 입력입니다. 1(CSV) 또는 2(Parquet)를 입력해 주세요.")


def collection_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    """Return the exact three-year window ending 15 minutes before now."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    start_time = (pd.Timestamp(current) - pd.DateOffset(years=YEARS_TO_COLLECT)).to_pydatetime()
    end_time = current - timedelta(minutes=END_DELAY_MINUTES)
    return start_time, end_time


def storage_path(
    symbol: str,
    data_type: str,
    storage_format: str,
    output_root: Path,
) -> Path:
    safe_symbol = symbol.replace("/", "-")
    return (
        output_root
        / data_type
        / storage_format
        / f"{safe_symbol}_1min_sip_historical.{storage_format}"
    )


def load_local_data(file_path: Path, storage_format: str) -> pd.DataFrame:
    if storage_format == "parquet":
        dataframe = pd.read_parquet(file_path)
    else:
        dataframe = pd.read_csv(file_path)
        required = {"symbol", "timestamp"}
        missing = required.difference(dataframe.columns)
        if missing:
            raise ValueError(f"필수 CSV 컬럼이 없습니다: {', '.join(sorted(missing))}")
        dataframe["timestamp"] = pd.to_datetime(
            dataframe["timestamp"], errors="raise", utc=True
        )
        dataframe = dataframe.set_index(["symbol", "timestamp"])

    if "timestamp" not in dataframe.index.names:
        raise ValueError("인덱스에 timestamp 레벨이 없습니다.")
    return dataframe


def save_local_data(
    dataframe: pd.DataFrame,
    file_path: Path,
    storage_format: str,
) -> None:
    """Write a completed symbol atomically so interrupted downloads stay recoverable."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = file_path.with_name(f".{file_path.name}.tmp")
    try:
        if storage_format == "parquet":
            dataframe.to_parquet(temporary_path)
        else:
            dataframe.to_csv(temporary_path, index=True)
        os.replace(temporary_path, file_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def trim_to_window(
    dataframe: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()
    timestamps = pd.DatetimeIndex(
        pd.to_datetime(
            dataframe.index.get_level_values("timestamp"),
            errors="raise",
            utc=True,
        )
    )
    mask = (timestamps >= start_time) & (timestamps < end_time)
    return dataframe.loc[mask].copy()


def merge_frames(
    existing: pd.DataFrame,
    new_frames: list[pd.DataFrame],
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    frames = [frame for frame in [existing, *new_frames] if not frame.empty]
    if not frames:
        return existing.iloc[0:0].copy()
    combined = pd.concat(frames)
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    return trim_to_window(combined, start_time, end_time)


def deduplicate_bars(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Keep the last bar when adjacent API chunks share a boundary timestamp."""
    if dataframe.empty:
        return dataframe.copy()
    return dataframe.loc[~dataframe.index.duplicated(keep="last")].sort_index()


def earliest_changed_timestamp(
    existing: pd.DataFrame,
    refreshed: pd.DataFrame,
    columns: tuple[str, ...] = (*PRICE_COLUMNS, "volume", "trade_count"),
) -> pd.Timestamp | None:
    """Return the first overlapping bar whose stored market values changed."""
    if existing.empty or refreshed.empty:
        return None

    existing = deduplicate_bars(existing)
    refreshed = deduplicate_bars(refreshed)
    overlap = existing.index.intersection(refreshed.index)
    compared_columns = [
        column
        for column in columns
        if column in existing.columns and column in refreshed.columns
    ]
    if overlap.empty or not compared_columns:
        return None

    left = existing.loc[overlap, compared_columns].sort_index()
    right = refreshed.loc[overlap, compared_columns].sort_index()
    left_numeric = left.apply(pd.to_numeric, errors="coerce")
    right_numeric = right.apply(pd.to_numeric, errors="coerce")
    tolerance = 1e-10 * left_numeric.abs().clip(lower=1)
    equal = (
        left.eq(right)
        | (left.isna() & right.isna())
        | (left_numeric.sub(right_numeric).abs() <= tolerance)
    )
    changed = ~equal.all(axis=1)
    if not changed.any():
        return None

    timestamp = left.index[changed][0]
    if isinstance(timestamp, tuple):
        timestamp = timestamp[left.index.names.index("timestamp")]
    return pd.Timestamp(timestamp).tz_convert("UTC")


def fetch_chunk(
    client: StockHistoricalDataClient,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    data_type: str,
    retries: int = 3,
) -> pd.DataFrame | None:
    adjustment = Adjustment.ALL if data_type == "adjusted" else Adjustment.RAW
    request = StockBarsRequest(
        symbol_or_symbols=[alpaca_symbol(symbol)],
        timeframe=TimeFrame.Minute,
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
                print(f"[{symbol}] 요청 실패 ({start_time} ~ {end_time}): {exc}")
                return None
            wait_seconds = 2 ** (attempt - 1)
            print(
                f"[{symbol}] 요청 재시도 {attempt}/{retries - 1} "
                f"({wait_seconds}초 후): {exc}"
            )
            time.sleep(wait_seconds)
    return None


def fetch_range(
    client: StockHistoricalDataClient,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    data_type: str,
    chunk_days: int,
    request_delay: float,
) -> list[pd.DataFrame] | None:
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
            f"[{symbol}] {chunk_start.date()} ~ {chunk_end.date()}: "
            f"{len(dataframe):,}행"
        )
        chunk_start = chunk_end
        if request_delay > 0:
            time.sleep(request_delay)
    return frames


def process_symbol(
    client: StockHistoricalDataClient,
    symbol: str,
    data_type: str,
    storage_format: str,
    output_root: Path,
    start_time: datetime,
    end_time: datetime,
    chunk_days: int,
    request_delay: float,
    asset_client: TradingClient | None = None,
    inactive_cache: InactiveSymbolCache | None = None,
) -> bool:
    file_path = storage_path(symbol, data_type, storage_format, output_root)
    existing = pd.DataFrame()
    if file_path.exists():
        try:
            existing = trim_to_window(
                load_local_data(file_path, storage_format),
                start_time,
                end_time,
            )
        except (OSError, ValueError, ImportError) as exc:
            print(f"[{symbol}] 기존 파일 읽기 실패: {exc}")
            return False

    if existing.empty:
        fetch_start = start_time
    else:
        last_timestamp = pd.Timestamp(
            existing.index.get_level_values("timestamp").max()
        ).to_pydatetime()
        if should_skip_inactive_symbol(
            asset_client, inactive_cache, symbol, last_timestamp, end_time
        ):
            return True
        fetch_start = max(start_time, last_timestamp + timedelta(minutes=1))

    new_frames: list[pd.DataFrame] = []
    if fetch_start < end_time:
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
            print(f"[{symbol}] 저장하지 않음: 일부 기간 요청 실패")
            return False
        new_frames = fetched

    combined = merge_frames(existing, new_frames, start_time, end_time)
    if combined.empty:
        print(
            f"[{symbol}] 수집 결과가 0행입니다. 성공으로 처리하지 않습니다."
        )
        return False

    save_local_data(combined, file_path, storage_format)
    added_rows = sum(len(frame) for frame in new_frames)
    print(
        f"[{symbol}] 저장 완료: +{added_rows:,}행, 총 {len(combined):,}행 -> {file_path}"
    )
    return True


def update_symbol_data(
    client: StockHistoricalDataClient,
    symbol: str,
    data_type: str,
    storage_format: str,
    output_root: Path,
    start_time: datetime,
    end_time: datetime,
    chunk_days: int,
    request_delay: float,
    refresh_start: datetime | None = None,
    asset_client: TradingClient | None = None,
    inactive_cache: InactiveSymbolCache | None = None,
) -> CollectionResult:
    """Append data and re-fetch adjusted overlap to detect historical revisions."""
    file_path = storage_path(symbol, data_type, storage_format, output_root)
    existing = pd.DataFrame()
    if file_path.exists():
        try:
            existing = trim_to_window(
                load_local_data(file_path, storage_format), start_time, end_time
            )
        except (OSError, ValueError, ImportError) as exc:
            print(f"[{symbol}] 기존 데이터 읽기 실패: {exc}")
            return CollectionResult(False)

    if existing.empty:
        fetch_start = start_time
    else:
        last_timestamp = pd.Timestamp(
            existing.index.get_level_values("timestamp").max()
        ).to_pydatetime()
        if should_skip_inactive_symbol(
            asset_client, inactive_cache, symbol, last_timestamp, end_time
        ):
            return CollectionResult(True, inactive=True)
        fetch_start = max(start_time, last_timestamp + timedelta(minutes=1))
        if refresh_start is not None:
            fetch_start = max(start_time, min(fetch_start, refresh_start))

    new_frames: list[pd.DataFrame] = []
    if fetch_start < end_time:
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
        new_frames = fetched

    refreshed = (
        deduplicate_bars(pd.concat(new_frames))
        if new_frames
        else existing.iloc[0:0].copy()
    )
    overlap_changed = earliest_changed_timestamp(existing, refreshed)
    adjusted_price_changed = earliest_changed_timestamp(
        existing, refreshed, PRICE_COLUMNS
    )
    adjustment_revision = (
        data_type == "adjusted" and adjusted_price_changed is not None
    )

    if adjustment_revision and fetch_start > start_time:
        print(
            f"[{symbol}] 수정주가 이력 변경 감지: "
            f"{adjusted_price_changed.isoformat()}, 최근 3년 전체를 갱신합니다."
        )
        historical_frames = fetch_range(
            client,
            symbol,
            start_time,
            fetch_start,
            data_type,
            chunk_days,
            request_delay,
        )
        if historical_frames is None:
            return CollectionResult(False)
        new_frames = [*historical_frames, *new_frames]
        refreshed = deduplicate_bars(pd.concat(new_frames))

    combined = merge_frames(existing, new_frames, start_time, end_time)
    if combined.empty:
        # Zero rows is not a success: reporting it as one lets the caller
        # checkpoint the symbol as complete and never retry it.
        print(
            f"[{symbol}] 수집 결과가 0행입니다. 성공으로 처리하지 않습니다."
        )
        return CollectionResult(False, empty=True)

    save_local_data(combined, file_path, storage_format)
    added_rows = len(refreshed.index.difference(existing.index))
    new_indexes = refreshed.index.difference(existing.index)
    first_new: pd.Timestamp | None = None
    if not new_indexes.empty:
        timestamp = new_indexes[0]
        if isinstance(timestamp, tuple):
            timestamp = timestamp[refreshed.index.names.index("timestamp")]
        first_new = pd.Timestamp(timestamp).tz_convert("UTC")

    changed_values = [
        value for value in (overlap_changed, first_new) if value is not None
    ]
    changed_from = min(changed_values) if changed_values else None
    if adjustment_revision:
        first_index = combined.index[0]
        if isinstance(first_index, tuple):
            first_index = first_index[combined.index.names.index("timestamp")]
        changed_from = pd.Timestamp(first_index).tz_convert("UTC")

    print(
        f"[{symbol}] 저장 완료: +{added_rows:,}행, 총 {len(combined):,}행 -> {file_path}"
    )
    return CollectionResult(
        True,
        changed_from_utc=changed_from,
        adjustment_revision=adjustment_revision,
        added_rows=added_rows,
    )


def resolve_symbol_universe(
    requested: str | None,
    *,
    ticker_file: Path | None = None,
    minimum_count: int = MIN_EXPECTED_TICKERS,
) -> list[str]:
    """Return the symbols to collect, refreshing the ticker file only when safe.

    An explicit ``--symbols`` list is used as given and never written to disk.
    Otherwise the S&P 500 universe is fetched; a fetch failure propagates as
    ``SP500UniverseError`` and a suspiciously small result is rejected. In both
    cases the ticker file is left untouched, so a degraded fetch can never
    shrink the persisted universe.
    """
    if requested:
        return sorted(
            {symbol.strip().upper() for symbol in requested.split(",") if symbol.strip()}
        )

    target = TICKER_FILE if ticker_file is None else ticker_file
    print("Wikipedia에서 최근 10년 S&P 500 관련 티커를 확인합니다...")
    symbols = sorted(set(get_historical_sp500_tickers(years=TICKER_LOOKBACK_YEARS)))
    if len(symbols) < minimum_count:
        raise SP500UniverseError(
            f"S&P 500 유니버스가 {len(symbols)}개뿐입니다 (최소 {minimum_count}개). "
            f"{target}를 덮어쓰지 않고 중단합니다."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        temporary.write_text("\n".join(symbols) + "\n", encoding="utf-8")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return symbols


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="최근 3년의 미국 주식 1분봉을 Alpaca SIP 피드로 수집합니다."
    )
    parser.add_argument(
        "--data-type",
        choices=("raw", "adjusted"),
        help="데이터 타입 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet"),
        help="저장 형식 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--symbols",
        help="쉼표로 구분한 종목 목록 (미지정 시 최근 10년 S&P 500 관련 종목)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "sip_market_data",
        help="결과 루트 폴더",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=DEFAULT_CHUNK_DAYS,
        help=f"API 요청당 기간 일수 (기본값: {DEFAULT_CHUNK_DAYS})",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help=f"요청 사이 대기 초 (기본값: {DEFAULT_REQUEST_DELAY_SECONDS})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.chunk_days <= 0 or args.request_delay < 0:
        print("[오류] chunk-days는 양수, request-delay는 0 이상이어야 합니다.", file=sys.stderr)
        return 2


    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        print("[오류] Alpaca API 키와 Secret 키를 설정해 주세요.", file=sys.stderr)
        return 1

    data_type = args.data_type or choose_data_type()
    storage_format = args.storage_format or choose_storage_format()
    output_root = args.output_dir.expanduser().resolve()
    start_time, end_time = collection_window()

    try:
        symbols = resolve_symbol_universe(args.symbols)
    except SP500UniverseError as exc:
        print(f"[오류] {exc}", file=sys.stderr)
        return 1

    print("=" * 70)
    print("피드       : SIP")
    print("봉 간격    : 1분")
    print(f"데이터 타입: {data_type}")
    print(f"저장 형식  : {storage_format}")
    print(f"수집 기간  : {start_time.isoformat()} ~ {end_time.isoformat()}")
    print(f"대상 종목  : {len(symbols)}개")
    print(f"저장 폴더  : {output_root}")
    print("=" * 70)
    print("주의: 전체 종목의 3년치 1분봉은 실행 시간과 저장 공간이 매우 큽니다.")

    client = StockHistoricalDataClient(api_key, secret_key)
    asset_client = TradingClient(api_key, secret_key)
    inactive_cache = InactiveSymbolCache()
    failed_symbols: list[str] = []
    for index, symbol in enumerate(symbols, 1):
        print(f"\n[{index}/{len(symbols)}] {symbol} 수집 시작")
        succeeded = process_symbol(
            client,
            symbol,
            data_type,
            storage_format,
            output_root,
            start_time,
            end_time,
            args.chunk_days,
            args.request_delay,
            asset_client,
            inactive_cache,
        )
        if not succeeded:
            failed_symbols.append(symbol)

    print("=" * 70)
    print(f"완료: 성공 {len(symbols) - len(failed_symbols)}개, 실패 {len(failed_symbols)}개")
    if failed_symbols:
        print("실패 종목: " + ", ".join(failed_symbols))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
