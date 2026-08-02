"""Audit filtered regular-session files for date coverage and missing bars."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_filtering.filter_regular_session import (
    DEFAULT_CALENDAR,
    choose_data_type,
    choose_storage_format,
    load_market_data,
)
from market_pipeline_lib.quality import (
    count_missing_bar_intervals,
    expected_session_bar_starts,
    missing_bar_intervals,
)


BAR_FREQUENCY = pd.Timedelta(minutes=5)
MISSING_INTERVAL_COLUMNS = [
    "symbol",
    "session_date",
    "missing_start_utc",
    "missing_end_utc",
    "missing_bars",
    "missing_minutes",
    "previous_bar_utc",
    "next_bar_utc",
]


def expected_regular_timestamps(
    first_timestamp: pd.Timestamp,
    last_timestamp: pd.Timestamp,
    calendar_name: str = DEFAULT_CALENDAR,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
) -> pd.DatetimeIndex:
    """Return expected bar starts within the observed boundaries.

    Thin adapter: the logic now lives in ``market_pipeline_lib.quality`` so the
    pipeline's own missing-bar check and this CSV audit cannot drift apart.
    """
    return expected_session_bar_starts(
        first_timestamp,
        last_timestamp,
        bar_frequency=bar_frequency,
        calendar_name=calendar_name,
    )


def build_missing_intervals(
    missing: pd.DatetimeIndex,
    expected: pd.DatetimeIndex,
    observed: pd.DatetimeIndex,
    symbol: str,
    calendar_name: str,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
) -> list[dict[str, object]]:
    """Group adjacent missing timestamps without joining separate sessions.

    Thin adapter over ``market_pipeline_lib.quality.missing_bar_intervals``;
    only the CSV row shape is built here.
    """
    minutes_per_bar = bar_frequency / pd.Timedelta(minutes=1)
    return [
        {
            "symbol": symbol,
            "session_date": interval.session_date.isoformat(),
            "missing_start_utc": interval.start.isoformat(),
            "missing_end_utc": interval.last_start.isoformat(),
            "missing_bars": interval.bar_count,
            "missing_minutes": int(interval.bar_count * minutes_per_bar),
            "previous_bar_utc": (
                interval.previous_observed.isoformat()
                if interval.previous_observed is not None
                else ""
            ),
            "next_bar_utc": (
                interval.next_observed.isoformat()
                if interval.next_observed is not None
                else ""
            ),
        }
        for interval in missing_bar_intervals(
            missing,
            expected,
            observed,
            bar_frequency=bar_frequency,
            calendar_name=calendar_name,
        )
    ]


def count_missing_intervals(
    missing: pd.DatetimeIndex,
    calendar_name: str,
    bar_frequency: pd.Timedelta,
) -> int:
    """Count contiguous gaps without materializing the detailed report rows."""
    return count_missing_bar_intervals(
        missing,
        bar_frequency=bar_frequency,
        calendar_name=calendar_name,
    )


def audit_dataframe(
    dataframe: pd.DataFrame,
    symbol: str,
    calendar_name: str = DEFAULT_CALENDAR,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
    include_intervals: bool = True,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Calculate observed coverage and missing regular-session intervals."""
    if dataframe.empty:
        return (
            {
                "symbol": symbol,
                "status": "empty",
                "first_timestamp_utc": "",
                "last_timestamp_utc": "",
                "first_session_date": "",
                "last_session_date": "",
                "observed_rows": 0,
                "unique_timestamps": 0,
                "expected_bars": 0,
                "missing_bars": 0,
                "coverage_pct": 0.0,
                "missing_intervals": 0,
                "duplicate_timestamps": 0,
                "unexpected_timestamps": 0,
            },
            [],
        )

    timestamps = pd.DatetimeIndex(
        pd.to_datetime(
            dataframe.index.get_level_values("timestamp"),
            errors="raise",
            utc=True,
        )
    )
    observed = timestamps.unique().sort_values()
    expected = expected_regular_timestamps(
        observed.min(), observed.max(), calendar_name, bar_frequency
    )
    missing = expected.difference(observed)
    unexpected = observed.difference(expected)
    missing_interval_count = count_missing_intervals(
        missing, calendar_name, bar_frequency
    )
    intervals = (
        build_missing_intervals(
            missing, expected, observed, symbol, calendar_name, bar_frequency
        )
        if include_intervals
        else []
    )
    covered_bars = len(expected.intersection(observed))
    coverage_pct = covered_bars / len(expected) * 100 if len(expected) else 0.0
    calendar = mcal.get_calendar(calendar_name)
    observed_local = observed.tz_convert(calendar.tz)

    summary = {
        "symbol": symbol,
        "status": "ok",
        "first_timestamp_utc": observed.min().isoformat(),
        "last_timestamp_utc": observed.max().isoformat(),
        "first_session_date": observed_local.min().date().isoformat(),
        "last_session_date": observed_local.max().date().isoformat(),
        "observed_rows": len(dataframe),
        "unique_timestamps": len(observed),
        "expected_bars": len(expected),
        "missing_bars": len(missing),
        "coverage_pct": round(coverage_pct, 6),
        "missing_intervals": missing_interval_count,
        "duplicate_timestamps": len(timestamps) - len(observed),
        "unexpected_timestamps": len(unexpected),
    }
    return summary, intervals


def save_report(dataframe: pd.DataFrame, destination: Path) -> None:
    """Atomically save a report CSV."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f".{destination.name}.tmp")
    try:
        dataframe.to_csv(temporary_path, index=False, encoding="utf-8-sig")
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def selected_sources(
    project_root: Path,
    data_type: str,
    storage_format: str,
    dataset: str = "standard",
) -> list[tuple[str, str, Path]]:
    data_types = ("raw", "adjusted") if data_type == "all" else (data_type,)
    formats = ("csv", "parquet") if storage_format == "all" else (storage_format,)
    root_name = (
        "regular_sip_1min_market_data"
        if dataset == "sip"
        else "regular_market_data"
    )
    roots = {
        "raw": project_root / root_name / "raw",
        "adjusted": project_root / root_name / "adjusted",
    }
    return [
        (selected_type, selected_format, roots[selected_type] / selected_format)
        for selected_type in data_types
        for selected_format in formats
    ]


def audit_source(
    source_dir: Path,
    storage_format: str,
    calendar_name: str,
    bar_frequency: pd.Timedelta = BAR_FREQUENCY,
    include_intervals: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    input_files = sorted(source_dir.glob(f"*.{storage_format}"))
    summaries: list[dict[str, object]] = []
    intervals: list[dict[str, object]] = []

    for index, input_path in enumerate(input_files, 1):
        symbol = input_path.name
        sip_suffix = f"_sip_historical.{storage_format}"
        if symbol.endswith(sip_suffix):
            symbol_with_interval = symbol[: -len(sip_suffix)]
            symbol = symbol_with_interval.rsplit("_", 1)[0]
        else:
            legacy_suffix = f"_5min_historical.{storage_format}"
            if symbol.endswith(legacy_suffix):
                symbol = symbol[: -len(legacy_suffix)]
        try:
            dataframe = load_market_data(input_path, storage_format)
            summary, missing_intervals = audit_dataframe(
                dataframe,
                symbol,
                calendar_name,
                bar_frequency,
                include_intervals,
            )
            summary["file"] = input_path.name
            summaries.append(summary)
            intervals.extend(missing_intervals)
            print(
                f"[{index}/{len(input_files)}] {symbol}: "
                f"{summary['first_timestamp_utc']} ~ {summary['last_timestamp_utc']}, "
                f"누락 {summary['missing_bars']:,}개 "
                f"(커버리지 {summary['coverage_pct']:.4f}%)"
            )
        except (OSError, ValueError, ImportError) as exc:
            summaries.append(
                {
                    "symbol": symbol,
                    "status": f"error: {exc}",
                    "file": input_path.name,
                }
            )
            print(f"[{index}/{len(input_files)}] {symbol}: 오류 - {exc}")

    return pd.DataFrame(summaries), pd.DataFrame(
        intervals, columns=MISSING_INTERVAL_COLUMNS
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="정규장 데이터의 종목별 기간과 누락된 봉 구간을 검사합니다."
    )
    parser.add_argument(
        "--dataset",
        choices=("standard", "sip"),
        default="standard",
        help="검사할 데이터셋 (기본값: standard 5분봉)",
    )
    parser.add_argument(
        "--data-type",
        choices=("raw", "adjusted", "all"),
        help="검사할 데이터 타입 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--format",
        dest="storage_format",
        choices=("csv", "parquet", "all"),
        help="검사할 파일 형식 (미지정 시 실행 중 선택)",
    )
    parser.add_argument(
        "--calendar",
        default=DEFAULT_CALENDAR,
        help=f"거래소 캘린더 이름 (기본값: {DEFAULT_CALENDAR})",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="보고서 저장 폴더 (미지정 시 데이터셋별 기본 폴더)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_type = args.data_type or choose_data_type()
    storage_format = args.storage_format or choose_storage_format()
    report_dir = (
        args.report_dir.expanduser().resolve()
        if args.report_dir
        else PROJECT_ROOT
        / "report"
        / (
            "regular_sip_session_audit"
            if args.dataset == "sip"
            else "regular_session_audit"
        )
    )
    bar_frequency = pd.Timedelta(minutes=1 if args.dataset == "sip" else 5)

    try:
        mcal.get_calendar(args.calendar)
    except RuntimeError as exc:
        print(f"[오류] 지원하지 않는 캘린더입니다: {args.calendar}\n{exc}", file=sys.stderr)
        return 2
    processed_files = 0
    failed_files = 0
    for selected_type, selected_format, source_dir in selected_sources(
        PROJECT_ROOT, data_type, storage_format, args.dataset
    ):
        if not source_dir.is_dir():
            print(f"[건너뜀] 입력 폴더 없음: {source_dir}")
            continue

        summary, intervals = audit_source(
            source_dir, selected_format, args.calendar, bar_frequency
        )
        if summary.empty:
            print(f"[건너뜀] 입력 파일 없음: {source_dir}")
            continue

        prefix = f"{selected_type}_{selected_format}"
        summary_path = report_dir / f"{prefix}_summary.csv"
        intervals_path = report_dir / f"{prefix}_missing_intervals.csv"
        save_report(summary, summary_path)
        save_report(intervals, intervals_path)
        processed_files += len(summary)
        # `audit_source` degrades a per-file failure to an `error: ...` status
        # row so one bad file cannot abort the whole sweep.  The report is still
        # written, but the run did not audit what it was asked to, so it must
        # not exit 0 -- a scheduled caller has no other signal.
        failed_files += int((summary["status"] != "ok").sum())
        print(f"요약 보고서: {summary_path}")
        print(f"누락 구간 보고서: {intervals_path}")

    if processed_files == 0:
        print("[오류] 검사할 정규장 데이터 파일이 없습니다.", file=sys.stderr)
        return 1

    if failed_files:
        print(
            f"[오류] {processed_files}개 중 {failed_files}개 파일을 검사하지 "
            "못했습니다. 보고서의 status 열을 확인하세요.",
            file=sys.stderr,
        )
        return 3

    print(f"완료: 총 {processed_files}개 파일 검사")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
