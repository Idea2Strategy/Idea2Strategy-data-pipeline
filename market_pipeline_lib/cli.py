"""Explicit non-interactive CLI for the DB-aligned market-data pipeline."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal
from dotenv import load_dotenv

from .catalog import LocalCatalog, PostgresCatalog, StorageObjectsPolicy
from .contracts import DATASET_CONTRACTS, ET, load_instrument_map
from .engine import AlpacaBarSource, MarketPipelineEngine, PipelineConfig
from .operations import (
    benchmark_catalog,
    export_db_plan,
    upload_catalog_objects,
    validate_catalog,
)
from .reference import (
    instrument_registration,
    load_reference_data,
    symbol_assignment,
    xnys_sessions,
)
from .storage import LocalObjectStore, S3ObjectStore


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--local-root", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--instrument-map", type=Path, required=True)
    parser.add_argument("--shard-count", type=int, default=16)
    parser.add_argument("--target-size-mib", type=int, default=256)
    parser.add_argument("--max-size-mib", type=int, default=512)
    parser.add_argument("--revision", type=int)
    parser.add_argument("--calendar", default="XNYS")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")


def _add_selection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--price-type",
        choices=("raw", "adjusted", "all"),
        default="all",
    )
    parser.add_argument(
        "--resolution",
        choices=("30m", "1h", "4h", "1d", "all"),
        default="all",
    )
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--max-symbols", type=int)


def _add_range(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Alpaca SIP RAW·ADJUSTED 30분봉과 XNYS 파생봉을 "
            "불변 객체와 DBML Catalog로 관리합니다."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("plan", "외부 변경 없는 API·파티션 실행 계획"),
        ("backfill", "Alpaca 180일 chunk 기반 연도별 10년 백필"),
        ("incremental", "완료된 XNYS 세션의 DAY 객체 수집"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        _add_common(command)
        _add_selection(command)
        _add_range(command)
    derive = subparsers.add_parser(
        "derive",
        help="게시된 RAW/ADJUSTED 30m에서 파생봉 재생성",
    )
    _add_common(derive)
    _add_selection(derive)
    _add_range(derive)
    compact = subparsers.add_parser(
        "compact",
        help="DAY→WEEK→MONTH→YEAR 불변 Compaction",
    )
    _add_common(compact)
    compact.add_argument(
        "--price-type",
        choices=("raw", "adjusted"),
        required=True,
    )
    compact.add_argument(
        "--layer",
        choices=("RAW", "ADJUSTED", "DERIVED"),
        required=True,
    )
    compact.add_argument(
        "--resolution",
        choices=("30m", "1h", "4h", "1d"),
        required=True,
    )
    compact.add_argument(
        "--granularity",
        choices=("WEEK", "MONTH", "YEAR"),
        required=True,
    )
    compact.add_argument("--period", required=True)
    migrate = subparsers.add_parser(
        "migrate",
        aliases=["transform"],
        help="기존 종목별 10년 Parquet을 YEAR shard 객체로 변환",
    )
    _add_common(migrate)
    _add_selection(migrate)
    migrate.add_argument("--input-root", type=Path, required=True)
    migrate.add_argument("--start-year", type=int, required=True)
    migrate.add_argument("--end-year", type=int, required=True)
    benchmark = subparsers.add_parser(
        "benchmark",
        help="파일 크기·메모리·대표 조회 시간 측정",
    )
    benchmark.add_argument("--local-root", type=Path, required=True)
    benchmark.add_argument("--year", type=int)
    benchmark.add_argument(
        "--price-type",
        choices=("raw", "adjusted"),
    )
    benchmark.add_argument(
        "--layer",
        choices=("RAW", "ADJUSTED", "DERIVED"),
    )
    benchmark.add_argument(
        "--resolution",
        choices=("30m", "1h", "4h", "1d"),
    )
    validate = subparsers.add_parser(
        "validate",
        help="Parquet·해시·스키마·Manifest 겹침 검증",
    )
    validate.add_argument("--local-root", type=Path, required=True)
    export = subparsers.add_parser(
        "export-db-plan",
        help="DBML 열 계약과 일치하는 JSONL 적재 계획 내보내기",
    )
    export.add_argument("--local-root", type=Path, required=True)
    export.add_argument("--output-root", type=Path, required=True)
    export.add_argument("--dbml", type=Path)
    apply_db = subparsers.add_parser(
        "apply-db",
        help="DBML 검증 후 명시적으로 PostgreSQL에 반영",
    )
    apply_db.add_argument("--local-root", type=Path, required=True)
    apply_db.add_argument("--dbml", type=Path, required=True)
    apply_db.add_argument("--execute", action="store_true")
    upload = subparsers.add_parser(
        "upload",
        help="명시적으로 요청할 때만 S3 호환 저장소 업로드",
    )
    upload.add_argument("--local-root", type=Path, required=True)
    upload.add_argument("--output-catalog-root", type=Path, required=True)
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--prefix", default="")
    upload.add_argument("--endpoint-url")
    upload.add_argument("--execute", action="store_true")
    upload.add_argument("--resume", action="store_true")
    reference = subparsers.add_parser(
        "register-reference-data",
        help="instrument map과 XNYS 캘린더를 종목·심볼이력·세션 테이블에 등록",
    )
    reference.add_argument("--instrument-map", type=Path, required=True)
    reference.add_argument("--local-root", type=Path, required=True)
    reference.add_argument(
        "--target",
        choices=("local", "postgres"),
        default="local",
        help="postgres는 DATABASE_URL이 필요합니다.",
    )
    reference.add_argument("--calendar-start")
    reference.add_argument("--calendar-end")
    reference.add_argument("--execute", action="store_true")
    cleanup = subparsers.add_parser(
        "cleanup-staging",
        help="성공 완료된 실행 staging만 명시적으로 정리",
    )
    cleanup.add_argument("--local-root", type=Path, required=True)
    cleanup.add_argument("--staging-root", type=Path, required=True)
    cleanup.add_argument("--run-id", required=True)
    cleanup.add_argument("--execute", action="store_true")
    bootstrap = subparsers.add_parser(
        "bootstrap-legacy-catalog",
        help="read-only legacy RDS/S3 audit and retry-safe canonical DB bootstrap",
    )
    bootstrap.add_argument("--artifact-root", type=Path, required=True)
    bootstrap.add_argument("--bucket", required=True)
    bootstrap.add_argument("--expected-object-count", type=int, required=True)
    bootstrap.add_argument("--expected-manifest-count", type=int, required=True)
    bootstrap.add_argument("--execute", action="store_true")
    return parser


def _price_types(value: str) -> tuple[str, ...]:
    return ("raw", "adjusted") if value == "all" else (value,)


def _resolutions(value: str) -> tuple[str, ...]:
    return ("30m", "1h", "4h", "1d") if value == "all" else (value,)


def _parse_utc(value: str) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(ET)
    return timestamp.tz_convert("UTC").to_pydatetime()


def _range(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.start and args.end:
        start, end = _parse_utc(args.start), _parse_utc(args.end)
    elif args.start_year is not None and args.end_year is not None:
        start = datetime(args.start_year, 1, 1, tzinfo=ET).astimezone(
            timezone.utc
        )
        end = datetime(args.end_year + 1, 1, 1, tzinfo=ET).astimezone(
            timezone.utc
        )
    else:
        raise ValueError(
            "--start/--end 또는 --start-year/--end-year가 필요합니다."
        )
    if end <= start:
        raise ValueError("end는 start보다 뒤여야 합니다.")
    return start, end


def _symbols(args: argparse.Namespace) -> list[str] | None:
    values = list(args.symbols or [])
    if args.max_symbols is not None:
        if args.max_symbols <= 0:
            raise ValueError("--max-symbols는 양수여야 합니다.")
        if values:
            values = values[: args.max_symbols]
    return values or None


def _config(args: argparse.Namespace, *, force_dry_run: bool = False) -> PipelineConfig:
    return PipelineConfig(
        local_root=args.local_root.expanduser().resolve(),
        staging_root=args.staging_root.expanduser().resolve(),
        instrument_map_path=args.instrument_map.expanduser().resolve(),
        shard_count=args.shard_count,
        target_size_mib=args.target_size_mib,
        max_size_mib=args.max_size_mib,
        calendar=args.calendar,
        revision=args.revision,
        resume=args.resume,
        dry_run=args.dry_run or force_dry_run,
    )


def _source() -> AlpacaBarSource:
    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise ValueError(
            "ALPACA_API_KEY와 ALPACA_SECRET_KEY 환경변수가 필요합니다."
        )
    return AlpacaBarSource(api_key, secret_key)


def _completed_sessions(
    start: datetime,
    end: datetime,
    calendar_name: str,
) -> list[date]:
    schedule = mcal.get_calendar(calendar_name).schedule(
        start_date=start.date(),
        end_date=(end - timedelta(microseconds=1)).date(),
        tz="UTC",
    )
    available_until = pd.Timestamp(datetime.now(timezone.utc))
    return [
        pd.Timestamp(index).date()
        for index, row in schedule.iterrows()
        if pd.Timestamp(row["market_close"]) <= available_until
    ]


def _register_reference_data(args: argparse.Namespace) -> dict[str, Any]:
    """D04: load the instrument map and the XNYS calendar into the catalog.

    Without ``--execute`` nothing is written: the map is parsed and validated and
    the counts are reported, so a malformed row is found before any row lands.
    """

    mappings = load_instrument_map(args.instrument_map.expanduser().resolve())
    calendar_start = date.fromisoformat(args.calendar_start) if args.calendar_start else None
    calendar_end = date.fromisoformat(args.calendar_end) if args.calendar_end else None
    if (calendar_start is None) != (calendar_end is None):
        raise ValueError("--calendar-start와 --calendar-end는 함께 지정해야 합니다.")
    # Validated whether or not anything is written, so a dry run is a real check.
    registrations = [instrument_registration(mapping) for mapping in mappings.values()]
    assignments = [symbol_assignment(mapping) for mapping in mappings.values()]
    session_count = (
        len(xnys_sessions(calendar_start, calendar_end))
        if calendar_start is not None and calendar_end is not None
        else 0
    )
    if not args.execute:
        return {
            "status": "DRY_RUN",
            "target": args.target,
            "instrument_count": len(registrations),
            "symbol_count": len(assignments),
            "trading_session_count": session_count,
        }
    local_root = args.local_root.expanduser().resolve()
    if args.target == "local":
        catalog: Any = LocalCatalog(local_root / "catalog-export")
        return {
            **load_reference_data(
                catalog,
                mappings,
                calendar_start=calendar_start,
                calendar_end=calendar_end,
            ),
            "target": "local",
        }
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("--target postgres에는 DATABASE_URL이 필요합니다.")
    target = PostgresCatalog.connect(
        database_url,
        artifact_root=local_root / "catalog-export",
        # Reference data never touches `storage.objects`; taking the read-only side
        # of the ownership contradiction keeps this command unable to write it.
        storage_objects=StorageObjectsPolicy.READ_ONLY,
    )
    try:
        target.verify_schema()
        return {
            **load_reference_data(
                target,
                mappings,
                calendar_start=calendar_start,
                calendar_end=calendar_end,
            ),
            "target": "postgres",
        }
    finally:
        target.close()


def execute(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "bootstrap-legacy-catalog":
        from .legacy_bootstrap import (
            S3LegacyObjectVerifier,
            connect_read_only_catalog,
            materialize_legacy_catalog,
            same_database,
        )

        source_url = os.getenv("LEGACY_DATABASE_URL")
        target_url = os.getenv("DATABASE_URL")
        if not source_url or not target_url:
            raise ValueError(
                "bootstrap-legacy-catalog requires LEGACY_DATABASE_URL and DATABASE_URL"
            )
        if same_database(source_url, target_url):
            raise ValueError("legacy source and canonical target database URLs must differ")
        import boto3

        artifact_root = args.artifact_root.expanduser().resolve()
        source = connect_read_only_catalog(source_url, artifact_root=artifact_root / "source")
        target = (
            PostgresCatalog.connect(
                target_url,
                artifact_root=artifact_root / "target",
                storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
            )
            if args.execute
            else connect_read_only_catalog(target_url, artifact_root=artifact_root / "target")
        )
        try:
            source.verify_schema()
            target.verify_schema()
            return materialize_legacy_catalog(
                source,
                target,
                object_verifier=S3LegacyObjectVerifier(
                    boto3.client("s3"), expected_bucket=args.bucket
                ),
                expected_object_count=args.expected_object_count,
                expected_manifest_count=args.expected_manifest_count,
                execute=args.execute,
            )
        finally:
            target.close()
            source.close()

    if args.command == "register-reference-data":
        return _register_reference_data(args)
    if args.command in {
        "benchmark",
        "validate",
        "export-db-plan",
        "apply-db",
        "upload",
        "cleanup-staging",
    }:
        local_root = args.local_root.expanduser().resolve()
        catalog = LocalCatalog(local_root / "catalog-export")
        store = LocalObjectStore(local_root)
        if args.command == "benchmark":
            return benchmark_catalog(
                catalog,
                store,
                year=args.year,
                price_type=args.price_type,
                layer=args.layer,
                resolution=args.resolution,
            )
        if args.command == "validate":
            return validate_catalog(catalog, store)
        if args.command == "export-db-plan":
            return export_db_plan(
                catalog,
                args.output_root,
                dbml_path=args.dbml,
            )
        if args.command == "apply-db":
            from .operations import apply_catalog_to_postgres

            return apply_catalog_to_postgres(
                catalog,
                store,
                dbml_path=args.dbml,
                execute=args.execute,
                database_url=os.getenv("DATABASE_URL"),
            )
        if args.command == "cleanup-staging":
            run = next(
                (
                    row
                    for row in catalog.records(
                        "market_data.pipeline_runs"
                    )
                    if row["id"] == args.run_id
                ),
                None,
            )
            if run is None or run["status"] != "SUCCEEDED":
                raise ValueError(
                    "성공 완료된 pipeline run의 staging만 정리할 수 있습니다."
                )
            staging_root = args.staging_root.expanduser().resolve()
            target = (staging_root / args.run_id).resolve()
            try:
                target.relative_to(staging_root)
            except ValueError as exc:
                raise ValueError("staging 정리 경로가 루트를 벗어납니다.") from exc
            if not args.execute:
                return {
                    "status": "DRY_RUN",
                    "target": str(target),
                    "exists": target.is_dir(),
                }
            if target.is_dir():
                shutil.rmtree(target)
            return {"status": "REMOVED", "target": str(target)}
        remote = S3ObjectStore(
            args.bucket,
            prefix=args.prefix,
            endpoint_url=args.endpoint_url,
        ) if args.execute else None
        if remote is None:
            class DryRunRemote:
                pass
            operations = [
                {
                    "object_id": row["id"],
                    "object_key": row["object_key"],
                    "byte_size": row["byte_size"],
                }
                for row in catalog.records("storage.objects")
            ]
            return {
                "status": "DRY_RUN",
                "operation_count": len(operations),
                "operations": operations,
            }
        return upload_catalog_objects(
            catalog,
            store,
            remote,
            args.output_catalog_root,
            dry_run=False,
            resume=args.resume,
        )

    command = "migrate" if args.command == "transform" else args.command
    config = _config(args, force_dry_run=command == "plan")
    needs_source = command in {"backfill", "incremental"}
    engine = MarketPipelineEngine(
        config,
        source=_source() if needs_source and not config.dry_run else None,
    )
    selected_symbols = _symbols(args)
    if (
        selected_symbols is None
        and getattr(args, "max_symbols", None) is not None
    ):
        selected_symbols = sorted(engine.mappings)[: args.max_symbols]
    if command == "plan":
        start, end = _range(args)
        return engine.plan(
            start=start,
            end=end,
            price_types=_price_types(args.price_type),
            resolutions=_resolutions(args.resolution),
            symbols=selected_symbols,
        )
    if command == "backfill":
        start, end = _range(args)
        return engine.backfill(
            start=start,
            end=end,
            price_types=_price_types(args.price_type),
            resolutions=_resolutions(args.resolution),
            symbols=selected_symbols,
        )
    if command == "incremental":
        start, end = _range(args)
        sessions = _completed_sessions(start, end, args.calendar)
        return engine.incremental(
            sessions=sessions,
            price_types=_price_types(args.price_type),
            resolutions=_resolutions(args.resolution),
            symbols=selected_symbols,
        )
    if command == "derive":
        start, end = _range(args)
        years = range(start.year, end.year + 1)
        resolutions = (
            ("1h", "4h", "1d")
            if args.resolution in {"all", "30m"}
            else (args.resolution,)
        )
        return engine.derive(
            years=years,
            price_types=_price_types(args.price_type),
            resolutions=resolutions,
        )
    if command == "compact":
        contract = DATASET_CONTRACTS.get(
            (args.price_type, args.layer, args.resolution)
        )
        if contract is None:
            raise ValueError("price-type/layer/resolution 조합이 잘못되었습니다.")
        return engine.compact(
            contract,
            granularity=args.granularity,
            period=date.fromisoformat(args.period),
        )
    if command == "migrate":
        resolutions = (
            ("30m", "1h", "4h", "1d")
            if args.resolution == "all"
            else (args.resolution,)
        )
        return engine.migrate_legacy(
            input_root=args.input_root.expanduser().resolve(),
            start_year=args.start_year,
            end_year=args.end_year,
            price_types=_price_types(args.price_type),
            resolutions=resolutions,
        )
    raise ValueError(f"지원하지 않는 command: {command}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if result.get("status") not in {"FAILED"} else 1
    except (
        FileNotFoundError,
        ImportError,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(f"[오류] {exc}", file=sys.stderr)
        return 2
