"""Command-line interface for local-first market-data backfills."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .pipeline import (
    BackfillConfig,
    benchmark_summary,
    inventory_summary,
    scan_inventory,
    selected_specs,
    transform,
)
from .core import load_instrument_map
from .remote import apply_database_plan, upload_objects
from .validation import validate_output


def _add_transform_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--instrument-map", type=Path, required=True)
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument(
        "--layer",
        choices=("ADJUSTED", "DERIVED", "all"),
        default="all",
    )
    parser.add_argument(
        "--resolution",
        choices=("30m", "1h", "4h", "1d", "all"),
        default="all",
    )
    parser.add_argument("--shard-count", type=int, default=16)
    parser.add_argument("--target-size-mib", type=int, default=256)
    parser.add_argument("--max-size-mib", type=int, default=512)
    parser.add_argument("--revision", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")


def _config(
    args: argparse.Namespace,
    *,
    benchmark: bool = False,
    force_dry_run: bool = False,
) -> BackfillConfig:
    output_root = args.output_root / "benchmark" if benchmark else args.output_root
    return BackfillConfig(
        input_root=args.input_root.expanduser().resolve(),
        output_root=output_root.expanduser().resolve(),
        instrument_map_path=args.instrument_map.expanduser().resolve(),
        start_year=args.start_year,
        end_year=args.end_year,
        specs=selected_specs(args.layer, args.resolution),
        shard_count=args.shard_count,
        target_size_mib=args.target_size_mib,
        max_size_mib=args.max_size_mib,
        revision=args.revision,
        resume=args.resume,
        dry_run=args.dry_run or force_dry_run,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Alpaca SIP adjustment=all staging Parquet을 Manifest 기반 "
            "연도·instrument shard 데이터셋으로 변환합니다."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="읽기 전용 변환 계획")
    _add_transform_arguments(plan)
    transform_parser = subparsers.add_parser(
        "transform",
        help="로컬 Parquet과 적재 계획 생성",
    )
    _add_transform_arguments(transform_parser)
    benchmark = subparsers.add_parser(
        "benchmark",
        help="대표 연도 로컬 변환과 파일 크기 측정",
    )
    _add_transform_arguments(benchmark)
    validate = subparsers.add_parser(
        "validate",
        help="Parquet·해시·Manifest 품질 검증",
    )
    validate.add_argument("--output-root", type=Path, required=True)
    upload = subparsers.add_parser(
        "upload",
        help="명시적으로 요청할 때만 S3 호환 저장소 업로드",
    )
    upload.add_argument("--output-root", type=Path, required=True)
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--prefix", default="")
    upload.add_argument("--endpoint-url")
    upload.add_argument("--execute", action="store_true")
    upload.add_argument("--resume", action="store_true")
    apply_db = subparsers.add_parser(
        "apply-db",
        help="DBML 계약 검증 후 명시적으로 PostgreSQL 반영",
    )
    apply_db.add_argument("--output-root", type=Path, required=True)
    apply_db.add_argument("--dbml", type=Path, required=True)
    apply_db.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "plan":
            config = _config(args, force_dry_run=True)
            config.validate()
            mappings = load_instrument_map(config.instrument_map_path)
            summary = inventory_summary(
                config,
                scan_inventory(config, mappings),
            )
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0
        if args.command in {"transform", "benchmark"}:
            config = _config(args, benchmark=args.command == "benchmark")
            results, summary = transform(config)
            payload = (
                benchmark_summary(results)
                if args.command == "benchmark" and not config.dry_run
                else {
                    **summary,
                    "manifest_available": sum(
                        result.available for result in results
                    ),
                    "manifest_failed": sum(
                        not result.available for result in results
                    ),
                }
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 1 if any(not result.available for result in results) else 0
        if args.command == "validate":
            report = validate_output(args.output_root.expanduser().resolve())
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0 if report["status"] == "PASSED" else 1
        if args.command == "upload":
            result = upload_objects(
                args.output_root.expanduser().resolve(),
                args.bucket,
                prefix=args.prefix,
                endpoint_url=args.endpoint_url,
                execute=args.execute,
                resume=args.resume,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        if args.command == "apply-db":
            result = apply_database_plan(
                args.output_root.expanduser().resolve(),
                args.dbml.expanduser().resolve(),
                execute=args.execute,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
    except (
        FileNotFoundError,
        ImportError,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(f"[오류] {exc}", file=sys.stderr)
        return 2
    return 2
