from __future__ import annotations

import json
import platform
import shutil
import sys
import tempfile
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import boto3
import typer

from market_loader.alpaca.client import AlpacaBarsClient
from market_loader.calendar.xnys import XnysCalendar
from market_loader.config import AppConfig, EnvironmentSettings, load_config
from market_loader.database.connection import Database
from market_loader.database.repositories import MarketRepository
from market_loader.errors import LoaderError
from market_loader.logging import configure_logging, redact
from market_loader.model.catalog import UniverseInstrument, read_universe
from market_loader.pipeline.backfill import BackfillEngine
from market_loader.pipeline.parquet_writer import validate_parquet
from market_loader.pipeline.planner import create_plan
from market_loader.pipeline.publisher import Publisher
from market_loader.storage.local_staging import LocalStaging
from market_loader.storage.s3 import ImmutableS3

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


@app.callback()
def _configure() -> None:
    configure_logging()


def _echo_json(payload: Any) -> None:
    typer.echo(json.dumps(redact(payload), indent=2, sort_keys=True, default=str))


def _split_csv(raw: str) -> list[str]:
    return [value.strip().lower() for value in raw.split(",") if value.strip()]


def _parse_date(raw: str, option: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise typer.BadParameter("must be an ISO date (YYYY-MM-DD)", param_hint=option) from exc


def _config_and_env(path: Path) -> tuple[AppConfig, EnvironmentSettings]:
    return load_config(path), EnvironmentSettings()


def _aws_session(settings: EnvironmentSettings) -> Any:
    return boto3.Session(
        profile_name=settings.AWS_PROFILE or None,
        region_name=settings.AWS_REGION or None,
    )


def _database(settings: EnvironmentSettings) -> Database:
    database = Database(settings)
    database.open()
    return database


def _historical_sip_probe_window(now: datetime) -> tuple[datetime, datetime]:
    end = now - timedelta(days=1)
    return end - timedelta(days=3), end


def _fail(exc: Exception) -> None:
    code = exc.code if isinstance(exc, LoaderError) else type(exc).__name__
    _echo_json({"ok": False, "error_code": code, "message": str(redact(str(exc)))})
    raise typer.Exit(1)


@app.command()
def plan(
    config: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    universe: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    start: Annotated[str, typer.Option()],
    end: Annotated[str, typer.Option()],
    adjustments: Annotated[str | None, typer.Option()] = None,
    resolutions: Annotated[str | None, typer.Option()] = None,
    max_symbols: Annotated[int | None, typer.Option(min=1)] = None,
) -> None:
    """Create a read-only execution plan."""
    try:
        start_date = _parse_date(start, "--start")
        end_date = _parse_date(end, "--end")
        loaded = load_config(config)
        result = create_plan(
            loaded,
            read_universe(universe),
            start_date,
            end_date,
            adjustments=_split_csv(adjustments) if adjustments else None,
            resolutions=_split_csv(resolutions) if resolutions else None,
            max_symbols=max_symbols,
        )
        _echo_json(result.as_dict())
        typer.echo(
            f"symbols={result.symbol_count} api_requests={result.expected_api_requests} "
            f"manifests={result.expected_manifests} objects={result.expected_s3_objects}"
        )
        if result.input_errors:
            raise typer.Exit(2)
    except typer.Exit:
        raise
    except Exception as exc:
        _fail(exc)


@app.command()
def doctor(
    config: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
) -> None:
    """Run read-only dependency, Alpaca, AWS, S3, and PostgreSQL checks."""
    checks: list[dict[str, Any]] = []

    def check(name: str, operation: Any, *, required: bool = True) -> None:
        try:
            detail = operation()
            checks.append({"name": name, "ok": True, "required": required, "detail": detail})
        except Exception as exc:
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "required": required,
                    "detail": f"{type(exc).__name__}: {redact(str(exc))}",
                }
            )

    try:
        loaded, settings = _config_and_env(config)
        check(
            "python",
            lambda: (
                platform.python_version()
                if sys.version_info[:2] == (3, 12)
                else (_ for _ in ()).throw(RuntimeError("Python 3.12 is required"))
            ),
        )
        check(
            "staging_disk",
            lambda: {
                "path": str(loaded.storage.staging_directory),
                "free_bytes": shutil.disk_usage(loaded.storage.staging_directory.parent).free,
            },
        )

        def rights_check() -> dict[str, str]:
            settings.require_rights_approval()
            return {"version": settings.PROVIDER_RIGHTS_VERSION}

        check("provider_rights", rights_check)

        def alpaca_check() -> str:
            start, end = _historical_sip_probe_window(datetime.now(UTC))
            with AlpacaBarsClient(
                loaded.alpaca, settings.ALPACA_API_KEY, settings.ALPACA_API_SECRET
            ) as client:
                next(client.iter_bar_pages(["SPY"], start, end, "raw"))
            return "authenticated historical SIP bars request succeeded"

        check("alpaca_sip", alpaca_check)
        session = _aws_session(settings)
        check(
            "aws_identity",
            lambda: session.client("sts").get_caller_identity()["Arn"],
        )
        check(
            "aws_region",
            lambda: (
                settings.AWS_REGION
                if settings.AWS_REGION == "ap-northeast-2"
                else (_ for _ in ()).throw(RuntimeError("AWS_REGION must be ap-northeast-2"))
            ),
        )
        s3 = session.client("s3")
        check(
            "s3_bucket",
            lambda: (
                s3.head_bucket(Bucket=settings.MARKET_DATA_BUCKET) or settings.MARKET_DATA_BUCKET
            ),
        )
        check(
            "s3_versioning",
            lambda: _require_equal(
                s3.get_bucket_versioning(Bucket=settings.MARKET_DATA_BUCKET).get("Status"),
                "Enabled",
            ),
        )
        check(
            "s3_block_public_access",
            lambda: _check_public_access(
                s3.get_public_access_block(Bucket=settings.MARKET_DATA_BUCKET)[
                    "PublicAccessBlockConfiguration"
                ]
            ),
        )
        check(
            "s3_default_encryption",
            lambda: _check_encryption(
                s3.get_bucket_encryption(Bucket=settings.MARKET_DATA_BUCKET)[
                    "ServerSideEncryptionConfiguration"
                ]
            ),
        )

        def db_check() -> str:
            database = _database(settings)
            try:
                with database.transaction() as connection:
                    repository = MarketRepository(connection)
                    repository.assert_schema()
                    with connection.cursor() as cursor:
                        cursor.execute("SHOW ssl")
                        if cursor.fetchone()[0] != "on":
                            raise RuntimeError("PostgreSQL TLS is not active")
                        cursor.execute(
                            """
                            SELECT version
                            FROM flyway_schema_history
                            WHERE success = true
                            ORDER BY installed_rank DESC LIMIT 1
                            """
                        )
                        version = cursor.fetchone()
                        if version is None:
                            raise RuntimeError("Flyway history is empty")
                return f"schema and TLS verified; Flyway={version[0]}"
            finally:
                database.close()

        check("postgresql", db_check)
        _echo_json(
            {"ok": all(item["ok"] or not item["required"] for item in checks), "checks": checks}
        )
        if any(not item["ok"] and item["required"] for item in checks):
            raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as exc:
        _fail(exc)


def _require_equal(actual: Any, expected: Any) -> Any:
    if actual != expected:
        raise RuntimeError(f"expected {expected}, got {actual}")
    return actual


def _check_public_access(configuration: dict[str, bool]) -> str:
    required = {
        "BlockPublicAcls",
        "IgnorePublicAcls",
        "BlockPublicPolicy",
        "RestrictPublicBuckets",
    }
    if not all(configuration.get(key) is True for key in required):
        raise RuntimeError("all four S3 Block Public Access settings must be enabled")
    return "all four controls enabled"


def _check_encryption(configuration: dict[str, Any]) -> str:
    rules = configuration.get("Rules", [])
    algorithms = {
        rule.get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm") for rule in rules
    }
    if "AES256" not in algorithms:
        raise RuntimeError("S3 default encryption must include SSE-S3 AES256")
    return "AES256"


@app.command("seed-catalog")
def seed_catalog(
    config: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    universe: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    execute: Annotated[bool, typer.Option()] = False,
) -> None:
    """Idempotently seed provider, feeds, instruments, symbols, and XNYS sessions."""
    database: Database | None = None
    try:
        _loaded, settings = _config_and_env(config)
        instruments = read_universe(universe)
        session_start = min(item.effective_from for item in instruments)
        session_end = max(item.effective_to or date.today() for item in instruments)
        summary = {
            "mode": "execute" if execute else "dry-run",
            "instrument_count": len(instruments),
            "feed_count": 8,
            "session_start": session_start,
            "session_end": session_end,
        }
        if not execute:
            _echo_json(summary)
            return
        settings.require_rights_approval()
        database = _database(settings)
        calendar = XnysCalendar()
        exclusive_session_end = session_end + timedelta(days=1)
        with database.transaction() as connection:
            repository = MarketRepository(connection)
            repository.assert_schema()
            repository.seed_provider_and_feeds(settings.PROVIDER_RIGHTS_VERSION)
            for item in instruments:
                repository.seed_instrument(item)
            repository.seed_sessions(
                calendar.sessions(session_start, exclusive_session_end),
                "exchange-calendars/XNYS",
            )
        _echo_json({**summary, "ok": True})
    except Exception as exc:
        _fail(exc)
    finally:
        if database is not None:
            database.close()


def _run_backfill(
    *,
    config_path: Path,
    universe_rows: list[UniverseInstrument],
    start: date,
    end: date,
    adjustments: list[str],
    resolutions: list[str],
    max_symbols: int | None,
) -> Any:
    loaded, settings = _config_and_env(config_path)
    settings.require_rights_approval()
    if not settings.MARKET_DATA_BUCKET:
        raise RuntimeError("MARKET_DATA_BUCKET is required")
    database = _database(settings)
    alpaca = AlpacaBarsClient(loaded.alpaca, settings.ALPACA_API_KEY, settings.ALPACA_API_SECRET)
    try:
        publisher = Publisher(
            loaded,
            LocalStaging(loaded.storage.staging_directory),
            ImmutableS3(_aws_session(settings)),
            settings.MARKET_DATA_BUCKET,
        )
        return BackfillEngine(
            config=loaded,
            database=database,
            alpaca=alpaca,
            calendar=XnysCalendar(),
            publisher=publisher,
        ).run(
            universe=universe_rows,
            start=start,
            end=end,
            adjustments=adjustments,
            resolutions=resolutions,
            max_symbols=max_symbols,
        )
    finally:
        alpaca.close()
        database.close()


@app.command()
def backfill(
    config: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    universe: Annotated[Path, typer.Option(exists=True, dir_okay=False)],
    start: Annotated[str, typer.Option()],
    end: Annotated[str, typer.Option()],
    adjustments: Annotated[str, typer.Option()] = "raw,all",
    resolutions: Annotated[str, typer.Option()] = "30m,1h,4h,1d",
    max_symbols: Annotated[int | None, typer.Option(min=1)] = None,
    execute: Annotated[bool, typer.Option()] = False,
) -> None:
    """Plan or execute a bounded historical backfill."""
    try:
        start_date = _parse_date(start, "--start")
        end_date = _parse_date(end, "--end")
        loaded = load_config(config)
        instruments = read_universe(universe)
        adjustment_values = _split_csv(adjustments)
        resolution_values = _split_csv(resolutions)
        if not execute:
            result = create_plan(
                loaded,
                instruments,
                start_date,
                end_date,
                adjustments=adjustment_values,
                resolutions=resolution_values,
                max_symbols=max_symbols,
            )
            _echo_json({"mode": "dry-run", **result.as_dict()})
            return
        result = _run_backfill(
            config_path=config,
            universe_rows=instruments,
            start=start_date,
            end=end_date,
            adjustments=adjustment_values,
            resolutions=resolution_values,
            max_symbols=max_symbols,
        )
        _echo_json({"ok": True, **asdict(result)})
    except Exception as exc:
        _fail(exc)


@app.command()
def resume(
    run_id: UUID,
    config: Annotated[Path, typer.Option(exists=True, dir_okay=False)] = Path("config.yaml"),
    execute: Annotated[bool, typer.Option()] = False,
) -> None:
    """Resume only incomplete partitions from an existing run."""
    database: Database | None = None
    try:
        if not execute:
            _echo_json({"mode": "dry-run", "run_id": run_id, "would_resume": True})
            return
        _, settings = _config_and_env(config)
        settings.require_rights_approval()
        database = _database(settings)
        with database.transaction() as connection:
            context = MarketRepository(connection).run_input(run_id)
        rows = [
            UniverseInstrument(
                provider_symbol=item["provider_symbol"],
                asset_type=item["asset_type"],
                primary_exchange_mic=item["primary_exchange_mic"],
                effective_from=date.fromisoformat(item["effective_from"]),
                effective_to=(
                    date.fromisoformat(item["effective_to"]) if item["effective_to"] else None
                ),
                support_status=item["support_status"],
                instrument_id=item["instrument_id"],
            )
            for item in context["universe"]
        ]
        database.close()
        database = None
        result = _run_backfill(
            config_path=config,
            universe_rows=rows,
            start=date.fromisoformat(context["start"]),
            end=date.fromisoformat(context["end"]),
            adjustments=list(context["adjustments"]),
            resolutions=list(context["resolutions"]),
            max_symbols=None,
        )
        if result.run_id != run_id:
            raise RuntimeError("resume idempotency mismatch")
        _echo_json({"ok": True, **asdict(result)})
    except Exception as exc:
        _fail(exc)
    finally:
        if database is not None:
            database.close()


def _validation_rows(
    settings: EnvironmentSettings, run_id: UUID | None, manifest_id: UUID | None
) -> list[dict[str, Any]]:
    database = _database(settings)
    try:
        with database.transaction() as connection:
            return MarketRepository(connection).validation_objects(
                run_id=run_id, manifest_id=manifest_id
            )
    finally:
        database.close()


@app.command()
def validate(
    run_id: Annotated[UUID | None, typer.Option()] = None,
    manifest_id: Annotated[UUID | None, typer.Option()] = None,
) -> None:
    """Re-read S3 versions and Parquet footers for a run or manifest."""
    try:
        settings = EnvironmentSettings()
        rows = _validation_rows(settings, run_id, manifest_id)
        s3 = _aws_session(settings).client("s3")
        checked = []
        with tempfile.TemporaryDirectory(prefix="market-loader-validate-") as temporary:
            for index, row in enumerate(rows):
                target = Path(temporary) / f"{index}.parquet"
                response = s3.get_object(
                    Bucket=settings.MARKET_DATA_BUCKET,
                    Key=row["object_key"],
                    VersionId=row["version_id"],
                    ChecksumMode="ENABLED",
                )
                target.write_bytes(response["Body"].read())
                if target.stat().st_size != row["byte_size"]:
                    raise RuntimeError("downloaded S3 byte size mismatch")
                count = validate_parquet(
                    target,
                    derived=row["resolution"] != "30m",
                    expected_sha256=row["content_sha256"],
                )
                if count != row["object_row_count"]:
                    raise RuntimeError("Parquet and RDS row count mismatch")
                checked.append(row["object_key"])
        _echo_json({"ok": True, "object_count": len(checked)})
    except Exception as exc:
        _fail(exc)


@app.command()
def reconcile(
    run_id: UUID,
    repair: Annotated[bool, typer.Option()] = False,
    execute: Annotated[bool, typer.Option()] = False,
) -> None:
    """Detect integrity failures; automatic repair is limited to verified registrations."""
    try:
        if execute and not repair:
            raise RuntimeError("--execute requires --repair")
        settings = EnvironmentSettings()
        rows = _validation_rows(settings, run_id, None)
        s3 = _aws_session(settings).client("s3")
        findings = []
        for row in rows:
            try:
                head = s3.head_object(
                    Bucket=settings.MARKET_DATA_BUCKET,
                    Key=row["object_key"],
                    VersionId=row["version_id"],
                )
                if head["ContentLength"] != row["byte_size"]:
                    findings.append({"type": "BYTE_SIZE_MISMATCH", "object_key": row["object_key"]})
                if head.get("Metadata", {}).get("content-sha256") != row["content_sha256"]:
                    findings.append({"type": "SHA256_MISMATCH", "object_key": row["object_key"]})
            except Exception as exc:
                findings.append(
                    {
                        "type": "S3_OBJECT_MISSING_OR_UNREADABLE",
                        "object_key": row["object_key"],
                        "detail": type(exc).__name__,
                    }
                )
        if execute and findings:
            raise RuntimeError("unsafe findings cannot be automatically repaired")
        _echo_json(
            {
                "ok": not findings,
                "mode": "repair" if execute else "read-only",
                "findings": findings,
            }
        )
        if findings:
            raise typer.Exit(2)
    except typer.Exit:
        raise
    except Exception as exc:
        _fail(exc)


@app.command()
def status(run_id: Annotated[UUID | None, typer.Option()] = None) -> None:
    """Show pipeline and partition status."""
    database: Database | None = None
    try:
        database = _database(EnvironmentSettings())
        with database.transaction() as connection:
            rows = MarketRepository(connection).status(run_id)
        _echo_json({"runs": rows})
    except Exception as exc:
        _fail(exc)
    finally:
        if database is not None:
            database.close()


def main() -> None:
    app()
