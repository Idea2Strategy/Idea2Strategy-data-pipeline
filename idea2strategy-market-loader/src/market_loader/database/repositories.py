from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID, uuid4

from market_loader.calendar.xnys import SessionWindow
from market_loader.model.catalog import UniverseInstrument
from market_loader.model.manifest import ManifestObject
from market_loader.model.status import WorkStatus
from market_loader.storage.s3 import S3ObjectIdentity


class MarketRepository:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def assert_schema(self) -> None:
        required = {
            "providers",
            "feeds",
            "instruments",
            "instrument_symbols",
            "trading_sessions",
            "pipeline_runs",
            "dataset_manifests",
            "dataset_objects",
            "dataset_lineage",
            "quality_incidents",
        }
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'market_data'
                """
            )
            found = {row[0] for row in cursor.fetchall()}
        missing = required - found
        if missing:
            raise RuntimeError(f"market_data schema is missing tables: {sorted(missing)}")

    def create_run(
        self,
        *,
        idempotency_key: str,
        processing_version: str,
        input_config: dict[str, Any],
    ) -> tuple[UUID, bool]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, status FROM market_data.pipeline_runs WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                return existing[0], existing[1] == WorkStatus.SUCCEEDED
            run_id = uuid4()
            now = datetime.now(UTC)
            cursor.execute(
                """
                INSERT INTO market_data.pipeline_runs
                  (id, pipeline_type, processing_version, status, idempotency_key,
                   requested_at, started_at, input_config)
                VALUES (%s, 'HISTORICAL_BACKFILL', %s, 'RUNNING', %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    processing_version,
                    idempotency_key,
                    now,
                    now,
                    json.dumps(input_config, separators=(",", ":")),
                ),
            )
            return run_id, False

    def has_successful_sample_run(self) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM market_data.pipeline_runs
                    WHERE status = 'SUCCEEDED'
                      AND COALESCE((input_config ->> 'symbol_count')::integer, 999999) <= 5
                      AND (input_config ->> 'end')::date
                          - (input_config ->> 'start')::date <= 366
                )
                """
            )
            return bool(cursor.fetchone()[0])

    def seed_instrument(self, item: UniverseInstrument) -> UUID:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT i.id, i.asset_type, i.primary_exchange_mic,
                       i.support_status, i.listed_from, i.listed_to
                FROM market_data.instrument_symbols s
                JOIN market_data.instruments i ON i.id = s.instrument_id
                WHERE s.symbol = %s AND s.exchange_mic = %s
                  AND daterange(s.effective_from, COALESCE(s.effective_to, 'infinity'::date), '[]')
                      && daterange(%s, COALESCE(%s, 'infinity'::date), '[]')
                """,
                (
                    item.provider_symbol,
                    item.primary_exchange_mic,
                    item.effective_from,
                    item.effective_to,
                ),
            )
            row = cursor.fetchone()
            if row is not None:
                if item.instrument_id is not None and str(row[0]) != item.instrument_id:
                    raise RuntimeError("universe instrument_id conflicts with the catalog")
                expected = (
                    item.asset_type,
                    item.primary_exchange_mic,
                    item.support_status,
                    item.effective_from,
                    item.effective_to,
                )
                if tuple(row[1:]) != expected:
                    raise RuntimeError(
                        f"catalog instrument differs for {item.provider_symbol}; "
                        "refusing silent overwrite"
                    )
                return UUID(str(row[0]))
            instrument_id = UUID(item.instrument_id) if item.instrument_id else uuid4()
            cursor.execute(
                """
                INSERT INTO market_data.instruments
                  (id, asset_type, primary_exchange_mic, currency, support_status,
                   listed_from, listed_to, created_at)
                VALUES (%s, %s, %s, 'USD', %s, %s, %s, now())
                """,
                (
                    instrument_id,
                    item.asset_type,
                    item.primary_exchange_mic,
                    item.support_status,
                    item.effective_from,
                    item.effective_to,
                ),
            )
            cursor.execute(
                """
                INSERT INTO market_data.instrument_symbols
                  (id, instrument_id, symbol, exchange_mic,
                   effective_from, effective_to, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                """,
                (
                    uuid4(),
                    instrument_id,
                    item.provider_symbol,
                    item.primary_exchange_mic,
                    item.effective_from,
                    item.effective_to,
                ),
            )
            return instrument_id

    def seed_provider_and_feeds(self, rights_version: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, rights_version, status FROM market_data.providers WHERE code = 'ALPACA'"
            )
            row = cursor.fetchone()
            if row is None:
                provider_id = uuid4()
                cursor.execute(
                    """
                    INSERT INTO market_data.providers
                      (id, code, name, rights_version, status)
                    VALUES (%s, 'ALPACA', 'Alpaca Markets', %s, 'ACTIVE')
                    """,
                    (provider_id, rights_version),
                )
            else:
                provider_id = row[0]
                if row[1] != rights_version or row[2] != "ACTIVE":
                    raise RuntimeError(
                        "existing ALPACA provider differs; refusing silent overwrite"
                    )
            for adjustment in ("RAW", "ALL"):
                for resolution in ("30M", "1H", "4H", "1D"):
                    code = f"ALPACA_SIP_{adjustment}_{resolution}"
                    cursor.execute(
                        """
                        INSERT INTO market_data.feeds
                          (id, provider_id, code, data_kind, resolution, session_scope, status)
                        VALUES (%s, %s, %s, 'BAR', %s, 'REGULAR', 'ACTIVE')
                        ON CONFLICT (code) DO NOTHING
                        """,
                        (
                            uuid4(),
                            provider_id,
                            code,
                            resolution.lower(),
                        ),
                    )
                    cursor.execute(
                        """
                        SELECT provider_id, data_kind, resolution, session_scope, status
                        FROM market_data.feeds
                        WHERE code = %s
                        """,
                        (code,),
                    )
                    actual = cursor.fetchone()
                    expected = (
                        provider_id,
                        "BAR",
                        resolution.lower(),
                        "REGULAR",
                        "ACTIVE",
                    )
                    if actual is None or tuple(actual) != expected:
                        raise RuntimeError(
                            f"existing feed differs for {code}; refusing silent overwrite"
                        )

    def seed_sessions(self, windows: dict[date, SessionWindow], calendar_version: str) -> None:
        with self.connection.cursor() as cursor:
            for session_date, window in windows.items():
                cursor.execute(
                    """
                    INSERT INTO market_data.trading_sessions
                      (id, exchange_mic, session_date, opens_at, closes_at,
                       session_type, calendar_version)
                    VALUES (%s, 'XNYS', %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange_mic, session_date) DO NOTHING
                    """,
                    (
                        uuid4(),
                        session_date,
                        window.opens_at,
                        window.closes_at,
                        "EARLY_CLOSE" if window.is_early_close else "REGULAR",
                        calendar_version,
                    ),
                )
                cursor.execute(
                    """
                    SELECT opens_at, closes_at, session_type, calendar_version
                    FROM market_data.trading_sessions
                    WHERE exchange_mic = 'XNYS' AND session_date = %s
                    """,
                    (session_date,),
                )
                actual = cursor.fetchone()
                expected = (
                    window.opens_at,
                    window.closes_at,
                    "EARLY_CLOSE" if window.is_early_close else "REGULAR",
                    calendar_version,
                )
                if actual is None or tuple(actual) != expected:
                    raise RuntimeError(
                        f"existing XNYS session differs for {session_date}; "
                        "refusing silent overwrite"
                    )

    def resolve_instruments(
        self, instruments: list[UniverseInstrument]
    ) -> list[UniverseInstrument]:
        resolved: list[UniverseInstrument] = []
        with self.connection.cursor() as cursor:
            for item in instruments:
                cursor.execute(
                    """
                    SELECT instrument_id
                    FROM market_data.instrument_symbols
                    WHERE symbol = %s AND exchange_mic = %s
                      AND effective_from <= %s
                      AND (effective_to IS NULL OR effective_to >= %s)
                    ORDER BY effective_from DESC
                    LIMIT 1
                    """,
                    (
                        item.provider_symbol,
                        item.primary_exchange_mic,
                        item.effective_from,
                        item.effective_from,
                    ),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"catalog has no persistent instrument for {item.provider_symbol}"
                    )
                if item.instrument_id and item.instrument_id != str(row[0]):
                    raise RuntimeError("universe instrument_id conflicts with catalog")
                resolved.append(replace(item, instrument_id=str(row[0])))
        return resolved

    def create_manifest(
        self,
        *,
        feed_code: str,
        data_layer: str,
        resolution: str,
        period_start: date,
        period_end: date,
        processing_version: str,
    ) -> tuple[UUID, int, UUID | None]:
        lock_key = f"{feed_code}:{period_start}:{period_end}"
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
            cursor.execute("SELECT id FROM market_data.feeds WHERE code = %s", (feed_code,))
            feed = cursor.fetchone()
            if feed is None:
                raise RuntimeError(f"feed is not seeded: {feed_code}")
            cursor.execute(
                """
                SELECT id, revision_number, status, processing_version,
                       supersedes_manifest_id
                FROM market_data.dataset_manifests
                WHERE feed_id = %s AND period_start = %s AND period_end = %s
                ORDER BY revision_number DESC
                LIMIT 1
                """,
                (feed[0], period_start, period_end),
            )
            previous = cursor.fetchone()
            if (
                previous is not None
                and previous[2] == "BUILDING"
                and previous[3] == processing_version
            ):
                return previous[0], previous[1], previous[4]
            revision = 1 if previous is None else previous[1] + 1
            previous_id = None if previous is None else previous[0]
            manifest_id = uuid4()
            cursor.execute(
                """
                INSERT INTO market_data.dataset_manifests
                  (id, feed_id, instrument_id, data_layer, resolution, period_start,
                   period_end, revision_number, as_of_at, processing_version,
                   status, supersedes_manifest_id)
                VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, now(), %s,
                        'BUILDING', %s)
                """,
                (
                    manifest_id,
                    feed[0],
                    data_layer,
                    resolution,
                    period_start,
                    period_end,
                    revision,
                    processing_version,
                    previous_id,
                ),
            )
            return manifest_id, revision, previous_id

    def finalize_manifest(
        self,
        *,
        manifest_id: UUID,
        previous_manifest_id: UUID | None,
        manifest_hash: str,
        quality_status: str,
        objects: list[
            tuple[S3ObjectIdentity, ManifestObject, datetime | None, datetime | None, str]
        ],
        source_manifest_id: UUID | None,
        warning_codes: tuple[str, ...] = (),
    ) -> None:
        with self.connection.cursor() as cursor:
            for identity, item, minimum, maximum, partition in objects:
                object_id = uuid4()
                cursor.execute(
                    """
                    INSERT INTO storage.objects
                      (id, storage_class, bucket_code, object_key, provider_version_id,
                       content_sha256, byte_size, media_type, format_version,
                       encryption_profile, verified_at)
                    VALUES (%s, 'S3_STANDARD', 'DEVELOPMENT_MARKET_DATA', %s, %s, %s, %s,
                            'application/vnd.apache.parquet', 'market-bars/1',
                            'SSE-S3-AES256', now())
                    """,
                    (
                        object_id,
                        identity.object_key,
                        identity.version_id,
                        identity.content_sha256,
                        identity.byte_size,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO market_data.dataset_objects
                      (id, dataset_manifest_id, object_id, object_kind, partition_key,
                       row_count, min_bar_start_at, max_bar_start_at)
                    VALUES (%s, %s, %s, 'BAR_PARQUET', %s, %s, %s, %s)
                    """,
                    (
                        uuid4(),
                        manifest_id,
                        object_id,
                        partition,
                        item.row_count,
                        minimum,
                        maximum,
                    ),
                )
            if source_manifest_id is not None:
                cursor.execute(
                    """
                    INSERT INTO market_data.dataset_lineage
                      (id, dataset_manifest_id, source_manifest_id, relationship_type)
                    VALUES (%s, %s, %s, 'DERIVED_FROM')
                    """,
                    (uuid4(), manifest_id, source_manifest_id),
                )
            for warning_code in warning_codes:
                cursor.execute(
                    """
                    INSERT INTO market_data.quality_incidents
                      (id, dataset_manifest_id, incident_type, severity, status, detail)
                    VALUES (%s, %s, %s, 'WARNING', 'OPEN', %s::jsonb)
                    """,
                    (
                        uuid4(),
                        manifest_id,
                        warning_code,
                        json.dumps(
                            {"source": "market-loader", "interpolated": False},
                            separators=(",", ":"),
                        ),
                    ),
                )
            if previous_manifest_id is not None:
                cursor.execute(
                    """
                    UPDATE market_data.dataset_manifests
                    SET status = 'SUPERSEDED'
                    WHERE id = %s AND status = 'AVAILABLE'
                    """,
                    (previous_manifest_id,),
                )
            cursor.execute(
                """
                UPDATE market_data.dataset_manifests
                SET row_count = %s, manifest_hash = %s, quality_status = %s,
                    status = 'AVAILABLE'
                WHERE id = %s AND status = 'BUILDING'
                """,
                (
                    sum(item.row_count for _, item, _, _, _ in objects),
                    manifest_hash,
                    quality_status,
                    manifest_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("manifest was not in BUILDING state")

    def complete_run(self, run_id: UUID, *, succeeded: bool, summary: dict[str, Any]) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE market_data.pipeline_runs
                SET status = %s, completed_at = now(), summary_result = %s::jsonb
                WHERE id = %s
                """,
                (
                    "SUCCEEDED" if succeeded else "FAILED",
                    json.dumps(summary, separators=(",", ":")),
                    run_id,
                ),
            )

    def status(self, run_id: UUID | None = None) -> list[dict[str, Any]]:
        # Per-partition counters used to come from `market_data.pipeline_partitions`,
        # a table that does not exist in the canonical `db/schema.dbml`. Until a
        # canonical progress source is agreed, only the run row itself is reported.
        query = """
            SELECT pr.id, pr.status, pr.requested_at, pr.completed_at, pr.summary_result
            FROM market_data.pipeline_runs pr
        """
        parameters: tuple[Any, ...] = ()
        if run_id is not None:
            query += " WHERE pr.id = %s"
            parameters = (run_id,)
        query += " ORDER BY pr.requested_at DESC"
        with self.connection.cursor() as cursor:
            cursor.execute(query, parameters)
            return [
                {
                    "run_id": str(row[0]),
                    "status": row[1],
                    "requested_at": row[2].isoformat() if row[2] is not None else None,
                    "completed_at": row[3].isoformat() if row[3] is not None else None,
                    "summary": dict(row[4]) if row[4] is not None else None,
                }
                for row in cursor.fetchall()
            ]

    def run_input(self, run_id: UUID) -> dict[str, Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT input_config FROM market_data.pipeline_runs WHERE id = %s",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"pipeline run not found: {run_id}")
            return dict(row[0])

    def validation_objects(self, *, manifest_id: UUID) -> list[dict[str, Any]]:
        # A run -> manifest link only existed through `market_data.pipeline_partitions`,
        # which is not part of the canonical schema, so validation is addressed by
        # manifest id only.
        query = """
            SELECT dm.id, dm.resolution, dm.status, dm.row_count, dm.manifest_hash,
                   so.object_key, so.provider_version_id, so.content_sha256,
                   so.byte_size, dmo.row_count
            FROM market_data.dataset_manifests dm
            JOIN market_data.dataset_objects dmo ON dmo.dataset_manifest_id = dm.id
            JOIN storage.objects so ON so.id = dmo.object_id
            WHERE dm.id = %s
            ORDER BY dm.id, so.object_key
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (manifest_id,))
            return [
                {
                    "manifest_id": str(row[0]),
                    "resolution": row[1],
                    "manifest_status": row[2],
                    "manifest_row_count": row[3],
                    "manifest_hash": row[4],
                    "object_key": row[5],
                    "version_id": row[6],
                    "content_sha256": row[7],
                    "byte_size": row[8],
                    "object_row_count": row[9],
                }
                for row in cursor.fetchall()
            ]
