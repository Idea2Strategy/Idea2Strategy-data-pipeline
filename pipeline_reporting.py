"""Versioned report storage for the daily market-data pipeline."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal

from data_validation.audit_regular_session import save_report


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_ROOT = PROJECT_ROOT / "report"
SUMMARY_RETENTION_DAYS = 365
DETAILED_RETENTION_DAYS = 30


def session_date_from_target(
    target_session_utc: str,
    calendar_name: str,
) -> str:
    """Return the exchange-local session date represented by a UTC close."""
    timestamp = pd.Timestamp(target_session_utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    calendar = mcal.get_calendar(calendar_name)
    return timestamp.tz_convert(calendar.tz).date().isoformat()


def atomic_write_json(payload: dict[str, Any], destination: Path) -> None:
    """Atomically save a UTF-8 JSON document."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f".{destination.name}.tmp")
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def replace_report_rows(
    previous: pd.DataFrame,
    current: pd.DataFrame,
    key: str,
    replaced_values: set[str],
) -> pd.DataFrame:
    """Keep skipped rows while replacing rows recalculated in the current run."""
    if previous.empty or key not in previous.columns:
        return current.reset_index(drop=True)
    retained = previous[
        ~previous[key].astype(str).isin(replaced_values)
    ]
    if current.empty:
        return retained.reset_index(drop=True)
    return pd.concat([retained, current], ignore_index=True)


@dataclass(frozen=True)
class DailyReportStore:
    """Write the latest report and a session-dated copy together."""

    report_root: Path
    session_date: str
    storage_format: str

    @classmethod
    def for_target_session(
        cls,
        target_session_utc: str,
        storage_format: str,
        calendar_name: str,
        report_root: Path = DEFAULT_REPORT_ROOT,
    ) -> DailyReportStore:
        return cls(
            report_root=report_root,
            session_date=session_date_from_target(
                target_session_utc,
                calendar_name,
            ),
            storage_format=storage_format,
        )

    @property
    def latest_root(self) -> Path:
        return self.report_root / "latest" / self.storage_format

    @property
    def history_root(self) -> Path:
        return (
            self.report_root
            / "history"
            / self.session_date
            / self.storage_format
        )

    def destinations(
        self,
        filename: str,
        *,
        detailed: bool = False,
    ) -> tuple[Path, Path]:
        relative_path = Path("deep_quality", filename) if detailed else Path(filename)
        return self.latest_root / relative_path, self.history_root / relative_path

    def save_dataframe(
        self,
        dataframe: pd.DataFrame,
        filename: str,
        *,
        detailed: bool = False,
    ) -> tuple[Path, Path]:
        destinations = self.destinations(filename, detailed=detailed)
        for destination in destinations:
            save_report(dataframe, destination)
        return destinations

    def save_json(
        self,
        payload: dict[str, Any],
        filename: str,
    ) -> tuple[Path, Path]:
        destinations = self.destinations(filename)
        for destination in destinations:
            atomic_write_json(payload, destination)
        return destinations

    def save_manifest_summary(
        self,
        *,
        pipeline_run_id: str,
        status: str,
        manifests: list[dict[str, Any]],
        objects: list[dict[str, Any]],
        incidents: list[dict[str, Any]],
    ) -> tuple[Path, Path]:
        """Preserve the daily report while adding Manifest/object health."""
        payload = {
            "pipeline_run_id": pipeline_run_id,
            "status": status,
            "session_date": self.session_date,
            "manifest_count": len(manifests),
            "available_manifest_count": sum(
                row.get("status") == "AVAILABLE" for row in manifests
            ),
            "quarantined_manifest_count": sum(
                row.get("status") == "QUARANTINED" for row in manifests
            ),
            "object_count": len(objects),
            "row_count": sum(int(row.get("row_count", 0)) for row in objects),
            "quality_incident_count": len(incidents),
            "quality_incidents_by_code": {
                code: sum(
                    row.get("incident_code") == code for row in incidents
                )
                for code in sorted(
                    {
                        str(row.get("incident_code"))
                        for row in incidents
                        if row.get("incident_code")
                    }
                )
            },
        }
        return self.save_json(payload, "manifest_summary.json")

    def load_history_dataframe(
        self,
        filename: str,
        *,
        detailed: bool = False,
    ) -> pd.DataFrame:
        """Load this session's prior report for checkpoint-safe reruns."""
        _, history_path = self.destinations(filename, detailed=detailed)
        if not history_path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_csv(history_path, encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def prune_history(
        self,
        summary_retention_days: int = SUMMARY_RETENTION_DAYS,
        detailed_retention_days: int = DETAILED_RETENTION_DAYS,
    ) -> None:
        """Keep compact daily reports longer than large deep-quality details."""
        if summary_retention_days < 1 or detailed_retention_days < 1:
            raise ValueError("report retention days must be positive")

        history_root = self.report_root / "history"
        if not history_root.is_dir():
            return

        reference_date = date.fromisoformat(self.session_date)
        summary_cutoff = reference_date - timedelta(days=summary_retention_days)
        detailed_cutoff = reference_date - timedelta(days=detailed_retention_days)

        for session_root in history_root.iterdir():
            if not session_root.is_dir():
                continue
            try:
                archived_date = date.fromisoformat(session_root.name)
            except ValueError:
                continue

            if archived_date < summary_cutoff:
                shutil.rmtree(session_root)
                continue
            if archived_date >= detailed_cutoff:
                continue

            for format_root in session_root.iterdir():
                detailed_root = format_root / "deep_quality"
                if format_root.is_dir() and detailed_root.is_dir():
                    shutil.rmtree(detailed_root)
