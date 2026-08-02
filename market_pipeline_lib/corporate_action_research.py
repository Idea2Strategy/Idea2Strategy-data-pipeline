"""Review-only corporate-action research candidates.

This module intentionally has no dependency on dataset publication, strategy
evaluation, or the admin approval path.  It accepts evidence collected by an
external adapter and persists only immutable review candidates.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]*$")
_EVENT_TYPE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _require_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be timezone-aware UTC")


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Evidence:
    """A reproducible pointer to a public research source."""

    source_uri: str
    source_title: str
    content_sha256: str
    retrieved_at: datetime

    def __post_init__(self) -> None:
        parsed = urlparse(self.source_uri)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("evidence source_uri must be a public HTTP(S) URI")
        if not self.source_title.strip():
            raise ValueError("evidence source_title is required")
        if not _SHA256_PATTERN.fullmatch(self.content_sha256):
            raise ValueError("evidence content_sha256 must be 64 lowercase hex characters")
        _require_utc(self.retrieved_at, "evidence retrieved_at")

    def to_record(self) -> dict[str, str]:
        return {
            "source_uri": self.source_uri,
            "source_title": self.source_title.strip(),
            "content_sha256": self.content_sha256,
            "retrieved_at": _format_utc(self.retrieved_at),
        }


@dataclass(frozen=True)
class ResearchCandidate:
    """An unapproved corporate-action proposal awaiting administrator review."""

    candidate_id: str
    ticker: str
    event_type: str
    proposed_date: date
    evidence: tuple[Evidence, ...]
    researched_at: datetime

    def __post_init__(self) -> None:
        if not _TICKER_PATTERN.fullmatch(self.ticker):
            raise ValueError("ticker must be a normalized market symbol")
        if not _EVENT_TYPE_PATTERN.fullmatch(self.event_type):
            raise ValueError("event_type must be a normalized identifier")
        if type(self.proposed_date) is not date:
            raise ValueError("proposed_date must be a date")
        _require_utc(self.researched_at, "researched_at")
        if not self.evidence:
            raise ValueError("at least one evidence source is required")
        if len(set(self.evidence)) != len(self.evidence):
            raise ValueError("duplicate evidence is not allowed")
        expected_id = self._identity(self.to_identity_record())
        if self.candidate_id != expected_id:
            raise ValueError("candidate_id does not match the candidate identity")

    @classmethod
    def create(
        cls,
        *,
        ticker: str,
        event_type: str,
        proposed_date: date,
        evidence: Iterable[Evidence],
        researched_at: datetime,
    ) -> "ResearchCandidate":
        normalized_ticker = ticker.strip().upper()
        normalized_event_type = event_type.strip().upper()
        if not _TICKER_PATTERN.fullmatch(normalized_ticker):
            raise ValueError("ticker must be a non-empty normalized market symbol")
        if not _EVENT_TYPE_PATTERN.fullmatch(normalized_event_type):
            raise ValueError("event_type must be a non-empty normalized identifier")
        if type(proposed_date) is not date:
            raise ValueError("proposed_date must be a date")
        _require_utc(researched_at, "researched_at")

        normalized_evidence = tuple(
            sorted(
                evidence,
                key=lambda item: (
                    item.source_uri,
                    item.content_sha256,
                    item.retrieved_at,
                ),
            )
        )
        if not normalized_evidence:
            raise ValueError("at least one evidence source is required")
        identity_payload = {
            "ticker": normalized_ticker,
            "event_type": normalized_event_type,
            "proposed_date": proposed_date.isoformat(),
            "researched_at": _format_utc(researched_at),
            "evidence": [item.to_record() for item in normalized_evidence],
        }
        candidate_id = cls._identity(identity_payload)
        return cls(
            candidate_id=candidate_id,
            ticker=normalized_ticker,
            event_type=normalized_event_type,
            proposed_date=proposed_date,
            evidence=normalized_evidence,
            researched_at=researched_at,
        )

    def to_identity_record(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "event_type": self.event_type,
            "proposed_date": self.proposed_date.isoformat(),
            "researched_at": _format_utc(self.researched_at),
            "evidence": [item.to_record() for item in self.evidence],
        }

    @staticmethod
    def _identity(identity_payload: dict[str, Any]) -> str:
        canonical = json.dumps(
            identity_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def to_record(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "ticker": self.ticker,
            "event_type": self.event_type,
            "proposed_date": self.proposed_date.isoformat(),
            "researched_at": _format_utc(self.researched_at),
            "workflow_state": "REVIEW_REQUIRED",
            "evidence": [item.to_record() for item in self.evidence],
        }


@dataclass(frozen=True)
class ResearchSlot:
    scheduled_at: datetime

    @property
    def slot_id(self) -> str:
        return _format_utc(self.scheduled_at)


@dataclass(frozen=True)
class TwiceDailySchedule:
    """Configured UTC schedule semantics without owning an external scheduler."""

    slots: tuple[time, time]

    def __post_init__(self) -> None:
        if len(self.slots) != 2:
            raise ValueError("a research schedule requires exactly two daily slots")
        if self.slots[0] == self.slots[1]:
            raise ValueError("the two daily slots must be distinct")
        for slot in self.slots:
            if slot.tzinfo is None or slot.utcoffset() != timedelta(0):
                raise ValueError("daily research slots must use UTC")
        object.__setattr__(self, "slots", tuple(sorted(self.slots)))

    def latest_due_slot(self, now: datetime) -> ResearchSlot:
        _require_utc(now, "now")
        utc_now = now.astimezone(timezone.utc)
        candidates = [
            datetime.combine(day, slot, tzinfo=timezone.utc)
            for day in (utc_now.date() - timedelta(days=1), utc_now.date())
            for slot in self.slots
        ]
        due = max(candidate for candidate in candidates if candidate <= utc_now)
        return ResearchSlot(scheduled_at=due)


class CandidateStore:
    """Append-only JSONL store with deterministic in-file deduplication."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, candidate: ResearchCandidate) -> bool:
        existing_ids = self._existing_ids()
        if candidate.candidate_id in existing_ids:
            return False

        self.path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(
            candidate.to_record(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self.path.open("a", encoding="utf-8", newline="\n") as output:
            output.write(serialized)
            output.write("\n")
        return True

    def _existing_ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        identifiers: set[str] = set()
        with self.path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                record = json.loads(line)
                candidate_id = record.get("candidate_id")
                if not isinstance(candidate_id, str):
                    raise ValueError(
                        f"candidate store line {line_number} has no candidate_id"
                    )
                identifiers.add(candidate_id)
        return identifiers
