from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReconciliationFinding:
    finding_type: str
    object_key: str | None
    repairable: bool
    detail: str


def compare_object(
    *,
    object_key: str,
    expected_version_id: str,
    expected_sha256: str,
    actual_version_id: str | None,
    actual_sha256: str | None,
) -> list[ReconciliationFinding]:
    findings: list[ReconciliationFinding] = []
    if actual_version_id is None:
        findings.append(
            ReconciliationFinding("S3_OBJECT_MISSING", object_key, False, "object not found")
        )
    elif actual_version_id != expected_version_id:
        findings.append(
            ReconciliationFinding("VERSION_MISMATCH", object_key, False, "VersionId differs")
        )
    if actual_sha256 is not None and actual_sha256 != expected_sha256:
        findings.append(
            ReconciliationFinding("SHA256_MISMATCH", object_key, False, "content hash differs")
        )
    return findings
