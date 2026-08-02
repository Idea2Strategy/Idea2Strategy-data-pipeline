"""Hash-verified resume checks for interrupted collection/publish runs.

Cards served: **D05** (수집 재개 / resume) and **D11** (워터마크 기반 이어받기).

``MarketPipelineEngine.collect_staging`` currently decides to skip a chunk on
*file existence alone*.  A process killed midway through a Parquet write leaves
a short, syntactically broken fragment on disk; on the next run that fragment
is accepted as "already collected" and the missing rows are never noticed.
The same hazard applies to the per-shard resume state used by the publish
path.

These helpers were salvaged from the deleted ``market_data_backfill`` package
(``pipeline.py::_load_completed_manifest`` / ``::_load_shard_state``), which was
the only place in the repository that verified a resume candidate by size and
SHA-256 before trusting it.  They are deliberately side-effect free and depend
only on the standard library plus ``market_pipeline_lib.contracts.sha256_file``
so that they can be called from the engine, the CLI, or a Lambda handler.

They are **not yet wired into** ``engine.py``: stage DP3 owns that integration.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .contracts import sha256_file

__all__ = [
    "ResumeEntry",
    "ResumeFingerprintMismatch",
    "ResumeVerdict",
    "require_matching_fingerprint",
    "verify_resumable_file",
    "verify_resumable_files",
]


# One recorded object: (path, sha256) or (path, sha256, byte_size).
ResumeEntry = (
    tuple["os.PathLike[str] | str", str]
    | tuple["os.PathLike[str] | str", str, "int | None"]
)


# Reason codes are stable strings so callers can record them on a
# ``market_data.quality_incidents`` row without inventing a second vocabulary.
REASON_MISSING = "MISSING"
REASON_EMPTY = "EMPTY"
REASON_TOO_SMALL = "TOO_SMALL"
REASON_SIZE_MISMATCH = "SIZE_MISMATCH"
REASON_HASH_MISMATCH = "HASH_MISMATCH"
REASON_NO_EXPECTED_HASH = "NO_EXPECTED_HASH"
REASON_NO_RECORDED_OBJECTS = "NO_RECORDED_OBJECTS"


class ResumeFingerprintMismatch(RuntimeError):
    """The on-disk resume state was produced by a different configuration.

    Silently restarting would mix two configurations inside one revision, so
    the caller must either point at a fresh revision or drop the stale state
    explicitly.
    """


@dataclass(frozen=True)
class ResumeVerdict:
    """Outcome of a single resume check.

    ``resumable`` is never ``True`` unless the file was found, its size matched
    every constraint the caller supplied, and its SHA-256 equalled the recorded
    digest.  ``reason`` is empty exactly when ``resumable`` is ``True``.
    """

    resumable: bool
    reason: str = ""
    path: Path | None = None
    expected_sha256: str = ""
    actual_sha256: str = ""
    expected_byte_size: int | None = None
    actual_byte_size: int = 0

    def __bool__(self) -> bool:
        return self.resumable


def verify_resumable_file(
    path: os.PathLike[str] | str,
    *,
    expected_sha256: str,
    expected_byte_size: int | None = None,
    minimum_byte_size: int = 1,
) -> ResumeVerdict:
    """Decide whether ``path`` may be reused instead of being re-fetched.

    The cheap checks run first so a truncated fragment is rejected without
    reading (and hashing) a partial file:

    1. the path must exist and be a regular file,
    2. it must not be empty and must reach ``minimum_byte_size``,
    3. it must match ``expected_byte_size`` when the caller recorded one,
    4. only then is the SHA-256 computed and compared.

    ``expected_sha256`` is required; a caller that has no recorded digest has
    no evidence of completeness and gets ``NO_EXPECTED_HASH``.
    """
    candidate = Path(path)
    normalized_expected = expected_sha256.strip().lower()

    if not normalized_expected:
        return ResumeVerdict(
            False,
            REASON_NO_EXPECTED_HASH,
            path=candidate,
            expected_byte_size=expected_byte_size,
        )

    try:
        is_file = candidate.is_file()
    except OSError:
        is_file = False
    if not is_file:
        return ResumeVerdict(
            False,
            REASON_MISSING,
            path=candidate,
            expected_sha256=normalized_expected,
            expected_byte_size=expected_byte_size,
        )

    try:
        actual_byte_size = candidate.stat().st_size
    except OSError:
        return ResumeVerdict(
            False,
            REASON_MISSING,
            path=candidate,
            expected_sha256=normalized_expected,
            expected_byte_size=expected_byte_size,
        )

    def reject(reason: str) -> ResumeVerdict:
        return ResumeVerdict(
            False,
            reason,
            path=candidate,
            expected_sha256=normalized_expected,
            expected_byte_size=expected_byte_size,
            actual_byte_size=actual_byte_size,
        )

    if actual_byte_size == 0:
        return reject(REASON_EMPTY)
    if minimum_byte_size > 0 and actual_byte_size < minimum_byte_size:
        return reject(REASON_TOO_SMALL)
    if expected_byte_size is not None and actual_byte_size != expected_byte_size:
        return reject(REASON_SIZE_MISMATCH)

    try:
        actual_sha256 = sha256_file(candidate)
    except OSError:
        return reject(REASON_MISSING)

    if actual_sha256 != normalized_expected:
        return ResumeVerdict(
            False,
            REASON_HASH_MISMATCH,
            path=candidate,
            expected_sha256=normalized_expected,
            actual_sha256=actual_sha256,
            expected_byte_size=expected_byte_size,
            actual_byte_size=actual_byte_size,
        )

    return ResumeVerdict(
        True,
        "",
        path=candidate,
        expected_sha256=normalized_expected,
        actual_sha256=actual_sha256,
        expected_byte_size=expected_byte_size,
        actual_byte_size=actual_byte_size,
    )


def verify_resumable_files(
    entries: Iterable[ResumeEntry],
    *,
    minimum_byte_size: int = 1,
) -> ResumeVerdict:
    """Verify a whole recorded object set, returning the first rejection.

    ``entries`` yields ``(path, expected_sha256)`` or
    ``(path, expected_sha256, expected_byte_size)`` tuples — the shape written
    by the shard/manifest state files.  A resume is all-or-nothing: one bad
    fragment invalidates the set, because partially reusing it would publish a
    dataset whose hash no longer describes its contents.

    An empty set is *not* resumable: "nothing recorded" is indistinguishable
    from "the state file was written before the first object landed".
    """
    checked = 0
    for entry in entries:
        if len(entry) == 2:
            path, expected_sha256 = entry
            expected_byte_size: int | None = None
        elif len(entry) == 3:
            path, expected_sha256, expected_byte_size = entry
        else:
            raise ValueError(
                "resume 항목은 (path, sha256[, byte_size]) 형식이어야 합니다: "
                f"{entry!r}"
            )
        verdict = verify_resumable_file(
            path,
            expected_sha256=expected_sha256,
            expected_byte_size=expected_byte_size,
            minimum_byte_size=minimum_byte_size,
        )
        if not verdict.resumable:
            return verdict
        checked += 1

    if checked == 0:
        return ResumeVerdict(False, REASON_NO_RECORDED_OBJECTS)
    return ResumeVerdict(True, "")


def require_matching_fingerprint(
    recorded: str | None,
    *,
    expected: str,
    location: str,
) -> None:
    """Raise unless the persisted configuration fingerprint still matches.

    A mismatch means the same revision path holds artefacts produced under a
    different configuration.  That is an operator error, not a recoverable
    condition, so it is raised rather than downgraded to "re-collect".
    """
    if recorded is not None and recorded == expected:
        return
    raise ResumeFingerprintMismatch(
        "resume 설정이 기존 상태와 다릅니다. 기존 파일을 덮어쓰지 말고 새 "
        f"revision을 사용하세요: {location} "
        f"(recorded={recorded!r}, expected={expected!r})"
    )
