"""Canonical serialisation and the `feature_materialization_version` grammar.

Every hash in this package is `sha256` over the UTF-8 bytes of one canonical JSON
rendering, produced here and nowhere else.  Two properties matter and both are
deliberate:

* ``sort_keys=True`` -- the order a caller happened to build a dict in is not part of
  the identity of what the dict describes.
* ``allow_nan=False`` -- ``NaN`` and ``Infinity`` are not JSON, and a value that only
  survives a round trip through one particular decoder cannot be part of a hash that a
  different process has to reproduce.

Version grammar
---------------
``feature_materialization_version`` is the string a downstream consumer pins.  It is
structured rather than opaque so that a consumer can tell, without a database, whether
the artefact in front of it is the one its request named::

    fmv1:<scope>:<identity16>:<inputs16>:<result16>

``scope``
    ``m`` for a single `feature_materializations` row, ``b`` for a
    `feature_snapshot_batches` row.  The backtest engine pins a batch; the pipeline's
    own lineage refers to individual materializations.  One grammar, so one parser.
``identity16``
    First 16 hex characters of `definition_hash` (``m``) or `feature_set_hash` (``b``):
    *what* was computed.
``inputs16``
    First 16 of `input_dataset_set_hash` (``m``) or `input_market_set_hash` (``b``):
    *over which inputs*.
``result16``
    First 16 of `result_hash` (``m``) or `batch_hash` (``b``): *and what came out*.

A version string therefore only exists for work that finished.  There is no rendering
of a ``PENDING`` or ``FAILED`` artefact, which is what makes a partially materialized
batch unconsumable rather than merely discouraged.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

__all__ = [
    "FEATURE_VERSION_PREFIX",
    "ParsedFeatureVersion",
    "batch_version",
    "canonical_json",
    "canonical_sha256",
    "is_sha256_hex",
    "iso_utc",
    "materialization_version",
    "parse_feature_materialization_version",
]


FEATURE_VERSION_PREFIX = "fmv1"

#: The number of leading hex characters of each component a version string carries.
VERSION_PREFIX_LENGTH = 16

SCOPE_MATERIALIZATION = "m"
SCOPE_BATCH = "b"

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(
    rf"^{FEATURE_VERSION_PREFIX}:(?P<scope>[{SCOPE_MATERIALIZATION}{SCOPE_BATCH}]):"
    rf"(?P<identity>[0-9a-f]{{{VERSION_PREFIX_LENGTH}}}):"
    rf"(?P<inputs>[0-9a-f]{{{VERSION_PREFIX_LENGTH}}}):"
    rf"(?P<result>[0-9a-f]{{{VERSION_PREFIX_LENGTH}}})$"
)


def canonical_json(payload: Any) -> str:
    """One rendering, everywhere: sorted keys, no spaces, no NaN, UTF-8 text."""

    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_HEX.fullmatch(value) is not None


def iso_utc(value: datetime) -> str:
    """`2026-01-05T14:30:00Z`.  Naive input is refused, never assumed to be UTC."""

    if not isinstance(value, datetime):
        raise TypeError(f"expected a datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        raise ValueError(
            f"{value!r} has no timezone; this pipeline works in ET and UTC at once, so a "
            "naive timestamp is ambiguous and is never assumed to be UTC"
        )
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def compact_utc(value: datetime) -> str:
    """`20260105T143000Z`, for identifiers with a length budget."""

    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _version(scope: str, identity: str, inputs: str, result: str) -> str:
    for label, digest in (("identity", identity), ("inputs", inputs), ("result", result)):
        if not is_sha256_hex(digest):
            raise ValueError(f"{label} component must be 64 lowercase hex characters, got {digest!r}")
    return ":".join(
        (
            FEATURE_VERSION_PREFIX,
            scope,
            identity[:VERSION_PREFIX_LENGTH],
            inputs[:VERSION_PREFIX_LENGTH],
            result[:VERSION_PREFIX_LENGTH],
        )
    )


def materialization_version(
    *,
    definition_hash: str,
    input_dataset_set_hash: str,
    result_hash: str,
) -> str:
    """The version string for one completed `feature_materializations` row."""

    return _version(SCOPE_MATERIALIZATION, definition_hash, input_dataset_set_hash, result_hash)


def batch_version(
    *,
    feature_set_hash: str,
    input_market_set_hash: str,
    batch_hash: str,
) -> str:
    """The version string a backtest request pins for a sealed snapshot batch."""

    return _version(SCOPE_BATCH, feature_set_hash, input_market_set_hash, batch_hash)


@dataclass(frozen=True)
class ParsedFeatureVersion:
    """A `feature_materialization_version` taken apart into its four components."""

    scope: str
    identity_prefix: str
    inputs_prefix: str
    result_prefix: str

    @property
    def is_batch(self) -> bool:
        return self.scope == SCOPE_BATCH

    def matches(self, *, identity: str, inputs: str, result: str) -> bool:
        """Whether this string was rendered from these three full digests."""

        return (
            self.identity_prefix == identity[:VERSION_PREFIX_LENGTH]
            and self.inputs_prefix == inputs[:VERSION_PREFIX_LENGTH]
            and self.result_prefix == result[:VERSION_PREFIX_LENGTH]
        )


def parse_feature_materialization_version(value: Any) -> ParsedFeatureVersion:
    """Parse a version string, or raise `ValueError`.

    Strict on purpose.  The COM06 contract types this field as a non-empty string, so
    the schema alone would accept ``"feature-materialization-v1"`` -- a placeholder that
    pins nothing.  Rejecting it here is the difference between a consumer that has
    verified what it is running against and one that has merely received a label.
    """

    if not isinstance(value, str):
        raise ValueError(f"feature_materialization_version must be a string, got {type(value).__name__}")
    match = _VERSION.fullmatch(value)
    if match is None:
        raise ValueError(
            f"{value!r} is not a feature_materialization_version; expected "
            f"{FEATURE_VERSION_PREFIX}:<m|b>:<16 hex>:<16 hex>:<16 hex>"
        )
    return ParsedFeatureVersion(
        scope=match.group("scope"),
        identity_prefix=match.group("identity"),
        inputs_prefix=match.group("inputs"),
        result_prefix=match.group("result"),
    )
