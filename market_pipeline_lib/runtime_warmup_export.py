"""Fail-closed one-shot publication of a trading warm-up runtime bundle."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from .fs_paths import long_path
from .realtime_warmup import (
    BLOCKED,
    READY,
    FeatureRequirement,
    WarmupPublicationSpec,
    WarmupReadiness,
    publish_blocked_warmup_manifest,
    publish_realtime_warmup_bundle,
    verify_realtime_warmup_bundle,
)

RECEIPT_NAME = "publication-receipt.json"


class RuntimeWarmupError(RuntimeError):
    """Explicit warm-up inputs are absent, stale, contradictory, or divergent."""


def _canonical_json(document: object) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(long_path(path), "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _requirements_document(
    requirements: Sequence[FeatureRequirement],
) -> list[dict[str, Any]]:
    return [
        {
            "requirement_id": item.requirement_id,
            "feature_id": item.feature_id,
            "feature_version": item.feature_version,
            "resolution": item.resolution,
            "value_field": item.value_field,
            "instruments": list(item.instruments),
            "required_observations": item.required_observations,
        }
        for item in requirements
    ]


def _input_digest(
    *,
    session_date: date,
    spec: WarmupPublicationSpec,
    readiness: WarmupReadiness,
    requirements: Sequence[FeatureRequirement],
    events: Mapping[str, Any] | None,
) -> str:
    document = {
        "session_date": session_date.isoformat(),
        "spec": {
            "price_type": spec.contract.price_type,
            "layer": spec.contract.data_layer,
            "resolution": spec.contract.resolution,
            "feed_code": spec.contract.feed_code,
            "event_type": spec.event_type,
            "granularity": spec.granularity,
            "revision": spec.revision,
            "shard_count": spec.shard_count,
        },
        "readiness": readiness.as_document(),
        "requirements": _requirements_document(requirements),
        "events": events,
    }
    return hashlib.sha256(_canonical_json(document)).hexdigest()


def _assert_current(
    readiness: WarmupReadiness,
    *,
    now: datetime,
    max_readiness_age: timedelta,
) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if max_readiness_age <= timedelta(0):
        raise ValueError("max_readiness_age must be positive")
    current = now.astimezone(UTC)
    if readiness.evaluated_at > current:
        raise RuntimeWarmupError("readiness was evaluated in the future")
    if current - readiness.evaluated_at > max_readiness_age:
        raise RuntimeWarmupError("readiness verdict is stale")


def _receipt(bundle_root: Path, manifest: Mapping[str, Any], input_digest: str) -> dict[str, Any]:
    manifest_path = bundle_root / "manifest.json"
    objects = []
    for item in manifest.get("objects", []):
        path = bundle_root / str(item["object_key"])
        actual = _sha256(path)
        if actual != item["content_hash"]:
            raise RuntimeWarmupError(f"published object hash differs: {item['object_key']}")
        objects.append(
            {
                "storage_object_id": str(item["storage_object_id"]),
                "object_key": str(item["object_key"]),
                "object_role": str(item["object_role"]),
                "schema_version": str(item["schema_version"]),
                "sha256": actual,
                "byte_size": path.stat().st_size,
            }
        )
    return {
        "contract_id": "idea2strategy.trading-warmup-publication-receipt",
        "schema_version": 1,
        "input_digest": input_digest,
        "manifest": {
            "manifest_id": str(manifest["manifest_id"]),
            "schema_version": int(manifest["schema_version"]),
            "revision": int(manifest["revision"]),
            "status": str(manifest["status"]),
            "sha256": _sha256(manifest_path),
            "byte_size": manifest_path.stat().st_size,
        },
        "objects": objects,
    }


def _verify_replay(output: Path, input_digest: str) -> dict[str, Any]:
    try:
        manifest = verify_realtime_warmup_bundle(output)
        receipt = json.loads((output / RECEIPT_NAME).read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeWarmupError(f"existing warm-up output is invalid: {exc}") from exc
    if receipt.get("input_digest") != input_digest:
        raise RuntimeWarmupError("warm-up output already exists for different explicit inputs")
    expected = _receipt(output, manifest, input_digest)
    if receipt != expected:
        raise RuntimeWarmupError("existing warm-up publication receipt differs from verified bytes")
    return {
        "status": "ALREADY_APPLIED",
        "manifest_id": manifest["manifest_id"],
        "manifest_sha256": receipt["manifest"]["sha256"],
        "object_count": len(receipt["objects"]),
        "output": str(output),
    }


def publish_trading_warmup(
    *,
    output: Path,
    session_date: date,
    spec: WarmupPublicationSpec,
    readiness: WarmupReadiness,
    requirements: Sequence[FeatureRequirement],
    events: Mapping[str, Any] | None,
    now: datetime,
    max_readiness_age: timedelta,
) -> dict[str, Any]:
    """Publish READY or BLOCKED through the existing D90 publisher and verifier."""

    output = output.expanduser().resolve()
    if readiness.session_date_et != session_date.isoformat():
        raise RuntimeWarmupError("readiness session does not match the explicit session date")
    if readiness.feed_id != spec.contract.feed_code:
        raise RuntimeWarmupError("readiness feed does not match the publication contract")
    _assert_current(readiness, now=now, max_readiness_age=max_readiness_age)
    if readiness.state == READY:
        if events is None:
            raise RuntimeWarmupError("READY publication requires an explicit events document")
        if not requirements:
            raise RuntimeWarmupError("READY publication requires explicit feature requirements")
    elif readiness.state == BLOCKED:
        if events is not None or requirements:
            raise RuntimeWarmupError("BLOCKED publication forbids events and feature requirements")
    else:  # WarmupReadiness already enforces this; retained at the boundary.
        raise RuntimeWarmupError(f"unsupported readiness state: {readiness.state}")
    input_digest = _input_digest(
        session_date=session_date,
        spec=spec,
        readiness=readiness,
        requirements=requirements,
        events=events,
    )
    if output.exists():
        return _verify_replay(output, input_digest)
    output.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    bundle_root = staging_root / "bundle"
    try:
        if readiness.state == READY:
            assert events is not None
            published = publish_realtime_warmup_bundle(
                events,
                bundle_root,
                requirements,
                spec=spec,
                readiness=readiness,
            )
            manifest = published.manifest
        else:
            publish_blocked_warmup_manifest(
                bundle_root,
                spec=spec,
                readiness=readiness,
            )
            manifest = verify_realtime_warmup_bundle(bundle_root)
        if (
            readiness.manifest_id is not None
            and str(manifest["manifest_id"]) != readiness.manifest_id
        ):
            raise RuntimeWarmupError(
                "readiness manifest_id does not match the deterministic publication"
            )
        verified = verify_realtime_warmup_bundle(bundle_root)
        if verified != manifest:
            raise RuntimeWarmupError("published manifest differs after verification")
        receipt = _receipt(bundle_root, manifest, input_digest)
        (bundle_root / RECEIPT_NAME).write_bytes(_canonical_json(receipt))
        os.replace(bundle_root, output)
    except Exception:
        shutil.rmtree(long_path(staging_root), ignore_errors=True)
        raise
    finally:
        shutil.rmtree(long_path(staging_root), ignore_errors=True)
    return {
        "status": "PUBLISHED",
        "manifest_id": manifest["manifest_id"],
        "manifest_sha256": receipt["manifest"]["sha256"],
        "object_count": len(receipt["objects"]),
        "output": str(output),
    }
