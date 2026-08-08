"""Publish durable watermarks from successful canonical market-data manifests."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.manifest_watermarks import advance_available_manifest_watermarks
from market_pipeline_lib.watermarks import SqlWatermarkRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Advance active-feed watermarks from AVAILABLE dataset manifests."
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("/tmp/idea2strategy-watermarks"),
    )
    return parser


def execute(
    argv: Sequence[str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> dict[str, object]:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    values = os.environ if environment is None else environment
    database_url = values.get("PIPELINE_WORKER_DATABASE_URL", "").strip()
    if not database_url:
        raise ValueError("PIPELINE_WORKER_DATABASE_URL is required")

    catalog = PostgresCatalog.connect(
        database_url,
        artifact_root=args.artifact_root,
        storage_objects=StorageObjectsPolicy.READ_ONLY,
    )
    try:
        catalog.verify_schema()
        return cast(
            dict[str, object],
            advance_available_manifest_watermarks(
                catalog,
                SqlWatermarkRepository(catalog.engine),
            ),
        )
    finally:
        catalog.close()


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = execute(argv)
    except (OSError, RuntimeError, ValueError) as error:
        print(f"publish-manifest-watermarks failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
