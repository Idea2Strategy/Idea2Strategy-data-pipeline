from __future__ import annotations

from uuid import UUID

from market_loader.database.repositories import MarketRepository
from market_loader.pipeline.publisher import PublishedObject


def register_manifest(
    repository: MarketRepository,
    *,
    manifest_id: UUID,
    previous_manifest_id: UUID | None,
    manifest_hash: str,
    quality_status: str,
    published: list[PublishedObject],
    source_manifest_id: UUID | None,
    warning_codes: tuple[str, ...] = (),
) -> None:
    repository.finalize_manifest(
        manifest_id=manifest_id,
        previous_manifest_id=previous_manifest_id,
        manifest_hash=manifest_hash,
        quality_status=quality_status,
        objects=[
            (
                item.identity,
                item.manifest_object,
                item.parquet.min_bar_start_at,
                item.parquet.max_bar_start_at,
                item.partition_key,
            )
            for item in published
        ],
        source_manifest_id=source_manifest_id,
        warning_codes=warning_codes,
    )
