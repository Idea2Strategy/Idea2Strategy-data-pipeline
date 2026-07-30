from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from uuid import UUID

from market_loader.config import AppConfig
from market_loader.model.bar import Bar
from market_loader.model.manifest import ManifestObject, object_key
from market_loader.pipeline.parquet_writer import WrittenParquet, write_parquet
from market_loader.storage.local_staging import LocalStaging
from market_loader.storage.s3 import ImmutableS3, S3ObjectIdentity


@dataclass(frozen=True, slots=True)
class PublishedObject:
    identity: S3ObjectIdentity
    manifest_object: ManifestObject
    parquet: WrittenParquet
    partition_key: str


class Publisher:
    def __init__(
        self,
        config: AppConfig,
        staging: LocalStaging,
        s3: ImmutableS3,
        bucket: str,
    ) -> None:
        self.config = config
        self.staging = staging
        self.s3 = s3
        self.bucket = bucket

    def publish_shard(
        self,
        *,
        run_id: UUID,
        manifest_id: UUID,
        revision: int,
        adjustment: str,
        resolution: str,
        period_start: date,
        period_end: date,
        shard: int,
        bars: list[Bar],
        partition_key: str,
    ) -> PublishedObject:
        path = self.staging.path_for(run_id, adjustment, resolution, period_start.year, shard)
        parquet = write_parquet(
            bars=bars,
            output_path=path,
            derived=resolution != "30m",
            schema_version=self.config.project.schema_version,
            processing_version=self.config.project.processing_version,
            adjustment=adjustment,
            resolution=resolution,
            period_start=period_start,
            period_end=period_end,
            revision=revision,
            manifest_id=manifest_id,
            compression=self.config.data.parquet_compression,
            compression_level=self.config.data.parquet_compression_level,
            row_group_size=self.config.data.parquet_row_group_size,
        )
        key = object_key(
            prefix=self.config.storage.prefix,
            adjustment=adjustment,
            resolution=resolution,
            revision=revision,
            year=period_start.year,
            shard=shard,
            shard_count=self.config.data.shard_count,
            manifest_id=manifest_id,
        )
        identity = self.s3.upload(
            bucket=self.bucket,
            object_key=key,
            path=path,
            schema_version=self.config.project.schema_version,
            processing_version=self.config.project.processing_version,
            manifest_id=str(manifest_id),
            allow_identical_reuse=True,
        )
        return PublishedObject(
            identity=identity,
            manifest_object=ManifestObject(
                content_sha256=identity.content_sha256,
                row_count=parquet.row_count,
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                shard=shard,
                part=1,
            ),
            parquet=parquet,
            partition_key=partition_key,
        )
