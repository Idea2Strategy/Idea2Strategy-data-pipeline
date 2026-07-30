SELECT
    dm.id AS manifest_id,
    f.code AS feed_code,
    dm.resolution,
    dm.period_start,
    dm.period_end,
    dm.revision_number,
    dm.manifest_hash,
    so.bucket_code,
    so.object_key,
    so.provider_version_id,
    so.content_sha256,
    dmo.partition_key,
    dmo.row_count
FROM market_data.dataset_manifests dm
JOIN market_data.feeds f
  ON f.id = dm.feed_id
JOIN market_data.dataset_objects dmo
  ON dmo.dataset_manifest_id = dm.id
JOIN storage.objects so
  ON so.id = dmo.object_id
WHERE dm.status = 'AVAILABLE'
  AND f.code = %(feed_code)s
  AND dm.resolution = %(resolution)s
  AND dm.period_start <= %(required_start)s
  AND dm.period_end >= %(required_end)s
ORDER BY dm.revision_number DESC, dmo.partition_key ASC;
