-- Reproduce only the six drifted tables from the retired market-loader V001.
-- All other objects remain at central V1 so describe_schema_drift can verify the
-- complete runtime metadata after the upgrade.

DROP TABLE market_data.dataset_object_lineage CASCADE;
DROP TABLE market_data.feature_materializations CASCADE;
DROP TABLE market_data.corporate_actions CASCADE;
DROP TABLE market_data.dataset_lineage CASCADE;
DROP TABLE market_data.quality_incidents CASCADE;
DROP TABLE market_data.dataset_objects CASCADE;

CREATE TABLE market_data.dataset_objects (
    id uuid PRIMARY KEY,
    dataset_manifest_id uuid NOT NULL REFERENCES market_data.dataset_manifests(id),
    object_id uuid NOT NULL REFERENCES storage.objects(id),
    object_kind varchar(30) NOT NULL CHECK (object_kind = 'BAR_PARQUET'),
    partition_key text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    min_bar_start_at timestamptz,
    max_bar_start_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset_manifest_id, object_id),
    UNIQUE (dataset_manifest_id, partition_key)
);

CREATE TABLE market_data.dataset_lineage (
    id uuid PRIMARY KEY,
    dataset_manifest_id uuid NOT NULL REFERENCES market_data.dataset_manifests(id),
    source_manifest_id uuid NOT NULL REFERENCES market_data.dataset_manifests(id),
    relationship_type varchar(30) NOT NULL CHECK (relationship_type = 'DERIVED_FROM'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset_manifest_id, source_manifest_id, relationship_type),
    CHECK (dataset_manifest_id <> source_manifest_id)
);

CREATE TABLE market_data.quality_incidents (
    id uuid PRIMARY KEY,
    dataset_manifest_id uuid REFERENCES market_data.dataset_manifests(id),
    instrument_id uuid REFERENCES market_data.instruments(id),
    incident_type varchar(100) NOT NULL,
    severity varchar(20) NOT NULL CHECK (severity IN ('WARNING', 'ERROR')),
    period_start timestamptz,
    period_end timestamptz,
    status varchar(30) NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    detail jsonb NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);
