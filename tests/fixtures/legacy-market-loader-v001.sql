CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE SCHEMA IF NOT EXISTS operations;
CREATE SCHEMA IF NOT EXISTS storage;
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TYPE operations.work_status AS ENUM
    ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED');
CREATE TYPE market_data.dataset_status AS ENUM
    ('BUILDING', 'AVAILABLE', 'QUARANTINED', 'SUPERSEDED', 'DELETED');

CREATE TABLE market_data.providers (
    id uuid PRIMARY KEY,
    code varchar(50) NOT NULL UNIQUE,
    name varchar(200) NOT NULL,
    rights_version varchar(200) NOT NULL,
    status varchar(30) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE market_data.feeds (
    id uuid PRIMARY KEY,
    provider_id uuid NOT NULL REFERENCES market_data.providers(id),
    code varchar(100) NOT NULL UNIQUE,
    data_kind varchar(30) NOT NULL CHECK (data_kind = 'BAR'),
    resolution varchar(10) NOT NULL CHECK (resolution IN ('30m', '1h', '4h', '1d')),
    session_scope varchar(30) NOT NULL CHECK (session_scope = 'REGULAR'),
    status varchar(30) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE market_data.instruments (
    id uuid PRIMARY KEY,
    asset_type varchar(20) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    primary_exchange_mic varchar(10) NOT NULL,
    currency char(3) NOT NULL CHECK (currency = 'USD'),
    support_status varchar(30) NOT NULL,
    listed_from date NOT NULL,
    listed_to date,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (listed_to IS NULL OR listed_to >= listed_from)
);
CREATE TABLE market_data.instrument_symbols (
    id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES market_data.instruments(id),
    symbol varchar(32) NOT NULL,
    exchange_mic varchar(10) NOT NULL,
    effective_from date NOT NULL,
    effective_to date,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    EXCLUDE USING gist (
        symbol WITH =,
        exchange_mic WITH =,
        daterange(effective_from, COALESCE(effective_to, 'infinity'::date), '[]') WITH &&
    )
);
CREATE TABLE market_data.trading_sessions (
    id uuid PRIMARY KEY,
    exchange_mic varchar(10) NOT NULL,
    session_date date NOT NULL,
    opens_at timestamptz NOT NULL,
    closes_at timestamptz NOT NULL,
    session_type varchar(30) NOT NULL CHECK (session_type IN ('REGULAR', 'EARLY_CLOSE')),
    calendar_version varchar(100) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (exchange_mic, session_date),
    CHECK (opens_at < closes_at)
);
CREATE TABLE market_data.pipeline_runs (
    id uuid PRIMARY KEY,
    pipeline_type varchar(50) NOT NULL CHECK (pipeline_type = 'HISTORICAL_BACKFILL'),
    processing_version varchar(200) NOT NULL,
    status operations.work_status NOT NULL,
    idempotency_key char(64) NOT NULL UNIQUE,
    requested_at timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    input_config jsonb NOT NULL,
    summary_result jsonb,
    failure_code varchar(100)
);
CREATE TABLE storage.objects (
    id uuid PRIMARY KEY,
    storage_class varchar(30) NOT NULL CHECK (storage_class = 'S3_STANDARD'),
    bucket_code varchar(100) NOT NULL CHECK (bucket_code = 'DEVELOPMENT_MARKET_DATA'),
    object_key text NOT NULL,
    provider_version_id text NOT NULL,
    content_sha256 char(64) NOT NULL,
    byte_size bigint NOT NULL CHECK (byte_size >= 0),
    media_type varchar(100) NOT NULL,
    format_version varchar(100) NOT NULL,
    encryption_profile varchar(50) NOT NULL CHECK (encryption_profile = 'SSE-S3-AES256'),
    created_at timestamptz NOT NULL DEFAULT now(),
    verified_at timestamptz NOT NULL,
    UNIQUE (bucket_code, object_key, provider_version_id)
);
CREATE TABLE market_data.dataset_manifests (
    id uuid PRIMARY KEY,
    feed_id uuid NOT NULL REFERENCES market_data.feeds(id),
    instrument_id uuid REFERENCES market_data.instruments(id),
    data_layer varchar(30) NOT NULL CHECK (data_layer IN ('RAW', 'ADJUSTED', 'DERIVED')),
    resolution varchar(10) NOT NULL CHECK (resolution IN ('30m', '1h', '4h', '1d')),
    period_start date NOT NULL,
    period_end date NOT NULL,
    revision_number integer NOT NULL CHECK (revision_number > 0),
    as_of_at timestamptz NOT NULL,
    processing_version varchar(200) NOT NULL,
    quality_status varchar(30) CHECK (
        quality_status IS NULL OR quality_status IN ('PASSED', 'PASSED_WITH_WARNINGS', 'FAILED')
    ),
    status market_data.dataset_status NOT NULL,
    row_count bigint,
    manifest_hash char(64),
    created_at timestamptz NOT NULL DEFAULT now(),
    supersedes_manifest_id uuid REFERENCES market_data.dataset_manifests(id),
    UNIQUE (feed_id, period_start, period_end, revision_number),
    CHECK (period_start < period_end),
    CHECK ((status = 'BUILDING' AND manifest_hash IS NULL) OR status <> 'BUILDING')
);
CREATE UNIQUE INDEX uq_available_manifest_period
    ON market_data.dataset_manifests(feed_id, period_start, period_end)
    WHERE status = 'AVAILABLE';
CREATE TABLE market_data.pipeline_partitions (
    id uuid PRIMARY KEY,
    pipeline_run_id uuid NOT NULL REFERENCES market_data.pipeline_runs(id),
    partition_key text NOT NULL,
    status operations.work_status NOT NULL,
    result_manifest_id uuid REFERENCES market_data.dataset_manifests(id),
    error_code varchar(100),
    error_summary text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_run_id, partition_key)
);
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
CREATE INDEX ix_manifest_available_lookup
    ON market_data.dataset_manifests(feed_id, resolution, period_start, period_end)
    WHERE status = 'AVAILABLE';
CREATE INDEX ix_dataset_objects_manifest
    ON market_data.dataset_objects(dataset_manifest_id, partition_key);
