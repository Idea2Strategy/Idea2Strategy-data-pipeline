"""SQLAlchemy Core table metadata for the canonical `market_data` schema.

Every definition here restates the applied central baseline
(`backend/db-migration/src/main/resources/db/migration/V1__initial_schema.sql`,
generated from `db/schema.dbml`).  It is a *description* of a schema this process does
not own the right to create:

* `create_type=False` on every enum, and this metadata is never passed to
  `MetaData.create_all()` in a production path.  The runtime creates nothing; see
  `market_pipeline_lib.db.engine.install_runtime_guards`.
* Foreign keys to tables outside this metadata (`strategy.element_catalog_versions`)
  are documented in comments rather than declared, so the metadata never pulls a
  foreign schema in.
* No migration SQL is authored anywhere in this repository.  These tables already
  exist centrally.

Ownership note for `storage.objects`
------------------------------------
`DatabaseAccessPolicy.java:36` registers the `storage` schema as ``SHARED`` while
`docs/backend-implementation-master-checklist.md` lists it under D's owned schemas.
`db/migration-contributions/contribution.properties` declares ``schemas=market_data,storage``
and its validator holds ``storage`` to declared-but-not-mutable, so this repository
authors no ``storage`` DDL.  The ownership contradiction itself is unresolved
centrally, so this module lists
`storage.objects` in `READ_ONLY_TABLES` and the write path is not enabled by default:
`PostgresCatalog` requires an explicit `StorageObjectsPolicy` from its caller.  The
pipeline genuinely has to insert `storage.objects` rows -- `dataset_objects.object_id`
is a NOT NULL foreign key to it -- so refusing outright would make the canonical
pipeline unimplementable.  Naming the choice is the honest middle: nothing writes
`storage` by accident, and the ownership question stays visible.

DBML / DDL agreement
--------------------
Every column name, type, nullability and default below was diffed against both sources.
They agree exactly at column level.  The disagreements are at constraint level and are
recorded in `SCHEMA_CONTRADICTIONS`.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    text,
)
from sqlalchemy.dialects.postgresql import CHAR, ENUM, JSONB, TIMESTAMP, UUID, VARCHAR

__all__ = [
    "ASSET_TYPE_LABELS",
    "DATASET_STATUS_LABELS",
    "MARKET_DATA_SCHEMA",
    "METADATA",
    "OBJECT_STATUS_LABELS",
    "OPERATIONS_SCHEMA",
    "PARTITION_GRANULARITY_LABELS",
    "READ_ONLY_TABLES",
    "SCHEMA_CONTRADICTIONS",
    "STORAGE_SCHEMA",
    "TABLES_BY_NAME",
    "WORK_STATUS_LABELS",
    "WRITABLE_TABLES",
    "corporate_actions",
    "dataset_lineage",
    "dataset_object_lineage",
    "dataset_objects",
    "dataset_manifests",
    "feature_definitions",
    "feature_materializations",
    "feature_snapshot_batches",
    "feeds",
    "instrument_symbols",
    "instruments",
    "pipeline_runs",
    "providers",
    "quality_incidents",
    "storage_objects",
    "stream_watermarks",
    "trading_sessions",
]


MARKET_DATA_SCHEMA = "market_data"
STORAGE_SCHEMA = "storage"
OPERATIONS_SCHEMA = "operations"
STRATEGY_SCHEMA = "strategy"

#: `market_data.asset_type`.
ASSET_TYPE_LABELS: tuple[str, ...] = ("STOCK", "ETF", "INDEX")

#: `market_data.dataset_status`.
DATASET_STATUS_LABELS: tuple[str, ...] = (
    "BUILDING",
    "AVAILABLE",
    "QUARANTINED",
    "SUPERSEDED",
    "DELETED",
)

#: `market_data.partition_granularity`.
PARTITION_GRANULARITY_LABELS: tuple[str, ...] = ("DAY", "WEEK", "MONTH", "YEAR")

#: `storage.object_status`.
OBJECT_STATUS_LABELS: tuple[str, ...] = (
    "STAGED",
    "AVAILABLE",
    "QUARANTINED",
    "SUPERSEDED",
    "DELETED",
)

#: `operations.work_status`, used by three `market_data` tables.
WORK_STATUS_LABELS: tuple[str, ...] = (
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "SKIPPED",
)

METADATA = MetaData()


def _asset_type() -> ENUM:
    return ENUM(*ASSET_TYPE_LABELS, name="asset_type", schema=MARKET_DATA_SCHEMA, create_type=False)


def _dataset_status() -> ENUM:
    return ENUM(*DATASET_STATUS_LABELS, name="dataset_status", schema=MARKET_DATA_SCHEMA, create_type=False)


def _partition_granularity() -> ENUM:
    return ENUM(
        *PARTITION_GRANULARITY_LABELS,
        name="partition_granularity",
        schema=MARKET_DATA_SCHEMA,
        create_type=False,
    )


def _object_status() -> ENUM:
    return ENUM(*OBJECT_STATUS_LABELS, name="object_status", schema=STORAGE_SCHEMA, create_type=False)


def _work_status() -> ENUM:
    return ENUM(*WORK_STATUS_LABELS, name="work_status", schema=OPERATIONS_SCHEMA, create_type=False)


#: Read-only from this repository unless the caller opts in; see the module docstring.
storage_objects = Table(
    "objects",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("status", _object_status(), nullable=False),
    Column("storage_provider", VARCHAR(40), nullable=False),
    Column("bucket_name", VARCHAR(160), nullable=False),
    Column("object_key", VARCHAR(900), nullable=False),
    Column("provider_version_id", VARCHAR(300), nullable=False),
    Column("content_hash", VARCHAR(128), nullable=False),
    Column("byte_size", BigInteger, nullable=False),
    Column("file_format", VARCHAR(40), nullable=False),
    Column("compression_codec", VARCHAR(40), nullable=False),
    Column("media_type", VARCHAR(120), nullable=False),
    Column("schema_version", VARCHAR(40), nullable=False),
    Column("row_count", BigInteger),
    Column("period_start", TIMESTAMP(timezone=True)),
    Column("period_end", TIMESTAMP(timezone=True)),
    Column("encryption_key_ref", VARCHAR(300)),
    Column("retention_policy_version", VARCHAR(80), nullable=False),
    Column("retention_until", TIMESTAMP(timezone=True)),
    Column("legal_hold", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("verified_at", TIMESTAMP(timezone=True)),
    Column("quarantined_at", TIMESTAMP(timezone=True)),
    Column("superseded_at", TIMESTAMP(timezone=True)),
    Column("deleted_at", TIMESTAMP(timezone=True)),
    Index(
        "uq_storage_objects_provider_bucket_key_version",
        "storage_provider",
        "bucket_name",
        "object_key",
        "provider_version_id",
        unique=True,
    ),
    Index("ix_storage_objects_content_hash_byte_size", "content_hash", "byte_size"),
    Index("ix_storage_objects_status_created_at", "status", "created_at"),
    Index("ix_storage_objects_retention_until", "retention_until"),
    schema=STORAGE_SCHEMA,
)

instruments = Table(
    "instruments",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("asset_type", _asset_type(), nullable=False),
    Column("primary_exchange_mic", CHAR(4), nullable=False),
    Column("currency_code", CHAR(3), nullable=False, server_default=text("'USD'")),
    Column("provider_reference", VARCHAR(160)),
    Column("listed_at", Date),
    Column("delisted_at", Date),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index("ix_instruments_asset_type_mic", "asset_type", "primary_exchange_mic"),
    schema=MARKET_DATA_SCHEMA,
)

instrument_symbols = Table(
    "instrument_symbols",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "instrument_id",
        UUID(as_uuid=True),
        ForeignKey(instruments.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("exchange_mic", CHAR(4), nullable=False),
    Column("symbol", VARCHAR(32), nullable=False),
    Column("effective_from", TIMESTAMP(timezone=True), nullable=False),
    Column("effective_to", TIMESTAMP(timezone=True)),
    Index("uq_instrument_symbols_mic_symbol_from", "exchange_mic", "symbol", "effective_from", unique=True),
    Index("ix_instrument_symbols_instrument_from", "instrument_id", "effective_from"),
    schema=MARKET_DATA_SCHEMA,
)

trading_sessions = Table(
    "trading_sessions",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("exchange_mic", CHAR(4), nullable=False),
    Column("session_date", Date, nullable=False),
    Column("opens_at", TIMESTAMP(timezone=True)),
    Column("closes_at", TIMESTAMP(timezone=True)),
    # Values are REGULAR, EARLY_CLOSE, CLOSED -- a note in the DBML, not an enum type.
    Column("session_type", VARCHAR(30), nullable=False),
    Column("calendar_version", VARCHAR(40), nullable=False),
    Index(
        "uq_trading_sessions_mic_date_calendar",
        "exchange_mic",
        "session_date",
        "calendar_version",
        unique=True,
    ),
    schema=MARKET_DATA_SCHEMA,
)

providers = Table(
    "providers",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("code", VARCHAR(80), nullable=False, unique=True),
    Column("display_name", VARCHAR(160), nullable=False),
    # 'the exact provider and licence rights need external approval evidence' (DBML).
    # See `operations.RightsAttestation` for how that evidence reaches this column.
    Column("rights_version", VARCHAR(80), nullable=False),
    Column("status", VARCHAR(30), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    schema=MARKET_DATA_SCHEMA,
)

feeds = Table(
    "feeds",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "provider_id",
        UUID(as_uuid=True),
        ForeignKey(providers.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("code", VARCHAR(80), nullable=False),
    Column("data_kind", VARCHAR(40), nullable=False),
    Column("resolution", VARCHAR(30), nullable=False),
    Column("timezone_name", VARCHAR(80), nullable=False),
    Column("feed_version", VARCHAR(40), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("retired_at", TIMESTAMP(timezone=True)),
    Index("uq_feeds_provider_code_version", "provider_id", "code", "feed_version", unique=True),
    schema=MARKET_DATA_SCHEMA,
)

dataset_manifests = Table(
    "dataset_manifests",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "feed_id",
        UUID(as_uuid=True),
        ForeignKey(feeds.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column(
        "instrument_id",
        UUID(as_uuid=True),
        ForeignKey(instruments.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    # Values are RAW, NORMALIZED, ADJUSTED, DERIVED -- a note in the DBML, not an enum.
    Column("data_layer", VARCHAR(40), nullable=False),
    Column("resolution", VARCHAR(30), nullable=False),
    Column("revision_number", Integer, nullable=False),
    Column("status", _dataset_status(), nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True), nullable=False),
    Column("schema_version", VARCHAR(40), nullable=False),
    Column("dataset_hash", VARCHAR(128), nullable=False),
    Column(
        "supersedes_manifest_id",
        UUID(as_uuid=True),
        ForeignKey("market_data.dataset_manifests.id", deferrable=True, initially="IMMEDIATE"),
    ),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Column("available_at", TIMESTAMP(timezone=True)),
    Index(
        "uq_dataset_manifests_feed_instrument_layer_resolution_start_rev",
        "feed_id",
        "instrument_id",
        "data_layer",
        "resolution",
        "period_start",
        "revision_number",
        unique=True,
    ),
    Index("ix_dataset_manifests_status_period", "status", "period_start", "period_end"),
    Index("uq_dataset_manifests_dataset_hash", "dataset_hash", unique=True),
    schema=MARKET_DATA_SCHEMA,
)

dataset_objects = Table(
    "dataset_objects",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "dataset_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column(
        "object_id",
        UUID(as_uuid=True),
        ForeignKey(storage_objects.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("object_kind", VARCHAR(40), nullable=False),
    Column("partition_granularity", _partition_granularity(), nullable=False),
    # ET calendar inclusive start / exclusive end.
    Column("partition_start", Date, nullable=False),
    Column("partition_end", Date, nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True), nullable=False),
    Column("shard_key", VARCHAR(120), nullable=False),
    Column("part_number", Integer, nullable=False),
    Column("row_count", BigInteger, nullable=False),
    Column("min_instrument_id", UUID(as_uuid=True)),
    Column("max_instrument_id", UUID(as_uuid=True)),
    Index(
        "uq_dataset_objects_manifest_kind_granularity_partition_shard_part",
        "dataset_manifest_id",
        "object_kind",
        "partition_granularity",
        "partition_start",
        "partition_end",
        "shard_key",
        "part_number",
        unique=True,
    ),
    Index("ix_dataset_objects_granularity_partition", "partition_granularity", "partition_start", "partition_end"),
    Index("ix_dataset_objects_manifest_period", "dataset_manifest_id", "period_start", "period_end"),
    schema=MARKET_DATA_SCHEMA,
)

dataset_lineage = Table(
    "dataset_lineage",
    METADATA,
    Column(
        "derived_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column(
        "source_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column("relation_type", VARCHAR(40), primary_key=True),
    schema=MARKET_DATA_SCHEMA,
)

pipeline_runs = Table(
    "pipeline_runs",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("pipeline_code", VARCHAR(80), nullable=False),
    Column("pipeline_version", VARCHAR(40), nullable=False),
    Column("idempotency_key", VARCHAR(160), nullable=False, unique=True),
    Column("status", _work_status(), nullable=False),
    Column("input_hash", VARCHAR(128), nullable=False),
    Column("output_hash", VARCHAR(128)),
    Column("started_at", TIMESTAMP(timezone=True)),
    Column("completed_at", TIMESTAMP(timezone=True)),
    Column("failure_code", VARCHAR(80)),
    Index("ix_pipeline_runs_code_status_started", "pipeline_code", "status", "started_at"),
    schema=MARKET_DATA_SCHEMA,
)

dataset_object_lineage = Table(
    "dataset_object_lineage",
    METADATA,
    Column(
        "derived_dataset_object_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_objects.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column(
        "source_dataset_object_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_objects.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column(
        "pipeline_run_id",
        UUID(as_uuid=True),
        ForeignKey(pipeline_runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    # Compaction is COMPACTED_FROM.
    Column("relation_type", VARCHAR(40), primary_key=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index("ix_dataset_object_lineage_source", "source_dataset_object_id"),
    Index("ix_dataset_object_lineage_pipeline_run", "pipeline_run_id"),
    schema=MARKET_DATA_SCHEMA,
)

feature_definitions = Table(
    "feature_definitions",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    # `strategy.element_catalog_versions.id`: read-only upstream reference, deliberately
    # not declared as a ForeignKey so `strategy` stays out of this metadata.
    Column("element_catalog_version_id", UUID(as_uuid=True), nullable=False),
    Column("feature_code", VARCHAR(120), nullable=False),
    Column("calculator_version", VARCHAR(80), nullable=False),
    Column("resolution", VARCHAR(30), nullable=False),
    Column("normalized_parameters", JSONB, nullable=False),
    Column("output_value_type", VARCHAR(40), nullable=False),
    Column("required_history_points", Integer, nullable=False),
    Column("definition_hash", VARCHAR(128), nullable=False, unique=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index(
        "uq_feature_definitions_catalog_code_calculator_resolution_hash",
        "element_catalog_version_id",
        "feature_code",
        "calculator_version",
        "resolution",
        "definition_hash",
        unique=True,
    ),
    schema=MARKET_DATA_SCHEMA,
)

feature_materializations = Table(
    "feature_materializations",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "feature_definition_id",
        UUID(as_uuid=True),
        ForeignKey(feature_definitions.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column(
        "instrument_id",
        UUID(as_uuid=True),
        ForeignKey(instruments.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column(
        "pipeline_run_id",
        UUID(as_uuid=True),
        ForeignKey(pipeline_runs.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
        unique=True,
    ),
    Column("input_dataset_set_hash", VARCHAR(128), nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True), nullable=False),
    Column("source_watermark", VARCHAR(300), nullable=False),
    Column(
        "output_dataset_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    Column("result_hash", VARCHAR(128)),
    Column("status", _work_status(), nullable=False),
    Column("available_at", TIMESTAMP(timezone=True)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index(
        "uq_feature_materializations_definition_instrument_inputs_period",
        "feature_definition_id",
        "instrument_id",
        "input_dataset_set_hash",
        "period_start",
        "period_end",
        unique=True,
    ),
    Index("ix_feature_materializations_instrument_period_status", "instrument_id", "period_end", "status"),
    Index("uq_feature_materializations_output_manifest", "output_dataset_manifest_id", unique=True),
    schema=MARKET_DATA_SCHEMA,
)

feature_snapshot_batches = Table(
    "feature_snapshot_batches",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("feature_set_hash", VARCHAR(128), nullable=False),
    Column("input_market_set_hash", VARCHAR(128), nullable=False),
    Column("source_start_watermark", VARCHAR(300), nullable=False),
    Column("source_end_watermark", VARCHAR(300), nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True), nullable=False),
    Column(
        "snapshot_object_id",
        UUID(as_uuid=True),
        ForeignKey(storage_objects.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    Column("batch_hash", VARCHAR(128)),
    Column("row_count", BigInteger),
    Column("status", _work_status(), nullable=False),
    Column("idempotency_key", VARCHAR(160), nullable=False, unique=True),
    Column("available_at", TIMESTAMP(timezone=True)),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index(
        "uq_feature_snapshot_batches_feature_input_period",
        "feature_set_hash",
        "input_market_set_hash",
        "period_start",
        "period_end",
        unique=True,
    ),
    Index("ix_feature_snapshot_batches_status_period_end", "status", "period_end"),
    schema=MARKET_DATA_SCHEMA,
)

corporate_actions = Table(
    "corporate_actions",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "instrument_id",
        UUID(as_uuid=True),
        ForeignKey(instruments.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column(
        "source_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
        nullable=False,
    ),
    Column("provider_event_key", VARCHAR(160), nullable=False),
    Column("action_type", VARCHAR(60), nullable=False),
    Column("effective_at", TIMESTAMP(timezone=True), nullable=False),
    Column("terms_document", JSONB, nullable=False),
    Column("terms_hash", VARCHAR(128), nullable=False),
    Column(
        "supersedes_action_id",
        UUID(as_uuid=True),
        ForeignKey("market_data.corporate_actions.id", deferrable=True, initially="IMMEDIATE"),
    ),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
    Index("uq_corporate_actions_source_manifest_event", "source_manifest_id", "provider_event_key", unique=True),
    Index("ix_corporate_actions_instrument_effective", "instrument_id", "effective_at"),
    schema=MARKET_DATA_SCHEMA,
)

quality_incidents = Table(
    "quality_incidents",
    METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column(
        "dataset_manifest_id",
        UUID(as_uuid=True),
        ForeignKey(dataset_manifests.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    Column(
        "instrument_id",
        UUID(as_uuid=True),
        ForeignKey(instruments.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    Column("severity", VARCHAR(20), nullable=False),
    Column("incident_code", VARCHAR(80), nullable=False),
    Column("period_start", TIMESTAMP(timezone=True), nullable=False),
    Column("period_end", TIMESTAMP(timezone=True)),
    Column("status", VARCHAR(30), nullable=False),
    Column(
        "evidence_object_id",
        UUID(as_uuid=True),
        ForeignKey(storage_objects.c.id, deferrable=True, initially="IMMEDIATE"),
    ),
    Column("detected_at", TIMESTAMP(timezone=True), nullable=False),
    Column("resolved_at", TIMESTAMP(timezone=True)),
    Index("ix_quality_incidents_status_severity_detected", "status", "severity", "detected_at"),
    Index("ix_quality_incidents_manifest_period", "dataset_manifest_id", "period_start"),
    schema=MARKET_DATA_SCHEMA,
)

stream_watermarks = Table(
    "stream_watermarks",
    METADATA,
    Column(
        "feed_id",
        UUID(as_uuid=True),
        ForeignKey(feeds.c.id, deferrable=True, initially="IMMEDIATE"),
        primary_key=True,
    ),
    Column("last_source_event_at", TIMESTAMP(timezone=True), nullable=False),
    Column("last_ingested_at", TIMESTAMP(timezone=True), nullable=False),
    Column("last_sequence", BigInteger),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
    schema=MARKET_DATA_SCHEMA,
)


#: Tables this repository writes, in foreign-key-safe insertion order.
WRITABLE_TABLES: tuple[Table, ...] = (
    providers,
    feeds,
    instruments,
    instrument_symbols,
    trading_sessions,
    pipeline_runs,
    dataset_manifests,
    dataset_objects,
    dataset_lineage,
    dataset_object_lineage,
    quality_incidents,
    stream_watermarks,
    corporate_actions,
    feature_definitions,
    feature_materializations,
    feature_snapshot_batches,
)

#: Read-only unless the caller states an explicit ownership policy; see the docstring.
READ_ONLY_TABLES: tuple[Table, ...] = (storage_objects,)

#: `"<schema>.<table>"` -> `Table`, the lookup the catalog boundary is addressed by.
TABLES_BY_NAME: dict[str, Table] = {
    f"{table.schema}.{table.name}": table for table in (*WRITABLE_TABLES, *READ_ONLY_TABLES)
}


#: Disagreements between `db/schema.dbml` and the applied `V1__initial_schema.sql`,
#: recorded rather than silently resolved.  Every one is a constraint the DBML asks a
#: migration to enforce and the applied migration does not; none is a column-level
#: difference.  This module follows the applied DDL, because that is what the database
#: actually contains, and the catalog enforces the missing invariants in application
#: code where it can.  Consumed by `tests/test_catalog_contract.py`.
SCHEMA_CONTRADICTIONS: tuple[tuple[str, str], ...] = (
    (
        "market_data.dataset_manifests",
        "DBML Note requires null-safe uniqueness for multi-instrument datasets, but the "
        "applied unique index on (feed_id, instrument_id, data_layer, resolution, "
        "period_start, revision_number) treats NULL instrument_id as distinct, so it "
        "does not constrain the multi-instrument manifests this pipeline publishes. "
        "PostgresCatalog enforces the equivalent invariant per unit of work.",
    ),
    (
        "market_data.instrument_symbols",
        "DBML Note requires the migration to prevent overlapping symbol validity "
        "periods; the applied DDL has no exclusion constraint, only a unique index on "
        "(exchange_mic, symbol, effective_from).",
    ),
    (
        "market_data.dataset_manifests",
        "Neither the DBML nor the applied DDL contains uq_available_manifest_period. It "
        "exists only in data-pipeline/idea2strategy-market-loader/db/migration/"
        "V001__market_data_initial_schema.sql, which is the illegal forked migration "
        "that spec section 1 marks for deletion. Nothing in the database prevents two "
        "AVAILABLE manifests for one feed and period.",
    ),
    (
        "market_data.dataset_manifests",
        "uq_dataset_manifests_dataset_hash is globally unique, but engine.publish_dataset "
        "derives dataset_hash from the manifest's objects, and every manifest that ends "
        "QUARANTINED with zero objects hashes the same empty list. Two empty quarantined "
        "manifests -- two contracts with no data for one year, for instance -- therefore "
        "collide on insert. LocalCatalog has no such constraint, so this only appears "
        "against PostgreSQL. Resolving it needs a central decision: either the hash is "
        "salted with the manifest identity when the object set is empty (which changes "
        "what dataset_hash means, and operations.validate_catalog recomputes it), or the "
        "unique index is narrowed. Neither is D's to make; raised as a separate issue.",
    ),
)
