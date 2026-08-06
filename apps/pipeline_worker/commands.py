"""Command envelope and domain execution for `pipeline-worker`.

The worker does real work today: `VALIDATE_CATALOG` runs the canonical catalog
validator over the configured roots and `VALIDATE_DATASET_MANIFEST` runs the
canonical manifest contract validator.  Neither is a placeholder.

`PUBLISH_DATASET` is the one command whose domain work belongs to a later stage
(DP4).  It is not omitted and it does not return an empty success: it is routed
through the typed :class:`DatasetPublicationPort`, and the default adapter
raises :class:`PortNotConfiguredError`.  Sending that command to an unwired
worker fails loudly and the message is retried, never silently dropped.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from apps.common.errors import MalformedEventError, PortNotConfiguredError, UnknownCommandError
from apps.common.events import (
    reject_unknown_fields,
    require_identifier,
    require_mapping,
    require_sequence,
    require_string,
)
from apps.pipeline_worker.realtime import (
    EngineRealtimeIngestPort,
    RealtimeIngestPort,
    UnconfiguredRealtimeIngestPort,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from apps.pipeline_worker.config import WorkerConfig

#: Top-level keys a command message may carry.  Anything else is malformed.
COMMAND_ENVELOPE_FIELDS: tuple[str, ...] = ("command", "command_id", "payload", "issued_at")

#: Commands this worker accepts.  An unlisted command is rejected, not ignored.
SUPPORTED_COMMANDS: tuple[str, ...] = (
    "VALIDATE_CATALOG",
    "VALIDATE_DATASET_MANIFEST",
    "PUBLISH_DATASET",
    "INGEST_REALTIME_BARS",
    "APPLY_CORPORATE_ACTION_APPROVAL",
    "MATERIALIZE_FEATURE_OUTPUT",
)


@dataclass(frozen=True)
class Command:
    """A validated worker command."""

    command: str
    command_id: str
    payload: Mapping[str, Any]

    @classmethod
    def parse(cls, body: Any, *, fallback_command_id: str) -> Command:
        document = require_mapping(body, "command message")
        reject_unknown_fields(document, COMMAND_ENVELOPE_FIELDS, "command message")
        name = require_string(document, "command", "command message")
        if name not in SUPPORTED_COMMANDS:
            raise UnknownCommandError(
                f"command {name!r} is not handled by pipeline-worker; "
                f"supported commands are {list(SUPPORTED_COMMANDS)}"
            )
        if "command_id" in document:
            command_id = require_identifier(document, "command_id", "command message")
        else:
            command_id = fallback_command_id
        payload_value = document.get("payload", {})
        payload = require_mapping(payload_value, "command message.payload")
        return cls(command=name, command_id=command_id, payload=payload)


@runtime_checkable
class DatasetPublicationPort(Protocol):
    """Port for publishing a validated dataset manifest (DP4 / D07-D08-D10)."""

    def publish(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Publish one dataset revision and return its manifest summary."""


class UnconfiguredDatasetPublicationPort:
    """Default adapter: refuses, loudly, with the stage that will supply one."""

    def publish(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        raise PortNotConfiguredError(
            "PUBLISH_DATASET requires a DatasetPublicationPort adapter. Partition and "
            "compaction publication over the DB catalog is delivered in DP4 "
            "(D07/D08/D10); no adapter is wired, so this command cannot be completed. "
            "Inject a DatasetPublicationPort into PipelineCommandExecutor to enable it."
        )


@runtime_checkable
class CorporateActionApprovalPort(Protocol):
    def apply(self, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...


class UnconfiguredCorporateActionApprovalPort:
    def apply(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        raise PortNotConfiguredError(
            "APPLY_CORPORATE_ACTION_APPROVAL requires the verified backend-relay "
            "consumer adapter; refusing rather than treating an unwired provider as empty"
        )


@runtime_checkable
class FeatureMaterializationPort(Protocol):
    def materialize(
        self, payload: Mapping[str, Any], *, command_id: str
    ) -> Mapping[str, Any]: ...


class UnconfiguredFeatureMaterializationPort:
    def materialize(
        self, payload: Mapping[str, Any], *, command_id: str
    ) -> Mapping[str, Any]:
        raise PortNotConfiguredError(
            "MATERIALIZE_FEATURE_OUTPUT requires PIPELINE_WORKER_FEATURE_OUTPUT and "
            "PIPELINE_WORKER_DATABASE_URL; refusing an unwired feature publisher"
        )


class PipelineCommandExecutor:
    """Dispatches a validated :class:`Command` to its domain implementation."""

    def __init__(
        self,
        config: WorkerConfig,
        *,
        publication_port: DatasetPublicationPort | None = None,
        realtime_port: RealtimeIngestPort | None = None,
        corporate_action_approval_port: CorporateActionApprovalPort | None = None,
        feature_materialization_port: FeatureMaterializationPort | None = None,
    ) -> None:
        self._config = config
        self._publication_port: DatasetPublicationPort = (
            publication_port or UnconfiguredDatasetPublicationPort()
        )
        if realtime_port is not None:
            self._realtime_port: RealtimeIngestPort = realtime_port
        elif config.realtime is not None:
            self._realtime_port = EngineRealtimeIngestPort(config)
        else:
            self._realtime_port = UnconfiguredRealtimeIngestPort()
        self._corporate_action_approval_port = (
            corporate_action_approval_port
            or self._build_corporate_action_approval_port(config)
        )
        self._feature_materialization_port = (
            feature_materialization_port or self._build_feature_materialization_port(config)
        )

    def prepare(self) -> None:
        """Create the configured roots so the first command has somewhere to read."""

        self._config.catalog_root.mkdir(parents=True, exist_ok=True)
        self._config.object_store_root.mkdir(parents=True, exist_ok=True)
        if self._config.realtime is not None:
            self._config.realtime.staging_root.mkdir(parents=True, exist_ok=True)
        prepare = getattr(self._corporate_action_approval_port, "prepare", None)
        if callable(prepare):
            prepare()
        prepare_feature = getattr(self._feature_materialization_port, "prepare", None)
        if callable(prepare_feature):
            prepare_feature()

    @staticmethod
    def _build_feature_materialization_port(config: WorkerConfig) -> FeatureMaterializationPort:
        settings = config.feature_output
        if settings is None:
            return UnconfiguredFeatureMaterializationPort()
        assert config.database_url is not None
        from apps.pipeline_worker.feature_output import ProductionFeatureMaterializationPort
        from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
        from market_pipeline_lib.storage import S3ObjectStore

        catalog = PostgresCatalog.connect(
            config.database_url,
            artifact_root=config.catalog_root,
            storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
        )
        output_store = S3ObjectStore(
            settings.object_bucket,
            prefix=settings.object_prefix,
            endpoint_url=config.aws_endpoint_url,
        )
        source_store = S3ObjectStore(
            settings.object_bucket,
            endpoint_url=config.aws_endpoint_url,
        )
        from apps.pipeline_worker.feature_output import CanonicalFeatureSourceReader

        return ProductionFeatureMaterializationPort(
            catalog,
            output_store,
            source_reader=CanonicalFeatureSourceReader(catalog, source_store),
            staging_root=settings.staging_root,
        )

    @staticmethod
    def _build_corporate_action_approval_port(config: WorkerConfig) -> CorporateActionApprovalPort:
        settings = config.corporate_action_approval
        if settings is None:
            return UnconfiguredCorporateActionApprovalPort()
        assert config.database_url is not None  # validated by WorkerConfig
        from market_pipeline_lib.catalog import PostgresCatalog, StorageObjectsPolicy
        from market_pipeline_lib.corporate_actions import (
            AdjustedDatasetRegenerator,
            ApprovalEvidenceVerifier,
            BackendRelayApprovalConsumer,
            CorporateActionReviewService,
        )
        from market_pipeline_lib.corporate_actions.object_bars import (
            CatalogObjectBarReader,
            ImmutableObjectBarWriter,
        )
        from market_pipeline_lib.corporate_actions.postgres_evidence import (
            PostgresApprovalAuditDirectory,
            PostgresOperatorDirectory,
        )
        from market_pipeline_lib.storage import S3ObjectStore

        catalog = PostgresCatalog.connect(
            config.database_url,
            artifact_root=config.catalog_root,
            storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
        )
        store = S3ObjectStore(
            settings.object_bucket,
            prefix=settings.object_prefix,
            endpoint_url=config.aws_endpoint_url,
        )
        regenerator = AdjustedDatasetRegenerator(
            catalog=catalog,
            reader=CatalogObjectBarReader(catalog=catalog, object_store=store),
            writer=ImmutableObjectBarWriter(
                object_store=store, staging_root=settings.staging_root
            ),
            require_feed_compatibility=True,
        )
        verifier = ApprovalEvidenceVerifier(
            operators=PostgresOperatorDirectory(catalog.engine),
            audits=PostgresApprovalAuditDirectory(catalog.engine),
            permission_id=settings.permission_id,
            request_schema_version=settings.request_schema_version,
        )
        service = CorporateActionReviewService(
            catalog=catalog,
            regenerator=regenerator,
            raw_manifest_id=None,
            adjusted_feed_id=settings.adjusted_feed_id,
            approval_verifier=verifier,
        )
        return cast(
            CorporateActionApprovalPort,
            BackendRelayApprovalConsumer(service, catalog=catalog),
        )

    def request_stop(self, reason: str) -> None:
        """Forward a shutdown request to adapters that expose cooperative cancellation."""

        for port in (
            self._publication_port,
            self._realtime_port,
            self._corporate_action_approval_port,
            self._feature_materialization_port,
        ):
            stop = getattr(port, "request_stop", None)
            if callable(stop):
                stop(reason)

    def execute(self, command: Command) -> Mapping[str, Any]:
        if command.command == "VALIDATE_CATALOG":
            return self._validate_catalog()
        if command.command == "VALIDATE_DATASET_MANIFEST":
            return self._validate_dataset_manifest(command.payload)
        if command.command == "PUBLISH_DATASET":
            return self._publication_port.publish(command.payload)
        if command.command == "INGEST_REALTIME_BARS":
            return self._ingest_realtime_bars(command.payload)
        if command.command == "APPLY_CORPORATE_ACTION_APPROVAL":
            return self._corporate_action_approval_port.apply(command.payload)
        if command.command == "MATERIALIZE_FEATURE_OUTPUT":
            return self._feature_materialization_port.materialize(
                command.payload, command_id=command.command_id
            )
        raise UnknownCommandError(f"no executor for command {command.command!r}")

    def _ingest_realtime_bars(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        events = require_sequence(payload, "events", "INGEST_REALTIME_BARS payload")
        if not events:
            raise MalformedEventError("INGEST_REALTIME_BARS payload.events must not be empty")
        for index, event in enumerate(events):
            require_mapping(event, f"INGEST_REALTIME_BARS payload.events[{index}]")
        flush = payload.get("flush", True)
        if not isinstance(flush, bool):
            raise MalformedEventError("INGEST_REALTIME_BARS payload.flush must be a boolean")
        return self._realtime_port.ingest(list(events), flush=flush)

    # -- real domain work -------------------------------------------------
    def _validate_catalog(self) -> Mapping[str, Any]:
        # Imported lazily: pyarrow/pandas cost ~1s to import and a worker that
        # only ever receives manifest-validation commands should not pay it.
        from market_pipeline_lib.catalog import LocalCatalog
        from market_pipeline_lib.operations import validate_catalog
        from market_pipeline_lib.storage import LocalObjectStore

        catalog = LocalCatalog(Path(self._config.catalog_root))
        object_store = LocalObjectStore(Path(self._config.object_store_root))
        report = validate_catalog(catalog, object_store, write_report=False)
        return {
            "status": report["status"],
            "manifest_count": report["manifest_count"],
            "object_count": report["object_count"],
            "error_count": report["error_count"],
            "warning_count": report["warning_count"],
            # Bounded: a validation run over a broken catalog can produce
            # thousands of errors and the full list belongs in the report file.
            "errors": list(report["errors"])[:20],
        }

    def _validate_dataset_manifest(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        from market_pipeline_lib.compatibility import (
            ContractValidationError,
            validate_dataset_manifest,
        )

        document = require_mapping(
            payload.get("manifest"), "VALIDATE_DATASET_MANIFEST payload.manifest"
        )
        try:
            validate_dataset_manifest(document)
        except ContractValidationError as error:
            # A contract violation is a real, reportable outcome — not a crash
            # and not a success.
            return {
                "status": "REJECTED",
                "manifest_id": document.get("manifest_id"),
                "violation": str(error),
            }
        return {
            "status": "ACCEPTED",
            "manifest_id": document.get("manifest_id"),
            "dataset_id": document.get("dataset_id"),
            "revision": document.get("revision"),
        }
