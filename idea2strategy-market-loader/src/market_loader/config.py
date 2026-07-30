from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from market_loader.errors import ConfigurationError, RightsApprovalError


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProjectConfig(StrictModel):
    environment: str
    processing_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)


class AlpacaConfig(StrictModel):
    base_url: str
    feed: str = "sip"
    request_timeframe: str = "30Min"
    chunk_days: int = Field(default=180, gt=0, le=180)
    symbols_per_request: int = Field(default=50, gt=0)
    page_limit: int = Field(default=10_000, gt=0, le=10_000)
    connect_timeout_seconds: float = Field(default=10, gt=0)
    read_timeout_seconds: float = Field(default=60, gt=0)
    max_attempts: int = Field(default=5, ge=1, le=10)


class DataConfig(StrictModel):
    session_calendar: str = "XNYS"
    adjustments: list[str]
    output_resolutions: list[str]
    shard_count: int = Field(default=8, gt=0, le=99)
    parquet_compression: str = "zstd"
    parquet_compression_level: int = Field(default=3, ge=1, le=22)
    parquet_row_group_size: int = Field(default=131_072, gt=0)

    @field_validator("adjustments")
    @classmethod
    def valid_adjustments(cls, value: list[str]) -> list[str]:
        if not value or not set(value) <= {"raw", "all"}:
            raise ValueError("adjustments must contain only raw and all")
        return value

    @field_validator("output_resolutions")
    @classmethod
    def valid_resolutions(cls, value: list[str]) -> list[str]:
        if not value or not set(value) <= {"30m", "1h", "4h", "1d"}:
            raise ValueError("unsupported output resolution")
        return value


class StorageConfig(StrictModel):
    prefix: str = "historical"
    staging_directory: Path = Path("./.staging")
    sse_algorithm: str = "AES256"


class QualityConfig(StrictModel):
    fail_on_duplicate: bool = True
    fail_on_invalid_ohlc: bool = True
    fail_on_out_of_session: bool = True
    fail_on_negative_activity: bool = True
    warn_on_missing_expected_bar: bool = True


class AppConfig(StrictModel):
    project: ProjectConfig
    alpaca: AlpacaConfig
    data: DataConfig
    storage: StorageConfig
    quality: QualityConfig


class EnvironmentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=True
    )

    ALPACA_API_KEY: str = ""
    ALPACA_API_SECRET: str = ""
    AWS_PROFILE: str = ""
    AWS_REGION: str = ""
    MARKET_DATA_BUCKET: str = ""
    PGHOST: str = ""
    PGHOSTADDR: str = ""
    PGPORT: int = 15432
    PGDATABASE: str = ""
    PGUSER: str = ""
    PGPASSWORD: str = ""
    PGSSLMODE: str = ""
    PGSSLROOTCERT: str = ""
    PROVIDER_RIGHTS_VERSION: str = ""
    PROVIDER_RIGHTS_APPROVED: bool = False

    def require_rights_approval(self) -> None:
        if not self.PROVIDER_RIGHTS_APPROVED or not self.PROVIDER_RIGHTS_VERSION.strip():
            raise RightsApprovalError(
                "write blocked: provider rights approval and version are required"
            )


def load_config(path: Path) -> AppConfig:
    try:
        with path.open("r", encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)
        if not isinstance(raw, dict):
            raise ConfigurationError("configuration root must be a mapping")
        config = AppConfig.model_validate(raw)
    except ConfigurationError:
        raise
    except Exception as exc:
        raise ConfigurationError(f"invalid configuration: {exc}") from exc
    config.storage.staging_directory = (path.parent / config.storage.staging_directory).resolve()
    return config
