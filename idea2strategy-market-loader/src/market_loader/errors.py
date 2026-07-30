from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    CONFIG_INVALID = "CONFIG_INVALID"
    INPUT_INVALID = "INPUT_INVALID"
    RIGHTS_NOT_APPROVED = "RIGHTS_NOT_APPROVED"
    ALPACA_TRANSIENT = "ALPACA_TRANSIENT"
    ALPACA_PERMANENT = "ALPACA_PERMANENT"
    CONTRACT_VIOLATION = "CONTRACT_VIOLATION"
    QUALITY_FAILED = "QUALITY_FAILED"
    S3_TRANSIENT = "S3_TRANSIENT"
    S3_CONFLICT = "S3_CONFLICT"
    S3_INTEGRITY = "S3_INTEGRITY"
    DATABASE_TRANSIENT = "DATABASE_TRANSIENT"
    DATABASE_PERMANENT = "DATABASE_PERMANENT"


class LoaderError(Exception):
    code = ErrorCode.CONTRACT_VIOLATION
    retryable = False


class ConfigurationError(LoaderError):
    code = ErrorCode.CONFIG_INVALID


class InputError(LoaderError):
    code = ErrorCode.INPUT_INVALID


class RightsApprovalError(LoaderError):
    code = ErrorCode.RIGHTS_NOT_APPROVED


class TransientAlpacaError(LoaderError):
    code = ErrorCode.ALPACA_TRANSIENT
    retryable = True


class PermanentAlpacaError(LoaderError):
    code = ErrorCode.ALPACA_PERMANENT


class QualityError(LoaderError):
    code = ErrorCode.QUALITY_FAILED


class S3ConflictError(LoaderError):
    code = ErrorCode.S3_CONFLICT


class S3IntegrityError(LoaderError):
    code = ErrorCode.S3_INTEGRITY
