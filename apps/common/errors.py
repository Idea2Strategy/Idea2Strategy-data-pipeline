"""Typed failures shared by every pipeline execution app.

Every one of these is a *loud* failure.  Nothing in this bundle may answer a
malformed input, an absent configuration value or an unwired port with an empty
success response.
"""

from __future__ import annotations

from collections.abc import Iterable


class PipelineAppError(Exception):
    """Base class for execution-app failures."""

    #: Stable machine-readable code emitted in structured results.
    code = "PIPELINE_APP_ERROR"


class ConfigurationError(PipelineAppError):
    """Required configuration is absent or unusable.

    Raised during boot.  The app must not start with a silent default.
    """

    code = "CONFIGURATION_ERROR"

    @classmethod
    def missing(cls, names: Iterable[str], *, hint: str = "") -> ConfigurationError:
        missing = sorted(set(names))
        message = "missing required environment variable(s): " + ", ".join(missing)
        if hint:
            message = f"{message} ({hint})"
        return cls(message)


class PortNotConfiguredError(PipelineAppError):
    """An explicit port exists but its adapter is not implemented or not wired.

    Used where the domain work is scheduled for a later stage.  The port is
    typed and named; selecting it fails immediately instead of succeeding
    vacuously.
    """

    code = "PORT_NOT_CONFIGURED"


class MalformedEventError(PipelineAppError):
    """An incoming event or message does not match its declared shape."""

    code = "MALFORMED_EVENT"


class UnknownCommandError(MalformedEventError):
    """A structurally valid message names a command this app does not handle."""

    code = "UNKNOWN_COMMAND"
