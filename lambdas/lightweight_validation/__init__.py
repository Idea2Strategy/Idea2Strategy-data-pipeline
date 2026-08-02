"""`lightweight-validation` Lambda: cheap contract validation of published pipeline documents."""

from lambdas.lightweight_validation.handler import LightweightValidationHandler, handler

__all__ = ["LightweightValidationHandler", "handler"]
