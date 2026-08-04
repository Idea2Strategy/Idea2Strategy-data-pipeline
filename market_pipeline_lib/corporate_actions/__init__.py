"""D15 -- corporate-action adjustment application and dataset regeneration.

The review gate and the data effect of passing it:

* :mod:`.decisions` -- the vocabulary of an administrator's decision.
* :mod:`.adjustment` -- split and dividend factors, and back-adjustment of a raw
  bar series.  Prices quantize to eight places, ROUND_HALF_EVEN (spec 2.3).
* :mod:`.regeneration` -- publishes the rebuilt dataset as a new immutable
  manifest revision with lineage to the raw source and to the revision it
  supersedes.  Never an in-place mutation.
* :mod:`.service` -- verifies backend relay evidence, applies an approval or
  explicit withdrawal, and regenerates exactly once.
"""

from __future__ import annotations

from .adjustment import (
    PRICE_EXPONENT,
    AdjustmentFactor,
    ApprovedAction,
    Bar,
    adjusted_bars,
    cash_dividend_factor,
    split_factor,
)
from .consumer import BackendRelayApprovalConsumer
from .decisions import (
    AdminDecision,
    ApprovalRefusedError,
    ApprovalResult,
    ConflictingDecisionError,
    DecisionType,
    ReviewState,
    UnknownCandidateError,
)
from .regeneration import (
    ADJUSTED_LAYER,
    AdjustedBarWriter,
    AdjustedDatasetRegenerator,
    RawBarReader,
    RegenerationCatalog,
    RegenerationResult,
    WrittenDataset,
)
from .service import (
    ApprovalEvidenceVerifier,
    CorporateActionReviewService,
    DecisionOutcome,
    RegeneratorNotConfiguredError,
)

__all__ = [
    "ADJUSTED_LAYER",
    "PRICE_EXPONENT",
    "AdjustedBarWriter",
    "AdjustedDatasetRegenerator",
    "AdjustmentFactor",
    "AdminDecision",
    "ApprovalEvidenceVerifier",
    "ApprovalRefusedError",
    "ApprovalResult",
    "ApprovedAction",
    "Bar",
    "BackendRelayApprovalConsumer",
    "ConflictingDecisionError",
    "CorporateActionReviewService",
    "DecisionOutcome",
    "DecisionType",
    "RawBarReader",
    "RegenerationCatalog",
    "RegenerationResult",
    "RegeneratorNotConfiguredError",
    "ReviewState",
    "UnknownCandidateError",
    "WrittenDataset",
    "adjusted_bars",
    "cash_dividend_factor",
    "split_factor",
]
