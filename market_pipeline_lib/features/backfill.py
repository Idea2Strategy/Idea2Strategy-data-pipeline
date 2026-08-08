"""Plans the ``MATERIALIZE_FEATURE_OUTPUT`` commands a historical backfill needs.

The worker already knows how to materialize *one* feature series for one instrument over
one period. What was missing is the step before that: deciding which commands to send so
every released strategy can actually read its indicator over the whole history.

This module only reads the catalog and returns a plan. It writes nothing, computes no
feature values, and needs no object store, so the decision of what to run is reviewable
before anything is run.

Why one command per instrument and definition
---------------------------------------------
A calculator returns "every value the series supports" over the bars it is given, and
``MaterializationRequest`` requires every bar to fall inside ``[period_start,
period_end)``. So the first ``required_history_points - 1`` bars of a period yield no
value: splitting a span into two periods does not split the series, it *punches a hole*
of that many bars at the seam. A 14-period RSI split by year loses the first fourteen
bars of every January.

Overlapping the periods to cover the seam does not help either, because the overlap
would publish the same ``bar_start_at`` twice and a pinned series must be strictly
increasing and unique in it.

So the plan issues exactly one command per instrument and definition, spanning that
instrument's whole available coverage. When the authoritative source-object set for a
span exceeds :data:`MAX_SOURCE_OBJECTS_PER_COMMAND`, the span cannot be requested at
once; the plan then splits it and records a :class:`BackfillWarning` naming the seams and
how many bars each one costs. A split is reported, never silent -- a series with
undeclared holes in it is worse than a backfill that refuses to start.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from market_pipeline_lib.features.definitions import FeatureDefinition
from market_pipeline_lib.features.tables import FeatureCatalog

__all__ = [
    "MAX_SOURCE_OBJECTS_PER_COMMAND",
    "BackfillCommand",
    "BackfillPlan",
    "BackfillWarning",
    "plan_feature_backfill",
]


MAX_SOURCE_OBJECTS_PER_COMMAND = 512
"""``MAX_FEATURE_SOURCE_OBJECTS`` in ``apps.pipeline_worker.feature_output``.

Restated rather than imported: this library must not depend on the worker application,
and a mismatch is caught by ``tests/test_feature_backfill.py`` rather than by a rejected
command in production.
"""

_MARKET_BARS = "MARKET_BARS"
_AVAILABLE = "AVAILABLE"


@dataclass(frozen=True)
class BackfillCommand:
    """One ``MATERIALIZE_FEATURE_OUTPUT`` command, ready to send."""

    definition_hash: str
    instrument_id: str
    resolution: str
    period_start: datetime
    period_end: datetime
    source_dataset_object_ids: tuple[str, ...]

    @property
    def command_id(self) -> str:
        """A stable id, so re-planning and re-sending is an idempotent overwrite.

        The worker derives the pipeline run and the materialization row from this, and
        rejects a reused id whose inputs changed. Deriving it from the request's own
        identity is therefore what makes an interrupted backfill safe to resume.
        """
        return (
            f"feature-backfill:{self.definition_hash}:{self.instrument_id}"
            f":{_stamp(self.period_start)}:{_stamp(self.period_end)}"
        )

    def payload(self) -> dict[str, Any]:
        """The command payload, with exactly the fields the worker accepts."""
        return {
            "definition_hash": self.definition_hash,
            "instrument_id": self.instrument_id,
            "period_start": _stamp(self.period_start),
            "period_end": _stamp(self.period_end),
            "source_dataset_object_ids": list(self.source_dataset_object_ids),
        }

    def message(self) -> dict[str, Any]:
        """The full worker message: envelope plus payload."""
        return {
            "command": "MATERIALIZE_FEATURE_OUTPUT",
            "command_id": self.command_id,
            "payload": self.payload(),
        }


@dataclass(frozen=True)
class BackfillWarning:
    """Something the operator must know before running the plan."""

    code: str
    instrument_id: str
    resolution: str
    detail: str


@dataclass(frozen=True)
class BackfillPlan:
    """Every command a backfill needs, and everything wrong with the inputs."""

    commands: tuple[BackfillCommand, ...] = ()
    warnings: tuple[BackfillWarning, ...] = ()
    #: (resolution, instrument) pairs that already hold a SUCCEEDED materialization
    #: covering the whole span, and so were left alone.
    satisfied: tuple[tuple[str, str], ...] = field(default=())

    @property
    def has_holes(self) -> bool:
        """Whether running this plan would leave a series with undeclared gaps."""
        return any(item.code == "PERIOD_SPLIT" for item in self.warnings)

    def messages(self) -> list[dict[str, Any]]:
        return [command.message() for command in self.commands]


def plan_feature_backfill(
    catalog: FeatureCatalog,
    definitions: Iterable[FeatureDefinition],
    *,
    instrument_ids: Iterable[str] | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    include_satisfied: bool = False,
) -> BackfillPlan:
    """The commands needed so every definition has a series for every instrument.

    ``period_start`` and ``period_end`` clamp the span; without them each instrument gets
    its own full available coverage, which is what a first backfill wants. Instruments
    with no adjusted bars at a resolution are reported rather than skipped silently --
    "the strategy cannot be backtested" and "nothing to do" are not the same answer.
    """
    wanted_instruments = None if instrument_ids is None else {str(item) for item in instrument_ids}
    commands: list[BackfillCommand] = []
    warnings: list[BackfillWarning] = []
    satisfied: list[tuple[str, str]] = []

    for definition in definitions:
        manifests = _available_bar_manifests(catalog, definition.resolution)
        instruments = sorted(
            {str(row["instrument_id"]) for row in manifests}
            if wanted_instruments is None
            else wanted_instruments
        )
        if not instruments:
            warnings.append(BackfillWarning(
                code="NO_INSTRUMENTS",
                instrument_id="",
                resolution=definition.resolution,
                detail=(
                    "no AVAILABLE adjusted-bar manifest exists at this resolution, so no "
                    "strategy on this clock can be backtested"
                ),
            ))
            continue

        for instrument_id in instruments:
            owned = [row for row in manifests if str(row["instrument_id"]) == instrument_id]
            if not owned:
                warnings.append(BackfillWarning(
                    code="NO_COVERAGE",
                    instrument_id=instrument_id,
                    resolution=definition.resolution,
                    detail="no AVAILABLE adjusted-bar manifest covers this instrument",
                ))
                continue

            start = min(_time(row["period_start"]) for row in owned)
            end = max(_time(row["period_end"]) for row in owned)
            if period_start is not None:
                start = max(start, _time(period_start))
            if period_end is not None:
                end = min(end, _time(period_end))
            if end <= start:
                warnings.append(BackfillWarning(
                    code="EMPTY_SPAN",
                    instrument_id=instrument_id,
                    resolution=definition.resolution,
                    detail=(
                        f"the requested span does not overlap this instrument's coverage "
                        f"[{_stamp(min(_time(row['period_start']) for row in owned))}, "
                        f"{_stamp(max(_time(row['period_end']) for row in owned))})"
                    ),
                ))
                continue

            objects = _authoritative_objects(catalog, owned, start, end)
            if not objects:
                warnings.append(BackfillWarning(
                    code="NO_SOURCE_OBJECTS",
                    instrument_id=instrument_id,
                    resolution=definition.resolution,
                    detail="the manifests covering this span hold no MARKET_BARS objects",
                ))
                continue

            gap = _first_gap(objects, start, end)
            if gap is not None:
                warnings.append(BackfillWarning(
                    code="SOURCE_GAP",
                    instrument_id=instrument_id,
                    resolution=definition.resolution,
                    detail=(
                        f"the source objects leave a gap at {_stamp(gap)}; the worker would "
                        "reject this period, so the bars must be published first"
                    ),
                ))
                continue

            if _is_satisfied(catalog, definition, instrument_id, start, end):
                satisfied.append((definition.resolution, instrument_id))
                if not include_satisfied:
                    continue

            for index, chunk in enumerate(_chunks(objects, start, end)):
                chunk_start, chunk_end, chunk_objects = chunk
                if index > 0:
                    warnings.append(BackfillWarning(
                        code="PERIOD_SPLIT",
                        instrument_id=instrument_id,
                        resolution=definition.resolution,
                        detail=(
                            f"the span needs more than {MAX_SOURCE_OBJECTS_PER_COMMAND} source "
                            f"objects, so it is split at {_stamp(chunk_start)}; the first "
                            f"{definition.required_history_points - 1} bars after that seam "
                            "will have no value"
                        ),
                    ))
                commands.append(BackfillCommand(
                    definition_hash=definition.definition_hash,
                    instrument_id=instrument_id,
                    resolution=definition.resolution,
                    period_start=chunk_start,
                    period_end=chunk_end,
                    source_dataset_object_ids=chunk_objects,
                ))

    return BackfillPlan(
        commands=tuple(commands),
        warnings=tuple(warnings),
        satisfied=tuple(satisfied),
    )


def _available_bar_manifests(catalog: FeatureCatalog, resolution: str) -> list[dict[str, Any]]:
    """The current AVAILABLE bar manifests at one resolution.

    Only the highest revision of each exact identity counts: the worker rejects a source
    manifest that is not the current AVAILABLE revision, so planning against a superseded
    one would produce a command that can only fail.
    """
    rows = [
        row
        for row in catalog.records("market_data.dataset_manifests", where={"resolution": resolution})
        if row.get("status") == _AVAILABLE and row.get("instrument_id") is not None
    ]
    current: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        identity = (
            str(row["instrument_id"]),
            row.get("data_layer"),
            row.get("resolution"),
            _time(row["period_start"]),
            _time(row["period_end"]),
        )
        held = current.get(identity)
        if held is None or int(row["revision_number"]) > int(held["revision_number"]):
            current[identity] = row
    return list(current.values())


def _authoritative_objects(
    catalog: FeatureCatalog,
    manifests: Sequence[Mapping[str, Any]],
    start: datetime,
    end: datetime,
) -> tuple[tuple[datetime, datetime, str], ...]:
    """Exactly the MARKET_BARS objects overlapping the span, oldest first.

    The worker requires the submitted ids to *equal* this set -- not to contain it -- so
    this selection is the plan's real work.
    """
    found: dict[str, tuple[datetime, datetime, str]] = {}
    for manifest in manifests:
        for row in catalog.records(
            "market_data.dataset_objects", where={"dataset_manifest_id": str(manifest["id"])}
        ):
            if row.get("object_kind") != _MARKET_BARS:
                continue
            row_start = _time(row["period_start"])
            row_end = _time(row["period_end"])
            if row_start < end and row_end > start:
                found[str(row["id"])] = (row_start, row_end, str(row["id"]))
    return tuple(sorted(found.values()))


def _first_gap(
    objects: Sequence[tuple[datetime, datetime, str]], start: datetime, end: datetime
) -> datetime | None:
    """The first instant the objects fail to cover, or ``None`` when they cover it all."""
    cursor = start
    for row_start, row_end, _ in objects:
        if row_start > cursor:
            return cursor
        cursor = max(cursor, row_end)
    return None if cursor >= end else cursor


def _chunks(
    objects: Sequence[tuple[datetime, datetime, str]], start: datetime, end: datetime
) -> list[tuple[datetime, datetime, tuple[str, ...]]]:
    """One chunk when the object set fits, otherwise the fewest that do."""
    if len(objects) <= MAX_SOURCE_OBJECTS_PER_COMMAND:
        return [(start, end, tuple(row[2] for row in objects))]

    chunks: list[tuple[datetime, datetime, tuple[str, ...]]] = []
    cursor = start
    batch: list[tuple[datetime, datetime, str]] = []
    for row in objects:
        batch.append(row)
        if len(batch) == MAX_SOURCE_OBJECTS_PER_COMMAND:
            boundary = max(item[1] for item in batch)
            chunks.append((cursor, boundary, tuple(item[2] for item in batch)))
            cursor = boundary
            batch = []
    if batch:
        chunks.append((cursor, end, tuple(item[2] for item in batch)))
    return chunks


def _is_satisfied(
    catalog: FeatureCatalog,
    definition: FeatureDefinition,
    instrument_id: str,
    start: datetime,
    end: datetime,
) -> bool:
    """Whether a SUCCEEDED materialization already covers the whole span."""
    rows = catalog.records(
        "market_data.feature_materializations",
        where={"feature_definition_id": definition.id, "instrument_id": instrument_id},
    )
    return any(
        row.get("status") == "SUCCEEDED"
        and _time(row["period_start"]) <= start
        and _time(row["period_end"]) >= end
        for row in rows
    )


def _time(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(f"naive timestamp in the catalog: {value!r}")
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    raise TypeError(f"expected a timestamp, got {type(value).__name__}")


def _stamp(value: datetime) -> str:
    return _time(value).isoformat().replace("+00:00", "Z")
