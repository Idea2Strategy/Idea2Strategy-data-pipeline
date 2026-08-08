"""What a historical feature backfill must plan, and what it must refuse to plan quietly."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from apps.pipeline_worker.backfill_features import _catalog, _instant, _summary, main, run
from market_pipeline_lib.catalog import StorageObjectsPolicy
from market_pipeline_lib.features.backfill import (
    MAX_SOURCE_OBJECTS_PER_COMMAND,
    plan_feature_backfill,
)
from market_pipeline_lib.features.definitions import (
    FeatureDefinition,
    production_rsi_14_definition,
)

INSTRUMENT = "00000000-0000-4000-8000-000000000301"
OTHER_INSTRUMENT = "00000000-0000-4000-8000-000000000302"
CATALOG_VERSION = "0f4a0000-0000-4000-8000-000000000001"

def _definition(resolution: str) -> FeatureDefinition:
    """The real production RSI_14 definition at one of the four strategy clocks.

    Taken from the library rather than restated, so the definition hash is the one the
    pipeline actually publishes.
    """
    return production_rsi_14_definition(resolution)


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


class FakeCatalog:
    """Only the two reads the planner performs, so the plan is tested and not the catalog."""

    def __init__(self) -> None:
        self.manifests: list[dict[str, Any]] = []
        self.objects: list[dict[str, Any]] = []
        self.materializations: list[dict[str, Any]] = []

    def add_manifest(
        self,
        manifest_id: str,
        *,
        resolution: str,
        start: str,
        end: str,
        instrument_id: str = INSTRUMENT,
        status: str = "AVAILABLE",
        revision_number: int = 1,
    ) -> str:
        self.manifests.append({
            "id": manifest_id,
            "instrument_id": instrument_id,
            "resolution": resolution,
            "data_layer": "ADJUSTED",
            "status": status,
            "revision_number": revision_number,
            "period_start": _utc(start),
            "period_end": _utc(end),
        })
        return manifest_id

    def add_object(
        self,
        object_id: str,
        *,
        manifest_id: str,
        start: str,
        end: str,
        object_kind: str = "MARKET_BARS",
    ) -> str:
        self.objects.append({
            "id": object_id,
            "dataset_manifest_id": manifest_id,
            "object_kind": object_kind,
            "period_start": _utc(start),
            "period_end": _utc(end),
        })
        return object_id

    def records(
        self, table: str, *, where: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        source = {
            "market_data.dataset_manifests": self.manifests,
            "market_data.dataset_objects": self.objects,
            "market_data.feature_materializations": self.materializations,
        }[table]
        if not where:
            return [dict(row) for row in source]
        return [
            dict(row)
            for row in source
            if all(str(row.get(key)) == str(value) for key, value in where.items())
        ]


def _one_month(resolution: str = "30m", instrument_id: str = INSTRUMENT) -> FakeCatalog:
    catalog = FakeCatalog()
    catalog.add_manifest(
        "manifest-1", resolution=resolution, instrument_id=instrument_id,
        start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
    )
    catalog.add_object(
        "object-1", manifest_id="manifest-1",
        start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
    )
    return catalog


class TestOneCommandPerInstrumentAndDefinition:
    def test_a_covered_instrument_gets_exactly_one_command(self) -> None:
        plan = plan_feature_backfill(_one_month(), [_definition("30m")])

        assert len(plan.commands) == 1
        assert plan.warnings == ()

    def test_the_command_spans_the_whole_available_coverage(self) -> None:
        catalog = _one_month()
        catalog.add_manifest(
            "manifest-2", resolution="30m",
            start="2016-02-01T00:00:00+00:00", end="2016-03-01T00:00:00+00:00",
        )
        catalog.add_object(
            "object-2", manifest_id="manifest-2",
            start="2016-02-01T00:00:00+00:00", end="2016-03-01T00:00:00+00:00",
        )

        command = plan_feature_backfill(catalog, [_definition("30m")]).commands[0]

        assert command.period_start == _utc("2016-01-01T00:00:00+00:00")
        assert command.period_end == _utc("2016-03-01T00:00:00+00:00")

    def test_the_command_carries_every_overlapping_source_object(self) -> None:
        """The worker requires the ids to *equal* the authoritative set, not contain it."""
        catalog = _one_month()
        catalog.add_object(
            "object-1b", manifest_id="manifest-1",
            start="2016-01-15T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )

        command = plan_feature_backfill(catalog, [_definition("30m")]).commands[0]

        assert set(command.source_dataset_object_ids) == {"object-1", "object-1b"}

    def test_each_resolution_is_planned_independently(self) -> None:
        catalog = FakeCatalog()
        for resolution in ("30m", "1h", "4h", "1d"):
            manifest = catalog.add_manifest(
                f"manifest-{resolution}", resolution=resolution,
                start="2016-01-01T00:00:00+00:00", end="2026-01-01T00:00:00+00:00",
            )
            catalog.add_object(
                f"object-{resolution}", manifest_id=manifest,
                start="2016-01-01T00:00:00+00:00", end="2026-01-01T00:00:00+00:00",
            )

        plan = plan_feature_backfill(
            catalog, [_definition(item) for item in ("30m", "1h", "4h", "1d")]
        )

        assert [command.resolution for command in plan.commands] == ["30m", "1h", "4h", "1d"]
        assert plan.warnings == ()

    def test_every_instrument_with_coverage_is_planned(self) -> None:
        catalog = _one_month()
        catalog.add_manifest(
            "manifest-other", resolution="30m", instrument_id=OTHER_INSTRUMENT,
            start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )
        catalog.add_object(
            "object-other", manifest_id="manifest-other",
            start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert {command.instrument_id for command in plan.commands} == {
            INSTRUMENT, OTHER_INSTRUMENT
        }

    def test_the_span_can_be_clamped(self) -> None:
        catalog = _one_month()

        command = plan_feature_backfill(
            catalog,
            [_definition("30m")],
            period_start=_utc("2016-01-10T00:00:00+00:00"),
            period_end=_utc("2016-01-20T00:00:00+00:00"),
        ).commands[0]

        assert command.period_start == _utc("2016-01-10T00:00:00+00:00")
        assert command.period_end == _utc("2016-01-20T00:00:00+00:00")


class TestTheCommandIsResumable:
    def test_the_command_id_is_derived_from_the_request(self) -> None:
        """An interrupted backfill must be safe to re-send, so the id cannot be random."""
        first = plan_feature_backfill(_one_month(), [_definition("30m")]).commands[0]
        second = plan_feature_backfill(_one_month(), [_definition("30m")]).commands[0]

        assert first.command_id == second.command_id

    def test_a_different_period_is_a_different_command(self) -> None:
        whole = plan_feature_backfill(_one_month(), [_definition("30m")]).commands[0]
        clamped = plan_feature_backfill(
            _one_month(), [_definition("30m")], period_end=_utc("2016-01-20T00:00:00+00:00")
        ).commands[0]

        assert whole.command_id != clamped.command_id

    def test_the_message_names_the_worker_command(self) -> None:
        message = plan_feature_backfill(_one_month(), [_definition("30m")]).commands[0].message()

        assert message["command"] == "MATERIALIZE_FEATURE_OUTPUT"
        assert set(message["payload"]) == {
            "definition_hash", "instrument_id", "period_start",
            "period_end", "source_dataset_object_ids",
        }


class TestAlreadyMaterializedWorkIsNotRepeated:
    def test_a_succeeded_materialization_covering_the_span_is_skipped(self) -> None:
        catalog = _one_month()
        catalog.materializations.append({
            "feature_definition_id": _definition("30m").id,
            "instrument_id": INSTRUMENT,
            "status": "SUCCEEDED",
            "period_start": _utc("2016-01-01T00:00:00+00:00"),
            "period_end": _utc("2016-02-01T00:00:00+00:00"),
        })

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert plan.commands == ()
        assert plan.satisfied == (("30m", INSTRUMENT),)

    def test_a_failed_materialization_is_planned_again(self) -> None:
        catalog = _one_month()
        catalog.materializations.append({
            "feature_definition_id": _definition("30m").id,
            "instrument_id": INSTRUMENT,
            "status": "FAILED",
            "period_start": _utc("2016-01-01T00:00:00+00:00"),
            "period_end": _utc("2016-02-01T00:00:00+00:00"),
        })

        assert len(plan_feature_backfill(catalog, [_definition("30m")]).commands) == 1

    def test_a_materialization_covering_only_part_of_the_span_is_retried(self) -> None:
        catalog = _one_month()
        catalog.materializations.append({
            "feature_definition_id": _definition("30m").id,
            "instrument_id": INSTRUMENT,
            "status": "SUCCEEDED",
            "period_start": _utc("2016-01-01T00:00:00+00:00"),
            "period_end": _utc("2016-01-15T00:00:00+00:00"),
        })

        assert len(plan_feature_backfill(catalog, [_definition("30m")]).commands) == 1


class TestNothingIsSkippedSilently:
    def test_a_resolution_with_no_bars_at_all_is_reported(self) -> None:
        plan = plan_feature_backfill(FakeCatalog(), [_definition("4h")])

        assert plan.commands == ()
        assert [item.code for item in plan.warnings] == ["NO_INSTRUMENTS"]

    def test_a_named_instrument_without_coverage_is_reported(self) -> None:
        plan = plan_feature_backfill(
            _one_month(), [_definition("30m")], instrument_ids=[OTHER_INSTRUMENT]
        )

        assert plan.commands == ()
        assert [item.code for item in plan.warnings] == ["NO_COVERAGE"]

    def test_a_gap_between_source_objects_is_reported_rather_than_requested(self) -> None:
        """The worker rejects a period its objects do not cover, so planning one is a bug."""
        catalog = FakeCatalog()
        catalog.add_manifest(
            "manifest-1", resolution="30m",
            start="2016-01-01T00:00:00+00:00", end="2016-03-01T00:00:00+00:00",
        )
        catalog.add_object(
            "object-1", manifest_id="manifest-1",
            start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )
        # Nothing covers February, and the manifest claims through March.

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert plan.commands == ()
        assert [item.code for item in plan.warnings] == ["SOURCE_GAP"]

    def test_a_clamp_outside_the_coverage_is_reported(self) -> None:
        plan = plan_feature_backfill(
            _one_month(),
            [_definition("30m")],
            period_start=_utc("2020-01-01T00:00:00+00:00"),
        )

        assert plan.commands == ()
        assert [item.code for item in plan.warnings] == ["EMPTY_SPAN"]

    def test_a_manifest_that_is_not_available_is_not_planned(self) -> None:
        catalog = FakeCatalog()
        catalog.add_manifest(
            "manifest-1", resolution="30m", status="BUILDING",
            start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert plan.commands == ()
        assert [item.code for item in plan.warnings] == ["NO_INSTRUMENTS"]

    def test_a_superseded_manifest_revision_is_not_planned(self) -> None:
        """The worker rejects a source that is not the current AVAILABLE revision."""
        catalog = FakeCatalog()
        for revision, manifest_id in ((1, "manifest-old"), (2, "manifest-new")):
            catalog.add_manifest(
                manifest_id, resolution="30m", revision_number=revision,
                start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
            )
            catalog.add_object(
                f"object-r{revision}", manifest_id=manifest_id,
                start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
            )

        command = plan_feature_backfill(catalog, [_definition("30m")]).commands[0]

        assert command.source_dataset_object_ids == ("object-r2",)

    def test_a_non_bar_object_is_not_a_feature_source(self) -> None:
        catalog = _one_month()
        catalog.add_object(
            "object-actions", manifest_id="manifest-1", object_kind="CORPORATE_ACTIONS",
            start="2016-01-01T00:00:00+00:00", end="2016-02-01T00:00:00+00:00",
        )

        command = plan_feature_backfill(catalog, [_definition("30m")]).commands[0]

        assert command.source_dataset_object_ids == ("object-1",)


class TestASplitSpanDeclaresItsHoles:
    """Splitting a period punches a warm-up hole, so it may never happen quietly."""

    @staticmethod
    def _many_objects(count: int) -> FakeCatalog:
        catalog = FakeCatalog()
        origin = _utc("2016-01-01T00:00:00+00:00")
        catalog.add_manifest(
            "manifest-1", resolution="30m",
            start="2016-01-01T00:00:00+00:00",
            end=(origin + timedelta(days=count)).isoformat(),
        )
        for index in range(count):
            catalog.add_object(
                f"object-{index:04d}", manifest_id="manifest-1",
                start=(origin + timedelta(days=index)).isoformat(),
                end=(origin + timedelta(days=index + 1)).isoformat(),
            )
        return catalog

    def test_an_object_set_that_fits_is_one_command_with_no_warning(self) -> None:
        catalog = self._many_objects(MAX_SOURCE_OBJECTS_PER_COMMAND)

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert len(plan.commands) == 1
        assert plan.has_holes is False

    def test_an_object_set_that_does_not_fit_is_split_and_reported(self) -> None:
        catalog = self._many_objects(MAX_SOURCE_OBJECTS_PER_COMMAND + 1)

        plan = plan_feature_backfill(catalog, [_definition("30m")])

        assert len(plan.commands) == 2
        assert plan.has_holes is True
        assert [item.code for item in plan.warnings] == ["PERIOD_SPLIT"]

    def test_the_split_warning_names_how_many_bars_are_lost(self) -> None:
        catalog = self._many_objects(MAX_SOURCE_OBJECTS_PER_COMMAND + 1)

        warning = plan_feature_backfill(catalog, [_definition("30m")]).warnings[0]

        # required_history_points is 15, so the first 14 bars after a seam have no value.
        assert "14 bars" in warning.detail

    def test_the_chunks_are_contiguous_and_cover_the_whole_span(self) -> None:
        catalog = self._many_objects(MAX_SOURCE_OBJECTS_PER_COMMAND + 1)

        commands = plan_feature_backfill(catalog, [_definition("30m")]).commands

        assert commands[0].period_end == commands[1].period_start
        assert commands[0].period_start == _utc("2016-01-01T00:00:00+00:00")
        assert commands[1].period_end == _utc(
            (_utc("2016-01-01T00:00:00+00:00")
             + timedelta(days=MAX_SOURCE_OBJECTS_PER_COMMAND + 1)).isoformat()
        )

    def test_no_object_is_dropped_or_repeated_across_the_split(self) -> None:
        count = MAX_SOURCE_OBJECTS_PER_COMMAND + 1
        catalog = self._many_objects(count)

        commands = plan_feature_backfill(catalog, [_definition("30m")]).commands
        planned = [item for command in commands for item in command.source_dataset_object_ids]

        assert len(planned) == count
        assert len(set(planned)) == count


def test_the_object_cap_matches_the_worker() -> None:
    """A cap that drifts from the worker's would only surface as a rejected command."""
    from apps.pipeline_worker.feature_output import MAX_FEATURE_SOURCE_OBJECTS

    assert MAX_SOURCE_OBJECTS_PER_COMMAND == MAX_FEATURE_SOURCE_OBJECTS


def test_a_naive_timestamp_in_the_catalog_is_refused() -> None:
    """The pipeline works in ET and UTC; a naive instant is ambiguous, not a default."""
    catalog = _one_month()
    catalog.manifests[0]["period_start"] = datetime(2016, 1, 1)

    with pytest.raises(ValueError, match="naive"):
        plan_feature_backfill(catalog, [_definition("30m")])


# ======================================================================================
# The operator entry point
# ======================================================================================


def _arguments(**overrides: Any) -> argparse.Namespace:
    defaults: dict[str, Any] = {
        "resolution": ["30m"],
        "instrument": None,
        "period_start": None,
        "period_end": None,
        "send": False,
        "queue_url": None,
        "allow_holes": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestTheEntryPointPlansBeforeItSends:
    def test_planning_alone_sends_nothing(self) -> None:
        sent: list[tuple[str, str]] = []

        code = run(
            _arguments(),
            catalog=_one_month(),
            send=lambda url, body: sent.append((url, body)),
        )

        assert code == 0
        assert sent == []

    def test_sending_enqueues_one_message_per_command(self) -> None:
        sent: list[tuple[str, str]] = []

        code = run(
            _arguments(send=True, queue_url="https://sqs.invalid/queue"),
            catalog=_one_month(),
            send=lambda url, body: sent.append((url, body)),
        )

        assert code == 0
        assert len(sent) == 1
        assert sent[0][0] == "https://sqs.invalid/queue"
        assert json.loads(sent[0][1])["command"] == "MATERIALIZE_FEATURE_OUTPUT"

    def test_nothing_to_send_is_success_not_failure(self) -> None:
        """An already-complete backfill is a finished job, not an error."""
        catalog = _one_month()
        catalog.materializations.append({
            "feature_definition_id": _definition("30m").id,
            "instrument_id": INSTRUMENT,
            "status": "SUCCEEDED",
            "period_start": _utc("2016-01-01T00:00:00+00:00"),
            "period_end": _utc("2016-02-01T00:00:00+00:00"),
        })
        sent: list[tuple[str, str]] = []

        code = run(
            _arguments(send=True, queue_url="https://sqs.invalid/queue"),
            catalog=catalog,
            send=lambda url, body: sent.append((url, body)),
        )

        assert code == 0
        assert sent == []


class TestTheEntryPointRefusesToHideHoles:
    @staticmethod
    def _split_catalog() -> FakeCatalog:
        catalog = FakeCatalog()
        origin = _utc("2016-01-01T00:00:00+00:00")
        count = MAX_SOURCE_OBJECTS_PER_COMMAND + 1
        catalog.add_manifest(
            "manifest-1", resolution="30m",
            start="2016-01-01T00:00:00+00:00",
            end=(origin + timedelta(days=count)).isoformat(),
        )
        for index in range(count):
            catalog.add_object(
                f"object-{index:04d}", manifest_id="manifest-1",
                start=(origin + timedelta(days=index)).isoformat(),
                end=(origin + timedelta(days=index + 1)).isoformat(),
            )
        return catalog

    def test_a_split_plan_is_refused_by_default(self) -> None:
        sent: list[tuple[str, str]] = []

        code = run(
            _arguments(send=True, queue_url="https://sqs.invalid/queue"),
            catalog=self._split_catalog(),
            send=lambda url, body: sent.append((url, body)),
        )

        assert code == 1
        assert sent == []

    def test_a_split_plan_is_sent_once_the_gaps_are_acknowledged(self) -> None:
        sent: list[tuple[str, str]] = []

        code = run(
            _arguments(send=True, queue_url="https://sqs.invalid/queue", allow_holes=True),
            catalog=self._split_catalog(),
            send=lambda url, body: sent.append((url, body)),
        )

        assert code == 0
        assert len(sent) == 2

    def test_a_split_plan_is_still_printed_when_only_planning(self) -> None:
        """Refusing to send must not mean refusing to explain."""
        code = run(_arguments(), catalog=self._split_catalog(), send=None)

        assert code == 0


class TestTheEntryPointArguments:
    def test_the_operating_catalog_connects_with_read_only_storage_objects(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        observed: dict[str, Any] = {}
        expected = object()

        class CapturingPostgresCatalog:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                raise AssertionError("the planner must use the guarded connect factory")

            @staticmethod
            def connect(
                database_url: str,
                *,
                artifact_root: Any,
                storage_objects: StorageObjectsPolicy,
            ) -> object:
                observed.update(
                    database_url=database_url,
                    artifact_root=artifact_root,
                    storage_objects=storage_objects,
                )
                return expected

        monkeypatch.setattr(
            "market_pipeline_lib.catalog.PostgresCatalog",
            CapturingPostgresCatalog,
        )
        arguments = argparse.Namespace(
            database_url="postgresql://planner:secret@db/idea2strategy",
            artifact_root=tmp_path,
        )

        assert _catalog(arguments) is expected
        assert observed == {
            "database_url": arguments.database_url,
            "artifact_root": tmp_path,
            "storage_objects": StorageObjectsPolicy.READ_ONLY,
        }

    def test_send_without_a_queue_is_rejected_before_any_work(self) -> None:
        assert main(["--database-url", "postgresql://unused", "--send"]) == 2

    def test_a_naive_instant_is_rejected(self) -> None:
        with pytest.raises(SystemExit):
            main(["--database-url", "postgresql://unused", "--from", "2016-01-01T00:00:00"])

    def test_an_aware_instant_is_accepted(self) -> None:
        assert _instant("2016-01-01T00:00:00Z") == _utc("2016-01-01T00:00:00+00:00")

    def test_an_unsupported_resolution_is_rejected(self) -> None:
        """Only the four strategy clocks exist; 5m is not a smaller version of 30m."""
        with pytest.raises(SystemExit):
            main(["--database-url", "postgresql://unused", "--resolution", "5m"])

    def test_the_summary_reports_every_warning(self) -> None:
        plan = plan_feature_backfill(FakeCatalog(), [_definition("4h")])

        summary = _summary(plan)

        assert summary["commands"] == 0
        assert [item["code"] for item in summary["warnings"]] == ["NO_INSTRUMENTS"]
        assert summary["hasHoles"] is False
