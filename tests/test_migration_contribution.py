"""COM07 contribution-root contract tests for the pipeline owner.

The central assembler lives in `backend/db-migration` and is owned by A.  These
tests hold this repository to the same contract locally so a malformed
contribution fails here instead of in the central Flyway bundle.
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTION_ROOT = REPO_ROOT / "db" / "migration-contributions"
VALIDATOR_PATH = CONTRIBUTION_ROOT / "validate_contribution.py"


def _load_validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("validate_contribution", VALIDATOR_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - import plumbing
        raise RuntimeError(f"cannot load validator from {VALIDATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validator = _load_validator()


class ContributionRootLayoutTests(unittest.TestCase):
    def test_contribution_root_has_the_required_files(self) -> None:
        self.assertTrue((CONTRIBUTION_ROOT / "contribution.properties").is_file())
        self.assertTrue((CONTRIBUTION_ROOT / "migrations").is_dir())
        self.assertTrue((CONTRIBUTION_ROOT / "fixtures").is_dir())
        self.assertTrue((CONTRIBUTION_ROOT / "README.md").is_file())


class ContributionContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contribution = validator.load_contribution(CONTRIBUTION_ROOT)

    def test_properties_file_parses(self) -> None:
        self.assertEqual(self.contribution.contract_version, "1")
        self.assertFalse(self.contribution.runtime_flyway_enabled)

    def test_owner_token_is_known_to_the_central_migration_owner_enum(self) -> None:
        self.assertEqual(self.contribution.owner, "pipeline")
        self.assertIn(self.contribution.owner, validator.KNOWN_OWNERS)

    def test_declared_schemas_match_the_spec(self) -> None:
        # Spec section 2.4: `owner=pipeline, schemas=market_data,storage`.
        self.assertEqual(self.contribution.schemas, frozenset({"market_data", "storage"}))

    def test_only_market_data_is_mutable_by_this_owner(self) -> None:
        # Root #139 settled both schemas on D.  A contribution still has to declare
        # each schema it touches; ownership now makes both declared schemas mutable.
        self.assertEqual(self.contribution.mutable_schemas, frozenset({"market_data", "storage"}))
        self.assertEqual(validator.SCHEMA_OWNERS["market_data"], "pipeline")
        self.assertEqual(validator.SCHEMA_OWNERS["storage"], "pipeline")

    def test_pipeline_partitions_is_never_contributed(self) -> None:
        # `market_data.pipeline_partitions` is absent from the canonical DBML; spec
        # section 2.4 says D must not add it.
        for path in self.contribution.migrations_directory.glob("*.sql"):
            with self.subTest(migration=path.name):
                self.assertNotIn("pipeline_partitions", path.read_text(encoding="utf-8"))

    def test_declared_filename_regex_matches_the_central_naming_rule(self) -> None:
        good = "V20260802120000__pipeline_add_stream_watermark_index.sql"
        self.assertRegex(good, self.contribution.filename_regex)
        self.assertIsNotNone(validator.CENTRAL_FILENAME_RULE.fullmatch(good))

    def test_legacy_numeric_version_is_rejected(self) -> None:
        for bad in (
            "V001__pipeline_initial.sql",
            "V20260802120000__backtest_add_table.sql",
            "V20260802120000__pipeline_AddTable.sql",
            "V20260802120000__pipeline.sql",
            "V20261302120000__pipeline_bad_month.sql",
        ):
            with self.subTest(filename=bad):
                with self.assertRaises(validator.ContributionError):
                    validator.check_migration_filename(bad, self.contribution)

    def test_every_file_in_migrations_matches_the_required_pattern(self) -> None:
        # No SQL is contributed yet (the tables already exist in the central
        # V1 baseline).  The assertion still runs so the first contributed file
        # is checked automatically.
        checked = validator.check_migration_directory(self.contribution)
        for name in checked:
            self.assertRegex(name, self.contribution.filename_regex)

    def test_only_sql_and_gitkeep_live_under_migrations(self) -> None:
        allowed = {".sql"}
        for path in self.contribution.migrations_directory.iterdir():
            if path.name == ".gitkeep":
                continue
                self.assertIn(path.suffix, allowed, f"unexpected file in migrations/: {path.name}")


class ContributionRejectionTests(unittest.TestCase):
    """The validator must reject a broken contribution, not tolerate it."""

    def _write(self, root: Path, body: str) -> Path:
        (root / "migrations").mkdir(parents=True, exist_ok=True)
        (root / "fixtures").mkdir(parents=True, exist_ok=True)
        (root / "contribution.properties").write_text(body, encoding="utf-8")
        return root

    def test_unknown_owner_token_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=datateam\n"
                "schemas=market_data\n"
                "migrations.directory=migrations\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]{14}__datateam_[a-z0-9]+[.]sql$\n"
                "runtime.flyway.enabled=false\n",
            )
            with self.assertRaises(validator.ContributionError):
                validator.load_contribution(root)

    def test_schema_owned_by_another_owner_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=pipeline\n"
                "schemas=market_data,identity\n"
                "migrations.directory=migrations\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]{14}__pipeline_[a-z0-9]+[.]sql$\n"
                "runtime.flyway.enabled=false\n",
            )
            with self.assertRaises(validator.ContributionError):
                validator.load_contribution(root)

    def test_ddl_against_pipeline_owned_storage_is_accepted(self) -> None:
        """Root #139 assigns `storage` to D, so its contribution may mutate it."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(Path(tmp), self._properties())
            (root / "migrations" / "V20260802120000__pipeline_touch_storage.sql").write_text(
                'ALTER TABLE "storage"."objects" ADD COLUMN pipeline_note text;\n',
                encoding="utf-8",
            )
            contribution = validator.load_contribution(root)
            accepted = validator.check_migration_directory(contribution)

        self.assertEqual(accepted, ["V20260802120000__pipeline_touch_storage.sql"])

    def test_ddl_against_an_undeclared_schema_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(Path(tmp), self._properties())
            (root / "migrations" / "V20260802120000__pipeline_touch_identity.sql").write_text(
                "CREATE TABLE identity.pipeline_scratch (id uuid);\n",
                encoding="utf-8",
            )
            contribution = validator.load_contribution(root)
            with self.assertRaises(validator.ContributionError):
                validator.check_migration_directory(contribution)

    def test_market_data_ddl_is_accepted(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(Path(tmp), self._properties())
            (root / "migrations" / "V20260802120000__pipeline_add_index.sql").write_text(
                "-- storage.objects is only mentioned in a comment, never mutated\n"
                "CREATE INDEX ix_stream_watermarks_updated\n"
                "  ON market_data.stream_watermarks (updated_at);\n",
                encoding="utf-8",
            )
            contribution = validator.load_contribution(root)

            accepted = validator.check_migration_directory(contribution)

        self.assertEqual(accepted, ["V20260802120000__pipeline_add_index.sql"])

    @staticmethod
    def _properties() -> str:
        return (
            "contract.version=1\n"
            "owner=pipeline\n"
            "schemas=market_data,storage\n"
            "migrations.directory=migrations\n"
            "fixtures.directory=fixtures\n"
            "filename.regex=^V[0-9]{14}__pipeline_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$\n"
            "runtime.flyway.enabled=false\n"
        )

    def test_runtime_flyway_enabled_true_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=pipeline\n"
                "schemas=market_data\n"
                "migrations.directory=migrations\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]{14}__pipeline_[a-z0-9]+[.]sql$\n"
                "runtime.flyway.enabled=true\n",
            )
            with self.assertRaises(validator.ContributionError):
                validator.load_contribution(root)

    def test_directory_escaping_the_root_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=pipeline\n"
                "schemas=market_data\n"
                "migrations.directory=../elsewhere\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]{14}__pipeline_[a-z0-9]+[.]sql$\n"
                "runtime.flyway.enabled=false\n",
            )
            with self.assertRaises(validator.ContributionError):
                validator.load_contribution(root)

    def test_declared_regex_looser_than_the_central_rule_is_rejected(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=pipeline\n"
                "schemas=market_data\n"
                "migrations.directory=migrations\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]+__pipeline_.*[.]sql$\n"
                "runtime.flyway.enabled=false\n",
            )
            with self.assertRaises(validator.ContributionError):
                validator.load_contribution(root)

    def test_migration_file_not_matching_the_rule_is_reported(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            root = self._write(
                Path(tmp),
                "contract.version=1\n"
                "owner=pipeline\n"
                "schemas=market_data\n"
                "migrations.directory=migrations\n"
                "fixtures.directory=fixtures\n"
                "filename.regex=^V[0-9]{14}__pipeline_[a-z0-9]+(?:_[a-z0-9]+)*[.]sql$\n"
                "runtime.flyway.enabled=false\n",
            )
            (root / "migrations" / "V001__pipeline_legacy.sql").write_text("select 1;\n", encoding="utf-8")
            contribution = validator.load_contribution(root)
            with self.assertRaises(validator.ContributionError):
                validator.check_migration_directory(contribution)


class ValidatorCliTests(unittest.TestCase):
    def test_cli_reports_success_for_this_repository(self) -> None:
        self.assertEqual(validator.main([str(CONTRIBUTION_ROOT)]), 0)

    def test_cli_reports_failure_for_a_missing_root(self) -> None:
        self.assertEqual(validator.main([str(REPO_ROOT / "does-not-exist")]), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
