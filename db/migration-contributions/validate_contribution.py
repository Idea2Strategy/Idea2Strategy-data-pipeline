"""Local validator for this repository's Flyway migration contribution root.

The central assembler (`backend/db-migration`, owned by A) reads every owner
repository's `contribution.properties`, checks the declared owner, schemas and
filename rule, and only then folds the SQL into the canonical bundle.  This
module mirrors that contract so a defect fails in this repository's CI instead
of at central assembly time.

Mirrored sources (read-only references, never edited from here):
  backend/db-migration/src/main/java/.../MigrationContribution.java
  backend/db-migration/src/main/java/.../MigrationOwner.java
  backend/db-migration/src/main/java/.../DatabaseAccessPolicy.java

Usage:
    python db/migration-contributions/validate_contribution.py [ROOT ...]
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

CONTRACT_FILE = "contribution.properties"
SUPPORTED_CONTRACT_VERSION = "1"

#: Mirror of ``MigrationOwner``.  A token outside this set is rejected centrally.
KNOWN_OWNERS: frozenset[str] = frozenset(
    {"backend", "trading", "backtest", "pipeline", "shared"}
)

#: Mirror of ``DatabaseAccessPolicy.SCHEMA_OWNERS`` after root #139 assigned
#: ``storage`` to D.
SCHEMA_OWNERS: Mapping[str, str] = {
    "identity": "backend",
    "strategy": "backend",
    "bot": "backend",
    "storage": "pipeline",
    "market_data": "pipeline",
    "trading": "trading",
    "backtest": "backtest",
    "performance": "backend",
    "competition": "backend",
    "operations": "backend",
}

#: Central naming rule: ``V<YYYYMMDDHHMMSS>__<owner>_<slug>.sql``.
#: Legacy ``V001__`` numbering is rejected because the version group is a fixed
#: 14 digits that must also parse as a UTC timestamp.
CENTRAL_FILENAME_RULE = re.compile(
    r"^V(?P<version>[0-9]{14})__(?P<owner>[a-z]+)_(?P<slug>[a-z0-9]+(?:_[a-z0-9]+)*)\.sql$"
)

#: Filenames the central assembler tolerates alongside SQL inside a directory.
IGNORED_ENTRIES = frozenset({".gitkeep", ".gitignore"})

#: The owner token every schema registered to more than one repository carries.
SHARED_OWNER = "shared"

_SQL_COMMENT = re.compile(r"(?s)/\*.*?\*/|--[^\n]*")

#: Statements that mutate a schema object.  ``CREATE INDEX ... ON <schema>.<table>``
#: names its target after ``ON``, so the schema is captured wherever it appears rather
#: than only immediately after the verb.
_DDL_STATEMENT = re.compile(
    r"^\s*(?:CREATE|ALTER|DROP|TRUNCATE|COMMENT\s+ON|GRANT|REVOKE)\b",
    re.I,
)
_QUALIFIED_NAME = re.compile(r'"?([a-z_][a-z0-9_]*)"?\s*\.\s*"?[a-z_][a-z0-9_]*"?', re.I)


class ContributionError(Exception):
    """Raised when a contribution root violates the COM07 contract."""


@dataclass(frozen=True)
class Contribution:
    """A parsed and validated `contribution.properties`."""

    root: Path
    contract_version: str
    owner: str
    #: Every schema this contribution touches at all, including read/insert-only ones.
    schemas: frozenset[str]
    migrations_directory: Path
    fixtures_directory: Path
    filename_regex: str
    runtime_flyway_enabled: bool

    @property
    def filename_pattern(self) -> re.Pattern[str]:
        return re.compile(self.filename_regex)

    @property
    def mutable_schemas(self) -> frozenset[str]:
        """The declared schemas this owner may actually emit DDL for.

        Declaring a schema grants scope; the registered owner independently decides
        which contribution may mutate it. Root #139 registers both ``market_data`` and
        ``storage`` to D.
        """

        return frozenset(schema for schema in self.schemas if SCHEMA_OWNERS.get(schema) == self.owner)


def parse_properties(text: str) -> dict[str, str]:
    """Parse the subset of the Java `.properties` grammar this contract uses."""

    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        separator = min(
            (index for index in (line.find("="), line.find(":")) if index != -1),
            default=-1,
        )
        if separator == -1:
            raise ContributionError(f"malformed properties line: {raw_line!r}")
        key = line[:separator].strip()
        value = line[separator + 1 :].strip()
        if not key:
            raise ContributionError(f"malformed properties line: {raw_line!r}")
        if key in values:
            raise ContributionError(f"duplicate contribution property: {key}")
        values[key] = value
    return values


def _required(values: Mapping[str, str], key: str) -> str:
    value = values.get(key)
    if value is None or not value.strip():
        raise ContributionError(f"missing contribution property: {key}")
    return value.strip()


def _required_directory(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute():
        raise ContributionError(f"contribution path must stay inside its root: {relative_path}")
    resolved = (root / relative).resolve()
    if not str(resolved).startswith(str(root)):
        raise ContributionError(f"contribution path must stay inside its root: {relative_path}")
    if not resolved.is_dir():
        raise ContributionError(f"contribution directory does not exist: {resolved}")
    return resolved


def _validate_owner(owner: str) -> str:
    if owner not in KNOWN_OWNERS:
        raise ContributionError(
            f"unknown migration owner {owner!r}; central MigrationOwner accepts "
            f"{sorted(KNOWN_OWNERS)}"
        )
    return owner


def _validate_schemas(owner: str, raw: str) -> frozenset[str]:
    """Accept the owner's own schemas plus shared ones; reject another owner's.

    A schema registered to a *different specific* owner is never this contribution's
    business.  A ``shared`` schema is: more than one repository writes rows to it, and
    which of them may issue DDL is enforced separately, per migration file, by
    `check_migration_directory`.
    """

    schemas = frozenset(item.strip() for item in raw.split(",") if item.strip())
    if not schemas:
        raise ContributionError("contribution must declare at least one schema")
    for schema in sorted(schemas):
        registered = SCHEMA_OWNERS.get(schema)
        if registered is None:
            raise ContributionError(f"no write owner is registered for schema: {schema}")
        if registered not in (owner, SHARED_OWNER):
            raise ContributionError(
                f"owner {owner!r} cannot claim schema {schema!r}; registered owner is {registered!r}"
            )
    if not any(SCHEMA_OWNERS[schema] == owner for schema in schemas):
        raise ContributionError(
            f"owner {owner!r} declares only shared schemas {sorted(schemas)}; a "
            "contribution root must own at least one of the schemas it declares"
        )
    return schemas


def mutated_schemas(sql: str) -> set[str]:
    """Every schema the DDL statements in `sql` name as a target.

    Comment-stripped first, so a schema mentioned only in prose is not reported, and
    limited to DDL statements, so an ``INSERT`` seeding a shared table is not mistaken
    for a schema change.
    """

    cleaned = _SQL_COMMENT.sub(" ", sql)
    found: set[str] = set()
    for statement in cleaned.split(";"):
        if not _DDL_STATEMENT.match(statement):
            continue
        found.update(match.group(1).lower() for match in _QUALIFIED_NAME.finditer(statement))
    return found


def _validate_filename_regex(owner: str, regex: str) -> str:
    try:
        pattern = re.compile(regex)
    except re.error as exc:
        raise ContributionError(f"invalid contribution filename.regex: {exc}") from exc

    # The declared regex must be at least as strict as the central rule: it has
    # to accept a canonical name for this owner and reject everything the
    # central assembler rejects.  A looser regex would let a bad name through
    # local review and fail only at central assembly.
    canonical = f"V20260101000000__{owner}_example_slug.sql"
    if not pattern.fullmatch(canonical):
        raise ContributionError(
            f"filename.regex rejects the canonical name {canonical!r} for owner {owner!r}"
        )
    rejected = (
        f"V001__{owner}_legacy.sql",
        f"V20260101000000__{owner}_Example.sql",
        f"V20260101000000__{owner}.sql",
        f"V2026010100000__{owner}_short_version.sql",
    )
    for name in rejected:
        if pattern.fullmatch(name):
            raise ContributionError(
                f"filename.regex is looser than the central rule: it accepts {name!r}"
            )
    return regex


def _validate_flyway_flag(raw: str) -> bool:
    if raw.lower() != "false":
        raise ContributionError("contribution runtime.flyway.enabled must be false")
    return False


def load_contribution(contribution_root: Path | str) -> Contribution:
    """Parse and validate a contribution root, raising `ContributionError`."""

    root = Path(contribution_root).resolve()
    contract = root / CONTRACT_FILE
    if not contract.is_file():
        raise ContributionError(f"unable to read contribution contract: {contract}")

    values = parse_properties(contract.read_text(encoding="utf-8"))
    contract_version = _required(values, "contract.version")
    if contract_version != SUPPORTED_CONTRACT_VERSION:
        raise ContributionError(
            f"unsupported contribution contract version: {contract_version}"
        )
    owner = _validate_owner(_required(values, "owner"))
    schemas = _validate_schemas(owner, _required(values, "schemas"))
    migrations = _required_directory(root, _required(values, "migrations.directory"))
    fixtures = _required_directory(root, _required(values, "fixtures.directory"))
    filename_regex = _validate_filename_regex(owner, _required(values, "filename.regex"))
    flyway_enabled = _validate_flyway_flag(_required(values, "runtime.flyway.enabled"))

    return Contribution(
        root=root,
        contract_version=contract_version,
        owner=owner,
        schemas=schemas,
        migrations_directory=migrations,
        fixtures_directory=fixtures,
        filename_regex=filename_regex,
        runtime_flyway_enabled=flyway_enabled,
    )


def check_migration_filename(filename: str, contribution: Contribution) -> str:
    """Validate one contributed SQL filename against both naming rules."""

    central = CENTRAL_FILENAME_RULE.fullmatch(filename)
    if central is None:
        raise ContributionError(
            f"{filename!r} does not match the central rule "
            f"V<YYYYMMDDHHMMSS>__{contribution.owner}_<slug>.sql "
            "(legacy V001-style numbering is rejected)"
        )
    if central.group("owner") != contribution.owner:
        raise ContributionError(
            f"{filename!r} declares owner {central.group('owner')!r} but this "
            f"contribution is owned by {contribution.owner!r}"
        )
    version = central.group("version")
    try:
        datetime.strptime(version, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise ContributionError(f"{filename!r} version {version!r} is not a valid UTC timestamp") from exc
    if not contribution.filename_pattern.fullmatch(filename):
        raise ContributionError(
            f"{filename!r} does not match the declared filename.regex "
            f"{contribution.filename_regex!r}"
        )
    return filename


def check_migration_content(filename: str, sql: str, contribution: Contribution) -> None:
    """Refuse DDL that targets a schema this owner may not mutate.

    This is the check the central assembler runs as
    ``Migration owner <owner> cannot mutate <schema>.<table>``. Declaring a schema
    grants scope; its registered owner independently grants mutation rights.
    """

    mutable = contribution.mutable_schemas
    for schema in sorted(mutated_schemas(sql) - mutable):
        registered = SCHEMA_OWNERS.get(schema)
        if registered is None:
            raise ContributionError(f"{filename!r} emits DDL for unregistered schema {schema!r}")
        raise ContributionError(
            f"migration owner {contribution.owner!r} cannot mutate schema {schema!r} "
            f"in {filename!r}; registered owner is {registered!r}"
        )


def check_migration_directory(contribution: Contribution) -> list[str]:
    """Validate every entry under `migrations/`; returns the accepted filenames."""

    accepted: list[str] = []
    seen_versions: dict[str, str] = {}
    for path in sorted(contribution.migrations_directory.iterdir()):
        if path.name in IGNORED_ENTRIES:
            continue
        if path.is_dir():
            raise ContributionError(
                f"migrations/ must contain SQL files only, found directory {path.name!r}"
            )
        if path.suffix != ".sql":
            raise ContributionError(
                f"migrations/ must contain SQL files only, found {path.name!r}"
            )
        check_migration_filename(path.name, contribution)
        check_migration_content(path.name, path.read_text(encoding="utf-8"), contribution)
        match = CENTRAL_FILENAME_RULE.fullmatch(path.name)
        assert match is not None  # noqa: S101 - guaranteed by check_migration_filename
        version = match.group("version")
        previous = seen_versions.get(version)
        if previous is not None:
            raise ContributionError(
                f"duplicate migration version {version}: {previous!r} and {path.name!r}"
            )
        seen_versions[version] = path.name
        accepted.append(path.name)
    return accepted


def validate_root(contribution_root: Path | str) -> list[str]:
    """Full validation of one contribution root; returns accepted filenames."""

    return check_migration_directory(load_contribution(contribution_root))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "roots",
        nargs="*",
        default=None,
        help="contribution roots to validate (default: db/migration-contributions)",
    )
    arguments = parser.parse_args(list(argv) if argv is not None else None)
    roots = arguments.roots or [str(Path(__file__).resolve().parent)]

    failed = False
    for root in roots:
        try:
            accepted = validate_root(root)
        except ContributionError as error:
            print(f"FAIL {root}: {error}", file=sys.stderr)
            failed = True
            continue
        print(f"OK   {root}: {len(accepted)} migration file(s) accepted")
        for name in accepted:
            print(f"       {name}")
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
