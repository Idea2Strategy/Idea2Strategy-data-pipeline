"""Shared fixtures, including the Testcontainers PostgreSQL 16 harness.

Everything that needs Docker is behind the already-registered `integration` marker and
behind lazily-created session fixtures, so `pytest -m "not integration"` never touches
Docker and never imports `testcontainers`.

The container is migrated with the **central** Flyway bundle -- never with
`MetaData.create_all()`.  Hand-written DDL in a test suite only proves the suite agrees
with itself; applying the real bundle is what makes a schema disagreement fail here
instead of in production.

Two sources are accepted, in order:

1. `db/migration-contributions/fixtures/central-migration/` -- a byte-for-byte vendored
   copy guarded by `central-migration.sha256`.  This is the form the sibling
   `backtest-engine` repository uses and the form a bare clone needs.
2. the superproject checkout, when this worktree sits inside or beside one.

When both are reachable they are cross-checked and a mismatch is a hard failure: a stale
vendored copy is exactly the drift this harness exists to catch.  When neither is
reachable the Docker suite skips with the reason, because silently running against a
schema nobody can name would be worse than not running.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, text

from market_pipeline_lib.catalog import LocalCatalog, PostgresCatalog, StorageObjectsPolicy
from market_pipeline_lib.db.tables import MARKET_DATA_SCHEMA, STORAGE_SCHEMA

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTION_ROOT = REPO_ROOT / "db" / "migration-contributions"
VENDORED_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "central-migration"
VENDORED_DIGESTS = CONTRIBUTION_ROOT / "fixtures" / "central-migration.sha256"

POSTGRES_IMAGE = os.environ.get("PIPELINE_TEST_POSTGRES_IMAGE", "postgres:16-alpine")

_SUPERPROJECT_ENV = "I2S_SUPERPROJECT_ROOT"
_SUPERPROJECT_SEARCH_DEPTH = 6
_CENTRAL_MIGRATION_RELATIVE = Path("backend/db-migration/src/main/resources/db/migration")

_VERSION = re.compile(r"^V(?P<version>[0-9]+)__")

#: Emptied between Docker tests, child tables first.  `storage.objects` is included
#: because `stage_object` writes it; see the ownership note in `db/tables.py`.
_TRUNCATED_TABLES = (
    f"{MARKET_DATA_SCHEMA}.dataset_object_lineage",
    f"{MARKET_DATA_SCHEMA}.dataset_lineage",
    f"{MARKET_DATA_SCHEMA}.quality_incidents",
    f"{MARKET_DATA_SCHEMA}.dataset_objects",
    f"{MARKET_DATA_SCHEMA}.feature_snapshot_batches",
    f"{MARKET_DATA_SCHEMA}.feature_materializations",
    f"{MARKET_DATA_SCHEMA}.feature_definitions",
    f"{MARKET_DATA_SCHEMA}.corporate_actions",
    f"{MARKET_DATA_SCHEMA}.dataset_manifests",
    f"{MARKET_DATA_SCHEMA}.stream_watermarks",
    f"{MARKET_DATA_SCHEMA}.pipeline_runs",
    f"{MARKET_DATA_SCHEMA}.trading_sessions",
    f"{MARKET_DATA_SCHEMA}.instrument_symbols",
    f"{MARKET_DATA_SCHEMA}.instruments",
    f"{MARKET_DATA_SCHEMA}.feeds",
    f"{MARKET_DATA_SCHEMA}.providers",
    f"{STORAGE_SCHEMA}.objects",
)


# ----------------------------------------------------------------------------------
# Locating the canonical migration bundle
# ----------------------------------------------------------------------------------


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def superproject_root() -> Path | None:
    """Best-effort location of the Idea2Strategy superproject, or `None`.

    `None` means "cannot cross-check", never "the central source agrees".
    """

    override = os.environ.get(_SUPERPROJECT_ENV)
    if override:
        candidate = Path(override).expanduser().resolve()
        return candidate if _is_superproject(candidate) else None

    ancestors = list(Path(__file__).resolve().parents)[:_SUPERPROJECT_SEARCH_DEPTH]
    for parent in ancestors:
        if _is_superproject(parent):
            return parent
    # Git worktrees live beside the superproject rather than inside it.
    for parent in ancestors:
        try:
            siblings = sorted(child for child in parent.iterdir() if child.is_dir())
        except OSError:
            continue
        for sibling in siblings:
            if _is_superproject(sibling):
                return sibling
    return None


def _is_superproject(candidate: Path) -> bool:
    """Whether `candidate` looks like the superproject checkout.

    Probing has to tolerate a directory we are not allowed to stat. The sibling
    scan above walks ancestors up to and including `/home`, and a CI runner has
    unreadable home directories beside our own - GitHub's Ubuntu image ships
    `/home/packer`, which raised PermissionError here and errored every
    database test in the suite. `parent.iterdir()` was guarded; this call was
    not, so the failure escaped through the inner loop.
    """

    try:
        return (
            (candidate / "db" / "schema.dbml").is_file()
            and (candidate / _CENTRAL_MIGRATION_RELATIVE).is_dir()
        )
    except OSError:
        # Unreadable or otherwise unstattable: not our superproject.
        return False


def recorded_digests() -> dict[str, str]:
    """Parse `central-migration.sha256` into `{filename: digest}`; empty when absent."""

    if not VENDORED_DIGESTS.is_file():
        return {}
    digests: dict[str, str] = {}
    for line in VENDORED_DIGESTS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        digest, _, name = stripped.partition("  ")
        digests[name.strip().lstrip("*")] = digest.strip()
    return digests


def _ordered(files: list[Path]) -> list[Path]:
    def order(path: Path) -> tuple[int, str]:
        match = _VERSION.match(path.name)
        if match is None:
            raise AssertionError(f"unversioned migration file: {path.name}")
        return int(match.group("version")), path.name

    return sorted(files, key=order)


def vendored_migration_files() -> list[Path]:
    if not VENDORED_MIGRATIONS.is_dir():
        return []
    found = [path for path in VENDORED_MIGRATIONS.iterdir() if path.is_file() and path.name.startswith("V")]
    return _ordered(found)


def superproject_migration_files() -> list[Path]:
    root = superproject_root()
    if root is None:
        return []
    directory = root / _CENTRAL_MIGRATION_RELATIVE
    return _ordered([path for path in directory.glob("V*.sql") if path.is_file()])


def _strip_fixture_suffix(name: str) -> str:
    return name[: -len(".fixture")] if name.endswith(".fixture") else name


def central_migration_scripts() -> list[str]:
    """The canonical bundle in Flyway order, cross-checked across both sources."""

    vendored = vendored_migration_files()
    upstream = superproject_migration_files()
    if not vendored and not upstream:
        raise LookupError(
            "the central Flyway bundle is not reachable: neither "
            f"{VENDORED_MIGRATIONS} nor a superproject checkout was found. "
            f"Set {_SUPERPROJECT_ENV} to the Idea2Strategy root, or vendor the bundle."
        )

    if vendored:
        _verify_vendored_digests(vendored)
    if vendored and upstream:
        _cross_check(vendored, upstream)

    chosen = vendored or upstream
    return [path.read_text(encoding="utf-8") for path in chosen]


def _verify_vendored_digests(vendored: list[Path]) -> None:
    recorded = recorded_digests()
    if not recorded:
        raise AssertionError(
            f"{VENDORED_MIGRATIONS} exists but {VENDORED_DIGESTS.name} does not; "
            "an unguarded vendored copy cannot be trusted to match the central bundle"
        )
    for path in vendored:
        expected = recorded.get(path.name)
        if expected is None:
            raise AssertionError(f"{path.name} is vendored but has no recorded digest")
        actual = sha256_of(path)
        if actual != expected:
            raise AssertionError(f"{path.name} digest is {actual}, recorded {expected}")


def _cross_check(vendored: list[Path], upstream: list[Path]) -> None:
    by_name = {_strip_fixture_suffix(path.name): path for path in vendored}
    for path in upstream:
        copy = by_name.get(path.name)
        if copy is None:
            raise AssertionError(f"central bundle has {path.name} but the vendored copy does not")
        if sha256_of(copy) != sha256_of(path):
            raise AssertionError(f"vendored {copy.name} differs from the central {path.name}")
    extra = sorted(set(by_name) - {path.name for path in upstream})
    if extra:
        raise AssertionError(f"vendored copy has migrations the central bundle does not: {extra}")


# ----------------------------------------------------------------------------------
# The container
# ----------------------------------------------------------------------------------


def docker_is_available() -> bool:
    try:
        import docker
    except ImportError:  # pragma: no cover - testcontainers depends on docker
        return False
    try:
        docker.from_env().ping()
    except Exception:  # pragma: no cover - depends on the developer's machine
        return False
    return True


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """A PostgreSQL 16 container with the canonical central schema applied."""

    if not docker_is_available():
        pytest.skip("Docker is not available; run with -m 'not integration' to skip this suite")
    try:
        scripts = central_migration_scripts()
    except LookupError as exc:
        pytest.skip(str(exc))

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _execute_scripts(url, scripts)
        yield url


def _execute_scripts(url: str, scripts: list[str]) -> None:
    """Run whole SQL scripts through the raw driver cursor.

    Not `exec_driver_sql`: SQLAlchemy hands psycopg an empty parameter tuple, which
    turns on client-side placeholder parsing, and `V1__initial_schema.sql` contains `%`
    inside Korean `COMMENT` strings.  Passing no parameters at all avoids that entirely.
    """

    engine = create_engine(url, future=True)
    try:
        raw = engine.raw_connection()
        try:
            cursor = raw.cursor()
            for script in scripts:
                cursor.execute(script)
                # Flyway commits each versioned migration independently. This is
                # observable for PostgreSQL enum additions, whose new values
                # cannot be referenced until the ALTER TYPE transaction commits.
                raw.commit()
        finally:
            raw.close()
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def admin_engine(postgres_url: str) -> Iterator[Engine]:
    """An unguarded engine, for arranging state and simulating drift."""

    engine = create_engine(postgres_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def truncate_market_data(admin_engine: Engine) -> Iterator[None]:
    """Leave `market_data.*` and `storage.objects` empty around each Docker test."""

    _truncate(admin_engine)
    yield
    _truncate(admin_engine)


def _truncate(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE " + ", ".join(_TRUNCATED_TABLES) + " RESTART IDENTITY CASCADE"))


@pytest.fixture
def postgres_catalog(postgres_url: str, truncate_market_data: None, tmp_path: Path) -> Iterator[PostgresCatalog]:
    """A `PostgresCatalog` on the canonical schema, verified at construction."""

    catalog = PostgresCatalog.connect(
        postgres_url,
        artifact_root=tmp_path / "catalog-artifacts",
        # Root issue #139 assigns immutable object registration to D.
        storage_objects=StorageObjectsPolicy.WRITE_D_OWNED,
    )
    try:
        catalog.verify_schema()
        yield catalog
    finally:
        catalog.close()


@pytest.fixture
def local_catalog(tmp_path: Path) -> LocalCatalog:
    return LocalCatalog(tmp_path / "catalog-export")


# ----------------------------------------------------------------------------------
# The shared contract parameterisation
# ----------------------------------------------------------------------------------


@pytest.fixture(
    params=[
        pytest.param("local", id="LocalCatalog"),
        pytest.param("postgres", id="PostgresCatalog", marks=pytest.mark.integration),
    ]
)
def catalog(request: pytest.FixtureRequest) -> Any:
    """Every `MarketDataCatalog` implementation, one at a time."""

    if request.param == "local":
        return request.getfixturevalue("local_catalog")
    return request.getfixturevalue("postgres_catalog")


@pytest.fixture
def _catalog_isolation() -> Iterator[None]:
    """Marker fixture: the per-implementation fixtures already isolate state."""

    yield
