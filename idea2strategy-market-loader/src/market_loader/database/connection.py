from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

from market_loader.config import EnvironmentSettings
from market_loader.errors import ConfigurationError


def connection_info(settings: EnvironmentSettings) -> str:
    required = {
        "PGHOST": settings.PGHOST,
        "PGHOSTADDR": settings.PGHOSTADDR,
        "PGDATABASE": settings.PGDATABASE,
        "PGUSER": settings.PGUSER,
        "PGPASSWORD": settings.PGPASSWORD,
        "PGSSLROOTCERT": settings.PGSSLROOTCERT,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ConfigurationError(f"missing PostgreSQL settings: {missing}")
    if settings.PGSSLMODE != "verify-full":
        raise ConfigurationError("PGSSLMODE must be verify-full")
    return make_conninfo(
        host=settings.PGHOST,
        hostaddr=settings.PGHOSTADDR,
        port=settings.PGPORT,
        dbname=settings.PGDATABASE,
        user=settings.PGUSER,
        password=settings.PGPASSWORD,
        sslmode=settings.PGSSLMODE,
        sslrootcert=settings.PGSSLROOTCERT,
        application_name="idea2strategy-market-loader",
    )


class Database:
    def __init__(self, settings: EnvironmentSettings) -> None:
        self._pool = ConnectionPool(connection_info(settings), min_size=1, max_size=4, open=False)

    def open(self) -> None:
        self._pool.open(wait=True)

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self._pool.connection() as connection, connection.transaction():
            yield connection
