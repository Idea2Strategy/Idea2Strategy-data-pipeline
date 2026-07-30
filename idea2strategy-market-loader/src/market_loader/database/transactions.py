from __future__ import annotations

from collections.abc import Callable
from typing import Any

from psycopg import errors
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

TRANSIENT_DATABASE_ERRORS = (
    errors.DeadlockDetected,
    errors.SerializationFailure,
    errors.ConnectionException,
)


@retry(
    retry=retry_if_exception_type(TRANSIENT_DATABASE_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=0.5, max=4),
    reraise=True,
)
def retry_transaction[T](operation: Callable[[], T]) -> T:
    return operation()


def fetch_one_value(connection: Any, query: str, parameters: tuple[Any, ...] = ()) -> Any:
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        row = cursor.fetchone()
        return None if row is None else row[0]
