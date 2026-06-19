"""Retry SQLAlchemy operations that fail with retryable YDB errors.

SQLAlchemy wraps the underlying ydb-dbapi error in ``sqlalchemy.exc.DBAPIError``,
while YDB's retry logic only recognises ``ydb.Error``. These helpers unwrap the
original YDB error and re-raise it so the SDK can apply its retryable-error
classification (Unavailable, Overloaded, Aborted, BadSession, ...) and back-off.
Only YDB errors are retried; any other error propagates unchanged.

The retried callable is run from scratch on every attempt, so it must be safe to
re-run (open its own connection/session, not depend on earlier state).

Call form (sync and async)::

    rows = retry_ydb_operation(read, idempotent=True)
    rows = await retry_ydb_operation_async(read, idempotent=True)

Decorator form (works on both sync and async functions)::

    @retry_ydb(idempotent=True)
    def read():
        with engine.connect() as conn:
            return conn.execute(stmt).fetchall()
"""

import functools
import inspect
from typing import Awaitable, Callable, Optional, TypeVar

import sqlalchemy.exc
import ydb

T = TypeVar("T")

DEFAULT_MAX_RETRIES = 10


def _retry_settings(max_retries: int, idempotent: bool) -> ydb.RetrySettings:
    return ydb.RetrySettings(max_retries=max_retries, idempotent=idempotent)


def _unwrap_ydb_error(exc: BaseException) -> Optional[ydb.Error]:
    """Return the ``ydb.Error`` wrapped inside a SQLAlchemy / ydb-dbapi error, if any."""
    original = getattr(getattr(exc, "orig", None), "original_error", None)
    return original if isinstance(original, ydb.Error) else None


def retry_ydb_operation(
    callee: Callable[[], T],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    idempotent: bool = False,
) -> T:
    """Run ``callee``, retrying transient YDB errors up to ``max_retries`` times.

    Set ``idempotent=True`` only when re-running ``callee`` is safe, so that
    ambiguous errors are retried too.
    """

    def attempt() -> T:
        try:
            return callee()
        except sqlalchemy.exc.DBAPIError as exc:
            ydb_error = _unwrap_ydb_error(exc)
            if ydb_error is not None:
                raise ydb_error from exc
            raise

    return ydb.retry_operation_sync(attempt, _retry_settings(max_retries, idempotent))


async def retry_ydb_operation_async(
    callee: Callable[[], Awaitable[T]],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    idempotent: bool = False,
) -> T:
    """Async counterpart of :func:`retry_ydb_operation`."""

    async def attempt() -> T:
        try:
            return await callee()
        except sqlalchemy.exc.DBAPIError as exc:
            ydb_error = _unwrap_ydb_error(exc)
            if ydb_error is not None:
                raise ydb_error from exc
            raise

    return await ydb.retry_operation_async(attempt, _retry_settings(max_retries, idempotent))


def retry_ydb(*, max_retries: int = DEFAULT_MAX_RETRIES, idempotent: bool = False):
    """Decorator that retries transient YDB errors raised by the wrapped function.

    Works on both sync and async functions::

        @retry_ydb(idempotent=True)
        def read(): ...

        @retry_ydb(idempotent=True)
        async def read(): ...
    """

    def decorator(func):
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await retry_ydb_operation_async(
                    lambda: func(*args, **kwargs), max_retries=max_retries, idempotent=idempotent
                )

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return retry_ydb_operation(lambda: func(*args, **kwargs), max_retries=max_retries, idempotent=idempotent)

        return sync_wrapper

    return decorator
