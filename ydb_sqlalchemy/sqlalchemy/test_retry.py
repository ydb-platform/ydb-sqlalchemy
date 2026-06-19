import asyncio

import pytest
import sqlalchemy.exc
import ydb
import ydb_dbapi

from .retry import _unwrap_ydb_error, retry_ydb, retry_ydb_operation, retry_ydb_operation_async


def _sa_dbapi_error(ydb_issue):
    """Wrap a ydb issue the way the real stack does: ydb.Error -> ydb-dbapi -> SQLAlchemy."""
    dbapi_error = ydb_dbapi.OperationalError(str(ydb_issue), original_error=ydb_issue)
    return sqlalchemy.exc.OperationalError("SELECT 1", {}, dbapi_error)


def test_unwrap_ydb_error_returns_original():
    issue = ydb.issues.Unavailable("node is down")
    assert _unwrap_ydb_error(_sa_dbapi_error(issue)) is issue


def test_unwrap_ydb_error_none_for_plain_exception():
    assert _unwrap_ydb_error(ValueError("boom")) is None


def test_retry_ydb_operation_passes_through_success():
    assert retry_ydb_operation(lambda: 42) == 42


def test_retry_ydb_operation_retries_transient_then_succeeds():
    issue = ydb.issues.Unavailable("node is down")
    calls = []

    def callee():
        calls.append(1)
        if len(calls) < 3:
            raise _sa_dbapi_error(issue)
        return "ok"

    assert retry_ydb_operation(callee, max_retries=5) == "ok"
    assert len(calls) == 3  # two transient failures, then success


def test_retry_ydb_operation_does_not_retry_non_retryable():
    issue = ydb.issues.BadRequest("bad query")
    calls = []

    def callee():
        calls.append(1)
        raise _sa_dbapi_error(issue)

    with pytest.raises(ydb.issues.BadRequest):
        retry_ydb_operation(callee, max_retries=5)
    assert len(calls) == 1  # not retried


def test_retry_ydb_decorator_sync():
    issue = ydb.issues.Unavailable("node is down")
    calls = []

    @retry_ydb(max_retries=5)
    def op():
        calls.append(1)
        if len(calls) < 3:
            raise _sa_dbapi_error(issue)
        return "ok"

    assert op() == "ok"
    assert len(calls) == 3


def test_retry_ydb_decorator_async():
    issue = ydb.issues.Unavailable("node is down")
    calls = []

    @retry_ydb(max_retries=5)
    async def op():
        calls.append(1)
        if len(calls) < 3:
            raise _sa_dbapi_error(issue)
        return "ok"

    assert asyncio.run(op()) == "ok"
    assert len(calls) == 3


def test_retry_ydb_operation_async_call_form():
    issue = ydb.issues.Unavailable("node is down")
    calls = []

    async def callee():
        calls.append(1)
        if len(calls) < 2:
            raise _sa_dbapi_error(issue)
        return "ok"

    assert asyncio.run(retry_ydb_operation_async(callee, max_retries=5)) == "ok"
    assert len(calls) == 2
