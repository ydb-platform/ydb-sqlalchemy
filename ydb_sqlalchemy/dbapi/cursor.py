import itertools
import logging

from typing import Any

import ydb
from .errors import (
    InternalError,
    IntegrityError,
    DataError,
    DatabaseError,
    ProgrammingError,
    OperationalError,
    NotSupportedError,
)


logger = logging.getLogger(__name__)


def get_column_type(type_obj: Any) -> str:
    return str(ydb.convert.type_to_native(type_obj))


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.arraysize = 1
        self.rows = None
        self._rows_prefetched = None

    def execute(self, sql, parameters=None, context=None):
        self.description = None

        if parameters:
            sql = sql % {k: f"${k}" for k, v in parameters.items()}
            sql_params = {f"${k}": v for k, v in parameters.items()}
        else:
            sql_params = parameters
        sql = sql.replace("%%", "%")
        sql = sql.replace(";,", ";\n")

        logger.info("execute sql: %s, params: %s", sql, sql_params)

        def _execute_in_pool(cli):
            try:
                if context and context.get("isddl"):
                    return cli.execute_scheme(sql)
                else:
                    prepared_query = cli.prepare(sql)
                    return cli.transaction().execute(prepared_query, sql_params, commit_tx=True)
            except (ydb.issues.AlreadyExists, ydb.issues.PreconditionFailed) as e:
                raise IntegrityError(e.message, e.issues, e.status) from e
            except (ydb.issues.Unsupported, ydb.issues.Unimplemented) as e:
                raise NotSupportedError(e.message, e.issues, e.status) from e
            except (ydb.issues.BadRequest, ydb.issues.SchemeError) as e:
                raise ProgrammingError(e.message, e.issues, e.status) from e
            except (
                ydb.issues.TruncatedResponseError,
                ydb.issues.ConnectionError,
                ydb.issues.Aborted,
                ydb.issues.Unavailable,
                ydb.issues.Overloaded,
                ydb.issues.Undetermined,
                ydb.issues.Timeout,
                ydb.issues.Cancelled,
                ydb.issues.SessionBusy,
                ydb.issues.SessionExpired,
                ydb.issues.SessionPoolEmpty,
            ) as e:
                raise OperationalError(e.message, e.issues, e.status) from e
            except ydb.issues.GenericError as e:
                raise DataError(e.message, e.issues, e.status) from e
            except ydb.issues.InternalError as e:
                raise InternalError(e.message, e.issues, e.status) from e
            except ydb.Error as e:
                raise DatabaseError(e.message, e.issues, e.status) from e

        chunks = self.connection.pool.retry_operation_sync(_execute_in_pool)
        rows = self._rows_iterable(chunks)
        # Prefetch the description:
        try:
            first_row = next(rows)
        except StopIteration:
            pass
        else:
            rows = itertools.chain((first_row,), rows)
        if self.rows is not None:
            rows = itertools.chain(self.rows, rows)

        self.rows = rows

    def _rows_iterable(self, chunks_iterable):
        try:
            for chunk in chunks_iterable:
                self.description = [
                    (
                        col.name,
                        get_column_type(col.type),
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    for col in chunk.columns
                ]
                for row in chunk.rows:
                    # returns tuple to be compatible with SqlAlchemy and because
                    #  of this PEP to return a sequence: https://www.python.org/dev/peps/pep-0249/#fetchmany
                    yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status) from e

    def _ensure_prefetched(self):
        if self.rows is not None and self._rows_prefetched is None:
            self._rows_prefetched = list(self.rows)
            self.rows = iter(self._rows_prefetched)
        return self._rows_prefetched

    def executemany(self, sql, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def executescript(self, script):
        return self.execute(script)

    def fetchone(self):
        return next(self.rows or [], None)

    def fetchmany(self, size=None):
        return list(itertools.islice(self.rows, size or self.arraysize))

    def fetchall(self):
        return list(self.rows)

    def nextset(self):
        self.fetchall()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    def close(self):
        self.rows = None
        self._rows_prefetched = None

    @property
    def rowcount(self):
        return len(self._ensure_prefetched())
