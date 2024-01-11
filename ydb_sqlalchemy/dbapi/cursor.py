import dataclasses
import itertools
import logging

from typing import Any, Mapping, Optional, Sequence, Union, Dict

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


@dataclasses.dataclass
class YdbQuery:
    yql_text: str
    parameters_types: Dict[str, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]] = dataclasses.field(
        default_factory=dict
    )
    is_ddl: bool = False


class Cursor(object):
    def __init__(self, connection, transaction: Optional[ydb.BaseTxContext] = None):
        self.connection = connection
        self.session: ydb.Session = self.connection.session
        self.transaction = transaction
        self.description = None
        self.arraysize = 1
        self.rows = None
        self._rows_prefetched = None

    def execute(self, operation: YdbQuery, parameters: Optional[Mapping[str, Any]] = None):
        if operation.is_ddl or not operation.parameters_types:
            query = operation.yql_text
            is_ddl = operation.is_ddl
        else:
            query = ydb.DataQuery(operation.yql_text, operation.parameters_types)
            is_ddl = operation.is_ddl

        chunks = self._execute(query, parameters, is_ddl)
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

    def _execute(self, query: Union[ydb.DataQuery, str], parameters: Optional[Mapping[str, Any]], is_ddl: bool):
        self.description = None
        logger.info("execute sql: %s, params: %s", query, parameters)
        try:
            if is_ddl:
                return ydb.retry_operation_sync(lambda: self.session.execute_scheme(query))

            prepared_query = query
            if isinstance(query, str) and parameters:
                prepared_query = self.session.prepare(query)

            if not self.transaction:
                return ydb.retry_operation_sync(
                    lambda: self.session.transaction().execute(prepared_query, parameters, commit_tx=True)
                )
            else:
                return self.transaction.execute(prepared_query, parameters)
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

    def executemany(self, operation: YdbQuery, seq_of_parameters: Optional[Sequence[Mapping[str, Any]]]):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

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
