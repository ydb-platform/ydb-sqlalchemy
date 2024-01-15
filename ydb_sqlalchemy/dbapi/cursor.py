import dataclasses
import itertools
import logging
from typing import Any, Mapping, Optional, Sequence, Union, Dict, Callable

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
    def __init__(
        self,
        session_pool: ydb.SessionPool,
        tx_context: Optional[ydb.BaseTxContext] = None,
    ):
        self.session_pool = session_pool
        self.tx_context = tx_context
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

        logger.info("execute sql: %s, params: %s", query, parameters)
        if is_ddl:
            chunks = self.session_pool.retry_operation_sync(self._execute_ddl, None, query)
        else:
            if self.tx_context:
                chunks = self._execute_dml(self.tx_context.session, query, parameters, self.tx_context)
            else:
                chunks = self.session_pool.retry_operation_sync(self._execute_dml, None, query, parameters)

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

    @classmethod
    def _execute_dml(
        cls,
        session: ydb.Session,
        query: ydb.DataQuery,
        parameters: Optional[Mapping[str, Any]] = None,
        tx_context: Optional[ydb.BaseTxContext] = None,
    ) -> ydb.convert.ResultSets:
        prepared_query = query
        if isinstance(query, str) and parameters:
            prepared_query = session.prepare(query)

        if tx_context:
            return cls._handle_ydb_errors(tx_context.execute, prepared_query, parameters)

        return cls._handle_ydb_errors(session.transaction().execute, prepared_query, parameters, commit_tx=True)

    @classmethod
    def _execute_ddl(cls, session: ydb.Session, query: str) -> ydb.convert.ResultSets:
        return cls._handle_ydb_errors(session.execute_scheme, query)

    @staticmethod
    def _handle_ydb_errors(callee: Callable, *args, **kwargs) -> Any:
        try:
            return callee(*args, **kwargs)
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

    def _rows_iterable(self, chunks_iterable: ydb.convert.ResultSets):
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
