from .connection import Connection, IsolationLevel  # noqa: F401
from .cursor import Cursor, YdbQuery  # noqa: F401
from .errors import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

apilevel = "1.0"

threadsafety = 0

paramstyle = "pyformat"

errors = (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
