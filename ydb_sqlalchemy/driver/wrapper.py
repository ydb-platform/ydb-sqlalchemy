from sqlalchemy.engine.interfaces import AdaptedConnection

from sqlalchemy.util.concurrency import await_only
from ydb_dbapi import AsyncConnection, AsyncCursor


class AdaptedAsyncConnection(AdaptedConnection):
    def __init__(self, connection: AsyncConnection):
        self._connection: AsyncConnection = connection

    @property
    def connection(self):
        return self._connection

    def cursor(self):
        return AdaptedAsyncCursor(self._connection.cursor())

    def commit(self):
        return await_only(self._connection.commit())

    def rollback(self):
        return await_only(self._connection.rollback())

    def close(self):
        return await_only(self._connection.close())

    def set_isolation_level(self, level):
        return await_only(self._connection.set_isolation_level(level))

    def get_isolation_level(self):
        return self._connection.get_isolation_level()

    def describe(self, table_path: str):
        return self._connection.describe(table_path)

    def check_exists(self, table_path: str):
        return self._connection.check_exists(table_path)

    def get_table_names(self):
        return self._connection.get_table_names()


class AdaptedAsyncCursor:
    def __init__(self, cursor: AsyncCursor):
        self._cursor = cursor

    @property
    def description(self):
        return self._cursor.description

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def fetchone(self):
        return await_only(self._cursor.fetchone())

    def fetchmany(self, size = None):
        return await_only(self._cursor.fetchmany(size=size))

    def fetchall(self):
        return await_only(self._cursor.fetchall())

    def execute(self, sql, parameters=None):
        return await_only(self._cursor.execute(sql, parameters))

    def executemany(self, sql, parameters=None):
        return await_only(self._cursor.executemany(sql, parameters))

    def close(self):
        return await_only(self._cursor.close())

    def setinputsizes(self, *args):
        pass

    def setoutputsizes(self, *args):
        pass

