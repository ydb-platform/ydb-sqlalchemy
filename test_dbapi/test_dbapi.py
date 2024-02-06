from contextlib import suppress

import pytest
import pytest_asyncio
import sqlalchemy.util as util
import ydb

import ydb_sqlalchemy.dbapi as dbapi


class BaseDBApiTestSuit:
    def _test_isolation_level_read_only(self, connection: dbapi.Connection, isolation_level: str, read_only: bool):
        connection.cursor().execute(
            dbapi.YdbQuery("CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))", is_ddl=True)
        )
        connection.set_isolation_level(isolation_level)

        cursor = connection.cursor()

        connection.begin()

        query = dbapi.YdbQuery("UPSERT INTO foo(id) VALUES (1)")
        if read_only:
            with pytest.raises(dbapi.DatabaseError):
                cursor.execute(query)
        else:
            cursor.execute(query)

        connection.rollback()

        connection.cursor().execute(dbapi.YdbQuery("DROP TABLE foo", is_ddl=True))
        connection.cursor().close()

    def _test_connection(self, connection: dbapi.Connection):
        connection.commit()
        connection.rollback()

        cur = connection.cursor()
        with suppress(dbapi.DatabaseError):
            cur.execute(dbapi.YdbQuery("DROP TABLE foo", is_ddl=True))

        assert not connection.check_exists("/local/foo")
        with pytest.raises(dbapi.ProgrammingError):
            connection.describe("/local/foo")

        cur.execute(dbapi.YdbQuery("CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))", is_ddl=True))

        assert connection.check_exists("/local/foo")

        col = connection.describe("/local/foo").columns[0]
        assert col.name == "id"
        assert col.type == ydb.PrimitiveType.Int64

        cur.execute(dbapi.YdbQuery("DROP TABLE foo", is_ddl=True))
        cur.close()

    def _test_cursor_raw_query(self, connection: dbapi.Connection):
        cur = connection.cursor()
        assert cur

        with suppress(dbapi.DatabaseError):
            cur.execute(dbapi.YdbQuery("DROP TABLE test", is_ddl=True))

        cur.execute(dbapi.YdbQuery("CREATE TABLE test(id Int64 NOT NULL, text Utf8, PRIMARY KEY (id))", is_ddl=True))

        cur.execute(
            dbapi.YdbQuery(
                """
                DECLARE $data AS List<Struct<id:Int64, text: Utf8>>;

                INSERT INTO test SELECT id, text FROM AS_TABLE($data);
                """,
                parameters_types={
                    "$data": ydb.ListType(
                        ydb.StructType()
                        .add_member("id", ydb.PrimitiveType.Int64)
                        .add_member("text", ydb.PrimitiveType.Utf8)
                    )
                },
            ),
            {
                "$data": [
                    {"id": 17, "text": "seventeen"},
                    {"id": 21, "text": "twenty one"},
                ]
            },
        )

        cur.execute(dbapi.YdbQuery("DROP TABLE test", is_ddl=True))

        cur.close()

    def _test_errors(self, connection: dbapi.Connection):
        with pytest.raises(dbapi.InterfaceError):
            dbapi.YdbDBApi().connect("localhost:2136", database="/local666")

        cur = connection.cursor()

        with suppress(dbapi.DatabaseError):
            cur.execute(dbapi.YdbQuery("DROP TABLE test", is_ddl=True))

        with pytest.raises(dbapi.DataError):
            cur.execute(dbapi.YdbQuery("SELECT 18446744073709551616"))

        with pytest.raises(dbapi.DataError):
            cur.execute(dbapi.YdbQuery("SELECT * FROM 拉屎"))

        with pytest.raises(dbapi.DataError):
            cur.execute(dbapi.YdbQuery("SELECT floor(5 / 2)"))

        with pytest.raises(dbapi.ProgrammingError):
            cur.execute(dbapi.YdbQuery("SELECT * FROM test"))

        cur.execute(dbapi.YdbQuery("CREATE TABLE test(id Int64, PRIMARY KEY (id))", is_ddl=True))

        cur.execute(dbapi.YdbQuery("INSERT INTO test(id) VALUES(1)"))
        with pytest.raises(dbapi.IntegrityError):
            cur.execute(dbapi.YdbQuery("INSERT INTO test(id) VALUES(1)"))

        cur.execute(dbapi.YdbQuery("DROP TABLE test", is_ddl=True))
        cur.close()


class TestSyncConnection(BaseDBApiTestSuit):
    @pytest.fixture
    def sync_connection(self) -> dbapi.Connection:
        conn = dbapi.YdbDBApi().connect(host="localhost", port="2136", database="/local")
        try:
            yield conn
        finally:
            conn.close()

    @pytest.mark.parametrize(
        "isolation_level, read_only",
        [
            (dbapi.IsolationLevel.SERIALIZABLE, False),
            (dbapi.IsolationLevel.AUTOCOMMIT, False),
            (dbapi.IsolationLevel.ONLINE_READONLY, True),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT, True),
            (dbapi.IsolationLevel.STALE_READONLY, True),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY, True),
        ],
    )
    def test_isolation_level_read_only(self, isolation_level: str, read_only: bool, sync_connection: dbapi.Connection):
        self._test_isolation_level_read_only(sync_connection, isolation_level, read_only)

    def test_connection(self, sync_connection: dbapi.Connection):
        self._test_connection(sync_connection)

    def test_cursor_raw_query(self, sync_connection: dbapi.Connection):
        return self._test_cursor_raw_query(sync_connection)

    def test_errors(self, sync_connection: dbapi.Connection):
        return self._test_errors(sync_connection)


class TestAsyncConnection(BaseDBApiTestSuit):
    @pytest_asyncio.fixture
    async def async_connection(self) -> dbapi.AsyncConnection:
        def connect():
            return dbapi.YdbDBApi().async_connect(host="localhost", port="2136", database="/local")

        conn = await util.greenlet_spawn(connect)
        try:
            yield conn
        finally:
            await util.greenlet_spawn(conn.close)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "isolation_level, read_only",
        [
            (dbapi.IsolationLevel.SERIALIZABLE, False),
            (dbapi.IsolationLevel.AUTOCOMMIT, False),
            (dbapi.IsolationLevel.ONLINE_READONLY, True),
            (dbapi.IsolationLevel.ONLINE_READONLY_INCONSISTENT, True),
            (dbapi.IsolationLevel.STALE_READONLY, True),
            (dbapi.IsolationLevel.SNAPSHOT_READONLY, True),
        ],
    )
    async def test_isolation_level_read_only(
        self, isolation_level: str, read_only: bool, async_connection: dbapi.AsyncConnection
    ):
        await util.greenlet_spawn(self._test_isolation_level_read_only, async_connection, isolation_level, read_only)

    @pytest.mark.asyncio
    async def test_connection(self, async_connection: dbapi.AsyncConnection):
        await util.greenlet_spawn(self._test_connection, async_connection)

    @pytest.mark.asyncio
    async def test_cursor_raw_query(self, async_connection: dbapi.AsyncConnection):
        await util.greenlet_spawn(self._test_cursor_raw_query, async_connection)

    @pytest.mark.asyncio
    async def test_errors(self, async_connection: dbapi.AsyncConnection):
        await util.greenlet_spawn(self._test_errors, async_connection)
