import asyncio
from datetime import date, datetime
from decimal import Decimal
from typing import NamedTuple

import pytest
import sqlalchemy as sa
import ydb
from sqlalchemy import Column, Integer, String, Table, Unicode
from sqlalchemy.testing.fixtures import TablesTest, TestBase, config
from ydb._grpc.v4.protos import ydb_common_pb2

from ydb_sqlalchemy import IsolationLevel, dbapi
from ydb_sqlalchemy import sqlalchemy as ydb_sa
from ydb_sqlalchemy.sqlalchemy import types


def clear_sql(stm):
    return stm.replace("\n", " ").replace("  ", " ").strip()


class TestText(TestBase):
    __backend__ = True

    def test_sa_text(self, connection):
        rs = connection.execute(sa.text("SELECT 1 AS value"))
        assert rs.fetchone() == (1,)

        rs = connection.execute(
            sa.text(
                """
                DECLARE :data AS List<Struct<x:Int64, y:Int64>>;
                SELECT x, y FROM AS_TABLE(:data)
                """
            ),
            [{"data": [{"x": 2, "y": 1}, {"x": 3, "y": 2}]}],
        )
        assert set(rs.fetchall()) == {(2, 1), (3, 2)}


class TestCrud(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("text", Unicode),
        )

    def test_sa_crud(self, connection):
        select_stm = sa.select(self.tables.test)
        assert clear_sql(str(select_stm)) == "SELECT test.id, test.text FROM test"
        assert connection.execute(select_stm).fetchall() == []

        insert_stm = sa.insert(self.tables.test).values(id=2, text="foo")
        assert clear_sql(str(insert_stm)) == "INSERT INTO test (id, text) VALUES (:id, :text)"
        assert connection.execute(insert_stm)

        insert_many = sa.insert(self.tables.test).values([(3, "a"), (4, "b"), (5, "c")])
        assert connection.execute(insert_many)

        rs = connection.execute(select_stm.order_by(self.tables.test.c.id))
        row = rs.fetchone()
        assert row.id == 2
        assert row.text == "foo"

        update_stm = sa.update(self.tables.test).where(self.tables.test.c.id == 2).values(text="bar")
        assert clear_sql(str(update_stm)) == "UPDATE test SET text=:text WHERE test.id = :id_1"
        assert connection.execute(update_stm)

        select_where_stm = sa.select(self.tables.test.c.text).filter(self.tables.test.c.id == 2)
        assert clear_sql(str(select_where_stm)) == "SELECT test.text FROM test WHERE test.id = :id_1"
        assert connection.execute(select_where_stm).fetchall() == [("bar",)]

        delete_stm = sa.delete(self.tables.test).where(self.tables.test.c.id == 2)
        assert connection.execute(delete_stm)
        assert connection.execute(select_stm.order_by(self.tables.test.c.id)).fetchall() == [
            (3, "a"),
            (4, "b"),
            (5, "c"),
        ]


class TestSimpleSelect(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True, nullable=False),
            Column("value", Unicode),
            Column("num", sa.Numeric(22, 9)),
        )

    @classmethod
    def insert_data(cls, connection):
        data = [
            {"id": 1, "value": "some text", "num": Decimal("3.141592653")},
            {"id": 2, "value": "test text", "num": Decimal("3.14159265")},
            {"id": 3, "value": "test test", "num": Decimal("3.1415926")},
            {"id": 4, "value": "text text", "num": Decimal("3.141592")},
            {"id": 5, "value": "some some", "num": Decimal("3.14159")},
            {"id": 6, "value": "some test", "num": Decimal("3.1415")},
            {"id": 7, "value": "text text", "num": Decimal("3.141")},
        ]
        connection.execute(cls.tables.test.insert(), data)

    def test_sa_select_simple(self, connection):
        tb = self.tables.test

        # simple WHERE
        row = connection.execute(sa.select(tb.c.value).where(tb.c.id == 7)).fetchone()
        assert row.value == "text text"

        # simple filter
        row = connection.execute(tb.select().filter(tb.c.id == 7)).fetchone()
        assert row == (7, "text text", Decimal("3.141"))

        # OR operator
        rows = connection.execute(tb.select().where((tb.c.id == 1) | (tb.c.value == "test test"))).fetchall()
        assert set(rows) == {
            (1, "some text", Decimal("3.141592653")),
            (3, "test test", Decimal("3.1415926")),
        }

        # AND operator, LIKE operator
        cur = connection.execute(tb.select().where(tb.c.value.like("some %") & (tb.c.num > Decimal("3.141592"))))
        assert cur.fetchall() == [(1, "some text", Decimal("3.141592653"))]

        # + operator, CAST
        rows = connection.execute(tb.select().where(tb.c.id + sa.cast(tb.c.num, sa.INTEGER) > 9)).fetchall()
        assert rows == [(7, "text text", Decimal("3.141"))]

        # REGEXP matching
        stm = tb.select().where(tb.c.value.regexp_match(r"s\w{3}\ss\w{3}"))
        rows = connection.execute(stm).fetchall()
        assert rows == [(5, "some some", Decimal("3.14159"))]

        stm = sa.select(tb.c.id).where(~tb.c.value.regexp_match(r"s\w{3}\ss\w{3}"))
        rows = connection.execute(stm).fetchall()
        assert set(rows) == {(1,), (2,), (3,), (4,), (6,), (7,)}

        # LIMIT/OFFSET
        # rows = connection.execute(tb.select().order_by(tb.c.id).limit(2)).fetchall()
        # assert rows == [
        #     (1, "some text", Decimal("3.141592653")),
        #     (2, "test text", Decimal("3.14159265")),
        # ]

        # ORDER BY ASC
        rows = connection.execute(sa.select(tb.c.id).order_by(tb.c.id)).fetchall()
        assert rows == [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]

        # ORDER BY DESC
        rows = connection.execute(sa.select(tb.c.id).order_by(tb.c.id.desc())).fetchall()
        assert rows == [(7,), (6,), (5,), (4,), (3,), (2,), (1,)]

        # BETWEEN operator
        rows = connection.execute(sa.select(tb.c.id).filter(tb.c.id.between(3, 5))).fetchall()
        assert set(rows) == {(3,), (4,), (5,)}

        # IN operator
        rows = connection.execute(sa.select(tb.c.id).filter(tb.c.id.in_([1, 3, 5, 7]))).fetchall()
        assert set(rows) == {(1,), (3,), (5,), (7,)}

        # aggregates: MIN, MAX, COUNT, AVG, SUM
        assert connection.execute(sa.func.min(tb.c.id)).first() == (1,)
        assert connection.execute(sa.func.max(tb.c.id)).first() == (7,)
        assert connection.execute(sa.func.count(tb.c.id)).first() == (7,)
        assert connection.execute(sa.func.sum(tb.c.id)).first() == (28,)
        assert connection.execute(sa.func.avg(tb.c.id)).first() == (4,)
        assert connection.execute(sa.func.sum(tb.c.num)).first() == (Decimal("21.990459903"),)
        assert connection.execute(sa.func.avg(tb.c.num)).first() == (Decimal("3.141494272"),)


class TestTypes(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_types",
            metadata,
            Column("id", Integer, primary_key=True),
            # Column("bin", sa.BINARY),
            Column("str", sa.String),
            Column("num", sa.Float),
            Column("bl", sa.Boolean),
            Column("ts", sa.TIMESTAMP),
            Column("date", sa.Date),
            # Column("interval", sa.Interval),
        )

    def test_select_types(self, connection):
        tb = self.tables.test_types

        now, today = datetime.now(), date.today()

        stm = tb.insert().values(
            id=1,
            # bin=b"abc",
            str="Hello World!",
            num=3.5,
            bl=True,
            ts=now,
            date=today,
            # interval=timedelta(minutes=45),
        )
        connection.execute(stm)

        row = connection.execute(sa.select(tb)).fetchone()
        assert row == (1, "Hello World!", 3.5, True, now, today)

    def test_integer_types(self, connection):
        stmt = sa.Select(
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_uint8", 8, types.UInt8))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_uint16", 16, types.UInt16))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_uint32", 32, types.UInt32))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_uint64", 64, types.UInt64))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_int8", -8, types.Int8))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_int16", -16, types.Int16))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_int32", -32, types.Int32))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_int64", -64, types.Int64))),
        )

        result = connection.execute(stmt).fetchone()
        assert result == (b"Uint8", b"Uint16", b"Uint32", b"Uint64", b"Int8", b"Int16", b"Int32", b"Int64")


class TestWithClause(TablesTest):
    __backend__ = True
    run_create_tables = "each"

    @staticmethod
    def _create_table_and_get_desc(connection, metadata, **kwargs):
        table = Table(
            "clause_with_test",
            metadata,
            Column("id", types.UInt32, primary_key=True),
            **kwargs,
        )
        table.create(connection)

        return connection.connection.driver_connection.describe(table.name)

    @pytest.mark.parametrize(
        "auto_partitioning_by_size,res",
        [
            (None, ydb_common_pb2.FeatureFlag.Status.ENABLED),
            (True, ydb_common_pb2.FeatureFlag.Status.ENABLED),
            (False, ydb_common_pb2.FeatureFlag.Status.DISABLED),
        ],
    )
    def test_auto_partitioning_by_size(self, connection, auto_partitioning_by_size, res, metadata):
        desc = self._create_table_and_get_desc(
            connection, metadata, ydb_auto_partitioning_by_size=auto_partitioning_by_size
        )
        assert desc.partitioning_settings.partitioning_by_size == res

    @pytest.mark.parametrize(
        "auto_partitioning_by_load,res",
        [
            (None, ydb_common_pb2.FeatureFlag.Status.DISABLED),
            (True, ydb_common_pb2.FeatureFlag.Status.ENABLED),
            (False, ydb_common_pb2.FeatureFlag.Status.DISABLED),
        ],
    )
    def test_auto_partitioning_by_load(self, connection, auto_partitioning_by_load, res, metadata):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_auto_partitioning_by_load=auto_partitioning_by_load,
        )
        assert desc.partitioning_settings.partitioning_by_load == res

    @pytest.mark.parametrize(
        "auto_partitioning_partition_size_mb,res",
        [
            (None, 2048),
            (2000, 2000),
        ],
    )
    def test_auto_partitioning_partition_size_mb(self, connection, auto_partitioning_partition_size_mb, res, metadata):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_auto_partitioning_partition_size_mb=auto_partitioning_partition_size_mb,
        )
        assert desc.partitioning_settings.partition_size_mb == res

    @pytest.mark.parametrize(
        "auto_partitioning_min_partitions_count,res",
        [
            (None, 1),
            (10, 10),
        ],
    )
    def test_auto_partitioning_min_partitions_count(
        self,
        connection,
        auto_partitioning_min_partitions_count,
        res,
        metadata,
    ):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_auto_partitioning_min_partitions_count=auto_partitioning_min_partitions_count,
        )
        assert desc.partitioning_settings.min_partitions_count == res

    @pytest.mark.parametrize(
        "auto_partitioning_max_partitions_count,res",
        [
            (None, 0),
            (10, 10),
        ],
    )
    def test_auto_partitioning_max_partitions_count(
        self,
        connection,
        auto_partitioning_max_partitions_count,
        res,
        metadata,
    ):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_auto_partitioning_max_partitions_count=auto_partitioning_max_partitions_count,
        )
        assert desc.partitioning_settings.max_partitions_count == res

    @pytest.mark.parametrize(
        "uniform_partitions,res",
        [
            (None, 1),
            (10, 10),
        ],
    )
    def test_uniform_partitions(
        self,
        connection,
        uniform_partitions,
        res,
        metadata,
    ):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_uniform_partitions=uniform_partitions,
        )
        # it not only do the initiation partition but also set up the minimum partition count
        assert desc.partitioning_settings.min_partitions_count == res

    @pytest.mark.parametrize(
        "partition_at_keys,res",
        [
            (None, 1),
            ((100, 1000), 3),
        ],
    )
    def test_partition_at_keys(
        self,
        connection,
        partition_at_keys,
        res,
        metadata,
    ):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_partition_at_keys=partition_at_keys,
        )
        assert desc.partitioning_settings.min_partitions_count == res

    def test_several_keys(self, connection, metadata):
        desc = self._create_table_and_get_desc(
            connection,
            metadata,
            ydb_auto_partitioning_by_size=True,
            ydb_auto_partitioning_by_load=True,
            ydb_auto_partitioning_min_partitions_count=3,
            ydb_auto_partitioning_max_partitions_count=5,
        )
        assert desc.partitioning_settings.partitioning_by_size == 1
        assert desc.partitioning_settings.partitioning_by_load == 1
        assert desc.partitioning_settings.min_partitions_count == 3
        assert desc.partitioning_settings.max_partitions_count == 5


class TestTransaction(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata: sa.MetaData):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
        )

    def test_rollback(self, connection_no_trans: sa.Connection, connection: sa.Connection):
        table = self.tables.test

        connection_no_trans.execution_options(isolation_level=IsolationLevel.SERIALIZABLE)
        with connection_no_trans.begin():
            stm1 = table.insert().values(id=1)
            connection_no_trans.execute(stm1)
            stm2 = table.insert().values(id=2)
            connection_no_trans.execute(stm2)
            connection_no_trans.rollback()

        cursor = connection.execute(sa.select(table))
        result = cursor.fetchall()
        assert result == []

    def test_commit(self, connection_no_trans: sa.Connection, connection: sa.Connection):
        table = self.tables.test

        connection_no_trans.execution_options(isolation_level=IsolationLevel.SERIALIZABLE)
        with connection_no_trans.begin():
            stm1 = table.insert().values(id=3)
            connection_no_trans.execute(stm1)
            stm2 = table.insert().values(id=4)
            connection_no_trans.execute(stm2)

        cursor = connection.execute(sa.select(table))
        result = cursor.fetchall()
        assert set(result) == {(3,), (4,)}

    @pytest.mark.parametrize("isolation_level", (IsolationLevel.SERIALIZABLE, IsolationLevel.SNAPSHOT_READONLY))
    def test_interactive_transaction(
        self, connection_no_trans: sa.Connection, connection: sa.Connection, isolation_level
    ):
        table = self.tables.test
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        stm1 = table.insert().values([{"id": 5}, {"id": 6}])
        connection.execute(stm1)

        connection_no_trans.execution_options(isolation_level=isolation_level)
        with connection_no_trans.begin():
            tx_id = dbapi_connection.tx_context.tx_id
            assert tx_id is not None
            cursor1 = connection_no_trans.execute(sa.select(table))
            cursor2 = connection_no_trans.execute(sa.select(table))
            assert dbapi_connection.tx_context.tx_id == tx_id

        assert set(cursor1.fetchall()) == {(5,), (6,)}
        assert set(cursor2.fetchall()) == {(5,), (6,)}

    @pytest.mark.parametrize(
        "isolation_level",
        (
            IsolationLevel.ONLINE_READONLY,
            IsolationLevel.ONLINE_READONLY_INCONSISTENT,
            IsolationLevel.STALE_READONLY,
            IsolationLevel.AUTOCOMMIT,
        ),
    )
    def test_not_interactive_transaction(
        self, connection_no_trans: sa.Connection, connection: sa.Connection, isolation_level
    ):
        table = self.tables.test
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        stm1 = table.insert().values([{"id": 7}, {"id": 8}])
        connection.execute(stm1)

        connection_no_trans.execution_options(isolation_level=isolation_level)
        with connection_no_trans.begin():
            assert dbapi_connection.tx_context is None
            cursor1 = connection_no_trans.execute(sa.select(table))
            cursor2 = connection_no_trans.execute(sa.select(table))
            assert dbapi_connection.tx_context is None

        assert set(cursor1.fetchall()) == {(7,), (8,)}
        assert set(cursor2.fetchall()) == {(7,), (8,)}


class TestTransactionIsolationLevel(TestBase):
    __backend__ = True

    class IsolationSettings(NamedTuple):
        ydb_mode: ydb.AbstractTransactionModeBuilder
        interactive: bool

    YDB_ISOLATION_SETTINGS_MAP = {
        IsolationLevel.AUTOCOMMIT: IsolationSettings(ydb.SerializableReadWrite().name, False),
        IsolationLevel.SERIALIZABLE: IsolationSettings(ydb.SerializableReadWrite().name, True),
        IsolationLevel.ONLINE_READONLY: IsolationSettings(ydb.OnlineReadOnly().name, False),
        IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
            ydb.OnlineReadOnly().with_allow_inconsistent_reads().name, False
        ),
        IsolationLevel.STALE_READONLY: IsolationSettings(ydb.StaleReadOnly().name, False),
        IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(ydb.SnapshotReadOnly().name, True),
    }

    def test_connection_set(self, connection_no_trans: sa.Connection):
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        for sa_isolation_level, ydb_isolation_settings in self.YDB_ISOLATION_SETTINGS_MAP.items():
            connection_no_trans.execution_options(isolation_level=sa_isolation_level)
            with connection_no_trans.begin():
                assert dbapi_connection.tx_mode.name == ydb_isolation_settings[0]
                assert dbapi_connection.interactive_transaction is ydb_isolation_settings[1]
                if dbapi_connection.interactive_transaction:
                    assert dbapi_connection.tx_context is not None
                    assert dbapi_connection.tx_context.tx_id is not None
                else:
                    assert dbapi_connection.tx_context is None


class TestEngine(TestBase):
    __backend__ = True
    __only_on__ = "yql+ydb"

    @pytest.fixture(scope="class")
    def ydb_driver(self):
        url = config.db_url
        driver = ydb.Driver(endpoint=f"grpc://{url.host}:{url.port}", database=url.database)
        try:
            driver.wait(timeout=5, fail_fast=True)
            yield driver
        finally:
            driver.stop()

        driver.stop()

    @pytest.fixture(scope="class")
    def ydb_pool(self, ydb_driver):
        session_pool = ydb.SessionPool(ydb_driver, size=5, workers_threads_count=1)

        try:
            yield session_pool
        finally:
            session_pool.stop()

    def test_sa_queue_pool_with_ydb_shared_session_pool(self, ydb_driver, ydb_pool):
        engine1 = sa.create_engine(config.db_url, poolclass=sa.QueuePool, connect_args={"ydb_session_pool": ydb_pool})
        engine2 = sa.create_engine(config.db_url, poolclass=sa.QueuePool, connect_args={"ydb_session_pool": ydb_pool})

        with engine1.connect() as conn1, engine2.connect() as conn2:
            dbapi_conn1: dbapi.Connection = conn1.connection.dbapi_connection
            dbapi_conn2: dbapi.Connection = conn2.connection.dbapi_connection

            assert dbapi_conn1.session_pool is dbapi_conn2.session_pool
            assert dbapi_conn1.driver is dbapi_conn2.driver

        engine1.dispose()
        engine2.dispose()
        assert not ydb_driver._stopped

    def test_sa_null_pool_with_ydb_shared_session_pool(self, ydb_driver, ydb_pool):
        engine1 = sa.create_engine(config.db_url, poolclass=sa.NullPool, connect_args={"ydb_session_pool": ydb_pool})
        engine2 = sa.create_engine(config.db_url, poolclass=sa.NullPool, connect_args={"ydb_session_pool": ydb_pool})

        with engine1.connect() as conn1, engine2.connect() as conn2:
            dbapi_conn1: dbapi.Connection = conn1.connection.dbapi_connection
            dbapi_conn2: dbapi.Connection = conn2.connection.dbapi_connection

            assert dbapi_conn1.session_pool is dbapi_conn2.session_pool
            assert dbapi_conn1.driver is dbapi_conn2.driver

        engine1.dispose()
        engine2.dispose()
        assert not ydb_driver._stopped


class TestAsyncEngine(TestEngine):
    __only_on__ = "yql+ydb_async"

    @pytest.fixture(scope="class")
    def ydb_driver(self):
        loop = asyncio.get_event_loop()
        url = config.db_url
        driver = ydb.aio.Driver(endpoint=f"grpc://{url.host}:{url.port}", database=url.database)
        try:
            loop.run_until_complete(driver.wait(timeout=5, fail_fast=True))
            yield driver
        finally:
            loop.run_until_complete(driver.stop())

    @pytest.fixture(scope="class")
    def ydb_pool(self, ydb_driver):
        session_pool = ydb.aio.SessionPool(ydb_driver, size=5)

        try:
            yield session_pool
        finally:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(session_pool.stop())


class TestUpsert(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_upsert",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("val", Integer),
        )

    def test_string(self, connection):
        tb = self.tables.test_upsert
        stm = ydb_sa.upsert(tb).values(id=0, val=5)

        assert str(stm) == "UPSERT INTO test_upsert (id, val) VALUES (?, ?)"

    def test_upsert_new_id(self, connection):
        tb = self.tables.test_upsert
        stm = ydb_sa.upsert(tb).values(id=0, val=1)
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()
        assert row == [(0, 1)]

        stm = ydb_sa.upsert(tb).values(id=1, val=2)
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()
        assert row == [(0, 1), (1, 2)]

    def test_upsert_existing_id(self, connection):
        tb = self.tables.test_upsert
        stm = ydb_sa.upsert(tb).values(id=0, val=5)
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()

        assert row == [(0, 5)]

        stm = ydb_sa.upsert(tb).values(id=0, val=6)
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()

        assert row == [(0, 6)]

    def test_upsert_several_diff_id(self, connection):
        tb = self.tables.test_upsert
        stm = ydb_sa.upsert(tb).values(
            [
                {"id": 0, "val": 4},
                {"id": 1, "val": 5},
                {"id": 2, "val": 6},
            ]
        )
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()

        assert row == [(0, 4), (1, 5), (2, 6)]

    def test_upsert_several_same_id(self, connection):
        tb = self.tables.test_upsert
        stm = ydb_sa.upsert(tb).values(
            [
                {"id": 0, "val": 4},
                {"id": 0, "val": 5},
                {"id": 0, "val": 6},
            ]
        )
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchall()

        assert row == [(0, 6)]

    def test_upsert_from_select(self, connection, metadata):
        table_to_select_from = Table(
            "table_to_select_from",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("val", Integer),
        )
        table_to_select_from.create(connection)
        stm = sa.insert(table_to_select_from).values(
            [
                {"id": 100, "val": 0},
                {"id": 110, "val": 1},
                {"id": 120, "val": 2},
                {"id": 130, "val": 3},
            ]
        )
        connection.execute(stm)

        tb = self.tables.test_upsert
        select_stm = sa.select(table_to_select_from.c.id, table_to_select_from.c.val).where(
            table_to_select_from.c.id > 115,
        )
        upsert_stm = ydb_sa.upsert(tb).from_select(["id", "val"], select_stm)
        connection.execute(upsert_stm)
        row = connection.execute(sa.select(tb)).fetchall()

        assert row == [(120, 2), (130, 3)]


class TestUpsertDoesNotReplaceInsert(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_upsert_does_not_replace_insert",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("VALUE_TO_INSERT", String),
        )

    def test_string(self, connection):
        tb = self.tables.test_upsert_does_not_replace_insert

        stm = ydb_sa.upsert(tb).values(id=0, VALUE_TO_INSERT="5")

        assert str(stm) == "UPSERT INTO test_upsert_does_not_replace_insert (id, `VALUE_TO_INSERT`) VALUES (?, ?)"

    def test_insert_in_name(self, connection):
        tb = self.tables.test_upsert_does_not_replace_insert
        stm = ydb_sa.upsert(tb).values(id=1, VALUE_TO_INSERT="5")
        connection.execute(stm)
        row = connection.execute(sa.select(tb).where(tb.c.id == 1)).fetchone()

        assert row == (1, "5")

    def test_insert_in_name_and_field(self, connection):
        tb = self.tables.test_upsert_does_not_replace_insert
        stm = ydb_sa.upsert(tb).values(id=2, VALUE_TO_INSERT="INSERT is my favourite operation")
        connection.execute(stm)
        row = connection.execute(sa.select(tb).where(tb.c.id == 2)).fetchone()

        assert row == (2, "INSERT is my favourite operation")
