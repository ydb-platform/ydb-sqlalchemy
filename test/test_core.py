import asyncio
import datetime
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

if sa.__version__ >= "2.":
    from sqlalchemy import NullPool
    from sqlalchemy import QueuePool
else:
    from sqlalchemy.pool import NullPool
    from sqlalchemy.pool import QueuePool


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
            [
                {
                    "data": ydb.TypedValue(
                        [{"x": 2, "y": 1}, {"x": 3, "y": 2}],
                        ydb.ListType(
                            ydb.StructType()
                            .add_member("x", ydb.PrimitiveType.Int64)
                            .add_member("y", ydb.PrimitiveType.Int64)
                        ),
                    )
                }
            ],
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

    def test_cached_query(self, connection_no_trans, connection):
        table = self.tables.test

        with connection_no_trans.begin() as transaction:
            connection_no_trans.execute(sa.insert(table).values([{"id": 1, "text": "foo"}]))
            connection_no_trans.execute(sa.insert(table).values([{"id": 2, "text": None}]))
            connection_no_trans.execute(sa.insert(table).values([{"id": 3, "text": "bar"}]))
            transaction.commit()

        result = connection.execute(sa.select(table)).fetchall()

        assert result == [(1, "foo"), (2, None), (3, "bar")]

    def test_sa_crud_with_add_declare(self):
        engine = sa.create_engine(config.db_url, _add_declare_for_yql_stmt_vars=True)
        with engine.connect() as connection:
            self.test_sa_crud(connection)


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
    def define_tables(cls, metadata: sa.MetaData):
        Table(
            "test_primitive_types",
            metadata,
            Column("int", sa.Integer, primary_key=True),
            # Column("bin", sa.BINARY),
            Column("str", sa.String),
            Column("float", sa.Float),
            Column("bool", sa.Boolean),
        )
        Table(
            "test_datetime_types",
            metadata,
            Column("datetime", sa.DATETIME, primary_key=True),
            Column("datetime_tz", sa.DATETIME(timezone=True)),
            Column("timestamp", sa.TIMESTAMP),
            Column("timestamp_tz", sa.TIMESTAMP(timezone=True)),
            Column("date", sa.Date),
            # Column("interval", sa.Interval),
        )

    def test_primitive_types(self, connection):
        table = self.tables.test_primitive_types

        statement = sa.insert(table).values(
            int=42,
            # bin=b"abc",
            str="Hello World!",
            float=3.5,
            bool=True,
            # interval=timedelta(minutes=45),
        )
        connection.execute(statement)

        row = connection.execute(sa.select(table)).fetchone()
        assert row == (42, "Hello World!", 3.5, True)

    def test_integer_types(self, connection):
        stmt = sa.select(
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

    def test_datetime_types(self, connection):
        stmt = sa.select(
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_datetime", datetime.datetime.now(), sa.DateTime))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_DATETIME", datetime.datetime.now(), sa.DATETIME))),
            sa.func.FormatType(sa.func.TypeOf(sa.bindparam("p_TIMESTAMP", datetime.datetime.now(), sa.TIMESTAMP))),
        )

        result = connection.execute(stmt).fetchone()
        assert result == (b"Timestamp", b"Datetime", b"Timestamp")

    def test_datetime_types_timezone(self, connection):
        table = self.tables.test_datetime_types
        tzinfo = datetime.timezone(datetime.timedelta(hours=3, minutes=42))

        timestamp_value = datetime.datetime.now()
        timestamp_value_tz = timestamp_value.replace(tzinfo=tzinfo)
        datetime_value = timestamp_value.replace(microsecond=0)
        datetime_value_tz = timestamp_value_tz.replace(microsecond=0)
        today = timestamp_value.date()

        statement = sa.insert(table).values(
            datetime=datetime_value,
            datetime_tz=datetime_value_tz,
            timestamp=timestamp_value,
            timestamp_tz=timestamp_value_tz,
            date=today,
            # interval=datetime.timedelta(minutes=45),
        )
        connection.execute(statement)

        row = connection.execute(sa.select(table)).fetchone()

        assert row == (
            datetime_value,
            datetime_value_tz.astimezone(datetime.timezone.utc),  # YDB doesn't store timezone, so it is always utc
            timestamp_value,
            timestamp_value_tz.astimezone(datetime.timezone.utc),  # YDB doesn't store timezone, so it is always utc
            today,
        )


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

    @pytest.mark.skipif(sa.__version__ < "2.", reason="Something was different in SA<2, good to fix")
    def test_rollback(self, connection_no_trans, connection):
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

    def test_commit(self, connection_no_trans, connection):
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
    def test_interactive_transaction(self, connection_no_trans, connection, isolation_level):
        table = self.tables.test
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        stm1 = table.insert().values([{"id": 5}, {"id": 6}])
        connection.execute(stm1)

        connection_no_trans.execution_options(isolation_level=isolation_level)
        with connection_no_trans.begin():
            cursor1 = connection_no_trans.execute(sa.select(table))
            tx_id = dbapi_connection._tx_context.tx_id
            assert tx_id is not None
            cursor2 = connection_no_trans.execute(sa.select(table))
            assert dbapi_connection._tx_context.tx_id == tx_id

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
    def test_not_interactive_transaction(self, connection_no_trans, connection, isolation_level):
        table = self.tables.test
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        stm1 = table.insert().values([{"id": 7}, {"id": 8}])
        connection.execute(stm1)

        connection_no_trans.execution_options(isolation_level=isolation_level)
        with connection_no_trans.begin():
            assert dbapi_connection._tx_context is None
            cursor1 = connection_no_trans.execute(sa.select(table))
            cursor2 = connection_no_trans.execute(sa.select(table))
            assert dbapi_connection._tx_context is None

        assert set(cursor1.fetchall()) == {(7,), (8,)}
        assert set(cursor2.fetchall()) == {(7,), (8,)}


class TestTransactionIsolationLevel(TestBase):
    __backend__ = True

    class IsolationSettings(NamedTuple):
        ydb_mode: ydb.AbstractTransactionModeBuilder
        interactive: bool

    YDB_ISOLATION_SETTINGS_MAP = {
        IsolationLevel.AUTOCOMMIT: IsolationSettings(ydb.QuerySerializableReadWrite().name, False),
        IsolationLevel.SERIALIZABLE: IsolationSettings(ydb.QuerySerializableReadWrite().name, True),
        IsolationLevel.ONLINE_READONLY: IsolationSettings(ydb.QueryOnlineReadOnly().name, False),
        IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
            ydb.QueryOnlineReadOnly().with_allow_inconsistent_reads().name, False
        ),
        IsolationLevel.STALE_READONLY: IsolationSettings(ydb.QueryStaleReadOnly().name, False),
        IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(ydb.QuerySnapshotReadOnly().name, True),
    }

    def test_connection_set(self, connection_no_trans):
        dbapi_connection: dbapi.Connection = connection_no_trans.connection.dbapi_connection

        for sa_isolation_level, ydb_isolation_settings in self.YDB_ISOLATION_SETTINGS_MAP.items():
            connection_no_trans.execution_options(isolation_level=sa_isolation_level)
            with connection_no_trans.begin():
                assert dbapi_connection._tx_mode.name == ydb_isolation_settings[0]
                assert dbapi_connection.interactive_transaction is ydb_isolation_settings[1]
                if dbapi_connection.interactive_transaction:
                    assert dbapi_connection._tx_context is not None
                    # assert dbapi_connection._tx_context.tx_id is not None
                else:
                    assert dbapi_connection._tx_context is None


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
        session_pool = ydb.QuerySessionPool(ydb_driver, size=5)

        try:
            yield session_pool
        finally:
            session_pool.stop()

    def test_sa_queue_pool_with_ydb_shared_session_pool(self, ydb_driver, ydb_pool):
        engine1 = sa.create_engine(config.db_url, poolclass=QueuePool, connect_args={"ydb_session_pool": ydb_pool})
        engine2 = sa.create_engine(config.db_url, poolclass=QueuePool, connect_args={"ydb_session_pool": ydb_pool})

        with engine1.connect() as conn1, engine2.connect() as conn2:
            dbapi_conn1: dbapi.Connection = conn1.connection.dbapi_connection
            dbapi_conn2: dbapi.Connection = conn2.connection.dbapi_connection

            assert dbapi_conn1._session_pool is dbapi_conn2._session_pool
            assert dbapi_conn1._driver is dbapi_conn2._driver

        engine1.dispose()
        engine2.dispose()
        assert not ydb_driver._stopped

    def test_sa_null_pool_with_ydb_shared_session_pool(self, ydb_driver, ydb_pool):
        engine1 = sa.create_engine(config.db_url, poolclass=NullPool, connect_args={"ydb_session_pool": ydb_pool})
        engine2 = sa.create_engine(config.db_url, poolclass=NullPool, connect_args={"ydb_session_pool": ydb_pool})

        with engine1.connect() as conn1, engine2.connect() as conn2:
            dbapi_conn1: dbapi.Connection = conn1.connection.dbapi_connection
            dbapi_conn2: dbapi.Connection = conn2.connection.dbapi_connection

            assert dbapi_conn1._session_pool is dbapi_conn2._session_pool
            assert dbapi_conn1._driver is dbapi_conn2._driver

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

    @pytest.mark.asyncio
    @pytest.fixture(scope="class")
    def ydb_pool(self, ydb_driver):
        loop = asyncio.get_event_loop()
        session_pool = ydb.aio.QuerySessionPool(ydb_driver, size=5, loop=loop)

        try:
            yield session_pool
        finally:
            loop.run_until_complete(session_pool.stop())


class TestCredentials(TestBase):
    __backend__ = True
    __only_on__ = "yql+ydb"

    @pytest.fixture(scope="class")
    def query_client_settings(self):
        yield (
            ydb.QueryClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )

    @pytest.fixture(scope="class")
    def driver_config_for_credentials(self, query_client_settings):
        url = config.db_url
        endpoint = f"grpc://{url.host}:{url.port}"
        database = url.database

        yield ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            query_client_settings=query_client_settings,
        )

    def test_ydb_credentials_good(self, query_client_settings, driver_config_for_credentials):
        credentials_good = ydb.StaticCredentials(
            driver_config=driver_config_for_credentials,
            user="root",
            password="1234",
        )
        engine = sa.create_engine(config.db_url, connect_args={"credentials": credentials_good})
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT 1 as value"))
            assert result.fetchone()

    def test_ydb_credentials_bad(self, query_client_settings, driver_config_for_credentials):
        credentials_bad = ydb.StaticCredentials(
            driver_config=driver_config_for_credentials,
            user="root",
            password="56",
        )
        engine = sa.create_engine(config.db_url, connect_args={"credentials": credentials_bad})
        with pytest.raises(Exception) as excinfo:
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1 as value"))
        assert "Invalid password" in str(excinfo.value)


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


class TestSecondaryIndex(TestBase):
    __backend__ = True

    def test_column_indexes(self, connection, metadata: sa.MetaData):
        table = Table(
            "test_column_indexes/table",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("index_col1", sa.Integer, index=True),
            sa.Column("index_col2", sa.Integer, index=True),
        )
        table.create(connection)

        table_desc: ydb.TableDescription = connection.connection.driver_connection.describe(table.name)
        indexes: list[ydb.TableIndex] = table_desc.indexes
        assert len(indexes) == 2
        indexes_map = {idx.name: idx for idx in indexes}

        assert "ix_test_column_indexes_table_index_col1" in indexes_map
        index1 = indexes_map["ix_test_column_indexes_table_index_col1"]
        assert index1.index_columns == ["index_col1"]

        assert "ix_test_column_indexes_table_index_col2" in indexes_map
        index1 = indexes_map["ix_test_column_indexes_table_index_col2"]
        assert index1.index_columns == ["index_col2"]

    def test_async_index(self, connection, metadata: sa.MetaData):
        table = Table(
            "test_async_index/table",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("index_col1", sa.Integer),
            sa.Column("index_col2", sa.Integer),
            sa.Index("test_async_index", "index_col1", "index_col2", ydb_async=True),
        )
        table.create(connection)

        table_desc: ydb.TableDescription = connection.connection.driver_connection.describe(table.name)
        indexes: list[ydb.TableIndex] = table_desc.indexes
        assert len(indexes) == 1
        index = indexes[0]
        assert index.name == "test_async_index"
        assert set(index.index_columns) == {"index_col1", "index_col2"}
        # TODO: Check type after https://github.com/ydb-platform/ydb-python-sdk/issues/351

    def test_cover_index(self, connection, metadata: sa.MetaData):
        table = Table(
            "test_cover_index/table",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("index_col1", sa.Integer),
            sa.Column("index_col2", sa.Integer),
            sa.Index("test_cover_index", "index_col1", ydb_cover=["index_col2"]),
        )
        table.create(connection)

        table_desc: ydb.TableDescription = connection.connection.driver_connection.describe(table.name)
        indexes: list[ydb.TableIndex] = table_desc.indexes
        assert len(indexes) == 1
        index = indexes[0]
        assert index.name == "test_cover_index"
        assert set(index.index_columns) == {"index_col1"}
        # TODO: Check covered columns after https://github.com/ydb-platform/ydb-python-sdk/issues/409

    def test_indexes_reflection(self, connection, metadata: sa.MetaData):
        table = Table(
            "test_indexes_reflection/table",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("index_col1", sa.Integer, index=True),
            sa.Column("index_col2", sa.Integer),
            sa.Index("test_index", "index_col1", "index_col2"),
            sa.Index("test_async_index", "index_col1", "index_col2", ydb_async=True),
            sa.Index("test_cover_index", "index_col1", ydb_cover=["index_col2"]),
            sa.Index("test_async_cover_index", "index_col1", ydb_async=True, ydb_cover=["index_col2"]),
        )
        table.create(connection)

        indexes = sa.inspect(connection).get_indexes(table.name)
        assert len(indexes) == 5
        indexes_names = {idx["name"]: set(idx["column_names"]) for idx in indexes}

        assert indexes_names == {
            "ix_test_indexes_reflection_table_index_col1": {"index_col1"},
            "test_index": {"index_col1", "index_col2"},
            "test_async_index": {"index_col1", "index_col2"},
            "test_cover_index": {"index_col1"},
            "test_async_cover_index": {"index_col1"},
        }

    def test_index_simple_usage(self, connection, metadata: sa.MetaData):
        persons = Table(
            "test_index_simple_usage/persons",
            metadata,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("tax_number", sa.Integer()),
            sa.Column("full_name", sa.Unicode()),
            sa.Index("ix_tax_number_cover_full_name", "tax_number", ydb_cover=["full_name"]),
        )
        persons.create(connection)
        connection.execute(
            sa.insert(persons).values(
                [
                    {"id": 1, "tax_number": 333333, "full_name": "John Connor"},
                    {"id": 2, "tax_number": 444444, "full_name": "Sarah Connor"},
                ]
            )
        )

        # Because of this bug https://github.com/ydb-platform/ydb/issues/3510,
        # it is not possible to use full qualified name of columns with VIEW clause
        select_stmt = (
            sa.select(sa.column(persons.c.full_name.name))
            .select_from(persons)
            .with_hint(persons, "VIEW `ix_tax_number_cover_full_name`")
            .where(sa.column(persons.c.tax_number.name) == 444444)
        )

        cursor = connection.execute(select_stmt)
        assert cursor.scalar_one() == "Sarah Connor"

    def test_index_with_join_usage(self, connection, metadata: sa.MetaData):
        persons = Table(
            "test_index_with_join_usage/persons",
            metadata,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("tax_number", sa.Integer()),
            sa.Column("full_name", sa.Unicode()),
            sa.Index("ix_tax_number_cover_full_name", "tax_number", ydb_cover=["full_name"]),
        )
        persons.create(connection)
        connection.execute(
            sa.insert(persons).values(
                [
                    {"id": 1, "tax_number": 333333, "full_name": "John Connor"},
                    {"id": 2, "tax_number": 444444, "full_name": "Sarah Connor"},
                ]
            )
        )
        person_status = Table(
            "test_index_with_join_usage/person_status",
            metadata,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("status", sa.Unicode()),
        )
        person_status.create(connection)
        connection.execute(
            sa.insert(person_status).values(
                [
                    {"id": 1, "status": "unknown"},
                    {"id": 2, "status": "wanted"},
                ]
            )
        )

        # Because of this bug https://github.com/ydb-platform/ydb/issues/3510,
        # it is not possible to use full qualified name of columns with VIEW clause
        persons_indexed = (
            sa.select(
                sa.column(persons.c.id.name),
                sa.column(persons.c.full_name.name),
                sa.column(persons.c.tax_number.name),
            )
            .select_from(persons)
            .with_hint(persons, "VIEW `ix_tax_number_cover_full_name`")
        )
        select_stmt = (
            sa.select(persons_indexed.c.full_name, person_status.c.status)
            .select_from(person_status.join(persons_indexed, persons_indexed.c.id == person_status.c.id))
            .where(persons_indexed.c.tax_number == 444444)
        )

        cursor = connection.execute(select_stmt)
        assert cursor.one() == ("Sarah Connor", "wanted")

    def test_index_deletion(self, connection, metadata: sa.MetaData):
        persons = Table(
            "test_index_deletion/persons",
            metadata,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("tax_number", sa.Integer()),
            sa.Column("full_name", sa.Unicode()),
        )
        persons.create(connection)
        index = sa.Index("ix_tax_number", "tax_number", _table=persons)
        index.create(connection)
        indexes = sa.inspect(connection).get_indexes(persons.name)
        assert len(indexes) == 1

        index.drop(connection)

        indexes = sa.inspect(connection).get_indexes(persons.name)
        assert len(indexes) == 0


class TestTablePathPrefix(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata: sa.MetaData):
        Table("some_dir/nested_dir/table", metadata, sa.Column("id", sa.Integer, primary_key=True))
        Table("table", metadata, sa.Column("id", sa.Integer, primary_key=True))

    @classmethod
    def insert_data(cls, connection):
        table = cls.tables["some_dir/nested_dir/table"]
        root_table = cls.tables["table"]

        connection.execute(sa.insert(table).values({"id": 1}))
        connection.execute(sa.insert(root_table).values({"id": 2}))

    def test_select(self):
        engine = sa.create_engine(config.db_url, connect_args={"ydb_table_path_prefix": "/local/some_dir/nested_dir"})
        rel_table = Table("table", sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True))
        abs_table = Table("/local/table", sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True))

        with engine.connect() as conn:
            result1 = conn.execute(sa.select(rel_table)).scalar()
            result2 = conn.execute(sa.select(abs_table)).scalar()

        assert result1 == 1
        assert result2 == 2

    def test_two_engines(self):
        create_engine = sa.create_engine(
            config.db_url, connect_args={"ydb_table_path_prefix": "/local/two/engines/test"}
        )
        select_engine = sa.create_engine(config.db_url, connect_args={"ydb_table_path_prefix": "/local/two"})
        table_to_create = Table("table", sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True))
        table_to_select = Table("engines/test/table", sa.MetaData(), sa.Column("id", sa.Integer, primary_key=True))

        table_to_create.create(create_engine)
        try:
            with create_engine.begin() as conn:
                conn.execute(sa.insert(table_to_create).values({"id": 42}))

            with select_engine.begin() as conn:
                result = conn.execute(sa.select(table_to_select)).scalar()
        finally:
            table_to_create.drop(create_engine)

        assert result == 42

    def test_reflection(self):
        reflection_engine = sa.create_engine(config.db_url, connect_args={"ydb_table_path_prefix": "/local/some_dir"})
        metadata = sa.MetaData()

        metadata.reflect(reflection_engine)

        assert "nested_dir/table" in metadata.tables
