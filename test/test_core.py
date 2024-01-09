from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, Unicode

from sqlalchemy.testing.fixtures import TestBase, TablesTest

from datetime import date, datetime

from ydb_sqlalchemy import sqlalchemy as ydb_sa


def clear_sql(stm):
    return stm.replace("\n", " ").replace("  ", " ").strip()


class TestText(TestBase):
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


class TestUpsert(TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_upsert",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("val", Integer),
        )

    def test_1(self, connection):
        tb = self.tables.test_upsert

        stm = ydb_sa.upsert(tb).values(id=5, val=5)

        # TODO: allow to get string
        # assert "UPSERT INTO" in str(stm)

        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchone()
        assert row == (5, 5)

        stm = ydb_sa.upsert(tb).values(id=5, val=6)
        connection.execute(stm)
        row = connection.execute(sa.select(tb)).fetchone()
        assert row == (5, 6)
