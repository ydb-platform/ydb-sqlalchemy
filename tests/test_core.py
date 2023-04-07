import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Integer, Unicode

meta = MetaData()


def clear_sql(stm):
    return stm.replace("\n", " ").replace("  ", " ").strip()


def test_sa_text(connection):
    rs = connection.execute(sa.text("SELECT 1 AS value"))
    assert rs.fetchone() == (1,)

    rs = connection.execute(
        sa.text("SELECT x, y FROM AS_TABLE(:data)"), [{"data": [{"x": 2, "y": 1}, {"x": 3, "y": 2}]}]
    )
    assert rs.fetchall() == [(2, 1), (3, 2)]


def test_sa_crud(connection):
    tb_test = Table(
        "test",
        meta,
        Column("id", Integer, primary_key=True),
        Column("text", Unicode),
    )
    tb_test.create(bind=connection.engine)

    select_stm = sa.select(tb_test)
    assert clear_sql(str(select_stm)) == "SELECT test.id, test.text FROM test"
    assert connection.execute(select_stm).fetchall() == []

    insert_stm = sa.insert(tb_test).values(id=2, text="foo")
    assert clear_sql(str(insert_stm)) == "INSERT INTO test (id, text) VALUES (:id, :text)"
    assert connection.execute(insert_stm)

    insert_many = sa.insert(tb_test).values([(3, "a"), (4, "b"), (5, "c")])
    assert connection.execute(insert_many)

    rs = connection.execute(select_stm.order_by(tb_test.c.id))
    row = rs.fetchone()
    assert row.id == 2
    assert row.text == "foo"

    update_stm = sa.update(tb_test).where(tb_test.c.id == 2).values(text="bar")
    assert clear_sql(str(update_stm)) == "UPDATE test SET text=:text WHERE test.id = :id_1"
    assert connection.execute(update_stm)

    select_where_stm = sa.select(tb_test.c.text).filter(tb_test.c.id == 2)
    assert clear_sql(str(select_where_stm)) == "SELECT test.text FROM test WHERE test.id = :id_1"
    assert connection.execute(select_where_stm).fetchall() == [("bar",)]

    delete_stm = sa.delete(tb_test).where(tb_test.c.id == 2)
    assert connection.execute(delete_stm)
    assert connection.execute(select_stm.order_by(tb_test.c.id)).fetchall() == [(3, "a"), (4, "b"), (5, "c")]

    tb_test.drop(bind=connection.engine)


def test_sa_select(connection):
    # where/filter
    # operations: and/or/like/regexp
    # operations: +-/%*, ||,
    # operations: in, between
    # order by: asc/desc
    # limit, offset
    # join
    # aggregates: min/max, count, sum, etc
    # group by
    # sub-select
    # union
    pass


def test_sa_types(connection):
    # test selects/inserts with different columns types
    pass
