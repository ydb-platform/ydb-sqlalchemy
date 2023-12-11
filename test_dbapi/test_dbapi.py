import pytest

import ydb
import ydb_sqlalchemy.dbapi as dbapi

from contextlib import suppress


def test_connection(connection):
    connection.commit()
    connection.rollback()

    cur = connection.cursor()
    with suppress(dbapi.DatabaseError):
        cur.execute(dbapi.YdbOperation("DROP TABLE foo", is_ddl=True))

    assert not connection.check_exists("/local/foo")
    with pytest.raises(dbapi.ProgrammingError):
        connection.describe("/local/foo")

    cur.execute(dbapi.YdbOperation("CREATE TABLE foo(id Int64 NOT NULL, PRIMARY KEY (id))", is_ddl=True))

    assert connection.check_exists("/local/foo")

    col = connection.describe("/local/foo").columns[0]
    assert col.name == "id"
    assert col.type == ydb.PrimitiveType.Int64

    cur.execute(dbapi.YdbOperation("DROP TABLE foo", is_ddl=True))
    cur.close()


def test_cursor_raw_query(connection):
    cur = connection.cursor()
    assert cur

    with suppress(dbapi.DatabaseError):
        cur.execute(dbapi.YdbOperation("DROP TABLE test", is_ddl=True))

    cur.execute(dbapi.YdbOperation("CREATE TABLE test(id Int64 NOT NULL, text Utf8, PRIMARY KEY (id))", is_ddl=True))

    cur.execute(
        dbapi.YdbOperation(
            """
            DECLARE $data AS List<Struct<id:Int64, text: Utf8>>;

            INSERT INTO test SELECT id, text FROM AS_TABLE($data);
            """,
            is_ddl=False,
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

    cur.execute(dbapi.YdbOperation("DROP TABLE test", is_ddl=True))

    cur.close()


def test_errors(connection):
    with pytest.raises(dbapi.InterfaceError):
        dbapi.connect("localhost:2136", database="/local666")

    cur = connection.cursor()

    with suppress(dbapi.DatabaseError):
        cur.execute(dbapi.YdbOperation("DROP TABLE test", is_ddl=True))

    with pytest.raises(dbapi.DataError):
        cur.execute(dbapi.YdbOperation("SELECT 18446744073709551616", is_ddl=False))

    with pytest.raises(dbapi.DataError):
        cur.execute(dbapi.YdbOperation("SELECT * FROM 拉屎", is_ddl=False))

    with pytest.raises(dbapi.DataError):
        cur.execute(dbapi.YdbOperation("SELECT floor(5 / 2)", is_ddl=False))

    with pytest.raises(dbapi.ProgrammingError):
        cur.execute(dbapi.YdbOperation("SELECT * FROM test", is_ddl=False))

    cur.execute(dbapi.YdbOperation("CREATE TABLE test(id Int64, PRIMARY KEY (id))", is_ddl=True))

    cur.execute(dbapi.YdbOperation("INSERT INTO test(id) VALUES(1)", is_ddl=False))
    with pytest.raises(dbapi.IntegrityError):
        cur.execute(dbapi.YdbOperation("INSERT INTO test(id) VALUES(1)", is_ddl=False))

    cur.execute(dbapi.YdbOperation("DROP TABLE test", is_ddl=True))
    cur.close()
