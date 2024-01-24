import pytest

import ydb_sqlalchemy.dbapi as dbapi


@pytest.fixture(scope="module")
def connection():
    conn = dbapi.YdbDBApi().connect(host="localhost", port="2136", database="/local")
    yield conn
    conn.close()
