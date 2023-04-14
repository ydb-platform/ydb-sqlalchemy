import sqlalchemy as sa


def test_get_columns(engine, test_table):
    inspect = sa.inspect(engine)
    columns = inspect.get_columns("test")
    for c in columns:
        c["type"] = type(c["type"])

    assert columns == [
        {"name": "id", "type": sa.INTEGER, "nullable": False, "default": None},
        {"name": "value", "type": sa.TEXT, "nullable": True, "default": None},
        {"name": "num", "type": sa.DECIMAL, "nullable": True, "default": None},
    ]


def test_has_table(engine, test_table):
    inspect = sa.inspect(engine)

    assert inspect.has_table("test")
    assert not inspect.has_table("foo")
