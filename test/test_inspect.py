import sqlalchemy as sa
from sqlalchemy import Column, Integer, Numeric, Table, Unicode
from sqlalchemy.testing.fixtures import TablesTest


class TestInspection(TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True, nullable=False),
            Column("value", Unicode),
            Column("num", Numeric(22, 9)),
        )

    def test_get_columns(self, connection):
        inspect = sa.inspect(connection)

        columns = inspect.get_columns("test")
        for c in columns:
            c["type"] = type(c["type"])

        assert columns == [
            {"name": "id", "type": sa.INTEGER, "nullable": False, "default": None},
            {"name": "value", "type": sa.TEXT, "nullable": True, "default": None},
            {"name": "num", "type": sa.DECIMAL, "nullable": True, "default": None},
        ]

    def test_has_table(self, connection):
        inspect = sa.inspect(connection)

        assert inspect.has_table("test")
        assert not inspect.has_table("foo")
