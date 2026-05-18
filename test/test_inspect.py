import posixpath

import pytest
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

    @pytest.fixture
    def test_view(self, connection):
        raw_connection = connection.connection
        driver_connection = getattr(raw_connection, "driver_connection", raw_connection)
        view_name = "test_view"
        table_path = posixpath.join(driver_connection.database, driver_connection.table_path_prefix, "test")
        cursor = driver_connection.cursor()
        try:
            try:
                cursor.execute_scheme(f"DROP VIEW `{view_name}`")
            except Exception:
                pass

            cursor.execute_scheme(
                f"CREATE VIEW `{view_name}` WITH (security_invoker = TRUE) AS "
                f"SELECT `id`, `value`, `num` FROM `{table_path}`"
            )
            yield view_name
        finally:
            try:
                cursor.execute_scheme(f"DROP VIEW `{view_name}`")
            except Exception:
                pass
            cursor.close()

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

    def test_view_reflection(self, connection, test_view):
        view_name = test_view
        inspect = sa.inspect(connection)

        assert view_name in inspect.get_view_names()
        assert inspect.has_table(view_name)
        assert inspect.get_view_definition(view_name).startswith(f"CREATE VIEW `{view_name}`")

        columns = {column["name"]: column for column in inspect.get_columns(view_name)}
        assert set(columns) == {"id", "value", "num"}
        assert isinstance(columns["id"]["type"], sa.INTEGER)
        assert columns["id"]["nullable"] is False
        assert isinstance(columns["value"]["type"], sa.TEXT)
        assert columns["value"]["nullable"] is True
        assert isinstance(columns["num"]["type"], sa.DECIMAL)
        assert columns["num"]["type"].precision == 22
        assert columns["num"]["type"].scale == 9
        assert columns["num"]["nullable"] is True
