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

    def test_reflection_ignores_schema(self, connection):
        # supports_schemas=False: the `schema` argument is ignored regardless of value,
        # so reflection always targets the connected database (the convention two-tier
        # SQLAlchemy tooling relies on).
        inspect = sa.inspect(connection)
        bound_database = connection.connection.driver_connection.database.strip("/")

        for schema in (bound_database, "some_other_database"):
            assert "test" in inspect.get_table_names(schema=schema)
            assert inspect.has_table("test", schema=schema)
            assert inspect.get_columns("test", schema=schema)

    def test_compile_ignores_schema_prefix(self, connection):
        bound_database = connection.connection.driver_connection.database.strip("/")

        # A table addressed via the connected database as schema (the way two-tier
        # tooling does) must compile without a schema prefix and execute against YDB.
        t = sa.Table("test", sa.MetaData(), schema=bound_database, autoload_with=connection)
        stmt = sa.select(sa.func.count()).select_from(t)
        compiled = str(stmt.compile(connection))
        assert f"{bound_database}.`test`" not in compiled
        assert f"{bound_database}.test" not in compiled
        connection.execute(stmt).scalar()

        # Any other schema is likewise dropped rather than leaking into the path.
        foreign = sa.Table("test", sa.MetaData(), Column("id", Integer), schema="some_other_database")
        compiled_foreign = str(sa.select(sa.func.count()).select_from(foreign).compile(connection))
        assert "some_other_database." not in compiled_foreign

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
