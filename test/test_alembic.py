"""Integration tests for running Alembic against YDB.

The suite covers the migration path the documentation promises: the shape of
the version table, ``upgrade``/``downgrade`` over a revision chain, the
individual operations available inside a revision, autogenerate diffs, and the
YDB limitations that migrations have to be written around.

Each test works on tables whose names carry a unique suffix, so the suite is
safe to run against a shared database and does not depend on it being empty.
"""

import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.testing import config as sa_test_config
from sqlalchemy.testing.fixtures import TestBase

alembic = pytest.importorskip("alembic", minversion="1.14")

from alembic import command  # noqa: E402
from alembic.autogenerate import compare_metadata  # noqa: E402
from alembic.config import Config  # noqa: E402
from alembic.migration import MigrationContext  # noqa: E402
from alembic.operations import Operations  # noqa: E402

from ydb_sqlalchemy.alembic import VERSION_TABLE_PK_COLUMN, YDBImpl  # noqa: E402,F401
from ydb_sqlalchemy.sqlalchemy import types as ydb_types  # noqa: E402

ENV_PY = """
import sqlalchemy as sa
from sqlalchemy import pool

from alembic import context
from ydb_sqlalchemy.alembic import YDBImpl  # noqa: F401

config = context.config
version_table = config.get_main_option("version_table")

connectable = sa.create_engine(config.get_main_option("sqlalchemy.url"), poolclass=pool.NullPool)
try:
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            version_table=version_table,
        )
        with context.begin_transaction():
            context.run_migrations()
finally:
    connectable.dispose()
"""

SCRIPT_MAKO = '''"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
"""
import sqlalchemy as sa

from alembic import op

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
'''


def unique_suffix() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def sync_url():
    """URL of the database under test, restricted to the sync driver.

    Alembic's ``command`` API and ``MigrationContext`` are synchronous, so the
    suite is skipped when the test session runs against ``ydb_async``.
    """
    url = sa_test_config.db.url
    if sa_test_config.db.dialect.is_async:
        pytest.skip("Alembic's synchronous API cannot drive the async YDB driver")
    return url


@pytest.fixture
def engine(sync_url):
    engine = sa.create_engine(sync_url, poolclass=sa.pool.NullPool)
    yield engine
    engine.dispose()


def drop_tables(engine, *names) -> None:
    """Drop tables if they exist, one connection per statement.

    The dialect only routes a statement to the YDB scheme service when
    SQLAlchemy marks it as DDL, and YDB refuses schema operations inside a
    transaction, so each drop goes through a ``Table.drop`` construct on a
    freshly committed connection rather than through raw SQL.
    """
    with engine.connect() as conn:
        existing = set(sa.inspect(conn).get_table_names())
    for name in names:
        if name not in existing:
            continue
        with engine.connect() as conn:
            sa.Table(name, sa.MetaData()).drop(conn)
            conn.commit()


class AlembicEnv:
    """A self-contained Alembic project in a temporary directory."""

    def __init__(self, root, url, version_table):
        self.root = root
        self.url = url
        self.version_table = version_table
        self.versions = root / "migrations" / "versions"
        self.versions.mkdir(parents=True)
        (root / "migrations" / "env.py").write_text(ENV_PY)
        (root / "migrations" / "script.py.mako").write_text(SCRIPT_MAKO)
        self._revisions = []

    @property
    def config(self) -> Config:
        cfg = Config()
        cfg.set_main_option("script_location", str(self.root / "migrations"))
        cfg.set_main_option("sqlalchemy.url", str(self.url))
        cfg.set_main_option("version_table", self.version_table)
        return cfg

    def add_revision(self, revision: str, upgrade: str, downgrade: str) -> None:
        """Write a revision file chained onto the previously added one."""
        down_revision = self._revisions[-1] if self._revisions else None
        self._revisions.append(revision)
        body = (
            "import sqlalchemy as sa\n"
            "from alembic import op\n"
            "\n"
            f"revision = {revision!r}\n"
            f"down_revision = {down_revision!r}\n"
            "branch_labels = None\n"
            "depends_on = None\n"
            "\n"
            "def upgrade():\n"
            f"{upgrade}\n"
            "\n"
            "def downgrade():\n"
            f"{downgrade}\n"
        )
        (self.versions / f"{revision}_.py").write_text(body)

    def upgrade(self, revision: str = "head") -> None:
        command.upgrade(self.config, revision)

    def downgrade(self, revision: str) -> None:
        command.downgrade(self.config, revision)

    def stamp(self, revision: str) -> None:
        command.stamp(self.config, revision)

    def current_heads(self, engine):
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn, opts={"version_table": self.version_table})
            return set(ctx.get_current_heads())


@pytest.fixture
def alembic_env(tmp_path, engine, sync_url):
    suffix = unique_suffix()
    env = AlembicEnv(tmp_path, sync_url, version_table=f"alembic_version_{suffix}")
    env.suffix = suffix
    env.table = f"orders_{suffix}"
    yield env
    drop_tables(engine, env.table, f"{env.table}_renamed", env.version_table)


@pytest.fixture
def migration_ctx(engine):
    """An ``Operations`` object bound to a live connection.

    Exercises revision bodies without going through revision files.
    """
    with engine.connect() as conn:
        yield Operations(MigrationContext.configure(conn))


class TestVersionTable(TestBase):
    def test_version_table_is_created_and_tracks_head(self, alembic_env, engine):
        alembic_env.add_revision(
            "0001",
            "    op.create_table(%r, sa.Column('id', sa.Integer, primary_key=True))" % alembic_env.table,
            "    op.drop_table(%r)" % alembic_env.table,
        )
        alembic_env.upgrade()

        assert alembic_env.current_heads(engine) == {"0001"}

    def test_version_table_shape_is_accepted_by_ydb(self, alembic_env, engine):
        """The version table needs a surrogate primary key on YDB.

        ``version_num`` cannot be the primary key because Alembic advances a
        revision with an ``UPDATE`` of that column and YDB cannot update a
        primary key column. Alembic's default named primary key constraint is
        also rejected by the YDB parser.
        """
        alembic_env.stamp("head")

        with engine.connect() as conn:
            inspector = sa.inspect(conn)
            columns = {c["name"]: c for c in inspector.get_columns(alembic_env.version_table)}
            pk = inspector.get_pk_constraint(alembic_env.version_table)

        assert set(columns) == {"version_num", VERSION_TABLE_PK_COLUMN}
        assert columns["version_num"]["nullable"] is False
        assert pk["constrained_columns"] == [VERSION_TABLE_PK_COLUMN]
        assert pk["name"] is None, "YDB rejects named primary key constraints"

    def test_stamp_does_not_run_migrations(self, alembic_env, engine):
        alembic_env.add_revision(
            "0001",
            "    op.create_table(%r, sa.Column('id', sa.Integer, primary_key=True))" % alembic_env.table,
            "    op.drop_table(%r)" % alembic_env.table,
        )
        alembic_env.stamp("0001")

        assert alembic_env.current_heads(engine) == {"0001"}
        with engine.connect() as conn:
            assert not sa.inspect(conn).has_table(alembic_env.table)


class TestUpgradeDowngrade(TestBase):
    @pytest.fixture
    def chain(self, alembic_env):
        table = alembic_env.table
        alembic_env.add_revision(
            "0001",
            "    op.create_table(\n"
            f"        {table!r},\n"
            "        sa.Column('id', sa.Integer, primary_key=True),\n"
            "        sa.Column('name', sa.Unicode),\n"
            "    )",
            f"    op.drop_table({table!r})",
        )
        alembic_env.add_revision(
            "0002",
            f"    op.add_column({table!r}, sa.Column('qty', sa.Integer))",
            f"    op.drop_column({table!r}, 'qty')",
        )
        alembic_env.add_revision(
            "0003",
            f"    op.create_index('ix_{alembic_env.suffix}_name', {table!r}, ['name'])",
            f"    op.drop_index('ix_{alembic_env.suffix}_name', table_name={table!r})",
        )
        return alembic_env

    def test_upgrade_head_applies_whole_chain(self, chain, engine):
        chain.upgrade()

        assert chain.current_heads(engine) == {"0003"}
        with engine.connect() as conn:
            inspector = sa.inspect(conn)
            assert [c["name"] for c in inspector.get_columns(chain.table)] == ["id", "name", "qty"]
            assert [i["name"] for i in inspector.get_indexes(chain.table)] == [f"ix_{chain.suffix}_name"]

    def test_upgrade_one_revision_at_a_time(self, chain, engine):
        chain.upgrade("0001")
        assert chain.current_heads(engine) == {"0001"}
        with engine.connect() as conn:
            assert [c["name"] for c in sa.inspect(conn).get_columns(chain.table)] == ["id", "name"]

        chain.upgrade("+1")
        assert chain.current_heads(engine) == {"0002"}
        with engine.connect() as conn:
            assert "qty" in [c["name"] for c in sa.inspect(conn).get_columns(chain.table)]

    def test_downgrade_reverses_each_revision(self, chain, engine):
        chain.upgrade()

        chain.downgrade("-1")
        assert chain.current_heads(engine) == {"0002"}
        with engine.connect() as conn:
            assert sa.inspect(conn).get_indexes(chain.table) == []

        chain.downgrade("-1")
        assert chain.current_heads(engine) == {"0001"}
        with engine.connect() as conn:
            assert "qty" not in [c["name"] for c in sa.inspect(conn).get_columns(chain.table)]

    def test_downgrade_to_base_removes_table_and_clears_version(self, chain, engine):
        chain.upgrade()
        chain.downgrade("base")

        assert chain.current_heads(engine) == set()
        with engine.connect() as conn:
            assert not sa.inspect(conn).has_table(chain.table)

    def test_upgrade_is_idempotent_at_head(self, chain, engine):
        chain.upgrade()
        chain.upgrade()

        assert chain.current_heads(engine) == {"0003"}


class TestOperations(TestBase):
    """The operations a revision body can use, run against a live connection."""

    @pytest.fixture
    def table_name(self, engine):
        name = f"ops_{unique_suffix()}"
        yield name
        drop_tables(engine, name, f"{name}_renamed")

    def _create(self, op, name):
        op.create_table(
            name,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.Unicode),
        )

    def test_create_and_drop_table(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)
        with engine.connect() as conn:
            assert sa.inspect(conn).has_table(table_name)

        migration_ctx.drop_table(table_name)
        with engine.connect() as conn:
            assert not sa.inspect(conn).has_table(table_name)

    def test_add_and_drop_column(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)

        migration_ctx.add_column(table_name, sa.Column("qty", sa.Integer))
        with engine.connect() as conn:
            assert "qty" in [c["name"] for c in sa.inspect(conn).get_columns(table_name)]

        migration_ctx.drop_column(table_name, "qty")
        with engine.connect() as conn:
            assert "qty" not in [c["name"] for c in sa.inspect(conn).get_columns(table_name)]

    def test_create_and_drop_index(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)
        index_name = f"ix_{table_name}_name"

        migration_ctx.create_index(index_name, table_name, ["name"])
        with engine.connect() as conn:
            assert [i["name"] for i in sa.inspect(conn).get_indexes(table_name)] == [index_name]

        migration_ctx.drop_index(index_name, table_name=table_name)
        with engine.connect() as conn:
            assert sa.inspect(conn).get_indexes(table_name) == []

    def test_rename_table(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)

        migration_ctx.rename_table(table_name, f"{table_name}_renamed")
        with engine.connect() as conn:
            inspector = sa.inspect(conn)
            assert not inspector.has_table(table_name)
            assert inspector.has_table(f"{table_name}_renamed")

    def test_bulk_insert_with_lightweight_table(self, migration_ctx, engine, table_name):
        """``sa.table()``/``sa.column()`` is the form Alembic documents here.

        Its columns are ``ColumnClause`` objects without ``nullable`` or
        ``primary_key``, which used to make bind-type inference raise
        ``AttributeError`` for the executemany path.
        """
        self._create(migration_ctx, table_name)
        table = sa.table(
            table_name,
            sa.column("id", sa.Integer),
            sa.column("name", sa.Unicode),
        )

        migration_ctx.bulk_insert(table, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT id, name FROM `{table_name}` ORDER BY id")).fetchall()
        assert rows == [(1, "a"), (2, "b")]

    def test_bulk_insert_with_full_table(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)
        table = sa.Table(
            table_name,
            sa.MetaData(),
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.Unicode),
        )

        migration_ctx.bulk_insert(table, [{"id": 1, "name": "a"}, {"id": 2, "name": None}])

        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT id, name FROM `{table_name}` ORDER BY id")).fetchall()
        assert rows == [(1, "a"), (2, None)]

    def test_create_unique_index_on_non_key_column(self, migration_ctx, engine, table_name):
        self._create(migration_ctx, table_name)
        index_name = f"ix_{table_name}_uniq"

        migration_ctx.create_index(index_name, table_name, ["name"], unique=True)

        with engine.connect() as conn:
            assert [i["name"] for i in sa.inspect(conn).get_indexes(table_name)] == [index_name]

    def test_alter_column_can_relax_not_null(self, migration_ctx, engine, table_name):
        """The one ``alter_column`` change YDB accepts, as ``DROP NOT NULL``."""
        migration_ctx.create_table(
            table_name,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("label", sa.Unicode(50), nullable=False),
        )
        with engine.connect() as conn:
            before = {c["name"]: c["nullable"] for c in sa.inspect(conn).get_columns(table_name)}
        assert before["label"] is False

        migration_ctx.alter_column(table_name, "label", existing_type=sa.Unicode(50), nullable=True)

        with engine.connect() as conn:
            after = {c["name"]: c["nullable"] for c in sa.inspect(conn).get_columns(table_name)}
        assert after["label"] is True

    def test_execute_can_set_column_options_via_ddl_construct(self, migration_ctx, engine, table_name):
        """``ALTER COLUMN`` options have no Alembic operation, so raw YQL it is.

        The statement has to be wrapped in ``sa.schema.DDL``. The dialect only
        routes a statement to the YDB scheme service when SQLAlchemy marks it
        as DDL, and YDB refuses schema operations inside a transaction, so a
        plain string reaches the query service instead and is rejected.
        """
        self._create(migration_ctx, table_name)
        statement = f"ALTER TABLE `{table_name}` ALTER COLUMN `name` SET FAMILY default"

        migration_ctx.execute(sa.schema.DDL(statement))

        with pytest.raises(sa.exc.DatabaseError, match="Scheme operations cannot be executed"):
            migration_ctx.execute(statement)

    def test_add_column_cannot_be_not_null(self, migration_ctx, table_name):
        """A column added to an existing table is always nullable."""
        self._create(migration_ctx, table_name)

        with pytest.raises(sa.exc.DatabaseError):
            migration_ctx.add_column(table_name, sa.Column("required", sa.Unicode(20), nullable=False))

    def test_execute_runs_a_data_migration(self, migration_ctx, engine, table_name):
        """The data-migration shape ``docs/migrations.rst`` documents."""
        self._create(migration_ctx, table_name)
        lightweight = sa.table(
            table_name,
            sa.column("id", sa.Integer),
            sa.column("name", sa.Unicode),
        )
        migration_ctx.bulk_insert(lightweight, [{"id": 1, "name": "old"}, {"id": 2, "name": "old"}])

        migration_ctx.execute(lightweight.update().values(name="new"))

        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT name FROM `{table_name}`")).fetchall()
        assert [r[0] for r in rows] == ["new", "new"]


class TestUnsupportedOperations(TestBase):
    """YDB limitations that migrations have to be written around.

    These are asserted rather than merely documented so that a future YDB or
    dialect release that lifts a limitation shows up as a failing test.
    """

    @pytest.fixture
    def table_name(self, engine, migration_ctx):
        name = f"lim_{unique_suffix()}"
        migration_ctx.create_table(
            name,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("label", sa.Unicode(50)),
            sa.Column("qty", sa.Integer),
        )
        yield name
        drop_tables(engine, name)

    def test_alter_column_type_is_rejected(self, migration_ctx, table_name):
        """YDB has no ``ALTER COLUMN ... TYPE``, not even to widen a string."""
        with pytest.raises(sa.exc.DatabaseError, match="ALTER COLUMN"):
            migration_ctx.alter_column(
                table_name,
                "label",
                existing_type=sa.Unicode(50),
                type_=sa.Unicode(100),
            )

    def test_alter_primary_key_column_type_is_rejected(self, migration_ctx, table_name):
        with pytest.raises(sa.exc.DatabaseError, match="ALTER COLUMN"):
            migration_ctx.alter_column(
                table_name,
                "id",
                existing_type=sa.Integer(),
                type_=sa.BigInteger(),
            )

    def test_setting_column_not_null_is_rejected(self, migration_ctx, table_name):
        with pytest.raises(sa.exc.DatabaseError, match="SET NOT NULL"):
            migration_ctx.alter_column(
                table_name,
                "qty",
                existing_type=sa.Integer(),
                nullable=False,
            )

    def test_foreign_key_is_rejected(self, migration_ctx, engine, table_name):
        """YDB has no foreign keys, so a referencing table cannot be created."""
        child = f"{table_name}_child"
        try:
            with pytest.raises(sa.exc.DatabaseError):
                migration_ctx.create_table(
                    child,
                    sa.Column("id", sa.Integer, primary_key=True),
                    sa.Column("parent_id", sa.Integer),
                    sa.ForeignKeyConstraint(["parent_id"], [f"{table_name}.id"]),
                )
        finally:
            drop_tables(engine, child)

    def test_second_head_is_rejected(self, alembic_env, engine):
        """Branched history needs a second version row, which cannot exist.

        The version table's primary key is a surrogate column that Alembic
        never populates, so every row collides on the same ``NULL`` key.
        """
        from alembic.runtime.migration import HeadMaintainer

        alembic_env.stamp("head")
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn, opts={"version_table": alembic_env.version_table})
            heads = HeadMaintainer(ctx, [])
            heads._insert_version("aaaaaaaaaaaa")

            with pytest.raises(sa.exc.DatabaseError):
                heads._insert_version("bbbbbbbbbbbb")


class TestYdbTypes(TestBase):
    def test_ydb_specific_types_survive_a_migration(self, migration_ctx, engine):
        """The types ``docs/migrations.rst`` recommends for revision bodies."""
        name = f"types_{unique_suffix()}"
        try:
            migration_ctx.create_table(
                name,
                sa.Column("id", ydb_types.UInt64, primary_key=True),
                sa.Column("small", ydb_types.UInt32),
                sa.Column("amount", ydb_types.Decimal(precision=15, scale=2)),
                sa.Column("meta", ydb_types.YqlJSON),
                sa.Column("created", ydb_types.YqlDateTime),
            )

            with engine.connect() as conn:
                columns = {c["name"]: c["type"] for c in sa.inspect(conn).get_columns(name)}

            assert isinstance(columns["id"], ydb_types.UInt64)
            assert isinstance(columns["small"], ydb_types.UInt32)
            assert isinstance(columns["amount"], sa.DECIMAL)
            assert (columns["amount"].precision, columns["amount"].scale) == (15, 2)
            assert isinstance(columns["meta"], sa.JSON)
            # YqlDateTime is stored with timestamp precision and reflects back
            # as TIMESTAMP rather than as the type it was declared with.
            assert isinstance(columns["created"], sa.TIMESTAMP)
        finally:
            drop_tables(engine, name)


class TestAutogenerate(TestBase):
    """``--autogenerate`` diffs, exercised through ``compare_metadata``.

    Every comparison is scoped to the table under test, because autogenerate
    otherwise reports every unrelated table in the database as ``remove_table``.
    """

    @pytest.fixture
    def table_name(self, engine):
        name = f"autogen_{unique_suffix()}"
        yield name
        drop_tables(engine, name)

    def _diff(self, engine, metadata, table_name):
        def include_name(name, type_, parent_names):
            if type_ == "table":
                return name == table_name
            return True

        with engine.connect() as conn:
            ctx = MigrationContext.configure(
                conn,
                opts={"compare_type": True, "include_name": include_name},
            )
            return compare_metadata(ctx, metadata)

    def _model(self, table_name, *, extra_column=False, index=False, drop_name=False):
        metadata = sa.MetaData()
        columns = [sa.Column("id", sa.Integer, primary_key=True)]
        if not drop_name:
            columns.append(sa.Column("name", sa.Unicode))
        if extra_column:
            columns.append(sa.Column("qty", sa.Integer))
        table = sa.Table(table_name, metadata, *columns)
        if index:
            sa.Index(f"ix_{table_name}_name", table.c.name)
        return metadata

    def test_detects_new_table(self, engine, table_name):
        diff = self._diff(engine, self._model(table_name), table_name)

        assert [op[0] for op in diff] == ["add_table"]
        assert diff[0][1].name == table_name

    def test_no_diff_when_model_matches_database(self, engine, table_name):
        metadata = self._model(table_name)
        with engine.connect() as conn:
            metadata.create_all(conn)
            conn.commit()

        assert self._diff(engine, metadata, table_name) == []

    def test_detects_added_column(self, engine, table_name):
        with engine.connect() as conn:
            self._model(table_name).create_all(conn)
            conn.commit()

        diff = self._diff(engine, self._model(table_name, extra_column=True), table_name)

        assert [op[0] for op in diff] == ["add_column"]
        assert diff[0][3].name == "qty"

    def test_detects_removed_column(self, engine, table_name):
        with engine.connect() as conn:
            self._model(table_name).create_all(conn)
            conn.commit()

        diff = self._diff(engine, self._model(table_name, drop_name=True), table_name)

        assert [op[0] for op in diff] == ["remove_column"]
        assert diff[0][3].name == "name"

    def test_detects_added_index(self, engine, table_name):
        with engine.connect() as conn:
            self._model(table_name).create_all(conn)
            conn.commit()

        diff = self._diff(engine, self._model(table_name, index=True), table_name)

        assert [op[0] for op in diff] == ["add_index"]
        assert diff[0][1].name == f"ix_{table_name}_name"

    def test_detects_removed_table(self, engine, table_name):
        with engine.connect() as conn:
            self._model(table_name).create_all(conn)
            conn.commit()

        diff = self._diff(engine, sa.MetaData(), table_name)

        assert [op[0] for op in diff] == ["remove_table"]
        assert diff[0][1].name == table_name
