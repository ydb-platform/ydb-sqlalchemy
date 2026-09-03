Database Migrations with Alembic
================================

This guide covers how to use Alembic for database schema migrations with YDB SQLAlchemy.

Overview
--------

Alembic is SQLAlchemy's database migration tool that allows you to:

- Track database schema changes over time
- Apply incremental schema updates
- Rollback to previous schema versions
- Generate migration scripts automatically

Support Status
--------------

Everything marked *supported* below is covered by an integration test in
``test/test_alembic.py`` and runs on every commit. Everything marked
*not supported* is likewise asserted by a test, so if a future YDB release
lifts a restriction the test starts failing and this table gets updated.

Alembic's own suite for third-party dialects, ``alembic.testing.suite``, runs
alongside it from ``test/test_alembic_suite.py``, with the YDB feature flags in
``test/alembic_requirements.py``.

Commands
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Command
     - Status
     - Notes
   * - ``upgrade`` (``head``, a revision, ``+1``)
     - Supported
     - Re-running at head is a no-op
   * - ``downgrade`` (``-1``, a revision, ``base``)
     - Supported
     -
   * - ``stamp``
     - Supported
     - Records the revision without running it
   * - ``current``, ``history``
     - Supported
     -
   * - ``revision --autogenerate``
     - Supported
     - See the operations table for what the generated script may contain
   * - ``upgrade --sql`` (offline mode)
     - Not covered
     - Untested; no claim is made either way

Operations inside a revision
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Operation
     - Status
     - Notes
   * - ``create_table``
     - Supported
     - Needs a primary key; no foreign keys
   * - ``drop_table``
     - Supported
     -
   * - ``rename_table``
     - Supported
     -
   * - ``add_column``
     - Supported
     - The new column is always nullable
   * - ``drop_column``
     - Supported
     -
   * - ``create_index`` / ``drop_index``
     - Supported
     - ``unique=True`` works on non-key columns
   * - ``bulk_insert``
     - Supported
     -
   * - ``execute``
     - Supported
     - Raw DDL must be wrapped in ``sa.schema.DDL``
   * - ``alter_column`` (``nullable=True``)
     - Supported
     - Maps onto YDB's ``DROP NOT NULL``
   * - ``alter_column`` (``nullable=False``)
     - **Not supported**
     - YDB: ``SET NOT NULL is currently not supported``
   * - ``alter_column`` (``type_=``)
     - **Not supported**
     - YDB's ``ALTER COLUMN`` changes options, not types
   * - Foreign key constraints
     - **Not supported**
     - YDB has no foreign keys
   * - Changing a primary key
     - **Not supported**
     - Create a new table, copy, drop, rename

Autogenerate
~~~~~~~~~~~~

Detects added and removed tables, added and removed columns, and added
indexes, and produces an empty diff when the model matches the database.
A changed column type has to be resolved by hand, since the ``alter_column``
autogenerate would emit for it is not executable on YDB.

Autogenerate compares against every table in the database, so scope it with
``include_name``/``include_object`` in ``env.py`` if the database holds tables
outside your model.

Other limits
~~~~~~~~~~~~

- **Branched history is not supported.** The version table can only hold one
  row, so a second head fails with a constraint violation. Keep the history
  linear.
- **Migrations are not atomic.** YDB cannot run schema operations in a
  transaction, so a revision that fails part way through leaves the schema
  partly migrated. Keep revisions small.

Installation
------------

Install Alembic alongside YDB SQLAlchemy:

.. code-block:: bash

   pip install alembic ydb-sqlalchemy

Initial Setup
-------------

1. Initialize Alembic in your project:

.. code-block:: bash

   alembic init migrations

This creates an ``alembic.ini`` configuration file and a ``migrations/`` directory.

2. Configure ``alembic.ini``:

.. code-block:: ini

   # alembic.ini
   [alembic]
   script_location = migrations
   prepend_sys_path = .
   version_path_separator = os

   # YDB connection string
   sqlalchemy.url = yql+ydb://localhost:2136/local

   [post_write_hooks]

   [loggers]
   keys = root,sqlalchemy,alembic

   [handlers]
   keys = console

   [formatters]
   keys = generic

   [logger_root]
   level = WARN
   handlers = console
   qualname =

   [logger_sqlalchemy]
   level = WARN
   handlers =
   qualname = sqlalchemy.engine

   [logger_alembic]
   level = INFO
   handlers =
   qualname = alembic

   [handler_console]
   class = StreamHandler
   args = (sys.stderr,)
   level = NOTSET
   formatter = generic

   [formatter_generic]
   format = %(levelname)-5.5s [%(name)s] %(message)s
   datefmt = %H:%M:%S

YDB-Specific Configuration
--------------------------

Alembic dispatches on the SQLAlchemy dialect name and refuses to start unless an
implementation is registered for that name, so ``env.py`` has to import the one
shipped with this package. The import is the whole integration: ``YDBImpl``
registers itself for the ``yql`` dialect, and it gives the ``alembic_version``
table a layout YDB accepts.

.. code-block:: python

   # migrations/env.py
   from logging.config import fileConfig
   from sqlalchemy import engine_from_config, pool
   from alembic import context

   from ydb_sqlalchemy.alembic import YDBImpl  # noqa: F401

   # Import your models
   from myapp.models import Base

   config = context.config

   if config.config_file_name is not None:
       fileConfig(config.config_file_name)

   target_metadata = Base.metadata

   def run_migrations_offline() -> None:
       """Run migrations in 'offline' mode."""
       url = config.get_main_option("sqlalchemy.url")
       context.configure(
           url=url,
           target_metadata=target_metadata,
           literal_binds=True,
           dialect_opts={"paramstyle": "named"},
       )

       with context.begin_transaction():
           context.run_migrations()

   def run_migrations_online() -> None:
       """Run migrations in 'online' mode."""
       connectable = engine_from_config(
           config.get_section(config.config_ini_section, {}),
           prefix="sqlalchemy.",
           poolclass=pool.NullPool,
       )

       with connectable.connect() as connection:
           context.configure(
               connection=connection,
               target_metadata=target_metadata
           )

           with context.begin_transaction():
               context.run_migrations()

   if context.is_offline_mode():
       run_migrations_offline()
   else:
       run_migrations_online()

The Version Table
~~~~~~~~~~~~~~~~~

``YDBImpl`` creates ``alembic_version`` with an extra, always-``NULL`` ``id``
column that serves as its primary key, instead of Alembic's usual primary key on
``version_num``. Two YDB rules force this: a primary key column cannot be
updated, and Alembic advances a revision with
``UPDATE alembic_version SET version_num = ...``; and the named primary key
constraint Alembic emits by default is rejected by the YDB parser.

A consequence is that branched migrations are not supported -- more than one
head would need more than one row, and the rows would collide on a ``NULL``
primary key. Keep the revision history linear.

Creating Your First Migration
-----------------------------

1. Define your models:

.. code-block:: python

   # models.py
   from sqlalchemy import Column, String, Integer
   from sqlalchemy.ext.declarative import declarative_base
   from ydb_sqlalchemy.sqlalchemy.types import UInt64

   Base = declarative_base()

   class User(Base):
       __tablename__ = 'users'

       id = Column(UInt64, primary_key=True)
       username = Column(String(50), nullable=False)
       email = Column(String(100), nullable=False)
       full_name = Column(String(200))

2. Generate the initial migration:

.. code-block:: bash

   alembic revision --autogenerate -m "Create users table"

This creates a migration file like ``001_create_users_table.py``:

.. code-block:: python

   """Create users table

   Revision ID: 001
   Revises:
   Create Date: 2024-01-01 12:00:00.000000
   """
   from alembic import op
   import sqlalchemy as sa
   from ydb_sqlalchemy.sqlalchemy.types import UInt64

   revision = '001'
   down_revision = None
   branch_labels = None
   depends_on = None

   def upgrade() -> None:
       op.create_table('users',
           sa.Column('id', UInt64(), nullable=False),
           sa.Column('username', sa.String(length=50), nullable=False),
           sa.Column('email', sa.String(length=100), nullable=False),
           sa.Column('full_name', sa.String(length=200), nullable=True),
           sa.PrimaryKeyConstraint('id')
       )

   def downgrade() -> None:
       op.drop_table('users')

3. Apply the migration:

.. code-block:: bash

   alembic upgrade head

Common Migration Operations
---------------------------

Adding a Column
~~~~~~~~~~~~~~~

.. code-block:: python

   # Add a new column
   def upgrade() -> None:
       op.add_column('users', sa.Column('created_at', sa.DateTime(), nullable=True))

   def downgrade() -> None:
       op.drop_column('users', 'created_at')

Modifying a Column
~~~~~~~~~~~~~~~~~~

``op.alter_column()`` on YDB can only relax nullability, not change a type --
see `What alter_column Can Change`_. Replace a column by adding the
replacement, copying the data and dropping the original:

.. code-block:: python

   def upgrade() -> None:
       op.add_column('users', sa.Column('username_v2', sa.Unicode(100)))
       op.execute('UPDATE `users` SET username_v2 = username')
       op.drop_column('users', 'username')

   def downgrade() -> None:
       op.add_column('users', sa.Column('username', sa.Unicode(50)))
       op.execute('UPDATE `users` SET username = username_v2')
       op.drop_column('users', 'username_v2')

Creating Indexes
~~~~~~~~~~~~~~~~

.. code-block:: python

   def upgrade() -> None:
       op.create_index('ix_users_email', 'users', ['email'])

   def downgrade() -> None:
       op.drop_index('ix_users_email', table_name='users')

Adding a New Table
~~~~~~~~~~~~~~~~~~

.. code-block:: python

YDB has no foreign keys, so a revision can only declare the column and, if the
lookup needs it, a secondary index:

.. code-block:: python

   def upgrade() -> None:
       op.create_table('posts',
           sa.Column('id', UInt64(), nullable=False),
           sa.Column('user_id', UInt64(), nullable=False),
           sa.Column('title', sa.String(200), nullable=False),
           sa.Column('content', sa.Text(), nullable=True),
           sa.Column('created_at', sa.DateTime(), nullable=False),
           sa.PrimaryKeyConstraint('id'),
       )
       op.create_index('ix_posts_user_id', 'posts', ['user_id'])

   def downgrade() -> None:
       op.drop_table('posts')

YDB-Specific Considerations
---------------------------

Primary Key Limitations
~~~~~~~~~~~~~~~~~~~~~~~~

YDB doesn't support modifying primary key columns. Plan your primary keys carefully:

.. code-block:: python

   # Good: Use appropriate primary key from the start
   class User(Base):
       __tablename__ = 'users'
       id = Column(UInt64, primary_key=True)  # Can't be changed later

   # If you need to change primary key structure, you'll need to:
   # 1. Create new table with correct primary key
   # 2. Migrate data
   # 3. Drop old table
   # 4. Rename new table

.. _What alter_column Can Change:

What ``alter_column`` Can Change
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

YDB's `ALTER COLUMN <https://ydb.tech/docs/en/yql/reference/syntax/alter_table/columns>`_
changes column *options* -- it can drop ``NOT NULL`` and set ``FAMILY``,
``DEFAULT`` or ``COMPRESSION``. It cannot change a column's type. Of the
changes Alembic can express, only relaxing nullability goes through.

Relaxing ``NOT NULL`` works, because it maps onto ``DROP NOT NULL``:

.. code-block:: python

   op.alter_column('users', 'username',
                  existing_type=sa.Unicode(50),
                  nullable=True)  # Works

Adding ``NOT NULL`` does not; YDB answers
``SET NOT NULL is currently not supported``:

.. code-block:: python

   op.alter_column('users', 'status', nullable=False)  # Fails

Changing a type does not either. Alembic emits ``ALTER COLUMN ... TYPE ...``,
which the YQL parser rejects with
``no viable alternative at input 'ALTER COLUMN'`` -- including for a widening
change that other databases accept:

.. code-block:: python

   op.alter_column('users', 'username',
                  existing_type=sa.Unicode(50),
                  type_=sa.Unicode(100))  # Fails

Use add-copy-drop, as shown in `Modifying a Column`_, to change a type. For a
primary key column even that is not enough, because the primary key of an
existing table cannot be changed; create a new table, copy the data into it,
drop the old one and rename.

The remaining ``ALTER COLUMN`` options have no Alembic operation, so reach them
with raw YQL. Wrap the statement in ``sa.schema.DDL``: the dialect only sends a
statement to the YDB scheme service when SQLAlchemy marks it as DDL, and a
plain string is not marked, so it reaches the query service and is rejected
with ``Scheme operations cannot be executed inside transaction``.

.. code-block:: python

   op.execute(sa.schema.DDL(
       'ALTER TABLE `users` ALTER COLUMN `username` SET FAMILY default'
   ))

This applies to any raw DDL passed to ``op.execute``. Raw DML -- an ``UPDATE``
in a data migration, say -- is fine as a plain string.

Working with YDB Types
~~~~~~~~~~~~~~~~~~~~~~

Use YDB-specific types in migrations:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import (
       UInt64, UInt32, Decimal, YqlJSON, YqlDateTime
   )

   def upgrade() -> None:
       op.create_table('financial_records',
           sa.Column('id', UInt64(), nullable=False),
           sa.Column('amount', Decimal(precision=15, scale=2), nullable=False),
           sa.Column('metadata', YqlJSON(), nullable=True),
           sa.Column('created_at', YqlDateTime(timezone=True), nullable=False),
           sa.PrimaryKeyConstraint('id')
       )

Advanced Migration Patterns
---------------------------

Data Migrations
~~~~~~~~~~~~~~~

Sometimes you need to migrate data along with schema:

.. code-block:: python

   from alembic import op
   import sqlalchemy as sa
   from sqlalchemy.sql import table, column

   def upgrade() -> None:
       # Add new column
       op.add_column('users', sa.Column('status', sa.String(20), nullable=True))

       # Create a temporary table representation for data migration
       users_table = table('users',
           column('id', UInt64),
           column('status', sa.String)
       )

       # Update existing records
       op.execute(
           users_table.update().values(status='active')
       )

       # The column stays nullable: YDB cannot add NOT NULL to an existing
       # column. Declare it NOT NULL at CREATE TABLE time, or enforce it in
       # the application.

   def downgrade() -> None:
       op.drop_column('users', 'status')

Conditional Migrations
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def upgrade() -> None:
       # Check if column already exists
       conn = op.get_bind()
       inspector = sa.inspect(conn)
       columns = [col['name'] for col in inspector.get_columns('users')]

       if 'new_column' not in columns:
           op.add_column('users', sa.Column('new_column', sa.String(50)))

Migration Best Practices
------------------------

1. **Test Migrations**: Always test migrations on a copy of production data
2. **Backup Data**: Backup your data before running migrations in production
3. **Review Generated Migrations**: Always review auto-generated migrations before applying
4. **Do Not Rely on Atomicity**: YDB cannot run schema operations inside a
   transaction, so a revision that fails part way through leaves the schema
   partly migrated. Keep revisions small so that re-running one after a manual
   fix is cheap.
5. **Plan Primary Keys**: Design primary keys carefully as they can't be changed
6. **Plan Nullability**: A column can only be made ``NOT NULL`` when the table is
   created

.. code-block:: python

   # Good migration practices
   def upgrade() -> None:
       # One schema change per revision, so a failure is easy to place
       op.add_column('users', sa.Column('new_field', sa.String(100), nullable=True))

       # Populate data
       # ... data migration code ...

Common Commands
---------------

.. code-block:: bash

   # Generate new migration
   alembic revision --autogenerate -m "Description of changes"

   # Apply all pending migrations
   alembic upgrade head

   # Apply specific migration
   alembic upgrade revision_id

   # Rollback one migration
   alembic downgrade -1

   # Rollback to specific revision
   alembic downgrade revision_id

   # Show current revision
   alembic current

   # Show migration history
   alembic history

   # Show pending migrations
   alembic show head

Troubleshooting
---------------

**Migration Fails with "Table already exists"**
   - Check if migration was partially applied
   - Use ``alembic stamp head`` to mark current state without running migrations

**Primary Key Constraint Errors**
   - YDB requires primary keys on all tables
   - Ensure all tables have appropriate primary keys

**Type Conversion Errors**
   - Some type changes aren't supported in YDB
   - Create new column, migrate data, drop old column instead

**Connection Issues**
   - Verify YDB is running and accessible
   - Check connection string in ``alembic.ini``

Example Project Structure
-------------------------

.. code-block:: text

   myproject/
   ├── alembic.ini
   ├── migrations/
   │   ├── env.py
   │   ├── script.py.mako
   │   └── versions/
   │       ├── 001_create_users_table.py
   │       ├── 002_add_posts_table.py
   │       └── 003_add_user_status.py
   ├── models/
   │   ├── __init__.py
   │   ├── user.py
   │   └── post.py
   └── main.py

This setup provides a robust foundation for managing YDB schema changes over time using Alembic migrations.
