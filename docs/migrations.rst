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

YDB SQLAlchemy provides full Alembic integration with some YDB-specific considerations.

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

YDB requires special configuration in ``env.py`` due to its unique characteristics:

.. code-block:: python

   # migrations/env.py
   from logging.config import fileConfig
   import sqlalchemy as sa
   from sqlalchemy import engine_from_config, pool
   from alembic import context
   from alembic.ddl.impl import DefaultImpl

   # Import your models
   from myapp.models import Base

   config = context.config

   if config.config_file_name is not None:
       fileConfig(config.config_file_name)

   target_metadata = Base.metadata

   # YDB-specific implementation
   class YDBImpl(DefaultImpl):
       __dialect__ = "yql"

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

           # YDB-specific: Custom version table structure
           ctx = context.get_context()
           ctx._version = sa.Table(
               ctx.version_table,
               sa.MetaData(),
               sa.Column("version_num", sa.String(32), nullable=False),
               sa.Column("id", sa.Integer(), nullable=True, primary_key=True),
           )

           with context.begin_transaction():
               context.run_migrations()

   if context.is_offline_mode():
       run_migrations_offline()
   else:
       run_migrations_online()

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

.. code-block:: python

   # Change column type (be careful with YDB limitations)
   def upgrade() -> None:
       op.alter_column('users', 'username',
                      existing_type=sa.String(50),
                      type_=sa.String(100),
                      nullable=False)

   def downgrade() -> None:
       op.alter_column('users', 'username',
                      existing_type=sa.String(100),
                      type_=sa.String(50),
                      nullable=False)

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

   def upgrade() -> None:
       op.create_table('posts',
           sa.Column('id', UInt64(), nullable=False),
           sa.Column('user_id', UInt64(), nullable=False),
           sa.Column('title', sa.String(200), nullable=False),
           sa.Column('content', sa.Text(), nullable=True),
           sa.Column('created_at', sa.DateTime(), nullable=False),
           sa.PrimaryKeyConstraint('id'),
           sa.ForeignKeyConstraint(['user_id'], ['users.id'])
       )

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

Data Type Constraints
~~~~~~~~~~~~~~~~~~~~~

Some type changes are not supported:

.. code-block:: python

   # Supported: Increasing string length
   op.alter_column('users', 'username',
                  existing_type=sa.String(50),
                  type_=sa.String(100))

   # Not supported: Changing fundamental type
   # op.alter_column('users', 'id',
   #                existing_type=UInt32(),
   #                type_=UInt64())  # This won't work

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

       # Make column non-nullable
       op.alter_column('users', 'status', nullable=False)

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
4. **Use Transactions**: Migrations run in transactions by default
5. **Plan Primary Keys**: Design primary keys carefully as they can't be easily changed

.. code-block:: python

   # Good migration practices
   def upgrade() -> None:
       # Add columns as nullable first
       op.add_column('users', sa.Column('new_field', sa.String(100), nullable=True))

       # Populate data
       # ... data migration code ...

       # Then make non-nullable if needed
       op.alter_column('users', 'new_field', nullable=False)

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
