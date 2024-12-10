# Alembic support

In this example we'll see how to use `alembic` with ydb.

## Installation

To make `alembic` work with `YDB` tables please follow these steps:

* Install `ydb-sqlalchemy` package from PyPi:

```bash
pip install ydb-sqlalchemy`
```

* Install `alembic` package from PyPi:

```bash
pip install alembic
```

## Preparation

We have to setup `alembic` correctly.
First of all, we should register `YDB` dialect in `env.py`:

```python3
from alembic.ddl.impl import DefaultImpl


class YDBImpl(DefaultImpl):
    __dialect__ = "yql"
```

Secondly, since `YDB` do not support updating primary key columns, we have to update alembic table structure.
For this purpose we should update `run_migrations_online` method in `env.py`:

```python3
def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        ctx = context.get_context()
        ctx._version = sa.Table(  # noqa: SLF001
            ctx.version_table,
            sa.MetaData(),
            sa.Column("version_num", sa.String(32), nullable=False),
            sa.Column("id", sa.Integer(), nullable=True, primary_key=True),
        )

        with context.begin_transaction():
            context.run_migrations()
```

## Example

To run this example:
1. Install all dependencies described in `Installation` section.
1. Update `sqlalchemy.url` field in `alembic.ini` config file.
1. Run `alembic upgrade head` to apply all migrations:

```bash
alembic upgrade head

INFO  [alembic.runtime.migration] Context impl YDBImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade  -> d91d9200b65c, create series table
INFO  [alembic.runtime.migration] Running upgrade d91d9200b65c -> 820b994ffa7c, create seasons table
INFO  [alembic.runtime.migration] Running upgrade 820b994ffa7c -> 9085c679f5dc, create episodes table
```

To create new migration just add a few changes in `models.py` and run:
```bash
alembic revision --autogenerate -m "name of your migration"
```
