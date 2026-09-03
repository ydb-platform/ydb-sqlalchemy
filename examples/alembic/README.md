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

`alembic` dispatches on the SQLAlchemy dialect name and refuses to start unless an
implementation is registered for it, so `env.py` has to import the one shipped with
`ydb-sqlalchemy`. The import is the whole setup:

```python3
from ydb_sqlalchemy.alembic import YDBImpl  # noqa: F401
```

`YDBImpl` registers itself for the `yql` dialect on import, and it also gives the
`alembic_version` table a layout `YDB` accepts. `alembic` normally makes
`version_num` the primary key, which does not work here for two reasons: `YDB`
cannot update a primary key column, and `alembic` advances a revision with
`UPDATE alembic_version SET version_num = ...`; and the named primary key
constraint `alembic` emits by default is rejected by the `YDB` parser. `YDBImpl`
adds a surrogate primary key column instead, so no changes to
`run_migrations_online` are needed.

Because the single version row is keyed on an always-`NULL` surrogate column,
branched migrations are not supported -- keep the revision history linear.

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
