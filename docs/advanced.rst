Advanced Usage
==============

This section describes advanced configuration options of the YDB SQLAlchemy dialect.

YQL Statement Prefixes
----------------------

You can prepend one or more YQL fragments (for example, ``PRAGMA`` directives) to every executed query. This is useful to set session-level behavior such as ``PRAGMA DistinctOverKeys;`` or other YQL pragmas without modifying application SQL.

The dialect option ``_statement_prefixes_list`` accepts a list of strings. Each string is prepended to the statement on a separate line, in order. Pass it to :func:`sqlalchemy.create_engine`; the argument is forwarded to the dialect.

.. code-block:: python

   import sqlalchemy as sa

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       _statement_prefixes_list=["PRAGMA DistinctOverKeys;", "PRAGMA Bar;"],
   )
   with engine.connect() as conn:
       conn.execute(sa.text("SELECT 1 AS value"))  # runs with prefixes prepended

When ``_statement_prefixes_list`` is omitted or empty, statements are executed unchanged.

Explicit DECLARE for query parameters
------------------------------------

The dialect option ``_add_declare_for_yql_stmt_vars`` (default ``False``) prepends explicit ``DECLARE`` statements for each bound parameter at the beginning of the query, e.g. ``DECLARE `$id` as Int64;``. Many YDB installations still require this form; without it, parameterized queries may fail.

Pass ``_add_declare_for_yql_stmt_vars=True`` to :func:`sqlalchemy.create_engine`:

.. code-block:: python

   import sqlalchemy as sa

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       _add_declare_for_yql_stmt_vars=True,
   )
   with engine.connect() as conn:
       conn.execute(sa.text("SELECT :id"), {"id": 1})  # runs as "DECLARE `$id` as Int64;\nSELECT $id" with param

Retrying operations
-------------------

YDB returns retryable errors (``Unavailable``, ``Overloaded``, ``Aborted``,
``BadSession``, ...) that the SDK knows how to retry. SQLAlchemy, however, wraps
the underlying driver error in :class:`sqlalchemy.exc.DBAPIError`, which the SDK
retry logic does not recognise. The helpers in ``ydb_sqlalchemy`` translate the
SQLAlchemy error back to the original ``ydb.Error`` and run the operation under
the SDK's retry policy:

.. code-block:: python

   import sqlalchemy as sa
   from ydb_sqlalchemy import retry_ydb_operation

   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   def read_modify_write():
       with engine.begin() as conn:
           value = conn.execute(sa.text("SELECT v FROM t WHERE id = 1")).scalar()
           conn.execute(sa.text("UPDATE t SET v = :v WHERE id = 1"), {"v": value + 1})

   # idempotent=True also retries ambiguous errors; set it only when re-running
   # the whole callable is safe.
   retry_ydb_operation(read_modify_write, max_retries=10, idempotent=True)

The callable is re-run from scratch on every attempt, so it must open its own
connection/transaction and not rely on earlier state. Only ``ydb.Error`` is
retried; any other error propagates unchanged.

There is also a decorator form that works on both sync and async functions, and
an async call form :func:`ydb_sqlalchemy.retry_ydb_operation_async`:

.. code-block:: python

   from ydb_sqlalchemy import retry_ydb

   @retry_ydb(idempotent=True)
   def read_modify_write():
       ...

.. note::

   In the default ``AUTOCOMMIT`` isolation level each statement is its own
   transaction and is already retried inside ``ydb-dbapi``, so the helper is
   mainly useful for **interactive transactions** (a multi-statement
   read-modify-write under ``SERIALIZABLE``/``SNAPSHOT``), where the whole
   transaction must be retried as a unit.
