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
