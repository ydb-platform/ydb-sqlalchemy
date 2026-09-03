"""Alembic support for the YDB SQLAlchemy dialect.

Alembic dispatches on the SQLAlchemy dialect name and refuses to start when no
implementation is registered for it, so importing this module is a hard
requirement for running Alembic against YDB::

    # migrations/env.py
    from ydb_sqlalchemy.alembic import YDBImpl  # noqa: F401

Importing the module is enough -- Alembic registers :class:`YDBImpl` for the
``yql`` dialect through the metaclass of its ``DefaultImpl`` base.

This module is optional and is not imported by ``ydb_sqlalchemy`` itself, so
Alembic stays an optional dependency of the package.
"""

from typing import Any, Optional

from alembic.ddl.impl import DefaultImpl
from sqlalchemy import Column, Integer, MetaData, String, Table

__all__ = ["YDBImpl"]

#: Name of the surrogate primary key column added to the Alembic version table.
VERSION_TABLE_PK_COLUMN = "id"


class YDBImpl(DefaultImpl):
    """Alembic implementation for YDB.

    Registered for the ``yql`` dialect name as a side effect of being defined.
    """

    __dialect__ = "yql"

    def version_table_impl(
        self,
        *,
        version_table: str,
        version_table_schema: Optional[str],
        version_table_pk: bool,
        **kw: Any,
    ) -> Table:
        """Build the ``alembic_version`` table in a form YDB accepts.

        Two YDB constraints rule out Alembic's default version table:

        * Alembic's default table carries a *named* ``PrimaryKeyConstraint``,
          which SQLAlchemy renders as
          ``CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)``. YDB's
          parser rejects named constraints.
        * ``version_num`` cannot itself be the primary key, because Alembic
          advances a revision with
          ``UPDATE alembic_version SET version_num=<to> WHERE version_num=<from>``
          and YDB cannot update a primary key column
          (``Cannot update primary key column: version_num``).

        So the table gets a surrogate primary key column and leaves
        ``version_num`` writable. YDB requires every table to have a primary
        key, which is why ``version_table_pk`` is deliberately ignored.

        Alembic only ever supplies ``version_num`` when it inserts a row, so
        the surrogate column stays ``NULL``. That is what keeps the update
        atomic, and it is also why branched migrations with more than one head
        are not supported: a second head would need a second row, and both
        rows would collide on a ``NULL`` primary key.
        """
        return Table(
            version_table,
            MetaData(),
            Column("version_num", String(32), nullable=False),
            Column(VERSION_TABLE_PK_COLUMN, Integer(), nullable=True, primary_key=True),
            schema=version_table_schema,
        )
