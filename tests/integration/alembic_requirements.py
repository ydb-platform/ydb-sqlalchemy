"""Requirements for the SQLAlchemy suite plus, when available, Alembic's.

Alembic's third-party dialect suite reads its own feature flags off the same
requirements object the SQLAlchemy suite uses, so the two sets have to be
combined into one class. Alembic stays optional: without it installed this is
the plain dialect requirements class.
"""

from ydb_sqlalchemy.sqlalchemy.requirements import Requirements as DialectRequirements

try:
    from alembic.testing.requirements import SuiteRequirements as AlembicRequirements
except ImportError:  # pragma: no cover
    AlembicRequirements = object

from sqlalchemy.testing import exclusions


class Requirements(DialectRequirements, AlembicRequirements):
    @property
    def alter_column(self):
        # YDB's ALTER COLUMN sets column options and can DROP NOT NULL; it
        # cannot change a type, which is what the suite alters.
        return exclusions.closed()

    @property
    def comments(self):
        return exclusions.closed()

    @property
    def computed_columns(self):
        return exclusions.closed()

    @property
    def identity_columns(self):
        return exclusions.closed()

    @property
    def identity_columns_alter(self):
        return exclusions.closed()

    @property
    def autoincrement_on_composite_pk(self):
        return exclusions.closed()

    @property
    def fk_names(self):
        return exclusions.closed()

    @property
    def fk_initially(self):
        return exclusions.closed()

    @property
    def fk_deferrable(self):
        return exclusions.closed()

    @property
    def fk_deferrable_is_reflected(self):
        return exclusions.closed()

    @property
    def fk_ondelete_is_reflected(self):
        return exclusions.closed()

    @property
    def fk_onupdate_is_reflected(self):
        return exclusions.closed()

    @property
    def fk_onupdate(self):
        return exclusions.closed()

    @property
    def fk_ondelete_restrict(self):
        return exclusions.closed()

    @property
    def fk_onupdate_restrict(self):
        return exclusions.closed()

    @property
    def fk_ondelete_noaction(self):
        return exclusions.closed()
