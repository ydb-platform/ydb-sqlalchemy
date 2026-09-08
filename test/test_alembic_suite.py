"""Alembic's test suite for third-party dialects, run against YDB.

Alembic ships ``alembic.testing.suite`` for dialect authors, the same way
SQLAlchemy ships its dialect compliance suite. It covers autogenerate
comparisons and the migration environment, and complements the YDB-specific
scenarios in ``test_alembic.py``.

Feature flags live in ``test/alembic_requirements.py``; only whole classes and
individual tests that no flag can express are skipped here.
"""

import pytest

from ydb_sqlalchemy.alembic import YDBImpl  # noqa: F401  isort:skip

from alembic.testing.suite import *  # noqa: E402,F401,F403
from alembic.testing.suite import AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest  # noqa: E402
from alembic.testing.suite import AutoincrementTest as _AutoincrementTest  # noqa: E402


@pytest.mark.skip("YDB has no foreign keys")
class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    pass


class AutoincrementTest(_AutoincrementTest):
    @pytest.mark.skip("The fixture table has no primary key, which YDB requires")
    def test_alter_column_autoincrement_none(self):
        pass
