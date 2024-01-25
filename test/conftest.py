import pytest
from sqlalchemy.dialects import registry

registry.register("yql.ydb", "ydb_sqlalchemy.sqlalchemy", "YqlDialect")
registry.register("yql.ydb_async", "ydb_sqlalchemy.sqlalchemy", "AsyncYqlDialect")
registry.register("ydb_async", "ydb_sqlalchemy.sqlalchemy", "AsyncYqlDialect")
registry.register("ydb", "ydb_sqlalchemy.sqlalchemy", "YqlDialect")
registry.register("yql", "ydb_sqlalchemy.sqlalchemy", "YqlDialect")
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403
