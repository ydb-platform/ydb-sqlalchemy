import pytest
from sqlalchemy.dialects import registry

registry.register("ydb", "ydb_sqlalchemy.sqlalchemy", "YDBDialect")
registry.register("ydb.ydb_sync", "ydb_sqlalchemy.sqlalchemy", "YDBDialect")
registry.register("ydb_async", "ydb_sqlalchemy.sqlalchemy", "AsyncYDBDialect")
registry.register("ydb.ydb_async", "ydb_sqlalchemy.sqlalchemy", "AsyncYDBDialect")
pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403
