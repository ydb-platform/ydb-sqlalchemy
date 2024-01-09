from typing import Any
from typing import Optional
from typing import Union

import sqlalchemy as sa


class Upsert(sa.sql.Insert):
    __visit_name__ = "upsert"
    _propagate_attrs = {"compile_state_plugin": "yql"}

    def compile(
        self,
        bind=None,
        dialect=None,
        **kw: Any,
    ):
        return super(Upsert, self).compile(bind, **kw)


@sa.sql.base.CompileState.plugin_for("yql", "upsert")
class InsertDMLState(sa.sql.dml.InsertDMLState):
    pass
