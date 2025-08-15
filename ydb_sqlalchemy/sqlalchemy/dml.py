import sqlalchemy as sa


class Upsert(sa.sql.Insert):
    __visit_name__ = "upsert"
    _propagate_attrs = {"compile_state_plugin": "ydb"}
    stringify_dialect = "ydb"
    inherit_cache = False


@sa.sql.base.CompileState.plugin_for("ydb", "upsert")
class UpsertDMLState(sa.sql.dml.InsertDMLState):
    pass
