from .base import (
    BaseYqlCompiler,
    BaseYqlDDLCompiler,
    BaseYqlIdentifierPreparer,
    BaseYqlTypeCompiler,
)


class YqlTypeCompiler(BaseYqlTypeCompiler):
    ...


class YqlIdentifierPreparer(BaseYqlIdentifierPreparer):
    ...


class YqlCompiler(BaseYqlCompiler):
    _type_compiler_cls = YqlTypeCompiler

    def visit_upsert(self, insert_stmt, **kw):
        return self.visit_insert(insert_stmt, **kw).replace("INSERT", "UPSERT", 1)


class YqlDDLCompiler(BaseYqlDDLCompiler):
    ...
