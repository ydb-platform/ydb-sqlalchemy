from typing import Union
import sqlalchemy as sa
import ydb

from sqlalchemy.exc import CompileError
from sqlalchemy.sql import literal_column
from sqlalchemy.util.compat import inspect_getfullargspec

from .base import (
    BaseYqlCompiler,
    BaseYqlDDLCompiler,
    BaseYqlIdentifierPreparer,
    BaseYqlTypeCompiler,
)


class YqlTypeCompiler(BaseYqlTypeCompiler):
    # We use YDB Double for sa.Float for compatibility with old dialect version
    def visit_FLOAT(self, type_: sa.FLOAT, **kw):
        return "DOUBLE"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        if isinstance(type_, sa.Float):
            ydb_type = ydb.PrimitiveType.Double
            if is_optional:
                return ydb.OptionalType(ydb_type)
            return ydb_type

        return super().get_ydb_type(type_, is_optional)


class YqlIdentifierPreparer(BaseYqlIdentifierPreparer):
    ...


class YqlCompiler(BaseYqlCompiler):
    _type_compiler_cls = YqlTypeCompiler

    def visit_json_getitem_op_binary(self, binary, operator, **kw):
        json_field = self.process(binary.left, **kw)
        index = self.process(binary.right, **kw)
        return self._yson_convert_to(f"{json_field}[{index}]", binary.type)

    def visit_json_path_getitem_op_binary(self, binary, operator, **kw):
        json_field = self.process(binary.left, **kw)
        path = self.process(binary.right, **kw)
        return self._yson_convert_to(f"Yson::YPath({json_field}, {path})", binary.type)

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " REGEXP ", **kw)

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " NOT REGEXP ", **kw)

    def visit_lambda(self, lambda_, **kw):
        func = lambda_.func
        spec = inspect_getfullargspec(func)

        if spec.varargs:
            raise CompileError("Lambdas with *args are not supported")
        if spec.varkw:
            raise CompileError("Lambdas with **kwargs are not supported")

        args = [literal_column("$" + arg) for arg in spec.args]
        text = f'({", ".join("$" + arg for arg in spec.args)}) -> ' f"{{ RETURN {self.process(func(*args), **kw)} ;}}"

        return text

    def _yson_convert_to(self, statement: str, target_type: sa.types.TypeEngine) -> str:
        if isinstance(target_type, sa.Float):
            # JSON.as_float() follows SQLAlchemy's generic FLOAT semantics.
            # The rest of the 1.4 dialect retains its historical mapping of
            # sa.Float to YDB Double.
            type_name = "FLOAT"
        else:
            type_name = target_type.compile(self.dialect)

        if isinstance(target_type, sa.Numeric) and not isinstance(target_type, sa.Float):
            # Since Decimal is stored in JSON either as String or as Float
            string_value = f"Yson::ConvertTo({statement}, Optional<String>, Yson::Options(true AS AutoConvert))"
            return f"CAST({string_value} AS Optional<{type_name}>)"
        return f"Yson::ConvertTo({statement}, Optional<{type_name}>)"

    def visit_upsert(self, insert_stmt, **kw):
        return self.visit_insert(insert_stmt, **kw).replace("INSERT", "UPSERT", 1)


class YqlDDLCompiler(BaseYqlDDLCompiler):
    ...
