"""
Experimental
Work in progress, breaking changes are possible.
"""
import collections
import collections.abc
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import sqlalchemy as sa
import ydb
from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultExecutionContext, StrCompileDialect
from sqlalchemy.exc import CompileError, NoSuchTableError
from sqlalchemy.sql import functions, literal_column
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    IdentifierPreparer,
    StrSQLCompiler,
    StrSQLTypeCompiler,
    selectable,
)
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.util.compat import inspect_getfullargspec

import ydb_sqlalchemy.dbapi as dbapi
from ydb_sqlalchemy.dbapi.constants import YDB_KEYWORDS
from ydb_sqlalchemy.sqlalchemy.dml import Upsert

from . import types

STR_QUOTE_MAP = {
    "'": "\\'",
    "\\": "\\\\",
    "\0": "\\0",
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "%": "%%",
}

COMPOUND_KEYWORDS = {
    selectable.CompoundSelect.UNION: "UNION ALL",
    selectable.CompoundSelect.UNION_ALL: "UNION ALL",
    selectable.CompoundSelect.EXCEPT: "EXCEPT",
    selectable.CompoundSelect.EXCEPT_ALL: "EXCEPT ALL",
    selectable.CompoundSelect.INTERSECT: "INTERSECT",
    selectable.CompoundSelect.INTERSECT_ALL: "INTERSECT ALL",
}


class YqlIdentifierPreparer(IdentifierPreparer):
    reserved_words = IdentifierPreparer.reserved_words
    reserved_words.update(YDB_KEYWORDS)

    def __init__(self, dialect):
        super(YqlIdentifierPreparer, self).__init__(
            dialect,
            initial_quote="`",
            final_quote="`",
        )


class YqlTypeCompiler(StrSQLTypeCompiler):
    def visit_CHAR(self, type_: sa.CHAR, **kw):
        return "UTF8"

    def visit_VARCHAR(self, type_: sa.VARCHAR, **kw):
        return "UTF8"

    def visit_unicode(self, type_: sa.Unicode, **kw):
        return "UTF8"

    def visit_uuid(self, type_: sa.Uuid, **kw):
        return "UTF8"

    def visit_NVARCHAR(self, type_: sa.NVARCHAR, **kw):
        return "UTF8"

    def visit_TEXT(self, type_: sa.TEXT, **kw):
        return "UTF8"

    def visit_FLOAT(self, type_: sa.FLOAT, **kw):
        return "FLOAT"

    def visit_BOOLEAN(self, type_: sa.BOOLEAN, **kw):
        return "BOOL"

    def visit_uint32(self, type_: types.UInt32, **kw):
        return "UInt32"

    def visit_uint64(self, type_: types.UInt64, **kw):
        return "UInt64"

    def visit_uint8(self, type_: types.UInt8, **kw):
        return "UInt8"

    def visit_INTEGER(self, type_: sa.INTEGER, **kw):
        return "Int64"

    def visit_NUMERIC(self, type_: sa.Numeric, **kw):
        """Only Decimal(22,9) is supported for table columns"""
        return f"Decimal({type_.precision}, {type_.scale})"

    def visit_BINARY(self, type_: sa.BINARY, **kw):
        return "String"

    def visit_BLOB(self, type_: sa.BLOB, **kw):
        return "String"

    def visit_DATETIME(self, type_: sa.TIMESTAMP, **kw):
        return "Timestamp"

    def visit_list_type(self, type_: types.ListType, **kw):
        inner = self.process(type_.item_type, **kw)
        return f"List<{inner}>"

    def visit_ARRAY(self, type_: sa.ARRAY, **kw):
        inner = self.process(type_.item_type, **kw)
        return f"List<{inner}>"

    def visit_struct_type(self, type_: types.StructType, **kw):
        text = "Struct<"
        for field, field_type in type_.fields_types:
            text += f"{field}:{self.process(field_type, **kw)}"
        return text + ">"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        if isinstance(type_, (sa.Text, sa.String, sa.Uuid)):
            ydb_type = ydb.PrimitiveType.Utf8
        elif isinstance(type_, sa.Integer):
            ydb_type = ydb.PrimitiveType.Int64
        elif isinstance(type_, sa.JSON):
            ydb_type = ydb.PrimitiveType.Json
        elif isinstance(type_, sa.DateTime):
            ydb_type = ydb.PrimitiveType.Timestamp
        elif isinstance(type_, sa.Date):
            ydb_type = ydb.PrimitiveType.Date
        elif isinstance(type_, sa.BINARY):
            ydb_type = ydb.PrimitiveType.String
        elif isinstance(type_, sa.Float):
            ydb_type = ydb.PrimitiveType.Float
        elif isinstance(type_, sa.Double):
            ydb_type = ydb.PrimitiveType.Double
        elif isinstance(type_, sa.Boolean):
            ydb_type = ydb.PrimitiveType.Bool
        elif isinstance(type_, sa.Numeric):
            ydb_type = ydb.DecimalType(type_.precision, type_.scale)
        elif isinstance(type_, (types.ListType, sa.ARRAY)):
            ydb_type = ydb.ListType(self.get_ydb_type(type_.item_type, is_optional=False))
        elif isinstance(type_, types.StructType):
            ydb_type = ydb.StructType()
            for field, field_type in type_.fields_types.items():
                ydb_type.add_member(field, self.get_ydb_type(field_type(), is_optional=False))
        else:
            raise dbapi.NotSupportedError(f"{type_} bind variables not supported")

        if is_optional:
            return ydb.OptionalType(ydb_type)

        return ydb_type


class ParametrizedFunction(functions.Function):
    __visit_name__ = "parametrized_function"

    def __init__(self, name, params, *args, **kwargs):
        super(ParametrizedFunction, self).__init__(name, *args, **kwargs)
        self._func_name = name
        self._func_params = params
        self.params_expr = ClauseList(operator=functions.operators.comma_op, group_contents=True, *params).self_group()


class YqlCompiler(StrSQLCompiler):
    compound_keywords = COMPOUND_KEYWORDS

    def render_bind_cast(self, type_, dbapi_type, sqltext):
        pass

    def group_by_clause(self, select, **kw):
        # Hack to ensure it is possible to define labels in groupby.
        kw.update(within_columns_clause=True)
        return super(YqlCompiler, self).group_by_clause(select, **kw)

    def render_literal_value(self, value, type_):
        if isinstance(value, str):
            value = "".join(STR_QUOTE_MAP.get(x, x) for x in value)
            return f"'{value}'"
        return super().render_literal_value(value, type_)

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

    def visit_parametrized_function(self, func, **kwargs):
        name = func.name
        name_parts = []
        for name in name.split("::"):
            fname = (
                self.preparer.quote(name)
                if self.preparer._requires_quotes_illegal_chars(name) or isinstance(name, sa.sql.elements.quoted_name)
                else name
            )

            name_parts.append(fname)

        name = "::".join(name_parts)
        params = func.params_expr._compiler_dispatch(self, **kwargs)
        args = self.function_argspec(func, **kwargs)
        return "%(name)s%(params)s%(args)s" % dict(name=name, params=params, args=args)

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        # Copypaste of `sa.sql.compiler.SQLCompiler.visit_function` with
        # `::` as namespace separator instead of `.`
        if add_to_result_map:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, f"visit_{func.name.lower()}_func", None)
        if disp:
            return disp(func, **kwargs)

        name = sa.sql.compiler.FUNCTIONS.get(func.__class__)
        if name:
            if func._has_args:
                name += "%(expr)s"
        else:
            name = func.name
            name = (
                self.preparer.quote(name)
                if self.preparer._requires_quotes_illegal_chars(name) or isinstance(name, sa.sql.elements.quoted_name)
                else name
            )
            name += "%(expr)s"

        return "::".join(
            [
                (
                    self.preparer.quote(tok)
                    if self.preparer._requires_quotes_illegal_chars(tok)
                    or isinstance(name, sa.sql.elements.quoted_name)
                    else tok
                )
                for tok in func.packagenames
            ]
            + [name]
        ) % {"expr": self.function_argspec(func, **kwargs)}

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " REGEXP ", **kw)

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " NOT REGEXP ", **kw)

    def _is_bound_to_nullable_column(self, bind_name: str) -> bool:
        if bind_name in self.column_keys and hasattr(self.compile_state, "dml_table"):
            if bind_name in self.compile_state.dml_table.c:
                column = self.compile_state.dml_table.c[bind_name]
                return not column.primary_key
        return False

    def _guess_bound_variable_type_by_parameters(
        self, bind: sa.BindParameter, post_compile_bind_values: list
    ) -> Optional[sa.types.TypeEngine]:
        if not bind.expanding:
            if isinstance(bind.type, sa.types.NullType):
                return None
            bind_type = bind.type
        else:
            not_null_values = [v for v in post_compile_bind_values if v is not None]
            if not_null_values:
                bind_type = sa.BindParameter("", not_null_values[0]).type
            else:
                return None

        return bind_type

    def _get_expanding_bind_names(self, bind_name: str, parameters_values: Mapping[str, List[Any]]) -> List[Any]:
        expanding_bind_names = []
        for parameter_name in parameters_values:
            parameter_bind_name = "_".join(parameter_name.split("_")[:-1])
            if parameter_bind_name == bind_name:
                expanding_bind_names.append(parameter_name)
        return expanding_bind_names

    def get_bind_types(
        self, post_compile_parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]
    ) -> Dict[str, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]]:
        """
        This method extracts information about bound variables from the table definition and parameters.
        """
        if isinstance(post_compile_parameters, collections.abc.Mapping):
            post_compile_parameters = [post_compile_parameters]

        parameters_values = collections.defaultdict(list)
        for parameters_entry in post_compile_parameters:
            for parameter_name, parameter_value in parameters_entry.items():
                parameters_values[parameter_name].append(parameter_value)

        parameter_types = {}
        for bind_name in self.bind_names.values():
            bind = self.binds[bind_name]

            if bind.literal_execute:
                continue

            if not bind.expanding:
                post_compile_bind_names = [bind_name]
                post_compile_bind_values = parameters_values[bind_name]
            else:
                post_compile_bind_names = self._get_expanding_bind_names(bind_name, parameters_values)
                post_compile_bind_values = []
                for parameter_name, parameter_values in parameters_values.items():
                    if parameter_name in post_compile_bind_names:
                        post_compile_bind_values.extend(parameter_values)

            is_optional = self._is_bound_to_nullable_column(bind_name)
            if not post_compile_bind_values or None in post_compile_bind_values:
                is_optional = True

            bind_type = self._guess_bound_variable_type_by_parameters(bind, post_compile_bind_values)

            if bind_type:
                for post_compile_bind_name in post_compile_bind_names:
                    parameter_types[post_compile_bind_name] = YqlTypeCompiler(self.dialect).get_ydb_type(
                        bind_type, is_optional
                    )

        return parameter_types

    def visit_upsert(self, insert_stmt, visited_bindparam=None, **kw):
        return self.visit_insert(insert_stmt, visited_bindparam, **kw).replace("INSERT", "UPSERT", 1)


class YqlDDLCompiler(DDLCompiler):
    def post_create_table(self, table: sa.Table) -> str:
        ydb_opts = table.dialect_options["ydb"]
        with_clause_list = self._render_table_partitioning_settings(ydb_opts)
        if with_clause_list:
            with_clause_text = ",\n".join(with_clause_list)
            return f"\nWITH (\n\t{with_clause_text}\n)"
        return ""

    def _render_table_partitioning_settings(self, ydb_opts: Dict[str, Any]) -> List[str]:
        table_partitioning_settings = []
        if ydb_opts["auto_partitioning_by_size"] is not None:
            auto_partitioning_by_size = "ENABLED" if ydb_opts["auto_partitioning_by_size"] else "DISABLED"
            table_partitioning_settings.append(f"AUTO_PARTITIONING_BY_SIZE = {auto_partitioning_by_size}")
        if ydb_opts["auto_partitioning_by_load"] is not None:
            auto_partitioning_by_load = "ENABLED" if ydb_opts["auto_partitioning_by_load"] else "DISABLED"
            table_partitioning_settings.append(f"AUTO_PARTITIONING_BY_LOAD = {auto_partitioning_by_load}")
        if ydb_opts["auto_partitioning_partition_size_mb"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_PARTITION_SIZE_MB = {ydb_opts['auto_partitioning_partition_size_mb']}"
            )
        if ydb_opts["auto_partitioning_min_partitions_count"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {ydb_opts['auto_partitioning_min_partitions_count']}"
            )
        if ydb_opts["auto_partitioning_max_partitions_count"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {ydb_opts['auto_partitioning_max_partitions_count']}"
            )
        if ydb_opts["uniform_partitions"] is not None:
            table_partitioning_settings.append(f"UNIFORM_PARTITIONS = {ydb_opts['uniform_partitions']}")
        if ydb_opts["partition_at_keys"] is not None:
            table_partitioning_settings.append(f"PARTITION_AT_KEYS = {ydb_opts['partition_at_keys']}")
        return table_partitioning_settings


def upsert(table):
    return Upsert(table)


COLUMN_TYPES = {
    ydb.PrimitiveType.Int8: sa.INTEGER,
    ydb.PrimitiveType.Int16: sa.INTEGER,
    ydb.PrimitiveType.Int32: sa.INTEGER,
    ydb.PrimitiveType.Int64: sa.INTEGER,
    ydb.PrimitiveType.Uint8: sa.INTEGER,
    ydb.PrimitiveType.Uint16: sa.INTEGER,
    ydb.PrimitiveType.Uint32: types.UInt32,
    ydb.PrimitiveType.Uint64: types.UInt64,
    ydb.PrimitiveType.Float: sa.FLOAT,
    ydb.PrimitiveType.Double: sa.FLOAT,
    ydb.PrimitiveType.String: sa.BINARY,
    ydb.PrimitiveType.Utf8: sa.TEXT,
    ydb.PrimitiveType.Json: sa.JSON,
    ydb.PrimitiveType.JsonDocument: sa.JSON,
    ydb.DecimalType: sa.DECIMAL,
    ydb.PrimitiveType.Yson: sa.TEXT,
    ydb.PrimitiveType.Date: sa.DATE,
    ydb.PrimitiveType.Datetime: sa.DATETIME,
    ydb.PrimitiveType.Timestamp: sa.DATETIME,
    ydb.PrimitiveType.Interval: sa.INTEGER,
    ydb.PrimitiveType.Bool: sa.BOOLEAN,
    ydb.PrimitiveType.DyNumber: sa.TEXT,
}


def _get_column_info(t):
    nullable = False
    if isinstance(t, ydb.OptionalType):
        nullable = True
        t = t.item

    if isinstance(t, ydb.DecimalType):
        return sa.DECIMAL(precision=t.precision, scale=t.scale), nullable

    return COLUMN_TYPES[t], nullable


class YqlDialect(StrCompileDialect):
    name = "yql"
    driver = "ydb"

    supports_alter = False
    max_identifier_length = 63
    supports_sane_rowcount = False
    supports_statement_cache = False

    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_smallserial = False
    supports_schemas = False
    supports_constraint_comments = False

    insert_returning = False
    update_returning = False
    delete_returning = False

    supports_sequences = False
    sequences_optional = False
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False

    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    default_paramstyle = "qmark"

    isolation_level = None

    preparer = YqlIdentifierPreparer
    statement_compiler = YqlCompiler
    ddl_compiler = YqlDDLCompiler
    type_compiler = YqlTypeCompiler

    construct_arguments = [
        (
            sa.schema.Table,
            {
                "auto_partitioning_by_size": None,
                "auto_partitioning_by_load": None,
                "auto_partitioning_partition_size_mb": None,
                "auto_partitioning_min_partitions_count": None,
                "auto_partitioning_max_partitions_count": None,
                "uniform_partitions": None,
                "partition_at_keys": None,
            },
        ),
    ]

    @classmethod
    def import_dbapi(cls: Any):
        return dbapi.YdbDBApi()

    def _describe_table(self, connection, table_name, schema=None):
        if schema is not None:
            raise dbapi.NotSupportedError("unsupported on non empty schema")

        qt = table_name if isinstance(table_name, str) else table_name.name
        raw_conn = connection.connection
        try:
            return raw_conn.describe(qt)
        except dbapi.DatabaseError as e:
            raise NoSuchTableError(qt) from e

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        table = self._describe_table(connection, table_name, schema)
        as_compatible = []
        for column in table.columns:
            col_type, nullable = _get_column_info(column.type)
            as_compatible.append(
                {
                    "name": column.name,
                    "type": col_type,
                    "nullable": nullable,
                    "default": None,
                }
            )

        return as_compatible

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw) -> List[str]:
        if schema:
            raise dbapi.NotSupportedError("unsupported on non empty schema")

        raw_conn = connection.connection
        return raw_conn.get_table_names()

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kwargs):
        try:
            self._describe_table(connection, table_name, schema)
            return True
        except NoSuchTableError:
            return False

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        table = self._describe_table(connection, table_name, schema)
        return {"constrained_columns": table.primary_key, "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        # foreign keys unsupported
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        # TODO: implement me
        return []

    def set_isolation_level(self, dbapi_connection: dbapi.Connection, level: str) -> None:
        dbapi_connection.set_isolation_level(level)

    def get_default_isolation_level(self, dbapi_conn: dbapi.Connection) -> str:
        return dbapi.IsolationLevel.AUTOCOMMIT

    def get_isolation_level(self, dbapi_connection: dbapi.Connection) -> str:
        return dbapi_connection.get_isolation_level()

    def connect(self, *cargs, **cparams):
        return self.loaded_dbapi.connect(*cargs, **cparams)

    def do_begin(self, dbapi_connection: dbapi.Connection) -> None:
        dbapi_connection.begin()

    def do_rollback(self, dbapi_connection: dbapi.Connection) -> None:
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection: dbapi.Connection) -> None:
        dbapi_connection.commit()

    def _format_variables(
        self,
        statement: str,
        parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]],
        execute_many: bool,
    ) -> Tuple[str, Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]]:
        formatted_statement = statement
        formatted_parameters = None

        if parameters:
            if execute_many:
                parameters_sequence: Sequence[Mapping[str, Any]] = parameters
                variable_names = set()
                formatted_parameters = []
                for i in range(len(parameters_sequence)):
                    variable_names.update(set(parameters_sequence[i].keys()))
                    formatted_parameters.append({f"${k}": v for k, v in parameters_sequence[i].items()})
            else:
                variable_names = set(parameters.keys())
                formatted_parameters = {f"${k}": v for k, v in parameters.items()}

            formatted_variable_names = {variable_name: f"${variable_name}" for variable_name in variable_names}
            formatted_statement = formatted_statement % formatted_variable_names

        formatted_statement = formatted_statement.replace("%%", "%")
        return formatted_statement, formatted_parameters

    def _make_ydb_operation(
        self,
        statement: str,
        context: Optional[DefaultExecutionContext] = None,
        parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]] = None,
        execute_many: bool = False,
    ) -> Tuple[dbapi.YdbQuery, Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]]:
        is_ddl = context.isddl if context is not None else False

        if not is_ddl and parameters:
            parameters_types = context.compiled.get_bind_types(parameters)
            parameters_types = {f"${k}": v for k, v in parameters_types.items()}
            statement, parameters = self._format_variables(statement, parameters, execute_many)
            return dbapi.YdbQuery(yql_text=statement, parameters_types=parameters_types, is_ddl=is_ddl), parameters

        statement, parameters = self._format_variables(statement, parameters, execute_many)
        return dbapi.YdbQuery(yql_text=statement, is_ddl=is_ddl), parameters

    def do_ping(self, dbapi_connection: dbapi.Connection) -> bool:
        cursor = dbapi_connection.cursor()
        statement, _ = self._make_ydb_operation(self._dialect_specific_select_one)
        try:
            cursor.execute(statement)
        finally:
            cursor.close()
        return True

    def do_executemany(
        self,
        cursor: dbapi.Cursor,
        statement: str,
        parameters: Optional[Sequence[Mapping[str, Any]]],
        context: Optional[DefaultExecutionContext] = None,
    ) -> None:
        operation, parameters = self._make_ydb_operation(statement, context, parameters, execute_many=True)
        cursor.executemany(operation, parameters)

    def do_execute(
        self,
        cursor: dbapi.Cursor,
        statement: str,
        parameters: Optional[Mapping[str, Any]] = None,
        context: Optional[DefaultExecutionContext] = None,
    ) -> None:
        operation, parameters = self._make_ydb_operation(statement, context, parameters, execute_many=False)
        cursor.execute(operation, parameters)


class AsyncYqlDialect(YqlDialect):
    driver = "ydb_async"
    is_async = True
    supports_statement_cache = False

    def connect(self, *cargs, **cparams):
        return self.loaded_dbapi.async_connect(*cargs, **cparams)
