from datetime import date, datetime
import uuid

import pytest
import sqlalchemy as sa
import ydb

from . import YqlDialect, types


def test_casts():
    dialect = YqlDialect()
    expr = sa.literal_column("1/2")

    res_exprs = [
        sa.cast(expr, types.UInt32),
        sa.cast(expr, types.UInt64),
        sa.cast(expr, types.UInt8),
    ]

    strs = [str(res_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) for res_expr in res_exprs]

    assert strs == [
        "CAST(1/2 AS UInt32)",
        "CAST(1/2 AS UInt64)",
        "CAST(1/2 AS UInt8)",
    ]


def test_lambda_compilation():
    dialect = YqlDialect()
    expr = sa.literal_column("1/2")
    statement = sa.func.String.JoinFromList(
        sa.func.ListMap(sa.func.TOPFREQ(expr, 5), types.Lambda(lambda x: sa.cast(x, sa.Text))),
        ", ",
    )

    compiled = statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == (
        "String::JoinFromList(ListMap(TOPFREQ(1/2, 5), ($x) -> { RETURN CAST($x AS UTF8) ;}), ', ')"
    )


@pytest.mark.parametrize(
    "type_,value,expected",
    [
        (types.YqlDate(), date(1996, 11, 19), "Date('1996-11-19')"),
        (types.YqlDate32(), date(1996, 11, 19), "Date32(Date('1996-11-19'))"),
        (
            types.YqlTimestamp64(),
            datetime(1996, 11, 19, 12, 34, 56, 789),
            "Timestamp64('1996-11-19 12:34:56.000789')",
        ),
        (
            types.YqlDateTime64(),
            datetime(1996, 11, 19, 12, 34, 56, 789),
            "DateTime64('1996-11-19 12:34:56.000789')",
        ),
    ],
)
def test_datetime_literal_compilation(type_, value, expected):
    dialect = YqlDialect()

    query = sa.literal(value, type_=type_)
    compiled = query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == expected


def test_binary_type():
    dialect = YqlDialect()
    expr = sa.literal(b"some bytes")
    compiled = expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert str(compiled) == "'some bytes'"

    expr_binary = sa.cast(expr, sa.BINARY)
    compiled_binary = expr_binary.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert str(compiled_binary) == "CAST('some bytes' AS String)"


def test_all_binary_types():
    dialect = YqlDialect()
    expr = sa.literal(b"some bytes")

    binary_types = [
        sa.BINARY,
        sa.LargeBinary,
        sa.BLOB,
        types.Binary,
    ]

    for type_ in binary_types:
        expr_binary = sa.cast(expr, type_)
        compiled_binary = expr_binary.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        assert str(compiled_binary) == "CAST('some bytes' AS String)"


def test_struct_type_generation():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    # Test default (non-optional)
    struct_type = types.StructType(
        {
            "id": sa.Integer,
            "val_int": sa.Integer,
        }
    )
    ydb_type = type_compiler.get_ydb_type(struct_type, is_optional=False)
    # Keys are sorted
    assert str(ydb_type) == "Struct<id:Int64,val_int:Int64>"

    # Test optional
    struct_type_opt = types.StructType(
        {
            "id": sa.Integer,
            "val_int": types.Optional(sa.Integer),
        }
    )
    ydb_type_opt = type_compiler.get_ydb_type(struct_type_opt, is_optional=False)
    assert str(ydb_type_opt) == "Struct<id:Int64,val_int:Int64?>"


def test_types_compilation():
    dialect = YqlDialect()

    def compile_type(type_):
        return dialect.type_compiler.process(type_)

    assert compile_type(types.UInt64()) == "UInt64"
    assert compile_type(types.UInt32()) == "UInt32"
    assert compile_type(types.UInt16()) == "UInt16"
    assert compile_type(types.UInt8()) == "UInt8"

    assert compile_type(types.Int64()) == "Int64"
    assert compile_type(types.Int32()) == "Int32"
    assert compile_type(types.Int16()) == "Int32"
    assert compile_type(types.Int8()) == "Int8"

    assert compile_type(types.ListType(types.Int64())) == "List<Int64>"

    struct = types.StructType({"a": types.Int32(), "b": types.ListType(types.Int32())})
    # Ordered by key: a, b
    assert compile_type(struct) == "Struct<a:Int32,b:List<Int32>>"


def test_native_uuid_is_explicit_opt_in():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    assert type_compiler.process(types.YqlUUID()) == "UUID"
    assert type_compiler.get_ydb_type(types.YqlUUID(), is_optional=False) == ydb.PrimitiveType.UUID
    assert type_compiler.get_ydb_type(types.YqlUUID(), is_optional=True).item == ydb.PrimitiveType.UUID

    if not hasattr(sa, "Uuid"):
        return

    assert dialect.supports_native_uuid is False
    assert type_compiler.process(sa.Uuid()) == "UTF8"
    assert type_compiler.get_ydb_type(sa.Uuid(), is_optional=False) == ydb.PrimitiveType.Utf8
    assert type_compiler.process(sa.UUID()) == "UUID"
    assert type_compiler.get_ydb_type(sa.UUID(), is_optional=False) == ydb.PrimitiveType.UUID

    dialect_impl = sa.UUID(as_uuid=False).dialect_impl(dialect)
    assert isinstance(dialect_impl, types.YqlUUID)
    assert dialect_impl.as_uuid is False


def test_native_uuid_processors():
    dialect = YqlDialect()
    value = uuid.uuid4()
    uuid_type = types.YqlUUID()

    bind_processor = uuid_type.bind_processor(dialect)
    assert bind_processor(None) is None
    assert bind_processor(value) == value
    assert bind_processor(str(value)) == value
    with pytest.raises(ValueError):
        bind_processor("not-a-uuid")

    result_processor = uuid_type.result_processor(dialect, None)
    assert result_processor(None) is None
    assert result_processor(value) == value
    assert result_processor(str(value)) == value
    assert uuid_type.literal_processor(dialect)(value) == f'Uuid("{value}")'

    text_uuid_type = types.YqlUUID(as_uuid=False)
    assert text_uuid_type.bind_processor(dialect)(str(value)) == value
    assert text_uuid_type.result_processor(dialect, None)(value) == str(value)

    text_literal = sa.literal(str(value), text_uuid_type)
    assert str(text_literal.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) == f'Uuid("{value}")'


def test_statement_prefixes_prepended_to_query():
    dialect = YqlDialect(_statement_prefixes_list=["PRAGMA DistinctOverKeys;"])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "PRAGMA DistinctOverKeys;\nSELECT 1"


def test_statement_prefixes_empty_list_unchanged():
    dialect = YqlDialect(_statement_prefixes_list=[])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "SELECT 1"


def test_statement_prefixes_none_unchanged():
    dialect = YqlDialect()
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "SELECT 1"


def test_statement_prefixes_multiple():
    dialect = YqlDialect(_statement_prefixes_list=["PRAGMA Foo;", "PRAGMA Bar;"])
    result = dialect._apply_statement_prefixes_impl("SELECT 1")
    assert result == "PRAGMA Foo;\nPRAGMA Bar;\nSELECT 1"


def test_optional_type_compilation():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    def compile_type(type_):
        return type_compiler.process(type_)

    # Test Optional(Integer)
    opt_int = types.Optional(sa.Integer)
    assert compile_type(opt_int) == "Optional<Int64>"

    # Test Optional(String)
    opt_str = types.Optional(sa.String)
    assert compile_type(opt_str) == "Optional<UTF8>"

    # Test Nested Optional
    opt_opt_int = types.Optional(types.Optional(sa.Integer))
    assert compile_type(opt_opt_int) == "Optional<Optional<Int64>>"

    # Test get_ydb_type
    ydb_type = type_compiler.get_ydb_type(opt_int, is_optional=False)
    import ydb

    assert isinstance(ydb_type, ydb.OptionalType)
    # Int64 corresponds to PrimitiveType.Int64
    # Note: ydb.PrimitiveType.Int64 is an enum member, but ydb_type.item is also an instance/enum?
    # get_ydb_type returns ydb.PrimitiveType.Int64 (enum) wrapped in OptionalType.
    # OptionalType.item is the inner type.
    assert ydb_type.item == ydb.PrimitiveType.Int64


def test_bind_expression_uses_runtime_parameter_type():
    class StringAsInt(sa.TypeDecorator):
        impl = sa.String(50)
        cache_ok = True

        def bind_expression(self, bindvalue):
            return sa.cast(bindvalue, sa.String(50))

    dialect = YqlDialect()
    table = sa.Table("type_decorator", sa.MetaData(), sa.Column("value", StringAsInt()))
    compiled = table.insert().compile(dialect=dialect, column_keys=["value"])

    assert str(compiled.get_bind_types({"value": 42})["value"]) == "Int64?"
