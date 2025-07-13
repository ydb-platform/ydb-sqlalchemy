from datetime import date
import sqlalchemy as sa

from . import YDBDialect, types


def test_casts():
    dialect = YDBDialect()
    expr = sa.literal_column("1/2")

    res_exprs = [
        sa.cast(expr, types.UInt32),
        sa.cast(expr, types.UInt64),
        sa.cast(expr, types.UInt8),
        sa.func.String.JoinFromList(
            sa.func.ListMap(sa.func.TOPFREQ(expr, 5), types.Lambda(lambda x: sa.cast(x, sa.Text))),
            ", ",
        ),
    ]

    strs = [str(res_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) for res_expr in res_exprs]

    assert strs == [
        "CAST(1/2 AS UInt32)",
        "CAST(1/2 AS UInt64)",
        "CAST(1/2 AS UInt8)",
        "String::JoinFromList(ListMap(TOPFREQ(1/2, 5), ($x) -> { RETURN CAST($x AS UTF8) ;}), ', ')",
    ]


def test_ydb_types():
    dialect = YqlDialect()

    query = sa.literal(date(1996, 11, 19))
    compiled = query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == "Date('1996-11-19')"
