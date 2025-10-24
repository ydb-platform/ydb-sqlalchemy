import ctypes
import datetime
import decimal

import pytest
import sqlalchemy as sa
import sqlalchemy.testing.suite.test_types
from sqlalchemy.testing import is_false, is_true
from sqlalchemy.testing.suite import *  # noqa: F401, F403
from sqlalchemy.testing.suite import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    column,
    config,
    eq_,
    exists,
    fixtures,
    func,
    inspect,
    literal_column,
    provide_metadata,
    requirements,
    select,
    testing,
    union,
)
from sqlalchemy.testing.suite.test_ddl import (
    LongNameBlowoutTest as _LongNameBlowoutTest,
)
from sqlalchemy.testing.suite.test_dialect import (
    DifficultParametersTest as _DifficultParametersTest,
)
from sqlalchemy.testing.suite.test_dialect import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite.test_insert import (
    InsertBehaviorTest as _InsertBehaviorTest,
)
from sqlalchemy.testing.suite.test_reflection import (
    ComponentReflectionTest as _ComponentReflectionTest,
)
from sqlalchemy.testing.suite.test_reflection import (
    ComponentReflectionTestExtra as _ComponentReflectionTestExtra,
)
from sqlalchemy.testing.suite.test_reflection import (
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
)
from sqlalchemy.testing.suite.test_reflection import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite.test_reflection import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite.test_reflection import (
    QuotedNameArgumentTest as _QuotedNameArgumentTest,
)
from sqlalchemy.testing.suite.test_results import RowFetchTest as _RowFetchTest
from sqlalchemy.testing.suite.test_select import ExistsTest as _ExistsTest
from sqlalchemy.testing.suite.test_select import (
    FetchLimitOffsetTest as _FetchLimitOffsetTest,
)
from sqlalchemy.testing.suite.test_select import JoinTest as _JoinTest
from sqlalchemy.testing.suite.test_select import LikeFunctionsTest as _LikeFunctionsTest
from sqlalchemy.testing.suite.test_select import OrderByLabelTest as _OrderByLabelTest
from sqlalchemy.testing.suite.test_types import BinaryTest as _BinaryTest
from sqlalchemy.testing.suite.test_types import DateTest as _DateTest
from sqlalchemy.testing.suite.test_types import (
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
)
from sqlalchemy.testing.suite.test_types import (
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
)
from sqlalchemy.testing.suite.test_types import DateTimeTest as _DateTimeTest
from sqlalchemy.testing.suite.test_types import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite.test_types import JSONTest as _JSONTest

from sqlalchemy.testing.suite.test_types import NumericTest as _NumericTest
from sqlalchemy.testing.suite.test_types import StringTest as _StringTest
from sqlalchemy.testing.suite.test_types import (
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
)
from sqlalchemy.testing.suite.test_types import (
    TimestampMicrosecondsTest as _TimestampMicrosecondsTest,
)
from sqlalchemy.testing.suite.test_types import TimeTest as _TimeTest

from ydb_sqlalchemy.sqlalchemy import types as ydb_sa_types

test_types_suite = sqlalchemy.testing.suite.test_types
col_creator = test_types_suite.Column


OLD_SA = sa.__version__ < "2."


def column_getter(*args, **kwargs):
    col = col_creator(*args, **kwargs)
    if col.name == "x":
        col.primary_key = True
    return col


test_types_suite.Column = column_getter


class ComponentReflectionTest(_ComponentReflectionTest):
    def _check_list(self, result, exp, req_keys=None, msg=None):
        try:
            return super()._check_list(result, exp, req_keys, msg)
        except AssertionError as err:
            err_info = err.args[0]
            if "nullable" in err_info:
                return "We changed nullable in define_reflected_tables method so won't check it."
            if "constrained_columns" in err_info and "contains one more item: 'data'" in err_info:
                return "We changed primary_keys in define_reflected_tables method so this will fail"
            raise

    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        Table(
            "users",
            metadata,
            Column("user_id", sa.INT, primary_key=True),
            Column("test1", sa.CHAR(5)),
            Column("test2", sa.Float()),
            Column("parent_user_id", sa.Integer),
            schema=schema,
            test_needs_fk=True,
        )

        Table(
            "dingalings",
            metadata,
            Column("dingaling_id", sa.Integer, primary_key=True),
            Column("address_id", sa.Integer),
            Column("id_user", sa.Integer),
            Column("data", sa.String(30)),
            schema=schema,
            test_needs_fk=True,
        )

        Table(
            "email_addresses",
            metadata,
            Column("address_id", sa.Integer, primary_key=True),
            Column("remote_user_id", sa.Integer),
            Column("email_address", sa.String(20)),
            schema=schema,
            test_needs_fk=True,
        )

        Table(
            "comment_test",
            metadata,
            Column("id", sa.Integer, primary_key=True, comment="id comment"),
            Column("data", sa.String(20), comment="data % comment"),
            Column("d2", sa.String(20), comment=r"""Comment types type speedily ' " \ '' Fun!"""),
            schema=schema,
            comment=r"""the test % ' " \ table comment""",
        )

        Table(
            "no_constraints",
            metadata,
            Column("data", sa.String(20), primary_key=True, nullable=True),
            schema=schema,
        )

    @pytest.mark.skip("views unsupported")
    def test_get_view_names(self, connection, use_schema):
        pass

    def test_metadata(self, connection, **kwargs):
        m = MetaData()
        m.reflect(connection, resolve_fks=False)

        insp = inspect(connection)
        tables = insp.get_table_names()
        eq_(sorted(m.tables), sorted(tables))


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "tb1",
            metadata,
            Column("id", Integer),
            Column("attr", Integer),
            Column("name", sa.VARCHAR(20)),
            # named pk unsupported
            sa.PrimaryKeyConstraint("name", "id", "attr"),
            schema=None,
            test_needs_fk=True,
        )


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):
    def _type_round_trip(self, connection, metadata, *types):
        t = Table(
            "t",
            metadata,
            # table without pk unsupported
            *[Column("t%d" % i, type_, primary_key=True) for i, type_ in enumerate(types)],
        )
        t.create(connection)
        return [c["type"] for c in inspect(connection).get_columns("t")]

    @pytest.mark.skip("YDB: Only Decimal(22,9) is supported for table columns")
    def test_numeric_reflection(self):
        pass

    @pytest.mark.skip("TODO: varchar with length unsupported")
    def test_varchar_reflection(self):
        pass

    @testing.requires.table_reflection
    def test_nullable_reflection(self, connection, metadata):
        t = Table(
            "t",
            metadata,
            # table without pk unsupported
            Column("a", Integer, nullable=True, primary_key=True),
            Column("b", Integer, nullable=False, primary_key=True),
        )
        t.create(connection)
        eq_(
            {col["name"]: col["nullable"] for col in inspect(connection).get_columns("t")},
            {"a": True, "b": False},
        )


class HasTableTest(_HasTableTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )

    def test_has_table_cache(self, metadata):
        insp = inspect(config.db)
        is_true(insp.has_table("test_table"))
        # table without pk unsupported
        nt = Table("new_table", metadata, Column("col", Integer, primary_key=True))
        is_false(insp.has_table("new_table"))
        nt.create(config.db)
        try:
            is_false(insp.has_table("new_table"))
            insp.clear_cache()
            is_true(insp.has_table("new_table"))
        finally:
            nt.drop(config.db)


@pytest.mark.skip("CREATE INDEX syntax unsupported")
class HasIndexTest(_HasIndexTest):
    pass


@pytest.mark.skip("quotes unsupported in table names")
class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    pass


class IntegerTest(_IntegerTest):
    @pytest.mark.skip("YQL doesn't support select with where without from")
    def test_huge_int_auto_accommodation(self, connection, intvalue):
        pass


@pytest.mark.skip("Use YdbDecimalTest for Decimal type testing")
class NumericTest(_NumericTest):
    # SqlAlchemy maybe eat Decimal and throw Double
    pass


@pytest.mark.skip("TODO: see issue #13")
class BinaryTest(_BinaryTest):
    pass


if not OLD_SA:
    from sqlalchemy.testing.suite.test_types import TrueDivTest as _TrueDivTest

    class TrueDivTest(_TrueDivTest):
        @pytest.mark.skip("Unsupported builtin: FLOOR")
        def test_floordiv_numeric(self, connection, left, right, expected):
            pass

        @pytest.mark.skip("Truediv unsupported for int")
        def test_truediv_integer(self, connection, left, right, expected):
            pass

        @pytest.mark.skip("Truediv unsupported for int")
        def test_truediv_integer_bound(self, connection):
            pass

        @pytest.mark.skip("Numeric is not Decimal")
        def test_truediv_numeric(self):
            # SqlAlchemy maybe eat Decimal and throw Double
            pass

        @testing.combinations(("6.25", "2.5", 2.5), argnames="left, right, expected")
        def test_truediv_float(self, connection, left, right, expected):
            eq_(
                connection.scalar(
                    select(literal_column(left, type_=sa.Float()) / literal_column(right, type_=sa.Float()))
                ),
                expected,
            )


class ExistsTest(_ExistsTest):
    """
    YDB says: Filtering is not allowed without FROM so rewrite queries
    """

    def test_select_exists(self, connection):
        stuff = self.tables.stuff
        eq_(connection.execute(select(exists().where(stuff.c.data == "some data"))).fetchall(), [(True,)])

    def test_select_exists_false(self, connection):
        stuff = self.tables.stuff
        eq_(connection.execute(select(exists().where(stuff.c.data == "no data"))).fetchall(), [(False,)])


class LikeFunctionsTest(_LikeFunctionsTest):
    @testing.requires.regexp_match
    def test_not_regexp_match(self):
        col = self.tables.some_table.c.data
        # YDB fetch NULL columns too
        self._test(~col.regexp_match("a.cde"), {2, 3, 4, 7, 8, 10, 11})


class EscapingTest(_EscapingTest):
    @provide_metadata
    def test_percent_sign_round_trip(self):
        """test that the DBAPI accommodates for escaped / nonescaped
        percent signs in a way that matches the compiler

        """
        m = self.metadata
        # table without pk unsupported
        t = Table("t", m, Column("data", String(50), primary_key=True))
        t.create(config.db)
        with config.db.begin() as conn:
            conn.execute(t.insert(), dict(data="some % value"))
            conn.execute(t.insert(), dict(data="some %% other value"))

            eq_(conn.scalar(select(t.c.data).where(t.c.data == literal_column("'some % value'"))), "some % value")

            eq_(
                conn.scalar(select(t.c.data).where(t.c.data == literal_column("'some %% other value'"))),
                "some %% other value",
            )


@pytest.mark.skip("unsupported tricky names for columns")
class DifficultParametersTest(_DifficultParametersTest):
    pass


@pytest.mark.skip("JOIN ON expression must be a conjunction of equality predicates")
class JoinTest(_JoinTest):
    pass


class OrderByLabelTest(_OrderByLabelTest):
    def test_composed_multiple(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label("lx")
        ly = (table.c.q + table.c.p).label("ly")  # unknown builtin: lower
        self._assert_result(
            select(lx, ly).order_by(lx, ly.desc()),
            [(3, "q1p3"), (5, "q2p2"), (7, "q3p1")],
        )

    @testing.requires.group_by_complex_expression
    def test_group_by_composed(self):
        """
        YDB says: column `some_table.x` must either be a key column in GROUP BY
        or it should be used in aggregation function
        """
        table = self.tables.some_table
        expr = (table.c.x + table.c.y).label("lx")
        stmt = select(func.count(table.c.id), column("lx")).group_by(expr).order_by(column("lx"))
        self._assert_result(stmt, [(1, 3), (1, 5), (1, 7)])


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    def test_limit_render_multiple_times(self, connection):
        """
        YQL does not support scalar subquery, so test was refiled with simple subquery
        """
        table = self.tables.some_table
        stmt = select(table.c.id).limit(1).subquery()

        u = union(select(stmt), select(stmt)).subquery().select()

        self._assert_result(
            connection,
            u,
            [
                (1,),
                (1,),
            ],
        )


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip("autoincrement unsupported")
    def test_insert_from_select_autoinc(self, connection):
        pass

    @pytest.mark.skip("autoincrement unsupported")
    def test_insert_from_select_autoinc_no_rows(self, connection):
        pass

    @pytest.mark.skip("implicit PK values unsupported")
    def test_no_results_for_non_returning_insert(self, connection):
        pass


class DateTest(_DateTest):
    run_dispose_bind = "once"


class Date32Test(_DateTest):
    run_dispose_bind = "once"
    datatype = ydb_sa_types.YqlDate32
    data = datetime.date(1969, 1, 1)

    @pytest.mark.skip("Default binding for DATE is not compatible with Date32")
    def test_select_direct(self, connection):
        pass


class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    run_dispose_bind = "once"


class DateTimeTest(_DateTimeTest):
    run_dispose_bind = "once"


class DateTime64Test(_DateTimeTest):
    datatype = ydb_sa_types.YqlDateTime64
    data = datetime.datetime(1969, 10, 15, 12, 57, 18)
    run_dispose_bind = "once"

    @pytest.mark.skip("Default binding for DATETIME is not compatible with DateTime64")
    def test_select_direct(self, connection):
        pass


class TimestampMicrosecondsTest(_TimestampMicrosecondsTest):
    run_dispose_bind = "once"


class Timestamp64MicrosecondsTest(_TimestampMicrosecondsTest):
    run_dispose_bind = "once"
    datatype = ydb_sa_types.YqlTimestamp64
    data = datetime.datetime(1969, 10, 15, 12, 57, 18, 396)

    @pytest.mark.skip("Default binding for TIMESTAMP is not compatible with Timestamp64")
    def test_select_direct(self, connection):
        pass


@pytest.mark.skip("unsupported Time data type")
class TimeTest(_TimeTest):
    pass


class JSONTest(_JSONTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True, default=1),
            Column("name", String(30), primary_key=True, nullable=False),
            Column("data", cls.datatype, nullable=False),
            Column("nulldata", cls.datatype(none_as_null=True)),
        )

    def _json_value_insert(self, connection, datatype, value, data_element):
        if datatype == "float" and value is not None:
            # As python's float is stored as C double, it needs to be shrank
            value = ctypes.c_float(value).value
        return super()._json_value_insert(connection, datatype, value, data_element)


class StringTest(_StringTest):
    @requirements.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData()
        # table without pk unsupported
        foo = Table("foo", metadata, Column("one", String, primary_key=True))
        foo.create(config.db)
        foo.drop(config.db)


class ContainerTypesTest(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "container_types_test",
            metadata,
            Column("id", Integer),
            sa.PrimaryKeyConstraint("id"),
            schema=None,
            test_needs_fk=True,
        )

    def test_ARRAY_bind_variable(self, connection):
        table = self.tables.container_types_test

        connection.execute(sa.insert(table).values([{"id": 1}, {"id": 2}, {"id": 3}]))

        stmt = select(table.c.id).where(table.c.id.in_(sa.bindparam("id", type_=sa.ARRAY(sa.Integer))))

        eq_(connection.execute(stmt, {"id": [1, 2]}).fetchall(), [(1,), (2,)])

    def test_list_type_bind_variable(self, connection):
        table = self.tables.container_types_test

        connection.execute(sa.insert(table).values([{"id": 1}, {"id": 2}, {"id": 3}]))

        stmt = select(table.c.id).where(table.c.id.in_(sa.bindparam("id", type_=ydb_sa_types.ListType(sa.Integer))))

        eq_(connection.execute(stmt, {"id": [1, 2]}).fetchall(), [(1,), (2,)])

    def test_struct_type_bind_variable(self, connection):
        table = self.tables.container_types_test

        connection.execute(sa.insert(table).values([{"id": 1}, {"id": 2}, {"id": 3}]))

        stmt = select(table.c.id).where(
            table.c.id
            == sa.text(":struct.id").bindparams(
                sa.bindparam("struct", type_=ydb_sa_types.StructType({"id": sa.Integer})),
            )
        )

        eq_(connection.scalar(stmt, {"struct": {"id": 1}}), 1)

    def test_struct_type_bind_variable_text(self, connection):
        rs = connection.execute(
            sa.text("SELECT :struct.x + :struct.y").bindparams(
                sa.bindparam(
                    key="struct",
                    type_=ydb_sa_types.StructType({"x": sa.Integer, "y": sa.Integer}),
                    value={"x": 1, "y": 2},
                )
            )
        )
        assert rs.scalar() == 3

    def test_from_as_table(self, connection):
        table = self.tables.container_types_test

        connection.execute(
            sa.insert(table).from_select(
                ["id"],
                sa.select(sa.column("id")).select_from(
                    sa.func.as_table(
                        sa.bindparam(
                            "data",
                            value=[{"id": 1}, {"id": 2}, {"id": 3}],
                            type_=ydb_sa_types.ListType(ydb_sa_types.StructType({"id": sa.Integer})),
                        )
                    )
                ),
            )
        )

        eq_(connection.execute(sa.select(table)).fetchall(), [(1,), (2,), (3,)])


class ConcatTest(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "concat_func_test",
            metadata,
            Column("A", String),
            Column("B", String),
            sa.PrimaryKeyConstraint("A"),
            schema=None,
            test_needs_fk=True,
        )

    def test_concat_func(self, connection):
        table = self.tables.concat_func_test

        connection.execute(sa.insert(table).values([{"A": "A", "B": "B"}]))

        stmt = select(func.concat(table.c.A, " ", table.c.B)).limit(1)

        eq_(connection.scalar(stmt), "A B")


if not OLD_SA:
    from sqlalchemy.testing.suite.test_types import NativeUUIDTest as _NativeUUIDTest

    @pytest.mark.skip("uuid unsupported for columns")
    class NativeUUIDTest(_NativeUUIDTest):
        pass


@pytest.mark.skip("unsupported Time data type")
class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    pass


@pytest.mark.skip("unsupported coerce dates from datetime")
class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    pass


@pytest.mark.skip("named constraints unsupported")
class LongNameBlowoutTest(_LongNameBlowoutTest):
    pass


class RowFetchTest(_RowFetchTest):
    @pytest.mark.skip("scalar subquery unsupported")
    def test_row_w_scalar_select(self, connection):
        pass


class DecimalTest(fixtures.TablesTest):
    """Tests for YDB Decimal type using standard sa.DECIMAL"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "decimal_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("decimal_default", sa.DECIMAL),  # Default: precision=22, scale=9
            Column("decimal_custom", sa.DECIMAL(precision=10, scale=2)),
            Column("decimal_as_float", sa.DECIMAL(asdecimal=False)),  # Should behave like Float
        )

    def test_decimal_basic_operations(self, connection):
        """Test basic insert and select operations with Decimal"""

        table = self.tables.decimal_test

        test_values = [
            decimal.Decimal("1"),
            decimal.Decimal("2"),
            decimal.Decimal("3"),
        ]

        # Insert test values
        for i, val in enumerate(test_values):
            connection.execute(table.insert().values(id=i + 1, decimal_default=val))

        # Select and verify
        results = connection.execute(select(table.c.decimal_default).order_by(table.c.id)).fetchall()

        for i, (result,) in enumerate(results):
            expected = test_values[i]
            assert isinstance(result, decimal.Decimal)
            assert result == expected

    def test_decimal_with_precision_scale(self, connection):
        """Test Decimal with specific precision and scale"""

        table = self.tables.decimal_test

        # Test value that fits precision(10, 2)
        test_value = decimal.Decimal("12345678.99")

        connection.execute(table.insert().values(id=100, decimal_custom=test_value))

        result = connection.scalar(select(table.c.decimal_custom).where(table.c.id == 100))

        assert isinstance(result, decimal.Decimal)
        assert result == test_value

    def test_decimal_literal_rendering(self, connection):
        """Test literal rendering of Decimal values"""
        from sqlalchemy import literal

        table = self.tables.decimal_test

        # Test literal in INSERT
        test_value = decimal.Decimal("999.99")

        connection.execute(table.insert().values(id=300, decimal_default=literal(test_value, sa.DECIMAL())))

        result = connection.scalar(select(table.c.decimal_default).where(table.c.id == 300))

        assert isinstance(result, decimal.Decimal)
        assert result == test_value

    def test_decimal_overflow(self, connection):
        """Test behavior when precision is exceeded"""

        table = self.tables.decimal_test

        # Try to insert value that exceeds precision=10, scale=2
        overflow_value = decimal.Decimal("99999.99999")

        with pytest.raises(Exception):  # Should raise some kind of database error
            connection.execute(table.insert().values(id=500, decimal_custom=overflow_value))
            connection.commit()

    def test_decimal_asdecimal_false(self, connection):
        """Test DECIMAL with asdecimal=False (should return float)"""

        table = self.tables.decimal_test

        test_value = decimal.Decimal("123.45")

        connection.execute(table.insert().values(id=600, decimal_as_float=test_value))

        result = connection.scalar(select(table.c.decimal_as_float).where(table.c.id == 600))

        assert isinstance(result, float), f"Expected float, got {type(result)}"
        assert abs(result - 123.45) < 0.01

    def test_decimal_arithmetic(self, connection):
        """Test arithmetic operations with Decimal columns"""

        table = self.tables.decimal_test

        val1 = decimal.Decimal("100.50")
        val2 = decimal.Decimal("25.25")

        connection.execute(table.insert().values(id=900, decimal_default=val1))
        connection.execute(table.insert().values(id=901, decimal_default=val2))

        # Test various arithmetic operations
        addition_result = connection.scalar(
            select(table.c.decimal_default + decimal.Decimal("10.00")).where(table.c.id == 900)
        )

        subtraction_result = connection.scalar(
            select(table.c.decimal_default - decimal.Decimal("5.25")).where(table.c.id == 900)
        )

        multiplication_result = connection.scalar(
            select(table.c.decimal_default * decimal.Decimal("2.0")).where(table.c.id == 901)
        )

        division_result = connection.scalar(
            select(table.c.decimal_default / decimal.Decimal("2.0")).where(table.c.id == 901)
        )

        # Verify results
        assert abs(addition_result - decimal.Decimal("110.50")) < decimal.Decimal("0.01")
        assert abs(subtraction_result - decimal.Decimal("95.25")) < decimal.Decimal("0.01")
        assert abs(multiplication_result - decimal.Decimal("50.50")) < decimal.Decimal("0.01")
        assert abs(division_result - decimal.Decimal("12.625")) < decimal.Decimal("0.01")

    def test_decimal_comparison_operations(self, connection):
        """Test comparison operations with Decimal columns"""

        table = self.tables.decimal_test

        values = [
            decimal.Decimal("10.50"),
            decimal.Decimal("20.75"),
            decimal.Decimal("15.25"),
        ]

        for i, val in enumerate(values):
            connection.execute(table.insert().values(id=1000 + i, decimal_default=val))

        # Test various comparisons
        greater_than = connection.execute(
            select(table.c.id).where(table.c.decimal_default > decimal.Decimal("15.00")).order_by(table.c.id)
        ).fetchall()

        less_than = connection.execute(
            select(table.c.id).where(table.c.decimal_default < decimal.Decimal("15.00")).order_by(table.c.id)
        ).fetchall()

        equal_to = connection.execute(
            select(table.c.id).where(table.c.decimal_default == decimal.Decimal("15.25"))
        ).fetchall()

        between_values = connection.execute(
            select(table.c.id)
            .where(table.c.decimal_default.between(decimal.Decimal("15.00"), decimal.Decimal("21.00")))
            .order_by(table.c.id)
        ).fetchall()

        # Verify results
        assert len(greater_than) == 2  # 20.75 and 15.25
        assert len(less_than) == 1  # 10.50
        assert len(equal_to) == 1  # 15.25
        assert len(between_values) == 2  # 20.75 and 15.25

    def test_decimal_null_handling(self, connection):
        """Test NULL handling with Decimal columns"""

        table = self.tables.decimal_test

        # Insert NULL value
        connection.execute(table.insert().values(id=1100, decimal_default=None))

        # Insert non-NULL value for comparison
        connection.execute(table.insert().values(id=1101, decimal_default=decimal.Decimal("42.42")))

        # Test NULL retrieval
        null_result = connection.scalar(select(table.c.decimal_default).where(table.c.id == 1100))

        non_null_result = connection.scalar(select(table.c.decimal_default).where(table.c.id == 1101))

        assert null_result is None
        assert non_null_result == decimal.Decimal("42.42")

        # Test IS NULL / IS NOT NULL
        null_count = connection.scalar(select(func.count()).where(table.c.decimal_default.is_(None)))

        not_null_count = connection.scalar(select(func.count()).where(table.c.decimal_default.isnot(None)))

        # Should have at least 1 NULL and several non-NULL values from other tests
        assert null_count >= 1
        assert not_null_count >= 1

    def test_decimal_input_type_conversion(self, connection):
        """Test that bind_processor handles different input types correctly (float, string, int, Decimal)"""

        table = self.tables.decimal_test

        # Test different input types that should all be converted to Decimal
        test_cases = [
            (1400, 123.45, "float input"),  # float
            (1401, "456.78", "string input"),  # string
            (1402, decimal.Decimal("789.12"), "decimal input"),  # already Decimal
            (1403, 100, "int input"),  # int
        ]

        for test_id, input_value, description in test_cases:
            connection.execute(table.insert().values(id=test_id, decimal_default=input_value))

            result = connection.scalar(select(table.c.decimal_default).where(table.c.id == test_id))

            # All should be returned as Decimal
            assert isinstance(result, decimal.Decimal), f"Failed for {description}: got {type(result)}"

            # Verify the value is approximately correct
            expected = decimal.Decimal(str(input_value))
            error_str = f"Failed for {description}: expected {expected}, got {result}"
            assert abs(result - expected) < decimal.Decimal("0.01"), error_str

    def test_decimal_asdecimal_comparison(self, connection):
        """Test comparison between asdecimal=True and asdecimal=False behavior"""

        table = self.tables.decimal_test

        test_value = decimal.Decimal("999.123")

        # Insert same value into both columns
        connection.execute(
            table.insert().values(
                id=1500,
                decimal_default=test_value,  # asdecimal=True (default)
                decimal_as_float=test_value,  # asdecimal=False
            )
        )

        # Get results from both columns
        result_as_decimal = connection.scalar(select(table.c.decimal_default).where(table.c.id == 1500))
        result_as_float = connection.scalar(select(table.c.decimal_as_float).where(table.c.id == 1500))

        # Check types are different
        assert isinstance(result_as_decimal, decimal.Decimal), f"Expected Decimal, got {type(result_as_decimal)}"
        assert isinstance(result_as_float, float), f"Expected float, got {type(result_as_float)}"

        # Check values are approximately equal
        assert abs(result_as_decimal - test_value) < decimal.Decimal("0.001")
        assert abs(result_as_float - float(test_value)) < 0.001

        # Check that converting between them gives same value
        assert abs(float(result_as_decimal) - result_as_float) < 0.001
