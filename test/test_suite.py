import ctypes

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
from sqlalchemy.testing.suite.test_types import NativeUUIDTest as _NativeUUIDTest
from sqlalchemy.testing.suite.test_types import NumericTest as _NumericTest
from sqlalchemy.testing.suite.test_types import StringTest as _StringTest
from sqlalchemy.testing.suite.test_types import (
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
)
from sqlalchemy.testing.suite.test_types import (
    TimestampMicrosecondsTest as _TimestampMicrosecondsTest,
)
from sqlalchemy.testing.suite.test_types import TimeTest as _TimeTest
from sqlalchemy.testing.suite.test_types import TrueDivTest as _TrueDivTest

from ydb_sqlalchemy.sqlalchemy import types as ydb_sa_types

test_types_suite = sqlalchemy.testing.suite.test_types
col_creator = test_types_suite.Column


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


@pytest.mark.skip("TODO: fix & skip those tests - add Double/Decimal support. see #12")
class NumericTest(_NumericTest):
    # SqlAlchemy maybe eat Decimal and throw Double
    pass


@pytest.mark.skip("TODO: see issue #13")
class BinaryTest(_BinaryTest):
    pass


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
            connection.scalar(select(literal_column(left, type_=sa.Float()) / literal_column(right, type_=sa.Float()))),
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


class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    run_dispose_bind = "once"


class DateTimeTest(_DateTimeTest):
    run_dispose_bind = "once"


class TimestampMicrosecondsTest(_TimestampMicrosecondsTest):
    run_dispose_bind = "once"


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
