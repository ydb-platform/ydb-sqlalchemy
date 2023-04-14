import pytest
import sqlalchemy as sa
import sqlalchemy.testing.suite.test_types
from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite.test_select import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite.test_reflection import (
    HasTableTest as _HasTableTest,
    HasIndexTest as _HasIndexTest,
    ComponentReflectionTest as _ComponentReflectionTest,
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
    ComponentReflectionTestExtra as _ComponentReflectionTestExtra,
    QuotedNameArgumentTest as _QuotedNameArgumentTest,
)
from sqlalchemy.testing.suite.test_types import (
    IntegerTest as _IntegerTest,
    TrueDivTest as _TrueDivTest,
    TimeTest as _TimeTest,
    TimeMicrosecondsTest as _TimeMicrosecondsTest,
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
)
from sqlalchemy.testing.suite.test_dialect import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing.suite.test_select import JoinTest as _JoinTest


test_types_suite = sqlalchemy.testing.suite.test_types
col_creator = test_types_suite.Column


def column_getter(*args, **kwargs):
    col = col_creator(*args, **kwargs)
    if col.name == "x":
        col.primary_key = True
    return col


test_types_suite.Column = column_getter


class ComponentReflectionTest(_ComponentReflectionTest):
    @property
    def _required_column_keys(self):
        # nullable had changed so don't check it.
        return {"name", "type", "default"}

    def _check_list(self, result, exp, req_keys=None, msg=None):
        try:
            return super()._check_list(result, exp, req_keys, msg)
        except AssertionError as err:
            if "nullable" in err.args[0]:
                return "We changed nullable in define_reflected_tables method so won't check it."
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

    @pytest.mark.skip("TODO: pk key reflection unsupported")
    def test_pk_column_order(self, connection):
        pass


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

    @pytest.mark.skip("TODO: numeric now int64??")
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
            {
                col["name"]: col["nullable"]
                for col in inspect(connection).get_columns("t")
            },
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

    @pytest.mark.skip("TODO: reflection cache unsupported")
    def test_has_table_cache(self, metadata):
        pass


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
        pass


class CompoundSelectTest(_CompoundSelectTest):
    @pytest.mark.skip("limit don't work")
    def test_distinct_selectable_in_unions(self):
        pass

    @pytest.mark.skip("limit don't work")
    def test_limit_offset_in_unions_from_alias(self):
        pass

    @pytest.mark.skip("limit don't work")
    def test_limit_offset_aliased_selectable_in_unions(self):
        pass

    @pytest.mark.skip("union with brackets don't work")
    def test_order_by_selectable_in_unions(self):
        pass

    @pytest.mark.skip("union with brackets don't work")
    def test_limit_offset_selectable_in_unions(self):
        pass


@pytest.mark.skip("unsupported tricky names for columns")
class DifficultParametersTest(_DifficultParametersTest):
    pass


@pytest.mark.skip("JOIN ON expression must be a conjunction of equality predicates")
class JoinTest(_JoinTest):
    pass


@pytest.mark.skip("unsupported Time data type")
class TimeTest(_TimeTest):
    pass


@pytest.mark.skip("unsupported Time data type")
class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    pass


@pytest.mark.skip("unsupported coerce dates from datetime")
class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    pass
