import pytest
import uuid
import decimal
from datetime import date, datetime, timedelta

from .cursor import (
    _generate_type_str,
    _generate_declare_stms,
    _generate_full_stm,
    check_identifier_valid,
    ProgrammingError,
)


def test_check_identifier_valid():
    assert check_identifier_valid("id")
    assert check_identifier_valid("_id")
    assert check_identifier_valid("id0")
    assert check_identifier_valid("foo_bar")
    assert check_identifier_valid("foo_bar_1")

    with pytest.raises(ProgrammingError):
        check_identifier_valid("")

    with pytest.raises(ProgrammingError):
        check_identifier_valid("01")

    with pytest.raises(ProgrammingError):
        check_identifier_valid("(a)")

    with pytest.raises(ProgrammingError):
        check_identifier_valid("drop table")


def test_generate_type_str():
    assert _generate_type_str(True) == "Bool"
    assert _generate_type_str(1) == "Int64"
    assert _generate_type_str("foo") == "Utf8"
    assert _generate_type_str(b"foo") == "String"
    assert _generate_type_str(3.1415) == "Double"
    assert _generate_type_str(uuid.uuid4()) == "Uuid"
    assert _generate_type_str(decimal.Decimal("3.1415926535")) == "Decimal(22, 9)"

    assert _generate_type_str([1, 2, 3]) == "List<Int64>"
    assert _generate_type_str((1, "2", False)) == "Tuple<Int64, Utf8, Bool>"
    assert _generate_type_str({1, 2, 3}) == "Set<Int64>"
    assert _generate_type_str({"foo": 1, "bar": 2, "kek": 3.14}) == "Struct<foo: Int64, bar: Int64, kek: Double>"

    assert _generate_type_str([[1], [2], [3]]) == "List<List<Int64>>"
    assert _generate_type_str([{"a": 1, "b": 2}, {"a": 11, "b": 22}]) == "List<Struct<a: Int64, b: Int64>>"
    assert _generate_type_str(("foo", [1], 3.14)) == "Tuple<Utf8, List<Int64>, Double>"

    assert _generate_type_str(datetime.now()) == "Timestamp"
    assert _generate_type_str(date.today()) == "Date"
    assert _generate_type_str(timedelta(days=2)) == "Interval"

    with pytest.raises(ProgrammingError):
        assert _generate_type_str(None)

    with pytest.raises(ProgrammingError):
        assert _generate_type_str(object())


def test_generate_declare_stm():
    assert _generate_declare_stms({}) == ""
    assert _generate_declare_stms({"$p1": 123}).strip() == "DECLARE $p1 AS Int64;"
    assert _generate_declare_stms({"$p1": 123, "$p2": "foo"}).strip() == "DECLARE $p1 AS Int64; DECLARE $p2 AS Utf8;"

    assert _generate_declare_stms({"$foo": decimal.Decimal("3.14")}).strip() == "DECLARE $foo AS Decimal(22, 9);"
    assert _generate_declare_stms({"$foo": [1, 2, 3]}).strip() == "DECLARE $foo AS List<Int64>;"


def test_generate_full_stm():
    assert _generate_full_stm("select 1") == ("select 1", None)
    assert _generate_full_stm("select %(p1)s as value", {"p1": 1}) == (
        "DECLARE $p1 AS Int64; select $p1 as value",
        {"$p1": 1},
    )
    assert _generate_full_stm("select %(p1)s as value1, %(P2)s as value2", {"p1": 1, "P2": "123"}) == (
        "DECLARE $p1 AS Int64; DECLARE $P2 AS Utf8; select $p1 as value1, $P2 as value2",
        {"$p1": 1, "$P2": "123"},
    )

    assert _generate_full_stm("select %(p1)s as value", {"p1": None}) == ("select NULL as value", {})
