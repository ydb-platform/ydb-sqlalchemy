import datetime
from typing import Optional

from sqlalchemy import types as sqltypes


def _iso_literal(value):
    if isinstance(value, datetime.datetime):
        value = value.isoformat(" ")
    else:
        value = value.isoformat()
    return f"'{value}'"


def _literal_processor(parent, constructor):
    def process(value):
        literal = parent(value) if parent is not None else _iso_literal(value)
        return f"{constructor}({literal})"

    return process


def _interval_literal(value: datetime.timedelta) -> str:
    total_microseconds = (value.days * 24 * 60 * 60 + value.seconds) * 1_000_000 + value.microseconds
    sign = "-" if total_microseconds < 0 else ""
    total_microseconds = abs(total_microseconds)

    days, remainder = divmod(total_microseconds, 24 * 60 * 60 * 1_000_000)
    hours, remainder = divmod(remainder, 60 * 60 * 1_000_000)
    minutes, remainder = divmod(remainder, 60 * 1_000_000)
    seconds, microseconds = divmod(remainder, 1_000_000)

    result = f"{sign}P"
    if days:
        result += f"{days}D"
    if hours or minutes or seconds or microseconds or not days:
        result += "T"
        if hours:
            result += f"{hours}H"
        if minutes:
            result += f"{minutes}M"
        if microseconds:
            result += f"{seconds}.{microseconds:06d}S"
        elif seconds or not (hours or minutes):
            result += f"{seconds}S"

    return f"'{result}'"


class YqlDate(sqltypes.Date):
    def literal_processor(self, dialect):
        parent = super().literal_processor(dialect)
        return _literal_processor(parent, "Date")


class YqlTimestamp(sqltypes.TIMESTAMP):
    def result_processor(self, dialect, coltype):
        def process(value: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
            if value is None:
                return None
            if not self.timezone:
                return value
            return value.replace(tzinfo=datetime.timezone.utc)

        return process


class YqlDateTime(YqlTimestamp, sqltypes.DATETIME):
    def bind_processor(self, dialect):
        def process(value: Optional[datetime.datetime]) -> Optional[int]:
            if value is None:
                return None
            if not self.timezone:  # if timezone is disabled, consider it as utc
                value = value.replace(tzinfo=datetime.timezone.utc)
            return int(value.timestamp())

        return process


class YqlDate32(YqlDate):
    __visit_name__ = "date32"

    def literal_processor(self, dialect):
        parent = super().literal_processor(dialect)
        return _literal_processor(parent, "Date32")


class YqlTimestamp64(YqlTimestamp):
    __visit_name__ = "timestamp64"

    def literal_processor(self, dialect):
        parent = super().literal_processor(dialect)
        return _literal_processor(parent, "Timestamp64")


class YqlDateTime64(YqlDateTime):
    __visit_name__ = "datetime64"

    def literal_processor(self, dialect):
        parent = super().literal_processor(dialect)
        return _literal_processor(parent, "DateTime64")


class YqlInterval64(sqltypes.Interval):
    """Store ``datetime.timedelta`` values using YDB's ``Interval64`` type."""

    __visit_name__ = "interval64"
    cache_ok = True

    def bind_processor(self, dialect):
        def process(value: Optional[datetime.timedelta]) -> Optional[datetime.timedelta]:
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value) -> Optional[datetime.timedelta]:
            if value is None or isinstance(value, datetime.timedelta):
                return value
            return datetime.timedelta(microseconds=value)

        return process

    def literal_processor(self, dialect):
        return _literal_processor(_interval_literal, "Interval64")
