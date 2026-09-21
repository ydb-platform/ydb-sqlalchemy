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
