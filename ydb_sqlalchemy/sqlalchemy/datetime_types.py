import datetime
from typing import Optional

from sqlalchemy import types as sqltypes


class YqlDate(sqltypes.Date):
    def literal_processor(self, dialect):
        parent = super().literal_processor(dialect)

        def process(value):
            return f"Date({parent(value)})"

        return process


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
