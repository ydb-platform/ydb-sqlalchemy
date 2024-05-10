import datetime
from typing import Optional

from sqlalchemy import Dialect
from sqlalchemy import types as sqltypes
from sqlalchemy.sql.type_api import _ResultProcessorType


class YqlDateTime(sqltypes.DateTime):
    def result_processor(self, dialect: Dialect, coltype: str) -> Optional[_ResultProcessorType[datetime.datetime]]:
        def process(value: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
            if value is None:
                return None
            return value.replace(tzinfo=datetime.timezone.utc)

        if self.timezone:
            return process
        return None
