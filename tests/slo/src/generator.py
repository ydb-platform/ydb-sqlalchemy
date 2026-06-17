"""Row payload generator shared by the create/write paths."""

import random
import string
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

MAX_UINT64 = 2**64 - 1


def _random_string(min_len: int, max_len: int) -> str:
    length = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_lowercase, k=length))


@dataclass
class Row:
    object_id: int
    payload_str: str
    payload_double: float
    payload_timestamp: datetime

    def as_params(self) -> dict:
        return {
            "object_id": self.object_id,
            "payload_str": self.payload_str,
            "payload_double": self.payload_double,
            "payload_timestamp": self.payload_timestamp,
        }


class RowGenerator:
    """Thread-safe generator of rows with monotonically increasing ids."""

    def __init__(self, start_id: int = 0) -> None:
        self._id = start_id
        self._lock = threading.Lock()

    def _next_id(self) -> int:
        with self._lock:
            self._id += 1
            if self._id >= MAX_UINT64:
                self._id = 1
            return self._id

    def get(self) -> Row:
        return Row(
            object_id=self._next_id(),
            payload_str=_random_string(20, 40),
            payload_double=random.random(),
            payload_timestamp=datetime.now(timezone.utc),
        )
