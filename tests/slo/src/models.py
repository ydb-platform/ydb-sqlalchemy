"""
SQLAlchemy plumbing for the SLO workload: engine factory, the key/value table
definition and an imperatively-mapped ORM class for the ``orm`` workload mode.
"""

import re

import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.orm import registry

from ydb_sqlalchemy import types as ydb_types

_ENDPOINT_RE = re.compile(r"^(?P<scheme>grpcs?|grpc)://(?P<host>[^:/]+):(?P<port>\d+)")


def build_url(endpoint: str, database: str) -> URL:
    """
    Turn a YDB endpoint (``grpc://ydb:2136``) and database path (``/Root/testdb``)
    into a ``yql+ydb`` SQLAlchemy URL.
    """
    match = _ENDPOINT_RE.match(endpoint.strip())
    if not match:
        raise ValueError(f"Cannot parse YDB endpoint: {endpoint!r}")

    scheme = match.group("scheme")
    host = match.group("host")
    port = int(match.group("port"))

    # The dialect re-adds the leading slash to the database, so strip it here to
    # avoid a doubled slash in the rendered URL.
    db = database.strip().lstrip("/")

    query = {"protocol": "grpcs"} if scheme == "grpcs" else {}
    return URL.create("yql+ydb", host=host, port=port, database=db, query=query)


def build_engine(endpoint: str, database: str, pool_size: int) -> sa.engine.Engine:
    url = build_url(endpoint, database)
    # No pool_pre_ping: it would add a SELECT 1 round-trip on every checkout.
    # ydb-dbapi already retries transient errors and re-acquires sessions from
    # its internal session pool, so broken connections recover without a ping.
    return sa.create_engine(
        url,
        pool_size=pool_size,
        max_overflow=max(4, pool_size),
    )


def build_table(
    metadata: sa.MetaData,
    table_name: str,
    *,
    min_partitions: int = 6,
    max_partitions: int = 100,
    partition_size_mb: int = 100,
) -> sa.Table:
    return sa.Table(
        table_name,
        metadata,
        sa.Column("object_id", ydb_types.UInt64, primary_key=True),
        sa.Column("payload_str", sa.Unicode),
        sa.Column("payload_double", sa.Float),
        sa.Column("payload_timestamp", sa.TIMESTAMP),
        ydb_auto_partitioning_by_size=True,
        ydb_auto_partitioning_by_load=True,
        ydb_auto_partitioning_min_partitions_count=min_partitions,
        ydb_auto_partitioning_max_partitions_count=max_partitions,
        ydb_auto_partitioning_partition_size_mb=partition_size_mb,
    )


class KeyValueRow:
    """Plain class mapped imperatively onto the workload table for ``orm`` mode."""

    def __init__(self, object_id, payload_str=None, payload_double=None, payload_timestamp=None):
        self.object_id = object_id
        self.payload_str = payload_str
        self.payload_double = payload_double
        self.payload_timestamp = payload_timestamp


_mapper_registry = registry()
_mapped = False


def ensure_mapped(table: sa.Table) -> type:
    """Map :class:`KeyValueRow` onto ``table`` exactly once."""
    global _mapped
    if not _mapped:
        _mapper_registry.map_imperatively(KeyValueRow, table)
        _mapped = True
    return KeyValueRow
