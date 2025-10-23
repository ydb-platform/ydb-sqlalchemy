Data Types
==========

YDB SQLAlchemy provides comprehensive support for YDB data types through custom SQLAlchemy types. This guide covers the available types and their usage.

Overview
--------

YDB has a rich type system that includes primitive types, optional types, containers, and special types. The YDB SQLAlchemy dialect maps these types to appropriate SQLAlchemy types and provides YDB-specific types for optimal performance.
For more information about YDB data types, see the `YDB Type System Documentation <https://ydb.tech/en/docs/yql/reference/types/>`_.

Type Mapping Summary
--------------------

The following table shows the complete mapping between YDB native types, YDB SQLAlchemy types, standard SQLAlchemy types, and Python types:

.. list-table:: YDB Type System Reference
   :header-rows: 1
   :widths: 15 20 20 15 30

   * - YDB Native Type
     - YDB SA Type
     - SA Type
     - Python Type
     - Notes
   * - ``Bool``
     -
     - ``Boolean``
     - ``bool``
     -
   * - ``Int8``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.Int8`
     -
     - ``int``
     - -2^7 to 2^7-1
   * - ``Int16``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.Int16`
     -
     - ``int``
     - -2^15 to 2^15-1
   * - ``Int32``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.Int32`
     -
     - ``int``
     - -2^31 to 2^31-1
   * - ``Int64``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.Int64`
     - ``Integer``
     - ``int``
     - -2^63 to 2^63-1, default integer type
   * - ``Uint8``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt8`
     -
     - ``int``
     - 0 to 2^8-1
   * - ``Uint16``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt16`
     -
     - ``int``
     - 0 to 2^16-1
   * - ``Uint32``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt32`
     -
     - ``int``
     - 0 to 2^32-1
   * - ``Uint64``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt64`
     -
     - ``int``
     - 0 to 2^64-1
   * - ``Float``
     -
     - ``Float``
     - ``float``
     -
   * - ``Double``
     -
     - ``Double``
     - ``float``
     - Available in SQLAlchemy 2.0+
   * - ``Decimal(p,s)``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.Decimal`
     - ``DECIMAL``
     - ``decimal.Decimal``
     -
   * - ``String``
     -
     - ``BINARY``
     - ``bytes``
     -
   * - ``Utf8``
     -
     - ``String`` / ``Text``
     - ``str``
     -
   * - ``Date``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDate`
     - ``Date``
     - ``datetime.date``
     -
   * - ``Date32``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDate32`
     -
     - ``datetime.date``
     - Extended date range support
   * - ``Datetime``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDateTime`
     - ``DATETIME``
     - ``datetime.datetime``
     -
   * - ``Datetime64``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDateTime64`
     -
     - ``datetime.datetime``
     - Extended datetime range
   * - ``Timestamp``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlTimestamp`
     - ``TIMESTAMP``
     - ``datetime.datetime``
     -
   * - ``Timestamp64``
     - :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlTimestamp64`
     -
     - ``datetime.datetime``
     - Extended timestamp range
   * - ``Json``
     - :class:`~ydb_sqlalchemy.sqlalchemy.json.YqlJSON`
     - ``JSON``
     - ``dict`` / ``list``
     -
   * - ``List<T>``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.ListType`
     - ``ARRAY``
     - ``list``
     -
   * - ``Struct<...>``
     - :class:`~ydb_sqlalchemy.sqlalchemy.types.StructType`
     -
     - ``dict``
     -
   * - ``Optional<T>``
     -
     - ``nullable=True``
     - ``None`` + base type
     -

Standard SQLAlchemy Types
-------------------------

Most standard SQLAlchemy types work with YDB:

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Boolean, Float, Text

   class MyTable(Base):
       __tablename__ = 'my_table'

       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       description = Column(Text)
       is_active = Column(Boolean)
       price = Column(Float)

YDB-Specific Integer Types
--------------------------

YDB provides specific integer types with defined bit widths:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import (
       Int8, Int16, Int32, Int64,
       UInt8, UInt16, UInt32, UInt64
   )

   class IntegerTypesExample(Base):
       __tablename__ = 'integer_types'

       id = Column(UInt64, primary_key=True)  # Unsigned 64-bit integer
       small_int = Column(Int16)              # Signed 16-bit integer
       byte_value = Column(UInt8)             # Unsigned 8-bit integer (0-255)
       counter = Column(UInt32)               # Unsigned 32-bit integer

For detailed API reference, see:
:class:`~ydb_sqlalchemy.sqlalchemy.types.Int8`, :class:`~ydb_sqlalchemy.sqlalchemy.types.Int16`, :class:`~ydb_sqlalchemy.sqlalchemy.types.Int32`, :class:`~ydb_sqlalchemy.sqlalchemy.types.Int64`,
:class:`~ydb_sqlalchemy.sqlalchemy.types.UInt8`, :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt16`, :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt32`, :class:`~ydb_sqlalchemy.sqlalchemy.types.UInt64`.

Decimal Type
------------

YDB supports high-precision decimal numbers:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import Decimal
   import decimal

   class FinancialData(Base):
       __tablename__ = 'financial_data'

       id = Column(UInt64, primary_key=True)
       # Default: Decimal(22, 9) - 22 digits total, 9 after decimal point
       amount = Column(Decimal())

       # Custom precision and scale
       precise_amount = Column(Decimal(precision=15, scale=4))

       # Return as float instead of Decimal object
       percentage = Column(Decimal(precision=5, scale=2, asdecimal=False))

   # Usage
   session.add(FinancialData(
       id=1,
       amount=decimal.Decimal('1234567890123.123456789'),
       precise_amount=decimal.Decimal('12345678901.1234'),
       percentage=99.99
   ))

For detailed API reference, see: :class:`~ydb_sqlalchemy.sqlalchemy.types.Decimal`.

Date and Time Types
-------------------

YDB provides several date and time types:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import (
       YqlDate, YqlDateTime, YqlTimestamp,
       YqlDate32, YqlDateTime64, YqlTimestamp64
   )
   from sqlalchemy import DateTime
   import datetime

   class EventLog(Base):
       __tablename__ = 'event_log'

       id = Column(UInt64, primary_key=True)

       # Date only (YYYY-MM-DD) - standard range
       event_date = Column(YqlDate)

       # Date32 - extended date range support
       extended_date = Column(YqlDate32)

       # DateTime with timezone support - standard range
       created_at = Column(YqlDateTime(timezone=True))

       # DateTime64 - extended range
       precise_datetime = Column(YqlDateTime64(timezone=True))

       # Timestamp (high precision) - standard range
       precise_time = Column(YqlTimestamp)

       # Timestamp64 - extended range with microsecond precision
       extended_timestamp = Column(YqlTimestamp64)

       # Standard SQLAlchemy DateTime also works
       updated_at = Column(DateTime)

   # Usage
   now = datetime.datetime.now(datetime.timezone.utc)
   today = datetime.date.today()

   session.add(EventLog(
       id=1,
       event_date=today,
       extended_date=today,
       created_at=now,
       precise_datetime=now,
       precise_time=now,
       extended_timestamp=now,
       updated_at=now
   ))

For detailed API reference, see:
:class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDate`, :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDateTime`, :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlTimestamp`,
:class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDate32`, :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlDateTime64`, :class:`~ydb_sqlalchemy.sqlalchemy.datetime_types.YqlTimestamp64`.
