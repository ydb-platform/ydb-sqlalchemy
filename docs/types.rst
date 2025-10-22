Data Types
==========

YDB SQLAlchemy provides comprehensive support for YDB data types through custom SQLAlchemy types. This guide covers the available types and their usage.

Overview
--------

YDB has a rich type system that includes primitive types, optional types, containers, and special types. The YDB SQLAlchemy dialect maps these types to appropriate SQLAlchemy types and provides YDB-specific types for optimal performance.
For more information about YDB data types, see the `YDB Type System Documentation <https://ydb.tech/en/docs/yql/reference/types/>`_.

Type Mapping Summary
--------------------

The following table shows the complete mapping between YDB native types, SQLAlchemy types, and Python types:

.. list-table:: YDB Type System Reference
   :header-rows: 1
   :widths: 20 25 20 35

   * - YDB Native Type
     - SQLAlchemy Type
     - Python Type
     - Notes
   * - ``Bool``
     - ``BOOLEAN``
     - ``bool``
     -
   * - ``Int8``
     -
     - ``int``
     - -2^7 to 2^7-1
   * - ``Int16``
     -
     - ``int``
     - -2^15 to 2^15-1
   * - ``Int32``
     -
     - ``int``
     - -2^31 to 2^31-1
   * - ``Int64``
     - ``INTEGER``
     - ``int``
     - -2^63 to 2^63-1
   * - ``Uint8``
     -
     - ``int``
     - 0 to 2^8-1
   * - ``Uint16``
     -
     - ``int``
     - 0 to 2^16-1
   * - ``Uint32``
     -
     - ``int``
     - 0 to 2^32-1
   * - ``Uint64``
     -
     - ``int``
     - 0 to 2^64-1
   * - ``Float``
     - ``FLOAT``
     - ``float``
     -
   * - ``Double``
     - ``Double``
     - ``float``
     - Available in SQLAlchemy 2.0+
   * - ``Decimal(p,s)``
     - ``DECIMAL`` / ``NUMERIC``
     - ``decimal.Decimal``
     -
   * - ``String``
     - ``BINARY`` / ``BLOB``
     - ``str`` / ``bytes``
     -
   * - ``Utf8``
     - ``CHAR`` / ``VARCHAR`` / ``TEXT`` / ``NVARCHAR``
     - ``str``
     -
   * - ``Date``
     - ``Date``
     - ``datetime.date``
     -
   * - ``Datetime``
     - ``DATETIME``
     - ``datetime.datetime``
     -
   * - ``Timestamp``
     - ``TIMESTAMP``
     - ``datetime.datetime``
     -
   * - ``Json``
     - ``JSON``
     - ``dict`` / ``list``
     -
   * - ``List<T>``
     - ``ARRAY``
     - ``list``
     -
   * - ``Struct<...>``
     -
     - ``dict``
     -
   * - ``Optional<T>``
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

Date and Time Types
-------------------

YDB provides several date and time types:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import YqlDate, YqlDateTime, YqlTimestamp
   from sqlalchemy import DateTime
   import datetime

   class EventLog(Base):
       __tablename__ = 'event_log'

       id = Column(UInt64, primary_key=True)

       # Date only (YYYY-MM-DD)
       event_date = Column(YqlDate)

       # DateTime with timezone support
       created_at = Column(YqlDateTime(timezone=True))

       # Timestamp (high precision)
       precise_time = Column(YqlTimestamp)

       # Standard SQLAlchemy DateTime also works
       updated_at = Column(DateTime)

   # Usage
   now = datetime.datetime.now(datetime.timezone.utc)
   today = datetime.date.today()

   session.add(EventLog(
       id=1,
       event_date=today,
       created_at=now,
       precise_time=now,
       updated_at=now
   ))
