YDB SQLAlchemy Documentation
============================

Welcome to the YDB SQLAlchemy dialect documentation. This package provides a SQLAlchemy dialect for YDB (Yandex Database), allowing you to use SQLAlchemy ORM and Core with YDB databases.

.. image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://github.com/ydb-platform/ydb-sqlalchemy/blob/main/LICENSE
   :alt: License

.. image:: https://badge.fury.io/py/ydb-sqlalchemy.svg
   :target: https://badge.fury.io/py/ydb-sqlalchemy
   :alt: PyPI version

.. image:: https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml
   :alt: Functional tests

Overview
--------

YDB SQLAlchemy is a dialect that enables SQLAlchemy to work with YDB databases. It supports both SQLAlchemy 2.0 (fully tested) and SQLAlchemy 1.4 (partially tested).

Key Features:
~~~~~~~~~~~~~

* **SQLAlchemy 2.0 Support**: Full compatibility with the latest SQLAlchemy version
* **Async/Await Support**: Full async support with ``yql+ydb_async`` dialect
* **Core and ORM**: Support for both SQLAlchemy Core and ORM patterns
* **Authentication**: Multiple authentication methods including static credentials, tokens, and service accounts
* **Type System**: Comprehensive YDB type mapping to SQLAlchemy types
* **Migrations**: Alembic integration for database schema migrations
* **Pandas Integration**: Compatible with pandas DataFrame operations

Quick Examples
~~~~~~~~~~~~~~

**Synchronous:**

.. code-block:: python

   import sqlalchemy as sa

   # Create engine
   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   # Execute query
   with engine.connect() as conn:
       result = conn.execute(sa.text("SELECT 1 AS value"))
       print(result.fetchone())

**Asynchronous:**

.. code-block:: python

   import asyncio
   from sqlalchemy.ext.asyncio import create_async_engine

   async def main():
       # Create async engine
       engine = create_async_engine("yql+ydb_async://localhost:2136/local")

       # Execute query
       async with engine.connect() as conn:
           result = await conn.execute(sa.text("SELECT 1 AS value"))
           print(await result.fetchone())

   asyncio.run(main())

Table of Contents
-----------------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   connection
   types
   migrations

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
