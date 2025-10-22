Installation
============

This guide covers the installation of YDB SQLAlchemy dialect and its dependencies.

Requirements
------------

* Python 3.7 or higher
* SQLAlchemy 1.4+ or 2.0+ (recommended)
* YDB Python SDK

Installing from PyPI
---------------------

The easiest way to install YDB SQLAlchemy is using pip:

.. code-block:: bash

   pip install ydb-sqlalchemy

This will install the YDB SQLAlchemy dialect along with all required dependencies.

Installing from Source
----------------------

If you want to install the latest development version or contribute to the project:

1. Clone the repository:

.. code-block:: bash

   git clone https://github.com/ydb-platform/ydb-sqlalchemy.git
   cd ydb-sqlalchemy

2. Install in development mode:

.. code-block:: bash

   pip install -e .

Or install directly:

.. code-block:: bash

   pip install .


Verifying Installation
----------------------

To verify that YDB SQLAlchemy is installed correctly:

.. code-block:: python

   import ydb_sqlalchemy
   import sqlalchemy as sa

   # Check if the dialect is available
   engine = sa.create_engine("yql+ydb://localhost:2136/local")
   print("YDB SQLAlchemy installed successfully!")

Docker Setup for Development
-----------------------------

For development and testing, you can use Docker to run a local YDB instance:

1. Clone the repository and navigate to the project directory
2. Start YDB using ``docker compose``:

.. code-block:: bash

   docker compose up -d

This will start a YDB instance accessible at ``localhost:2136``.

Getting Help
~~~~~~~~~~~~

If you encounter issues during installation:

1. Check the `GitHub Issues <https://github.com/ydb-platform/ydb-sqlalchemy/issues>`_
2. Review the `YDB documentation <https://ydb.tech/en/docs/>`_
3. Create a new issue with detailed error information

Next Steps
----------

After successful installation, proceed to the :doc:`quickstart` guide to learn how to use YDB SQLAlchemy in your projects.
