Connection Configuration
========================

This guide covers various ways to configure connections to YDB using SQLAlchemy.

Connection URL Format
---------------------

YDB SQLAlchemy uses the following URL format:

.. code-block:: text

   yql+ydb://host:port/database

Basic Examples:

.. code-block:: python

   # Synchronous connection
   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   # Asynchronous connection
   from sqlalchemy.ext.asyncio import create_async_engine
   async_engine = create_async_engine("yql+ydb_async://localhost:2136/local")

   # Remote YDB instance
   engine = sa.create_engine("yql+ydb://ydb.example.com:2135/prod")
   async_engine = create_async_engine("yql+ydb_async://ydb.example.com:2135/prod")

   # With database path
   engine = sa.create_engine("yql+ydb://localhost:2136/local/my_database")
   async_engine = create_async_engine("yql+ydb_async://localhost:2136/local/my_database")

Authentication Methods
----------------------

YDB SQLAlchemy supports multiple authentication methods through the ``connect_args`` parameter.

Anonymous Access
~~~~~~~~~~~~~~~~

For local development or testing:

.. code-block:: python

   import sqlalchemy as sa

   engine = sa.create_engine("yql+ydb://localhost:2136/local")

Static Credentials
~~~~~~~~~~~~~~~~~~

Use username and password authentication:

.. code-block:: python

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": {
               "username": "your_username",
               "password": "your_password"
           }
       }
   )

Token Authentication
~~~~~~~~~~~~~~~~~~~~

Use access token for authentication:

.. code-block:: python

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": {
               "token": "your_access_token"
           }
       }
   )

Service Account Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use service account JSON key:

.. code-block:: python

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": {
               "service_account_json": {
                   "id": "your_key_id",
                   "service_account_id": "your_service_account_id",
                   "created_at": "2023-01-01T00:00:00Z",
                   "key_algorithm": "RSA_2048",
                   "public_key": "-----BEGIN PUBLIC KEY-----\\n...\\n-----END PUBLIC KEY-----",
                   "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----"
               }
           }
       }
   )

Or load from file:

.. code-block:: python

   import json

   with open('service_account_key.json', 'r') as f:
       service_account_json = json.load(f)

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": {
               "service_account_json": service_account_json
           }
       }
   )

YDB SDK Credentials
~~~~~~~~~~~~~~~~~~~

Use any credentials from the YDB Python SDK:

.. code-block:: python

   import ydb.iam

   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": ydb.iam.MetadataUrlCredentials()
       }
   )

   # OAuth token credentials
   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": ydb.iam.OAuthCredentials("your_oauth_token")
       }
   )

   # Static credentials
   engine = sa.create_engine(
       "yql+ydb://localhost:2136/local",
       connect_args={
           "credentials": ydb.iam.StaticCredentials("username", "password")
       }
   )

TLS Configuration
---------------------

For secure connections to YDB:

.. code-block:: python

   engine = sa.create_engine(
       "yql+ydb://ydb.example.com:2135/prod",
       connect_args={
           "credentials": {"token": "your_token"},
           "protocol": "grpc",
           "root_certificates_path": "/path/to/ca-certificates.crt",
           # "root_certificates": crt_string,
       }
   )
