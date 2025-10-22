Quick Start
===========

This guide will help you get started with YDB SQLAlchemy quickly. We'll cover basic usage patterns for both SQLAlchemy Core and ORM.

Prerequisites
-------------

Before starting, make sure you have:

1. YDB SQLAlchemy installed (see :doc:`installation`)
2. A running YDB instance (local or remote)
3. Basic familiarity with SQLAlchemy

Basic Connection
----------------

The simplest way to connect to YDB:

.. code-block:: python

   import sqlalchemy as sa

   # Create engine for local YDB
   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   # Test connection
   with engine.connect() as conn:
       result = conn.execute(sa.text("SELECT 1 AS value"))
       print(result.fetchone())  # (1,)

SQLAlchemy Core Example
-----------------------

Using SQLAlchemy Core for direct SQL operations:

.. code-block:: python

   import sqlalchemy as sa
   from sqlalchemy import MetaData, Table, Column, Integer, String

   # Create engine
   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   # Define table structure
   metadata = MetaData()
   users = Table(
       'users',
       metadata,
       Column('id', Integer, primary_key=True),
       Column('name', String(50)),
       Column('email', String(100))
   )

   # Create table
   metadata.create_all(engine)

   # Insert data
   with engine.connect() as conn:
       # Single insert
       conn.execute(
           users.insert().values(id=1, name='John Doe', email='john@example.com')
       )

       # Multiple inserts
       conn.execute(
           users.insert(),
           [
               {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com'},
               {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com'}
           ]
       )

       # Commit changes
       conn.commit()

   # Query data
   with engine.connect() as conn:
       # Select all
       result = conn.execute(sa.select(users))
       for row in result:
           print(f"ID: {row.id}, Name: {row.name}, Email: {row.email}")

       # Select with conditions
       result = conn.execute(
           sa.select(users).where(users.c.name.like('John%'))
       )
       print(result.fetchall())

SQLAlchemy ORM Example
----------------------

Using SQLAlchemy ORM for object-relational mapping:

.. code-block:: python

   import sqlalchemy as sa
   from sqlalchemy import Column, Integer, String
   from sqlalchemy.orm import declarative_base
   from sqlalchemy.orm import sessionmaker

   # Create engine
   engine = sa.create_engine("yql+ydb://localhost:2136/local")

   # Define base class
   Base = declarative_base()

   # Define model
   class User(Base):
       __tablename__ = 'users_orm'

       id = Column(Integer, primary_key=True)
       name = Column(String(50))
       email = Column(String(100))

       def __repr__(self):
           return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"

   # Create tables
   Base.metadata.create_all(engine)

   # Create session
   Session = sessionmaker(bind=engine)
   session = Session()

   # Create and add users
   user1 = User(id=1, name='Alice Brown', email='alice@example.com')
   user2 = User(id=2, name='Charlie Davis', email='charlie@example.com')

   session.add_all([user1, user2])
   session.commit()

   # Query users
   users = session.query(User).all()
   for user in users:
       print(user)

   # Query with filters
   alice = session.query(User).filter(User.name == 'Alice Brown').first()
   print(f"Found user: {alice}")

   # Update user
   alice.email = 'alice.brown@example.com'
   session.commit()

   # Delete user
   session.delete(user2)
   session.commit()

   session.close()

Working with YDB-Specific Features
-----------------------------------

YDB has some unique features that you can leverage:

Upsert Operations
~~~~~~~~~~~~~~~~~

YDB supports efficient upsert operations:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy import upsert

   # Using upsert instead of insert
   with engine.connect() as conn:
       stmt = upsert(users).values(
           id=1,
           name='John Updated',
           email='john.updated@example.com'
       )
       conn.execute(stmt)
       conn.commit()

YDB-Specific Types
~~~~~~~~~~~~~~~~~~

Use YDB-specific data types for better performance:

.. code-block:: python

   from ydb_sqlalchemy.sqlalchemy.types import UInt64, YqlJSON

   # Table with YDB-specific types
   ydb_table = Table(
       'ydb_example',
       metadata,
       Column('id', UInt64, primary_key=True),
       Column('data', YqlJSON),
       Column('created_at', sa.DateTime)
   )

Next Steps
----------

Now that you have the basics working:

1. Learn about :doc:`connection` configuration and authentication
2. Explore :doc:`types` for YDB-specific data types
3. Set up :doc:`migrations` with Alembic
4. Check out the examples in the repository

Common Patterns
---------------

Here are some common patterns you'll use frequently:

.. code-block:: python

   # Counting records
   count = conn.execute(sa.func.count(users.c.id)).scalar()

   # Aggregations
   result = conn.execute(
       sa.select(sa.func.max(users.c.id), sa.func.count())
       .select_from(users)
   )

   # Joins (when you have related tables)
   # result = conn.execute(
   #     sa.select(users, orders)
   #     .select_from(users.join(orders, users.c.id == orders.c.user_id))
   # )
