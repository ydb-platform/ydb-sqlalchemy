# YDB Dialect for SQLAlchemy
---
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/blob/main/LICENSE)
[![Functional tests](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml/badge.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/tests.yml)
[![Style checks](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/style.yml/badge.svg)](https://github.com/ydb-platform/ydb-sqlalchemy/actions/workflows/style.yml)

This repository contains __work in progress__ YQL dialect for SqlAlchemy 2.0.
Api may be changed in future without backward compatibility.

## Installation
To work with current ydb-sqlalchemy version clone this repo and run from source root:

```bash
$ pip install -U .
```

## Getting started

Connect to local YDB throw SqlAlchemy:

```python3
import sqlalchemy as sa


engine = sa.create_engine("yql+ydb://localhost:2136/local")

with engine.connect() as conn:
  rs = conn.execute(sa.text("SELECT 1 AS value"))
  print(rs.fetchone())

```

## Development

### Run Tests:

For run local YDB throw docker, run in source root:
```bash
$ docker-compose up
```

For run all tests from source root make:
```bash
$ tox -e test-all
```

Run specific test:
```bash
$ tox -e test -- test_dbapi/test_dbapi.py
```

Check code style:
```bash
$ tox -e style
```

Reformat code:
```bash
$ tox -e isort
$ tox -e black-format
```

Run example (needs running local YDB):
```bash
$ python -m pip install virtualenv
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ python examples/example.py
```
