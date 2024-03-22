# -*- coding: utf-8 -*-
import setuptools

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = []
    for line in f.readlines():
        line = line.strip()
        if line:
            requirements.append(line)

setuptools.setup(
    name="ydb-sqlalchemy",
    version="0.0.1b5",  # AUTOVERSION
    description="YDB Dialect for SQLAlchemy",
    author="Yandex LLC",
    author_email="ydb@yandex-team.ru",
    url="http://github.com/ydb-platform/ydb-sqlalchemy",
    license="Apache 2.0",
    package_dir={"": "."},
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages("."),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="SQLAlchemy YDB YQL",
    install_requires=requirements,  # requirements.txt
    options={"bdist_wheel": {"universal": True}},
    extras_require={
        "yc": [
            "yandexcloud",
        ],
    },
    entry_points={
        "sqlalchemy.dialects": [
            "yql.ydb=ydb_sqlalchemy.sqlalchemy:YqlDialect",
            "yql.ydb_async=ydb_sqlalchemy.sqlalchemy:AsyncYqlDialect",
            "ydb_async=ydb_sqlalchemy.sqlalchemy:AsyncYqlDialect",
            "ydb=ydb_sqlalchemy.sqlalchemy:YqlDialect",
            "yql=ydb_sqlalchemy.sqlalchemy:YqlDialect",
        ]
    },
)
