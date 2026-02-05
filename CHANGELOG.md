* Ability to add prefixes to YQL statements

## 0.1.16 ##
* optimize string literal escaping
* Fix: StructType and ListType can't handle compound types

## 0.1.15 ##
* Support for sa.BINARY and sa.BLOB types
* Support nullable StructType fields via Optional wrapper

## 0.1.14 ##
* Add ability to propagate RetrySettings

## 0.1.13 ##
* Add TupleType mapping support for YDB

## 0.1.12 ##
* Fix sa 2.0.44 compatibility

## 0.1.11 ##
* Date32, Datetime64 and Timestamp64 support

## 0.1.10 ##
* YDB Decimal support

## 0.1.9 ##
* Implement YDB specific concat

## 0.1.8 ##
* Fix async cursor close method

## 0.1.7 ##
* Fix async cursor fetch methods

## 0.1.6 ##
* Bump ydb-dbapi version to 0.1.7

## 0.1.5 ##
* Bump ydb-dbapi version

## 0.1.4 ##
* Add slash to DB name

## 0.1.3 ##
* Fix declare param_name cutting

## 0.1.2 ##
* Bump DBAPI version

## 0.1.1 ##
* sqlalchemy 1.4+ partial support

## 0.1.1b1 ##
* Attempt to support sqlalchemy 1.4+

## 0.1.0 ##
* Update DBAPI to QueryService

## 0.0.1b23 ##
* Add request settings to execution options

## 0.0.1b22 ##
* Get rid of logging queries in cursor

## 0.0.1b21 ##
* Add support of DROP INDEX statement

## 0.0.1b20 ##
* sqlalchemy's DATETIME type now rendered as YDB's Datetime instead of Timestamp

## 0.0.1b19 ##
* Do not use set for columns in index, use dict (preserve order)

## 0.0.1b18 ##
* Supprted scan query
* Added use sqlalchemy cache query text internally

## 0.0.1b17 ##
* Fixed false cache hit

## 0.0.1b16 ##
* Added ydb_table_path_prefix parameter

## 0.0.1b15 ##
* Added support of timezone

## 0.0.1b14 ##
* Added secondary index support

## 0.0.1b13 ##
* Added declare for yql statement variables (opt in) - temporary flag

## 0.0.1b12 ##
* supported ydb connection credentials

## 0.0.1b11 ##
* test release

## 0.0.1b10 ##
* test release

## 0.0.1b9 ##
* test release

## 0.0.1b8 ##
* Improve publish script

## 0.0.1b6 ##
* Fixed import version

## 0.0.1b5 ##
* Initial version
