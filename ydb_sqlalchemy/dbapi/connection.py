import posixpath
from typing import Optional

import ydb
from .cursor import Cursor
from .errors import InterfaceError, ProgrammingError, DatabaseError, InternalError, NotSupportedError


class IsolationLevel:
    SERIALIZABLE = "SERIALIZABLE"
    ONLINE_READONLY = "ONLINE READONLY"
    ONLINE_READONLY_INCONSISTENT = "ONLINE READONLY INCONSISTENT"
    STALE_READONLY = "STALE READONLY"
    SNAPSHOT_READONLY = "SNAPSHOT READONLY"
    AUTOCOMMIT = "AUTOCOMMIT"


class Connection:
    def __init__(self, endpoint=None, host=None, port=None, database=None, **conn_kwargs):
        self.endpoint = endpoint or f"grpc://{host}:{port}"
        self.database = database
        self.table_client_settings = self._get_table_client_settings()
        self.driver = self._create_driver(**conn_kwargs)
        self.session = self._create_session()
        self.interactive_transaction: bool = False  # AUTOCOMMIT
        self.tx_mode: ydb.AbstractTransactionModeBuilder = ydb.SerializableReadWrite()
        self.transaction: Optional[ydb.TxContext] = None

    def cursor(self):
        return Cursor(self, transaction=self.transaction)

    def describe(self, table_path):
        full_path = posixpath.join(self.database, table_path)
        try:
            return ydb.retry_operation_sync(lambda: self.session.describe_table(full_path))
        except ydb.issues.SchemeError as e:
            raise ProgrammingError(e.message, e.issues, e.status) from e
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status) from e
        except Exception as e:
            raise DatabaseError(f"Failed to describe table {table_path}") from e

    def check_exists(self, table_path):
        try:
            self.driver.scheme_client.describe_path(table_path)
            return True
        except ydb.SchemeError:
            return False

    def set_isolation_level(self, isolation_level: str):
        ydb_isolation_settings_map = {
            IsolationLevel.AUTOCOMMIT: (ydb.SerializableReadWrite(), False),
            IsolationLevel.SERIALIZABLE: (ydb.SerializableReadWrite(), True),
            IsolationLevel.ONLINE_READONLY: (ydb.OnlineReadOnly(), False),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: (ydb.OnlineReadOnly().with_allow_inconsistent_reads(), False),
            IsolationLevel.STALE_READONLY: (ydb.StaleReadOnly(), False),
            IsolationLevel.SNAPSHOT_READONLY: (ydb.SnapshotReadOnly(), True),
        }
        ydb_isolation_settings = ydb_isolation_settings_map[isolation_level]
        if self.transaction and self.transaction.tx_id:
            raise InternalError("Failed to set transaction mode: transaction is already began")
        self.tx_mode = ydb_isolation_settings[0]
        self.interactive_transaction = ydb_isolation_settings[1]

    def get_isolation_level(self) -> str:
        if self.tx_mode.name == ydb.SerializableReadWrite().name:
            if self.interactive_transaction:
                return IsolationLevel.SERIALIZABLE
            else:
                return IsolationLevel.AUTOCOMMIT
        elif self.tx_mode.name == ydb.OnlineReadOnly().name:
            if self.tx_mode.settings.allow_inconsistent_reads:
                return IsolationLevel.ONLINE_READONLY_INCONSISTENT
            else:
                return IsolationLevel.ONLINE_READONLY
        elif self.tx_mode.name == ydb.StaleReadOnly().name:
            return IsolationLevel.STALE_READONLY
        elif self.tx_mode.name == ydb.SnapshotReadOnly().name:
            return IsolationLevel.SNAPSHOT_READONLY
        else:
            raise NotSupportedError(f"{self.tx_mode.name} is not supported")

    def begin(self):
        if not self.session.initialized():
            raise InternalError("Failed to begin transaction: session closed")
        self.transaction = None
        if self.interactive_transaction:
            self.transaction = self.session.transaction(self.tx_mode)
            self.transaction.begin()

    def commit(self):
        if self.transaction and self.transaction.tx_id:
            self.transaction.commit()

    def rollback(self):
        if self.transaction and self.transaction.tx_id:
            self.transaction.rollback()

    def close(self):
        self._delete_session()
        self._stop_driver()

    @staticmethod
    def _get_table_client_settings() -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(True)
        )

    def _create_driver(self, **conn_kwargs):
        # TODO: add cache for initialized drivers/pools?
        driver_config = ydb.DriverConfig(
            endpoint=self.endpoint,
            database=self.database,
            table_client_settings=self.table_client_settings,
            **conn_kwargs,
        )
        driver = ydb.Driver(driver_config)
        try:
            driver.wait(timeout=5, fail_fast=True)
        except ydb.Error as e:
            raise InterfaceError(e.message, e.issues, e.status) from e
        except Exception as e:
            driver.stop()
            raise InterfaceError(f"Failed to connect to YDB, details {driver.discovery_debug_details()}") from e
        return driver

    def _stop_driver(self):
        self.driver.stop()

    def _create_session(self) -> ydb.BaseSession:
        session = ydb.Session(self.driver, self.table_client_settings)
        session.create()
        return session

    def _delete_session(self):
        if self.session.initialized():
            self.rollback()
            self.session.delete()
