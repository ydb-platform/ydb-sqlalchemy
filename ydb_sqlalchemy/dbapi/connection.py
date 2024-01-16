import posixpath
from typing import Optional, NamedTuple, Any

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
    def __init__(
        self,
        host: str = "",
        port: str = "",
        database: str = "",
        **conn_kwargs: Any,
    ):
        self.endpoint = f"grpc://{host}:{port}"
        self.database = database
        self.conn_kwargs = conn_kwargs

        if "ydb_session_pool" in self.conn_kwargs:  # Use session pool managed manually
            self._shared_session_pool = True
            self.session_pool: ydb.SessionPool = self.conn_kwargs.pop("ydb_session_pool")
            self.driver = self.session_pool._pool_impl._driver
            self.driver.table_client = ydb.TableClient(self.driver, self._get_table_client_settings())
        else:
            self._shared_session_pool = False
            self.driver = self._create_driver()
            self.session_pool = ydb.SessionPool(self.driver, size=5, workers_threads_count=1)

        self.interactive_transaction: bool = False  # AUTOCOMMIT
        self.tx_mode: ydb.AbstractTransactionModeBuilder = ydb.SerializableReadWrite()
        self.tx_context: Optional[ydb.TxContext] = None

    def cursor(self):
        return Cursor(self.session_pool, self.tx_context)

    def describe(self, table_path):
        full_path = posixpath.join(self.database, table_path)
        try:
            return self.session_pool.retry_operation_sync(lambda session: session.describe_table(full_path))
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
        class IsolationSettings(NamedTuple):
            ydb_mode: ydb.AbstractTransactionModeBuilder
            interactive: bool

        ydb_isolation_settings_map = {
            IsolationLevel.AUTOCOMMIT: IsolationSettings(ydb.SerializableReadWrite(), interactive=False),
            IsolationLevel.SERIALIZABLE: IsolationSettings(ydb.SerializableReadWrite(), interactive=True),
            IsolationLevel.ONLINE_READONLY: IsolationSettings(ydb.OnlineReadOnly(), interactive=False),
            IsolationLevel.ONLINE_READONLY_INCONSISTENT: IsolationSettings(
                ydb.OnlineReadOnly().with_allow_inconsistent_reads(), interactive=False
            ),
            IsolationLevel.STALE_READONLY: IsolationSettings(ydb.StaleReadOnly(), interactive=False),
            IsolationLevel.SNAPSHOT_READONLY: IsolationSettings(ydb.SnapshotReadOnly(), interactive=True),
        }
        ydb_isolation_settings = ydb_isolation_settings_map[isolation_level]
        if self.tx_context and self.tx_context.tx_id:
            raise InternalError("Failed to set transaction mode: transaction is already began")
        self.tx_mode = ydb_isolation_settings.ydb_mode
        self.interactive_transaction = ydb_isolation_settings.interactive

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
        self.tx_context = None
        if self.interactive_transaction:
            session = self.session_pool.acquire(blocking=True)
            self.tx_context = session.transaction(self.tx_mode)
            self.tx_context.begin()

    def commit(self):
        if self.tx_context and self.tx_context.tx_id:
            self.tx_context.commit()
            self.session_pool.release(self.tx_context.session)
            self.tx_context = None

    def rollback(self):
        if self.tx_context and self.tx_context.tx_id:
            self.tx_context.rollback()
            self.session_pool.release(self.tx_context.session)
            self.tx_context = None

    def close(self):
        self.rollback()
        if not self._shared_session_pool:
            self.session_pool.stop()
            self._stop_driver()

    def _get_table_client_settings(self) -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(True)
        )

    def _create_driver(self):
        driver_config = ydb.DriverConfig(
            endpoint=self.endpoint,
            database=self.database,
            table_client_settings=self._get_table_client_settings(),
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
