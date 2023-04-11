import ydb
import pytest
import time
import sqlalchemy as sa

from ydb_sqlalchemy import register_dialect


def wait_container_ready(driver):
    driver.wait(timeout=30)

    with ydb.SessionPool(driver) as pool:
        started_at = time.time()
        while time.time() - started_at < 30:
            try:
                with pool.checkout() as session:
                    session.execute_scheme("CREATE TABLE `.sys_health/test_table` (A Int32, PRIMARY KEY(A));")
                return True

            except ydb.Error:
                time.sleep(1)

    raise RuntimeError("Container is not ready after timeout.")


@pytest.fixture(scope="module")
def endpoint(pytestconfig, module_scoped_container_getter):
    with ydb.Driver(endpoint="localhost:2136", database="/local") as driver:
        wait_container_ready(driver)
    yield "localhost:2136"


@pytest.fixture(scope="module")
def database():
    return "/local"


@pytest.fixture(scope="module")
def engine(endpoint, database):
    register_dialect()
    engine = sa.create_engine(
        "yql:///ydb/",
        connect_args={"database": database, "endpoint": endpoint},
    )

    yield engine
    engine.dispose()


@pytest.fixture(scope="module")
def connection(engine):
    with engine.connect() as conn:
        yield conn


@pytest.fixture
async def driver(endpoint, database):
    driver_config = ydb.DriverConfig(endpoint, database)

    driver = ydb.aio.Driver(driver_config=driver_config)
    await driver.wait(timeout=15)

    yield driver

    await driver.stop(timeout=10)


@pytest.fixture
def driver_sync(endpoint, database):
    driver_config = ydb.DriverConfig(endpoint, database)

    driver = ydb.Driver(driver_config=driver_config)
    driver.wait(timeout=15)

    yield driver

    driver.stop(timeout=10)


@pytest.fixture
def ydb_session(driver_sync):
    session = ydb.retry_operation_sync(lambda: driver_sync.table_client.session().create())
    yield session
    session.delete()


@pytest.fixture
def test_table(ydb_session):
    ydb_session.execute_scheme("CREATE TABLE test(id Int64 NOT NULL, value UTF8, num DECIMAL(22, 9), PRIMARY KEY (id))")
    yield
    ydb_session.execute_scheme("DROP TABLE test")
