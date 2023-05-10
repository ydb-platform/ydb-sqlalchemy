#!/usr/bin/env python

import ydb
import time

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


def main():
    with ydb.Driver(endpoint="localhost:2136", database="/local") as driver:
        wait_container_ready(driver)


if __name__ == "__main__":
    main()
