"""
Core of the SQLAlchemy SLO workload.

A single workload run keeps several reader and writer threads busy against one
YDB table through the ``ydb_sqlalchemy`` dialect, while a metrics thread ships
latency/availability samples to Prometheus over OTLP. Two execution modes are
supported, selected by ``WORKLOAD_NAME`` / ``--mode``:

  * ``core`` — SQLAlchemy Core (``Connection.execute``)
  * ``orm``  — SQLAlchemy ORM (``Session`` + imperatively mapped entity)

Every operation is wrapped in an idempotent retry loop so that transient
failures injected by ydb-slo-action's chaos layer turn into latency rather than
into availability drops.
"""

import logging
import random
import threading
import time

import sqlalchemy as sa
from sqlalchemy.orm import Session

from ydb_sqlalchemy import upsert as ydb_upsert

from generator import RowGenerator
from metrics import OP_TYPE_READ, OP_TYPE_WRITE, create_metrics
from models import build_engine, build_table, ensure_mapped

logger = logging.getLogger(__name__)


class SyncRateLimiter:
    """Thread-safe limiter enforcing a minimum interval between permits."""

    def __init__(self, min_interval_s: float) -> None:
        self._min_interval_s = max(0.0, float(min_interval_s))
        self._lock = threading.Lock()
        self._next_allowed_ts = 0.0

    def __enter__(self):
        if self._min_interval_s <= 0.0:
            return self
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed_ts:
                    self._next_allowed_ts = now + self._min_interval_s
                    return self
                sleep_for = self._next_allowed_ts - now
            if sleep_for > 0:
                time.sleep(sleep_for)

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _limiter_for_rps(rps: int) -> SyncRateLimiter:
    return SyncRateLimiter(0.0 if rps <= 0 else 1.0 / rps)


def retry_call(fn, *, deadline: float, max_attempts: int):
    """
    Run ``fn`` until it succeeds, the deadline passes or ``max_attempts`` is hit.

    Returns ``(attempts, error)`` where ``error`` is ``None`` on success. Reads
    and UPSERT writes are idempotent, so retrying any failure is safe.
    """
    attempt = 0
    last_error = None
    while True:
        attempt += 1
        try:
            fn()
            return attempt, None
        except Exception as err:  # noqa: BLE001 - idempotent ops, retry everything
            last_error = err

        if attempt >= max_attempts or time.monotonic() >= deadline:
            return attempt, last_error

        backoff = min(1.0, 0.02 * (2 ** min(attempt, 6)))
        time.sleep(backoff * (0.5 + random.random()))


class Workload:
    def __init__(self, args):
        self.args = args
        self.mode = getattr(args, "mode", "core")
        self.table_name = args.table_name

    # -- schema lifecycle ---------------------------------------------------

    def create(self):
        engine = build_engine(self.args.endpoint, self.args.database, pool_size=4)
        try:
            metadata = sa.MetaData()
            table = build_table(
                metadata,
                self.table_name,
                min_partitions=self.args.min_partitions_count,
                max_partitions=self.args.max_partitions_count,
                partition_size_mb=self.args.partition_size,
            )

            # current + baseline containers create the table in parallel; the
            # loser of the race sees "already exists" and we just move on.
            try:
                table.create(engine, checkfirst=True)
                logger.info("Table %s is ready", self.table_name)
            except Exception:
                logger.warning("Create table %s reported an error (assuming it exists)", self.table_name, exc_info=True)

            self._fill_initial_data(engine, table)
        finally:
            engine.dispose()

    def _fill_initial_data(self, engine, table):
        total = self.args.initial_data_count
        batch_size = self.args.batch_size
        generator = RowGenerator(start_id=0)
        logger.info("Filling %s with %s initial rows", self.table_name, total)

        inserted = 0
        while inserted < total:
            size = min(batch_size, total - inserted)
            batch = [generator.get().as_params() for _ in range(size)]
            deadline = time.monotonic() + self.args.write_timeout / 1000
            attempts, error = retry_call(
                lambda b=batch: self._upsert_batch(engine, table, b),
                deadline=deadline,
                max_attempts=self.args.max_retries,
            )
            if error is not None:
                raise error
            inserted += size

        logger.info("Inserted %s rows into %s", inserted, self.table_name)

    @staticmethod
    def _upsert_batch(engine, table, batch):
        with engine.begin() as conn:
            conn.execute(ydb_upsert(table), batch)

    def cleanup(self):
        engine = build_engine(self.args.endpoint, self.args.database, pool_size=2)
        try:
            metadata = sa.MetaData()
            table = build_table(metadata, self.table_name)
            table.drop(engine, checkfirst=True)
            logger.info("Dropped table %s", self.table_name)
        finally:
            engine.dispose()

    # -- load ---------------------------------------------------------------

    def run(self):
        args = self.args
        metrics = create_metrics(args.otlp_endpoint)
        pool_size = args.read_threads + args.write_threads + 2
        engine = build_engine(args.endpoint, args.database, pool_size=pool_size)

        metadata = sa.MetaData()
        table = build_table(metadata, self.table_name)
        if self.mode == "orm":
            ensure_mapped(table)

        max_id = self._max_id(engine, table)
        logger.info("Starting '%s' SLO load on %s (max_id=%s)", self.mode, self.table_name, max_id)

        read_stmt = sa.select(table).where(table.c.object_id == sa.bindparam("object_id"))
        read_limiter = _limiter_for_rps(args.read_rps)
        write_limiter = _limiter_for_rps(args.write_rps)
        row_generator = RowGenerator(start_id=max_id)
        end_time = time.monotonic() + args.time

        threads = []
        for i in range(args.read_threads):
            threads.append(
                threading.Thread(
                    name=f"slo_read_{i}",
                    target=self._reader_loop,
                    args=(engine, table, read_stmt, metrics, read_limiter, max_id, end_time),
                )
            )
        for i in range(args.write_threads):
            threads.append(
                threading.Thread(
                    name=f"slo_write_{i}",
                    target=self._writer_loop,
                    args=(engine, table, metrics, write_limiter, row_generator, end_time),
                )
            )
        metrics_thread = threading.Thread(
            name="slo_metrics",
            target=self._metrics_loop,
            args=(metrics, end_time, args.report_period),
        )

        for t in threads:
            t.start()
        metrics_thread.start()

        for t in threads:
            t.join()
        metrics_thread.join()

        metrics.push()
        metrics.reset()
        engine.dispose()
        logger.info("Finished '%s' SLO load", self.mode)

    def _max_id(self, engine, table) -> int:
        try:
            with engine.connect() as conn:
                value = conn.execute(sa.select(sa.func.max(table.c.object_id))).scalar()
            return int(value) if value else self.args.initial_data_count
        except Exception:
            logger.warning("Could not read max(object_id); falling back to initial-data-count", exc_info=True)
            return max(1, self.args.initial_data_count)

    # -- per-operation work -------------------------------------------------

    def _reader_loop(self, engine, table, read_stmt, metrics, limiter, max_id, end_time):
        model = ensure_mapped(table) if self.mode == "orm" else None
        timeout_s = self.args.read_timeout / 1000

        while time.monotonic() < end_time:
            with limiter:
                object_id = random.randint(1, max(1, max_id))

                if self.mode == "orm":

                    def do_read(oid=object_id):
                        with Session(engine) as session:
                            session.get(model, oid)

                else:

                    def do_read(oid=object_id):
                        with engine.connect() as conn:
                            conn.execute(read_stmt, {"object_id": oid}).fetchall()

                start = metrics.start(OP_TYPE_READ)
                attempts, error = retry_call(
                    do_read,
                    deadline=time.monotonic() + timeout_s,
                    max_attempts=self.args.max_retries,
                )
                metrics.stop(OP_TYPE_READ, start, attempts=attempts, error=error)

    def _writer_loop(self, engine, table, metrics, limiter, row_generator, end_time):
        timeout_s = self.args.write_timeout / 1000

        while time.monotonic() < end_time:
            with limiter:
                params = row_generator.get().as_params()

                if self.mode == "orm":

                    def do_write(p=params):
                        with Session(engine) as session:
                            session.execute(ydb_upsert(table).values(**p))
                            session.commit()

                else:

                    def do_write(p=params):
                        with engine.begin() as conn:
                            conn.execute(ydb_upsert(table).values(**p))

                start = metrics.start(OP_TYPE_WRITE)
                attempts, error = retry_call(
                    do_write,
                    deadline=time.monotonic() + timeout_s,
                    max_attempts=self.args.max_retries,
                )
                metrics.stop(OP_TYPE_WRITE, start, attempts=attempts, error=error)

    @staticmethod
    def _metrics_loop(metrics, end_time, report_period_ms):
        limiter = SyncRateLimiter(max(1, int(report_period_ms)) / 1000.0)
        while time.monotonic() < end_time:
            with limiter:
                metrics.push()


def run_from_args(args):
    workload = Workload(args)
    command = args.command
    if command == "create":
        workload.create()
    elif command == "run":
        workload.run()
    elif command == "cleanup":
        workload.cleanup()
    else:
        raise ValueError(f"Unknown command: {command}")
