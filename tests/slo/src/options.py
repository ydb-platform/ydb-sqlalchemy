import argparse
from os import environ

_DEFAULT_MODE = (environ.get("WORKLOAD_NAME") or "core").strip().lower()
if _DEFAULT_MODE not in ("core", "orm"):
    _DEFAULT_MODE = "core"


def _add_common(parser):
    parser.add_argument("endpoint", help="YDB endpoint, e.g. grpc://localhost:2136")
    parser.add_argument("database", help="YDB database path, e.g. /local")
    parser.add_argument("-t", "--table-name", default="slo_sqlalchemy", help="Workload table name")
    parser.add_argument(
        "--mode",
        choices=("core", "orm"),
        default=_DEFAULT_MODE,
        help="SQLAlchemy layer to exercise (defaults to WORKLOAD_NAME)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")


def _add_create(subparsers):
    parser = subparsers.add_parser("create", help="Create the table and fill it with initial rows")
    _add_common(parser)
    parser.add_argument("-c", "--initial-data-count", default=1000, type=int, help="Initial row count")
    parser.add_argument("--batch-size", default=100, type=int, help="Rows per insert batch")
    parser.add_argument("-p-min", "--min-partitions-count", default=6, type=int, help="Min partitions")
    parser.add_argument("-p-max", "--max-partitions-count", default=100, type=int, help="Max partitions")
    parser.add_argument("-p-size", "--partition-size", default=100, type=int, help="Partition size [MB]")
    parser.add_argument("--write-timeout", default=20000, type=int, help="Write timeout [ms]")
    parser.add_argument("--max-retries", default=30, type=int, help="Max attempts per operation")


def _add_run(subparsers):
    parser = subparsers.add_parser("run", help="Run the parallel read/write SLO load")
    _add_common(parser)
    parser.add_argument("--read-rps", default=500, type=int, help="Target read RPS")
    parser.add_argument("--write-rps", default=50, type=int, help="Target write RPS")
    parser.add_argument("--read-threads", default=8, type=int, help="Reader threads")
    parser.add_argument("--write-threads", default=4, type=int, help="Writer threads")
    parser.add_argument("--read-timeout", default=20000, type=int, help="Read timeout [ms]")
    parser.add_argument("--write-timeout", default=20000, type=int, help="Write timeout [ms]")
    parser.add_argument("--initial-data-count", default=1000, type=int, help="Fallback id space when table is empty")
    parser.add_argument("--time", default=60, type=int, help="Run duration [s]")
    parser.add_argument("--shutdown-time", default=10, type=int, help="Graceful shutdown time [s]")
    parser.add_argument("--max-retries", default=30, type=int, help="Max attempts per operation")
    parser.add_argument(
        "--otlp-endpoint",
        default="",
        type=str,
        help="OTLP metrics endpoint; empty disables metrics unless OTEL_* env vars are set",
    )
    parser.add_argument("--report-period", default=1000, type=int, help="Metrics push period [ms]")


def _add_cleanup(subparsers):
    parser = subparsers.add_parser("cleanup", help="Drop the workload table")
    _add_common(parser)


def parse_options():
    parser = argparse.ArgumentParser(description="YDB SQLAlchemy SLO workload")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_create(subparsers)
    _add_run(subparsers)
    _add_cleanup(subparsers)
    return parser.parse_args()
