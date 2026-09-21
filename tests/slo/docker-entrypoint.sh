#!/bin/sh
# Workload entrypoint used by ydb-slo-action v2.
#
# The action launches the same image for both `current` and `baseline` workloads
# in parallel; both prepare the schema (idempotent) and then run the load.
#
# Env vars injected by the action:
#   WORKLOAD_NAME       core | orm          (selects the SQLAlchemy layer)
#   WORKLOAD_REF        current / <branch>  (used as the `ref` metric label)
#   WORKLOAD_DURATION   run duration in seconds
#   YDB_ENDPOINT        grpc://ydb:2136
#   YDB_DATABASE        /Root/testdb
#   OTEL_EXPORTER_OTLP_METRICS_ENDPOINT   Prometheus OTLP receiver
#
# Anything passed after the script name is appended to the `run` command — this
# is how tuning flags from `workload_current_command` (e.g. --read-rps) arrive.

set -e

ENDPOINT="${YDB_ENDPOINT:-grpc://localhost:2136}"
DATABASE="${YDB_DATABASE:-/local}"
DURATION="${WORKLOAD_DURATION:-60}"
MODE="${WORKLOAD_NAME:-core}"

echo "SLO workload: mode=${MODE} ref=${WORKLOAD_REF:-current} endpoint=${ENDPOINT} db=${DATABASE} duration=${DURATION}s"

# Tolerate a parallel container winning the create race.
python ./tests/slo/src create "$ENDPOINT" "$DATABASE" --mode "$MODE" \
    || echo "WARN: create exited non-zero (treated as already-prepared)" >&2

exec python ./tests/slo/src run "$ENDPOINT" "$DATABASE" \
    --mode "$MODE" \
    --time "$DURATION" \
    "$@"
