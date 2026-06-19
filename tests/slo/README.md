# SQLAlchemy SLO workload

A load generator that exercises the `ydb-sqlalchemy` dialect under
[`ydb-platform/ydb-slo-action`](https://github.com/ydb-platform/ydb-slo-action).
It keeps reader and writer threads busy against a single key/value table and
reports latency / throughput / availability to Prometheus over OTLP, so the
action can compare the current (PR) dialect against a baseline and gate the PR
on regressions.

## What it does

A reader looks up a row by primary key; a writer adds a new row. Both run in
parallel from dedicated thread pools.

Two execution modes (selected by `WORKLOAD_NAME` / `--mode`) exercise the two
ways real applications use the dialect:

| mode    | read path (point lookup)        | write path                                       |
|---------|---------------------------------|--------------------------------------------------|
| `core`  | `Connection.execute(select())`  | `Connection.execute(upsert())` (bulk KV upsert)  |
| `orm`   | `Session.get(KeyValueRow, id)`  | `Session.add(KeyValueRow(...))` + `commit()` (ORM insert) |

Each operation is a single autocommit statement — there is no app-level retry.
The dialect runs in AUTOCOMMIT by default, so every `execute` already goes
through the YDB SDK's `retry_operation_sync` inside `ydb-dbapi`; any exception
that still surfaces is recorded as a real SLO failure.

`core` writes use a monotonic id and UPSERT (idempotent, safe for the shared
table); `orm` writes use a random id so each `session.add` INSERT is collision-free
and avoids a hot last partition.

## Layout

```
tests/slo/
├── Dockerfile               # image used by ydb-slo-action (build context = repo root)
├── docker-entrypoint.sh     # create (idempotent) then run, honouring injected env
├── requirements.txt         # hdrhistogram + opentelemetry
└── src/
    ├── __main__.py          # entrypoint: python ./tests/slo/src <command> ...
    ├── options.py           # argparse (create / run / cleanup)
    ├── models.py            # engine factory, table + imperatively-mapped ORM entity
    ├── generator.py         # row payload generator
    ├── metrics.py           # OTLP metrics (names match the action's metrics.yaml)
    └── workload.py          # rate limiting, retries, parallel read/write jobs
```

## CLI

```bash
# Create the table and fill it with initial rows.
python ./tests/slo/src create grpc://localhost:2136 /local --mode core

# Run the parallel read/write load for 60s.
python ./tests/slo/src run grpc://localhost:2136 /local \
    --mode core --time 60 --read-rps 500 --write-rps 50 \
    --otlp-endpoint http://localhost:9090/api/v1/otlp/v1/metrics

# Drop the table.
python ./tests/slo/src cleanup grpc://localhost:2136 /local
```

When run by the action, `YDB_ENDPOINT`, `YDB_DATABASE`, `WORKLOAD_DURATION`,
`WORKLOAD_NAME`, `WORKLOAD_REF` and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` are
injected automatically; the metrics endpoint is picked up from the environment.

## Metrics

Emitted via OTLP/HTTP and consumed by the action's default `metrics.yaml`:

| Prometheus name                     | type    | labels                                   |
|-------------------------------------|---------|------------------------------------------|
| `sdk_operations_total`              | counter | `ref`, `operation_type`, `operation_status` |
| `sdk_operations_success_total`      | counter | `ref`, `operation_type`                  |
| `sdk_operations_failure_total`      | counter | `ref`, `operation_type`                  |
| `sdk_retry_attempts_total`          | counter | `ref`, `operation_type`                  |
| `sdk_operation_latency_p{50,95,99}_seconds` | gauge | `ref`, `operation_type`, `operation_status` |

## CI

`.github/workflows/slo.yml` runs the workload on pull requests labelled `SLO`.
It builds the current and baseline images, hands them to `ydb-slo-action/init`,
then publishes a comparison report with `ydb-slo-action/report`. The workload
job runs on the `large-runner-sqlalchemy` self-hosted runner with the full YDB
cluster; the report job runs on `ubuntu-latest`.
