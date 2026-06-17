# syntax=docker/dockerfile:1
#
# Packages the SQLAlchemy SLO workload runner for ydb-slo-action v2.
#
# Build context is the repository root. The action builds this image twice — for
# the `current` (PR) and `baseline` (merge-base) versions of the dialect — and
# runs both in parallel against the same YDB cluster, distinguishing them by the
# WORKLOAD_REF metric label (see tests/slo/docker-entrypoint.sh).
#
# Notes:
# - OpenTelemetry 1.39.x requires Python >= 3.9.
# - gcc/libc6-dev are needed to build hdrhistogram from sdist.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# 1. ydb-sqlalchemy dialect (current or baseline source tree).
COPY setup.py pyproject.toml requirements.txt README.md ./
COPY ydb_sqlalchemy/ ydb_sqlalchemy/
RUN pip install --no-cache-dir .

# 2. SLO runner deps.
COPY tests/slo/requirements.txt tests/slo/requirements.txt
RUN pip install --no-cache-dir -r tests/slo/requirements.txt

# 3. Workload source + entrypoint.
COPY tests/slo/src /src/tests/slo/src
COPY tests/slo/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
