"""
OTLP metrics for the SQLAlchemy SLO workload.

The instrument names here are chosen so that, once exported through the
Prometheus OTLP receiver, they line up with the queries shipped in
ydb-slo-action's default ``metrics.yaml``:

  * ``sdk_operations_total``            (counter, labels: ref, operation_type, operation_status)
  * ``sdk_operations_success_total``    (counter)
  * ``sdk_operations_failure_total``    (counter)
  * ``sdk_retry_attempts_total``        (counter, labels: ref, operation_type)
  * ``sdk_operation_latency_p50_seconds`` / ``_p95_`` / ``_p99_`` (gauge)

Latency percentiles are computed client-side per push window via HdrHistogram
and emitted as gauges; the histogram is reset after every push so each sample
describes only the most recent window.
"""

import logging
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from os import environ
from typing import Optional

OP_TYPE_READ, OP_TYPE_WRITE = "read", "write"
OP_STATUS_SUCCESS, OP_STATUS_FAILURE = "success", "error"

REF = environ.get("WORKLOAD_REF") or environ.get("REF") or "current"
WORKLOAD = environ.get("WORKLOAD_NAME") or environ.get("WORKLOAD") or "core"

logger = logging.getLogger(__name__)


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("ydb-sqlalchemy")
    except Exception:
        return "0.0.0"


class BaseMetrics(ABC):
    @abstractmethod
    def start(self, op_type: str) -> float: ...

    @abstractmethod
    def stop(self, op_type: str, start_time: float, attempts: int = 1, error: Optional[BaseException] = None) -> None: ...

    @abstractmethod
    def push(self) -> None: ...

    @abstractmethod
    def reset(self) -> None: ...

    @contextmanager
    def measure(self, op_type: str):
        start_ts = self.start(op_type)
        error = None
        try:
            yield self
        except Exception as err:
            error = err
            raise
        finally:
            self.stop(op_type, start_ts, error=error)


class DummyMetrics(BaseMetrics):
    """No-op metrics used when no OTLP endpoint is configured (local runs)."""

    def start(self, op_type: str) -> float:
        return time.time()

    def stop(self, op_type, start_time, attempts=1, error=None) -> None:
        return None

    def push(self) -> None:
        return None

    def reset(self) -> None:
        return None


class OtlpMetrics(BaseMetrics):
    _HDR_MIN_US = 1
    _HDR_MAX_US = 60_000_000  # 60s
    _HDR_SIG_FIGS = 3
    _PERCENTILES = (("p50", 50.0), ("p95", 95.0), ("p99", 99.0))

    def __init__(self, otlp_metrics_endpoint: str):
        from hdrh.histogram import HdrHistogram
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource

        self._HdrHistogram = HdrHistogram

        resource = Resource.create(
            {
                "service.name": f"workload-{WORKLOAD}",
                "service.instance.id": environ.get("SLO_INSTANCE_ID", f"{REF}-{WORKLOAD}"),
                "ref": REF,
                "sdk": "ydb-sqlalchemy",
                "sdk_version": _sdk_version(),
                "workload": WORKLOAD,
            }
        )

        exporter = OTLPMetricExporter(endpoint=otlp_metrics_endpoint)
        reader = PeriodicExportingMetricReader(exporter)
        self._provider = MeterProvider(resource=resource, metric_readers=[reader])
        self._meter = self._provider.get_meter("ydb-sqlalchemy-slo")

        self._errors = self._meter.create_counter(
            name="sdk.errors.total",
            description="Total number of errors, categorized by error type.",
        )
        self._operations_total = self._meter.create_counter(
            name="sdk.operations.total",
            description="Total number of operations attempted.",
        )
        self._operations_success_total = self._meter.create_counter(
            name="sdk.operations.success.total",
            description="Total number of successful operations.",
        )
        self._operations_failure_total = self._meter.create_counter(
            name="sdk.operations.failure.total",
            description="Total number of failed operations.",
        )
        self._retry_attempts_total = self._meter.create_counter(
            name="sdk.retry.attempts.total",
            description="Total number of attempts (including the first one).",
        )
        self._pending = self._meter.create_up_down_counter(
            name="sdk.pending.operations",
            description="Current number of in-flight operations.",
        )
        self._latency_gauges = {
            name: self._meter.create_gauge(
                name=f"sdk.operation.latency.{name}.seconds",
                unit="s",
                description=f"Operation latency {name} over the last push window.",
            )
            for name, _ in self._PERCENTILES
        }

        self._lock = threading.Lock()
        self._hdr: dict = {}

    def _get_hdr(self, op_type: str, op_status: str):
        key = (op_type, op_status)
        hist = self._hdr.get(key)
        if hist is None:
            hist = self._HdrHistogram(self._HDR_MIN_US, self._HDR_MAX_US, self._HDR_SIG_FIGS)
            self._hdr[key] = hist
        return hist

    def start(self, op_type: str) -> float:
        self._pending.add(1, attributes={"ref": REF, "operation_type": op_type})
        return time.time()

    def stop(self, op_type: str, start_time: float, attempts: int = 1, error: Optional[BaseException] = None) -> None:
        duration = time.time() - start_time
        duration_us = min(max(int(duration * 1_000_000), self._HDR_MIN_US), self._HDR_MAX_US)

        op_status = OP_STATUS_SUCCESS if error is None else OP_STATUS_FAILURE
        base_attrs = {"ref": REF, "operation_type": op_type}
        op_attrs = {**base_attrs, "operation_status": op_status}

        self._retry_attempts_total.add(int(attempts), attributes=base_attrs)
        self._pending.add(-1, attributes=base_attrs)
        self._operations_total.add(1, attributes=op_attrs)

        if error is not None:
            self._errors.add(1, attributes={**base_attrs, "error_type": type(error).__name__})
            self._operations_failure_total.add(1, attributes=base_attrs)
        else:
            self._operations_success_total.add(1, attributes=base_attrs)

        with self._lock:
            self._get_hdr(op_type, op_status).record_value(duration_us)

    def push(self) -> None:
        with self._lock:
            for (op_type, op_status), hist in self._hdr.items():
                if hist.get_total_count() == 0:
                    continue
                attrs = {"ref": REF, "operation_type": op_type, "operation_status": op_status}
                for name, percentile in self._PERCENTILES:
                    value_s = hist.get_value_at_percentile(percentile) / 1_000_000
                    self._latency_gauges[name].set(value_s, attributes=attrs)
            for hist in self._hdr.values():
                hist.reset()
        self._provider.force_flush()

    def reset(self) -> None:
        with self._lock:
            for hist in self._hdr.values():
                hist.reset()
        self._provider.force_flush()


def _resolve_metrics_endpoint(cli_endpoint: Optional[str]) -> str:
    """
    Resolution order:
      1. OTEL_EXPORTER_OTLP_METRICS_ENDPOINT (used as-is)
      2. OTEL_EXPORTER_OTLP_ENDPOINT + /v1/metrics suffix
      3. CLI --otlp-endpoint
    """
    metrics_env = environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
    if metrics_env:
        return metrics_env

    base_env = environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if base_env:
        base = base_env.rstrip("/")
        return base if base.endswith("/v1/metrics") else f"{base}/v1/metrics"

    return (cli_endpoint or "").strip()


def create_metrics(otlp_endpoint: Optional[str]) -> BaseMetrics:
    endpoint = _resolve_metrics_endpoint(otlp_endpoint)
    if not endpoint:
        logger.info("Metrics disabled (no OTLP endpoint); using DummyMetrics")
        return DummyMetrics()

    logger.info("Exporting metrics via OTLP to: %s", endpoint)
    try:
        return OtlpMetrics(endpoint)
    except Exception:
        logger.exception("Failed to init OTLP metrics; falling back to DummyMetrics")
        return DummyMetrics()
