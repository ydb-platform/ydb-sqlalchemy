import importlib.util
from typing import Optional


def maybe_get_current_trace_id() -> Optional[str]:
    # Check if OpenTelemetry is available
    if importlib.util.find_spec("opentelemetry"):
        from opentelemetry import trace

        current_span = trace.get_current_span()

        if current_span.get_span_context().is_valid:
            return format(current_span.get_span_context().trace_id, "032x")

    # Return None if OpenTelemetry is not available or trace ID is invalid
    return None
