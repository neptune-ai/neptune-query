"""
Middleware for tracking request metrics in performance tests.
Records timing information and adds metrics to requests.
"""
import json
import time
from typing import Callable

from fastapi import (
    Request,
    Response,
)
from performance.backend.utils.logging import (
    request_id_filter,
    setup_logger,
)
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance.backend.utils.metrics import RequestMetrics
from tests.performance.backend.utils.random_utils import random_string

logger = setup_logger("request_metrics_middleware")


class RequestMetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking request metrics and adding request IDs."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add request ID and metrics tracking to requests."""
        # Generate and attach request ID
        request_id = f"req-{random_string(8)}"
        request.state.id = request_id
        request_id_filter.request_id = request_id

        # Create metrics object on request state
        request.state.metrics = RequestMetrics()

        # Process the request through the middleware chain
        start_time_ms = time.time() * 1000
        response = await call_next(request)

        # Record basic processing time in the metrics
        request.state.metrics.total_time_ms = time.time() * 1000 - start_time_ms

        # Add basic timing headers to response
        response.headers["X-Process-Time-Ms"] = str(round(request.state.metrics.total_time_ms, 2))
        response.headers["X-Request-ID"] = request_id

        # Add total time with latency information to response headers if latency was added
        if request.state.metrics.latency_added_ms > 0:
            total_time_with_latency_ms = request.state.metrics.total_time_ms + request.state.metrics.latency_added_ms
            response.headers["X-Total-Time-With-Latency-Ms"] = str(round(total_time_with_latency_ms, 2))

        # Log complete metrics
        log_metrics(request_id, request.state.metrics)

        # Reset request ID after processing
        request_id_filter.request_id = "-"

        return response


def log_metrics(request_id: str, metrics: RequestMetrics) -> None:
    metrics_data = {
        "request_id": request_id,
        "total_processing_time_ms": round(metrics.total_time_ms, 2),
        "parsing_time_ms": round(metrics.parse_time_ms, 2),
        "generation_time_ms": round(metrics.generation_time_ms, 2),
        "returned_payload_size_bytes": metrics.returned_payload_size_bytes,
        "latency_added_ms": round(metrics.latency_added_ms, 2) if metrics.latency_added_ms > 0 else 0,
    }

    logger.info(f"Request metrics: {json.dumps(metrics_data)}")
