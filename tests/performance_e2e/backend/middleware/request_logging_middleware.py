"""
Middleware for tracking request metrics in performance_e2e tests.
Records timing information and adds metrics to requests.
"""
import json
import time
from contextvars import Token
from typing import Callable

from fastapi import (
    Request,
    Response,
)
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance_e2e.backend.utils.logging import (
    request_id_ctx,
    request_id_filter,
    scenario_name_ctx,
    setup_logger,
)
from tests.performance_e2e.backend.utils.metrics import RequestMetrics
from tests.performance_e2e.backend.utils.random_utils import random_string

logger = setup_logger("request_metrics_middleware")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking request metrics and adding request IDs."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate and attach request ID
        request_id = request.headers.get("X-Request-ID", default=f"req-{random_string(8)}")
        scenario_name = request.headers.get("X-Scenario-Name", default="-")
        request.state.id = request_id

        request_id_token: Token = request_id_ctx.set(request_id)
        scenario_name_token: Token = scenario_name_ctx.set(scenario_name)
        try:

            logger.info(f"Handling {request.method} {request.url.path}")

            # Create metrics object on request state
            request.state.metrics = RequestMetrics()

            # Process the request through the middleware chain
            start_time_ms = time.perf_counter_ns() / 1_000_000  # Convert to milliseconds
            response = await call_next(request)

            # Record basic processing time in the metrics
            request.state.metrics.total_time_ms = (time.perf_counter_ns() / 1_000_000) - start_time_ms

            # Add basic timing headers to response
            response.headers["X-Process-Time-Ms"] = str(round(request.state.metrics.total_time_ms, 2))
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Scenario-Name"] = scenario_name

            # Log complete metrics
            log_result(request.state.metrics, response.status_code)

            # Reset request ID after processing
            request_id_filter.request_id = "-"

            return response
        finally:
            request_id_ctx.reset(request_id_token)
            scenario_name_ctx.reset(scenario_name_token)


def log_result(metrics: RequestMetrics, status_code: int) -> None:
    metrics_data = {
        "total_processing_time_ms": round(metrics.total_time_ms, 2),
        "parsing_time_ms": round(metrics.parse_time_ms, 2),
        "generation_time_ms": round(metrics.generation_time_ms, 2),
        "returned_payload_size_bytes": metrics.returned_payload_size_bytes,
        "artificial_latency_added_ms": round(metrics.latency_added_ms, 2) if metrics.latency_added_ms > 0 else 0,
    }

    logger.info(f"Response status_code={status_code}, metrics: {json.dumps(metrics_data)}")
