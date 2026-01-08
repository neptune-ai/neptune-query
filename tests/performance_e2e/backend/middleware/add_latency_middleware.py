"""
Middleware for simulating network latency in performance_e2e tests.
"""

import asyncio
import random
import time
from typing import Callable

from fastapi import (
    Request,
    Response,
)
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance_e2e.backend.middleware.read_perf_config_middleware import PERF_REQUEST_CONFIG_ATTRIBUTE_NAME
from tests.performance_e2e.backend.utils.exceptions import MalformedRequestError
from tests.performance_e2e.backend.utils.logging import setup_logger

# Configure logger
logger = setup_logger("latency_middleware")


class LatencyAddingMiddleware(BaseHTTPMiddleware):
    """Middleware for simulating latency in performance_e2e testing."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = getattr(request.state, "id", "unknown")
        start_time_ms = time.perf_counter_ns() / 1_000_000  # Convert to milliseconds

        # Call the next handler (endpoint)
        response = await call_next(request)

        # Check for latency configuration
        perf_request_config = getattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, None)
        if not perf_request_config:
            logger.error("No performance_e2e request configuration found; skipping latency addition.")
            raise MalformedRequestError("No performance_e2e request configuration found; skipping latency addition.")

        # Get endpoint-specific configuration
        path = request.url.path
        method = request.method
        endpoint_config = perf_request_config.get_endpoint_config(path, method)

        # Check if we have endpoint config with latency settings
        if endpoint_config.latency:
            # Calculate how long we've spent processing so far
            elapsed_time_ms = (time.perf_counter_ns() / 1_000_000) - start_time_ms

            # Generate random latency in the specified range using uniform distribution
            target_latency_ms = random.uniform(endpoint_config.latency.min_ms, endpoint_config.latency.max_ms)

            # Subtract the time already spent processing
            remaining_latency_ms = max(0.0, target_latency_ms - elapsed_time_ms)

            # Add artificial latency if needed
            if remaining_latency_ms > 0:
                logger.debug(
                    f"[{request_id}] Adding artificial latency: {target_latency_ms:.2f}ms "
                    f"(remaining: {remaining_latency_ms:.2f}ms)"
                )
                # Sleep for the remaining time to reach the desired latency (convert back to seconds)
                await asyncio.sleep(remaining_latency_ms / 1000.0)

                # Store latency information in metrics if available
                if hasattr(request.state, "metrics"):
                    request.state.metrics.latency_added_ms = remaining_latency_ms
                    request.state.metrics.latency_target_ms = target_latency_ms

                # Add headers with latency information
                response.headers["X-Artificial-Latency-Ms"] = str(round(remaining_latency_ms, 2))
                response.headers["X-Target-Latency-Ms"] = str(round(target_latency_ms, 2))

        return response
