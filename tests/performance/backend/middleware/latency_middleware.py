"""
Middleware for simulating network latency in performance tests.
"""
import asyncio
import random
import time
from typing import Callable

from fastapi import (
    Request,
    Response,
)
from performance.backend.utils.logging import setup_logger
from starlette.middleware.base import BaseHTTPMiddleware

# Configure logger
logger = setup_logger("latency_middleware")


class LatencyAddingMiddleware(BaseHTTPMiddleware):
    """Middleware for simulating latency in performance testing."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add simulated latency to responses.

        Args:
            request: The incoming request
            call_next: The next middleware or endpoint in the chain

        Returns:
            The response with simulated latency applied if configured
        """
        request_id = getattr(request.state, "id", "unknown")
        start_time_ms = time.time() * 1000

        # Call the next handler (endpoint)
        response = await call_next(request)

        # Check for latency configuration
        perf_config = getattr(request.state, "perf_config", None)
        if perf_config and perf_config.latency:
            # Calculate how long we've spent processing so far
            elapsed_time_ms = (time.time() * 1000) - start_time_ms

            # Generate random latency in the specified range using uniform distribution
            target_latency_ms = random.uniform(perf_config.latency.min_ms, perf_config.latency.max_ms)

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
