"""
Middleware for handling performance testing configuration parsing.
Parses X-Perf-Request headers and attaches config to request state.
"""
from typing import Callable

from fastapi import (
    Request,
    Response,
)
from performance.backend.utils.logging import setup_logger
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance.backend.perf_request import PerfRequestConfig

# Configure logger
logger = setup_logger("perf_config_middleware")


class PerfRequestConfigMiddleware(BaseHTTPMiddleware):
    """Middleware for handling performance testing configuration from X-Perf-Request header."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request and parse performance testing configuration.

        Args:
            request: The incoming request
            call_next: The next middleware or endpoint in the chain

        Returns:
            The response from downstream handlers
        """
        request_id = getattr(request.state, "id", "unknown")

        # Parse the X-Perf-Request header if present
        try:
            header_value = request.headers.get("X-Perf-Request")
            if header_value:
                perf_config = PerfRequestConfig.from_json(header_value)
                # Attach the parsed config to the request state for endpoint handlers
                # and other middleware components to use
                request.state.perf_config = perf_config
                logger.debug(f"[{request_id}] Parsed X-Perf-Request header")
        except Exception as e:
            logger.warning(f"[{request_id}] Error parsing X-Perf-Request header: {str(e)}")

        # Call the next handler (endpoint)
        response = await call_next(request)
        return response
