"""
Middleware for handling performance testing configuration parsing.
Parses X-Perf-Request headers and attaches config to request state.
"""
from typing import (
    Callable,
    Final,
)

from fastapi import (
    Request,
    Response,
)
from performance.backend.utils.exceptions import MalformedRequestError
from performance.backend.utils.logging import setup_logger
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance.backend.perf_request import PerfRequestConfig

# Configure logger
logger = setup_logger("perf_config_middleware")


PERF_REQUEST_CONFIG_ATTRIBUTE_NAME: Final[str] = "perf_request_config"


class PerfRequestConfigMiddleware(BaseHTTPMiddleware):
    """Middleware for handling performance testing configuration from X-Perf-Request header."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        request_id = getattr(request.state, "id", "unknown")

        header_value = None
        try:
            header_value = request.headers.get("X-Perf-Request")
            perf_request_config = PerfRequestConfig.from_json(header_value)
            # Attach the parsed config to the request state for endpoint handlers
            # and other middleware components to use
            setattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, perf_request_config)
            logger.debug(f"[{request_id}] Parsed X-Perf-Request header")
        except Exception as e:
            logger.warning(f"[{request_id}] Error parsing X-Perf-Request header ({header_value}): {str(e)}")
            raise MalformedRequestError(f"Invalid X-Perf-Request header {header_value}") from e

        # Call the next handler (endpoint)
        response = await call_next(request)
        return response
