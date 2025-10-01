"""
Middleware for handling performance_e2e testing configuration parsing.
Parses X-Perf-Request headers and attaches config to request state.
"""
import json
from typing import (
    Callable,
    Final,
)

from fastapi import (
    Request,
    Response,
)
from starlette.middleware.base import BaseHTTPMiddleware

from tests.performance_e2e.backend.perf_request import PerfRequestConfig
from tests.performance_e2e.backend.utils.logging import setup_logger

# Configure logger
logger = setup_logger("perf_config_middleware")


PERF_REQUEST_CONFIG_ATTRIBUTE_NAME: Final[str] = "perf_request_config"


class PerfRequestConfigMiddleware(BaseHTTPMiddleware):
    """Middleware for handling performance_e2e testing configuration from X-Perf-Request header."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        header_value = None
        try:
            header_value = request.headers.get("X-Perf-Request")
            perf_request_config = PerfRequestConfig.from_json(header_value)
            # Attach the parsed config to the request state for endpoint handlers
            # and other middleware components to use
            setattr(request.state, PERF_REQUEST_CONFIG_ATTRIBUTE_NAME, perf_request_config)
            logger.debug("Parsed X-Perf-Request header")
        except Exception as e:
            logger.error(f"Error parsing X-Perf-Request header ({header_value}): {str(e)}")
            response_content = {"code": 400, "message": f"Malformed X-Perf-Request header: {str(e)}"}
            return Response(status_code=400, content=json.dumps(response_content))

        # Call the next handler (endpoint)
        response = await call_next(request)
        return response
