"""
Main entry point for the performance test backend server.
Configures the FastAPI app and includes all endpoints.
"""
import time
from typing import Dict

from fastapi import FastAPI
from performance.backend.middleware.add_latency_middleware import LatencyAddingMiddleware
from performance.backend.middleware.read_perf_config_middleware import PerfRequestConfigMiddleware
from performance.backend.middleware.request_metrics_middleware import RequestMetricsMiddleware
from performance.backend.utils.logging import (
    configure_root_logger,
    setup_logger,
)

from tests.performance.backend.endpoints.search_leaderboard_entries import router as search_leaderboard_entries_router

# Configure root logger first to prevent any duplicate logging
configure_root_logger()

# Configure logger
logger = setup_logger("performance_test_backend")

# Create main FastAPI application
app = FastAPI(title="Performance Test Backend", version="0.0.1")

# Create sub-application for API endpoints with middleware
api_app = FastAPI(title="Performance Test API", version="0.0.1")

# Add middleware for performance testing to the API sub-app only
# IMPORTANT: middleware are executed in reverse order from how they're added
# The last middleware added is executed first, so we add them in the reverse order:
# 1. LatencyMiddleware (added first, executed last)
# 2. PerfConfigMiddleware (added second, executed second)
# 3. RequestMetricsMiddleware (added last, executed first)
api_app.add_middleware(LatencyAddingMiddleware)  # Executed last (innermost)
api_app.add_middleware(PerfRequestConfigMiddleware)  # Executed second
api_app.add_middleware(RequestMetricsMiddleware)  # Executed first (outermost)

# Include routers for API endpoints in the sub-app
api_app.include_router(search_leaderboard_entries_router)

# Mount the API sub-app under the /api path
app.mount("/api", api_app)


@app.get("/health")
async def health() -> Dict[str, str]:
    logger.debug("Health check requested")
    return {"status": "ok", "timestamp": str(time.time())}


def run(host: str = "127.0.0.1", port: int = 8080) -> None:
    """Run the FastAPI server with uvicorn."""
    import uvicorn

    logger.info(f"Starting server on {host}:{port}")

    # Use our custom log config - we've already configured the loggers
    uvicorn.run(app, host=host, port=port, log_level="info", log_config=None)


if __name__ == "__main__":  # pragma: no cover
    run()
