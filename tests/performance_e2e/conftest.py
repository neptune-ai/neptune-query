import base64
import contextlib
import json
import multiprocessing
import os
import signal
import time
import traceback
from typing import (
    Any,
    Dict,
    Iterator,
    Optional,
)
from urllib.parse import urlparse

import httpx
import pytest
from humanize import naturalsize
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials
from neptune_api.types import OAuthToken

import neptune_query.internal.client as client
from neptune_query import set_api_token
from tests.performance_e2e.backend.utils.logging import setup_logger

# Get a logger for the test framework using our centralized configuration
logger = setup_logger("performance_tests")


# Configuration constants with environment variable overrides
SERVER_HOST = os.environ.get("PERF_TEST_HOST", "127.0.0.1")
SERVER_PORT = int(os.environ.get("PERF_TEST_PORT", "8080"))
SERVER_STARTUP_TIMEOUT = int(os.environ.get("PERF_TEST_STARTUP_TIMEOUT", "20"))
SERVER_HEALTH_CHECK_INTERVAL = float(os.environ.get("PERF_TEST_HEALTH_INTERVAL", "0.25"))
HTTP_CLIENT_TIMEOUT = int(os.environ.get("PERF_TEST_CLIENT_TIMEOUT", "10"))


@pytest.fixture(scope="session")
def backend_base_url() -> str:
    """Provide the base URL for the test backend server.

    Note:
        Can be overridden with PERF_TEST_HOST and PERF_TEST_PORT environment variables
    """
    return f"http://{SERVER_HOST}:{SERVER_PORT}"


def _run_server(host: str, port: int) -> None:  # pragma: no cover - helper for spawning process
    """Run the FastAPI server for performance_e2e testing."""
    try:
        import uvicorn

        logger.info(f"Starting performance_e2e test server at {host}:{port}")
        uvicorn.run(
            app="tests.performance_e2e.backend.main:app",
            host=host,
            port=port,
            log_level="info",
            access_log=False,
            workers=8,
        )
    except Exception:
        logger.error(f"Error in test server process:\n{traceback.format_exc()}")
        # Make sure the process exits so the test doesn't hang
        os._exit(1)


@pytest.fixture(scope="session", autouse=True)
def backend_server(backend_base_url: str) -> Iterator[None]:
    """Start the test FastAPI backend in a separate process for the test session.

    Raises:
        RuntimeError: If the server fails to start or doesn't respond to health checks
    """
    parsed = urlparse(backend_base_url)
    host = parsed.hostname or SERVER_HOST
    port = parsed.port or SERVER_PORT

    # Create a server process with proper signal handling
    ctx = multiprocessing.get_context("spawn")  # Use spawn for better cross-platform compatibility
    proc = ctx.Process(target=_run_server, args=(host, port), daemon=False)

    logger.info(f"Starting backend server process at {backend_base_url}")
    proc.start()

    # Health check initialization
    deadline = time.time() + SERVER_STARTUP_TIMEOUT
    healthy = False

    logger.info("Waiting for server to become healthy...")

    # Use a more readable approach to health checking
    for attempt in range(1, int(SERVER_STARTUP_TIMEOUT / SERVER_HEALTH_CHECK_INTERVAL) + 1):
        # Check if server process is still alive
        if not proc.is_alive():
            logger.error("Server process died during startup")
            break

        # Try to connect to the health endpoint
        try:
            response = httpx.get(
                url=f"{backend_base_url}/health", timeout=1.0, headers={"User-Agent": "Neptune-Performance-Test/1.0"}
            )
            if response.status_code == 200:
                healthy = True
                logger.info(f"Server is healthy after {attempt} attempts")
                break
        except Exception as e:
            if attempt % 10 == 0:  # Log less frequently to avoid spamming
                logger.debug(f"Health check attempt {attempt} failed: {str(e)}")

        # Wait before next attempt, but check if we're past the deadline
        if time.time() > deadline:
            logger.error(f"Reached timeout after {attempt} attempts")
            break

        time.sleep(SERVER_HEALTH_CHECK_INTERVAL)

    if not healthy:
        error_msg = f"Backend server failed to start or become healthy within {SERVER_STARTUP_TIMEOUT}s"
        logger.error(error_msg)

        # Ensure process terminated before raising to avoid zombie
        if proc.is_alive():
            logger.info("Terminating unresponsive server process")
            with contextlib.suppress(Exception):
                # Try graceful termination first
                os.kill(proc.pid, signal.SIGTERM)
                proc.join(timeout=2)

            # Force kill if still running
            if proc.is_alive():
                logger.warning("Server process didn't terminate gracefully, forcing kill")
                with contextlib.suppress(Exception):
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.join(timeout=1)

        raise RuntimeError(error_msg)

    # Server is up and healthy, yield control to tests
    logger.info("Backend server is ready for tests")
    yield

    # Cleanup after tests
    logger.info("Shutting down backend server")
    if proc.is_alive():
        with contextlib.suppress(Exception):
            # Try graceful termination first
            os.kill(proc.pid, signal.SIGTERM)
            proc.join(timeout=3)

        # Force kill if still running
        if proc.is_alive():
            logger.warning("Server process didn't terminate gracefully, forcing kill")
            with contextlib.suppress(Exception):
                os.kill(proc.pid, signal.SIGKILL)
                proc.join(timeout=2)

    logger.info("Backend server shutdown complete")


@pytest.fixture(scope="session", autouse=True)
def api_token(backend_base_url: str) -> str:
    """Create and configure a fake API token for testing.

    Args:
        backend_base_url: The base URL of the test server

    Returns:
        A base64-encoded fake API token
    """
    api_token_bytes = json.dumps(
        {"api_address": backend_base_url, "api_url": backend_base_url, "api_key": "fake"}
    ).encode("utf-8")
    api_token = base64.b64encode(api_token_bytes).decode("utf-8")

    # set globally
    set_api_token(api_token)

    # and return
    return api_token


class ClientProviderWithHeaderInjection:
    """Wrapper for the Neptune API client with header management."""

    def __init__(self, real_client: Optional[AuthenticatedClient] = None):
        self._headers: Dict[str, str] = {}
        self._client = real_client

    def set_x_perf_request_header(self, value: str) -> None:
        """Update request headers for subsequent API calls."""
        self._headers.update({"X-Perf-Request": value})

    def set_scenario_name_header(self, scenario_name: str) -> None:
        """Update request headers for subsequent API calls."""
        self._headers.update({"X-Scenario-Name": scenario_name})

    def __call__(self, context: Any, proxies: Optional[Dict[str, str]] = None) -> AuthenticatedClient:
        """Return a configured client with the current headers."""
        return self._client.with_headers(self._headers)


@pytest.fixture(scope="function")
def http_client(monkeypatch, backend_base_url: str, api_token: str) -> ClientProviderWithHeaderInjection:
    """Create and configure an HTTP client for API testing.

    Returns:
        A wrapper for the Neptune API client with header management
    """
    never_expiring_token = OAuthToken(access_token="x", refresh_token="x", expiration_time=time.time() + 10_000_000)
    patched_client = AuthenticatedClient(
        base_url=backend_base_url,
        credentials=Credentials.from_api_key(api_token),
        client_id="",
        token_refreshing_endpoint="",
        api_key_exchange_callback=lambda _client, _credentials: never_expiring_token,
        verify_ssl=False,
        httpx_args={"http2": False},
        timeout=httpx.Timeout(timeout=HTTP_CLIENT_TIMEOUT),
        headers={"User-Agent": "Neptune-Performance-Test/1.0"},
    )

    client_provider = ClientProviderWithHeaderInjection(patched_client)

    monkeypatch.setattr(client, "get_client", client_provider)

    return client_provider


def resolve_timeout(default_seconds: float) -> float:
    test_mode = os.environ.get("NEPTUNE_PERFORMANCE_TEST_MODE", "normal")
    if test_mode == "baseline_discovery":
        return 3_600.0  # 1 hour for baseline discovery

    tolerance = float(os.environ.get("NEPTUNE_PERFORMANCE_TEST_TOLERANCE_FACTOR", 1.1))

    return default_seconds * tolerance


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Let pytest create the report first
    outcome = yield
    rep = outcome.get_result()

    # Only act after the test body ran
    if rep.when != "call":
        return
    # if not item.config.getoption("--df-summary"):
    #     return

    # Look for properties added via record_property(...)
    recorded_properties = dict(rep.user_properties or [])

    # Write via terminal reporter to bypass capture
    tr = item.config.pluginmanager.get_plugin("terminalreporter")
    if tr is not None:
        if "dataframe_memory_usage" in recorded_properties:
            tr.write(f"df_mem_usage={naturalsize(recorded_properties['dataframe_memory_usage'])} ")
        tr.write(f"duration={rep.duration:.3f}s ")
