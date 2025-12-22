import inspect
import itertools as it
import os
import pathlib
import random
import string
import tempfile
from concurrent.futures import Executor
from datetime import (
    datetime,
    timezone,
)

import pytest
from _pytest.outcomes import Failed
from neptune_api import AuthenticatedClient
from neptune_api.credentials import Credentials

from neptune_query.internal.api_utils import (
    create_auth_api_client,
    get_config_and_token_urls,
)
from neptune_query.internal.composition import concurrency
from neptune_query.internal.context import set_api_token
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    ingest_project,
)


def pytest_set_filtered_exceptions() -> list[type[BaseException]]:
    """
    Specify exceptions that should be retried by pytest-retry plugin.
    """
    return [AssertionError, ValueError, Failed]


def pytest_configure(config):
    """
    Pytest hook: called once at the beginning of the test run.
    Generate a unique test execution ID to be shared across all workers.
    """
    if not hasattr(config, "workerinput"):
        # Controller (no xdist or before workers spawn): generate once
        execution_id = os.getenv("NEPTUNE_TEST_EXECUTION_ID")
        if execution_id is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            random_suffix = "".join(random.choices(string.ascii_lowercase, k=6))
            execution_id = f"{timestamp}_{random_suffix}"
        config.neptune_test_execution_id = execution_id
    else:
        # Worker: get from worker input
        config.neptune_test_execution_id = config.workerinput["__neptune_test_execution_id"]


def pytest_configure_node(node):
    """
    Controller hook: called for each worker being created.
    Share the test execution ID with the worker.
    """
    # Send the already-generated value to each worker
    node.workerinput["__neptune_test_execution_id"] = node.config.neptune_test_execution_id


@pytest.fixture(scope="session")
def api_token() -> str:
    api_token = os.getenv("NEPTUNE_E2E_API_TOKEN")
    if api_token is None:
        raise RuntimeError("NEPTUNE_E2E_API_TOKEN environment variable is not set")
    return api_token


@pytest.fixture(scope="session")
def test_execution_id(request) -> str:
    return request.config.neptune_test_execution_id


@pytest.fixture(scope="session")
def workspace() -> str:
    value = os.getenv("NEPTUNE_E2E_WORKSPACE")
    if not value:
        raise RuntimeError("NEPTUNE_E2E_WORKSPACE environment variable is not set")
    return value


@pytest.fixture(autouse=True)
def set_api_token_auto(api_token) -> None:
    """Set the API token for the session."""
    set_api_token(api_token)


@pytest.fixture(scope="session")
def client(api_token) -> AuthenticatedClient:
    credentials = Credentials.from_api_key(api_key=api_token)
    config, token_urls = get_config_and_token_urls(credentials=credentials, proxies=None)
    client = create_auth_api_client(
        credentials=credentials, config=config, token_refreshing_urls=token_urls, proxies=None
    )

    return client


@pytest.fixture(scope="module")
def executor() -> Executor:
    return concurrency.create_thread_pool_executor()


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


def extract_pages(generator):
    return list(it.chain.from_iterable(i.items for i in generator))


class EnsureProjectFunction:
    """
    See ensure_project fixture docstring for usage details.
    """

    def __init__(self, client: AuthenticatedClient, api_token: str, workspace: str, test_execution_id: str):
        self.client = client
        self.api_token = api_token
        self.workspace = workspace
        self.test_execution_id = test_execution_id

    def __call__(self, project_data: ProjectData) -> IngestedProjectData:
        caller_frame = inspect.currentframe().f_back
        caller_module = inspect.getmodule(caller_frame)
        caller_module_name = caller_module.__name__
        caller_fn_name = caller_frame.f_code.co_name

        return ingest_project(
            client=self.client,
            api_token=self.api_token,
            workspace=self.workspace,
            project_name=f"pye2e__{self.test_execution_id}__{caller_module_name}.{caller_fn_name}",
            project_data=project_data,
        )


@pytest.fixture(scope="session")
def ensure_project(client, api_token, workspace, test_execution_id) -> EnsureProjectFunction:
    """Fixture returning a function-like object that can be used to create or retrieve projects with specified data.

    Arguments for the returned callable:
        project_data: Data to initialize the project with

    Returns:
        IngestedProjectData containing information about the created/retrieved project

    Example:
        @pytest.fixture(scope="module")
        def project_gamma(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
            ingested_project = ensure_project(
                ProjectData(
                    runs=[
                        RunData(
                            experiment_name="experiment_gamma",
                            config={"param": "value"},
                            float_series={"metrics/accuracy": {0: 0.5, 1: 0.75, 2: 0.9}},
                            string_series={"logs/status": {0: "started", 1: "in_progress", 2: "completed"}},
                            histogram_series={ ... },
                            files={ ... },
                        )
                        ...
                    ],
                ))
    """
    return EnsureProjectFunction(client, api_token, workspace, test_execution_id)
