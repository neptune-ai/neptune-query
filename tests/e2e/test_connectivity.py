import warnings

import pytest

from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)


@pytest.fixture(scope="module")
def project_1(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    return ensure_project(
        project_data=ProjectData(
            runs=[
                RunData(
                    experiment_name="test_connectivity_experiment",
                    run_id="test_connectivity_run",
                    configs={"im_alive": 1},
                ),
            ]
        )
    )


def test_connectivity(client, project_1: IngestedProjectData) -> None:
    """A placeholder test to ensure connectivity to the Neptune server"""
    assert True
