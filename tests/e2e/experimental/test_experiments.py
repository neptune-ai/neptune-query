from __future__ import annotations

import pandas as pd
import pytest

from neptune_query import filters
from neptune_query.experimental import fetch_experiments_table
from tests.e2e.data_model import IngestedProjectData
from tests.e2e.experimental.test_experiments_data import (
    project_1_data,
    project_2_data,
)
from tests.e2e.ingestion import ensure_project

pytestmark = pytest.mark.filterwarnings("ignore:.*fetch_experiments_table.*:neptune_query.warnings.ExperimentalWarning")


@pytest.fixture(scope="session")
def project_1(client, api_token, workspace, test_execution_id) -> IngestedProjectData:
    return ensure_project(
        client=client,
        api_token=api_token,
        workspace=workspace,
        execution_id=test_execution_id,
        project_data=project_1_data,
    )


@pytest.fixture(scope="session")
def project_2(client, api_token, workspace, test_execution_id) -> IngestedProjectData:
    return ensure_project(
        client=client,
        api_token=api_token,
        workspace=workspace,
        execution_id=test_execution_id,
        project_data=project_2_data,
    )


def test_fetch_experiments_table_isolated_to_execution(project_1, project_2, test_execution_id):
    dataframe = fetch_experiments_table(
        experiments=f"{test_execution_id}",  # all experiments in this test execution
        attributes=["config/int"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_config_mapping = {
        (run.project_identifier, run.experiment_name): run.configs.get("config/int")
        for project in (project_1, project_2)
        for run in project.ingested_runs
    }
    expected_index_entries = sorted(expected_config_mapping.keys())

    expected_dataframe = pd.DataFrame(
        data={
            "config/int": [value for _, value in sorted(expected_config_mapping.items())],
        },
        index=pd.MultiIndex.from_tuples(expected_index_entries, names=["project", "experiment"]),
        columns=pd.Index(["config/int"], name="attribute"),
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)
