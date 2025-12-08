import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    Filter,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)

RUNS_IN_EXPERIMENT_A = [
    "experiment_a_run1",
    "experiment_a_run2",
]
RUNS_IN_EXPERIMENT_B = [
    "experiment_b_run1",
    "experiment_b_run2",
]
ALL_RUN_IDS = RUNS_IN_EXPERIMENT_A + RUNS_IN_EXPERIMENT_B


# TODO: remove once all e2e tests use the ensure_project framework
@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse():
    # Override autouse ingestion from shared v1 fixtures; this module ingests its own data.
    return None


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    runs_data = [
        RunData(
            experiment_name="experiment_a",
            run_id="experiment_a_run1",
            configs={"group": "a"},
            string_sets={"tags": ["group_a"]},
        ),
        RunData(
            experiment_name="experiment_a",
            run_id="experiment_a_run2",
            configs={"group": "a"},
            string_sets={"tags": ["group_a"]},
        ),
        RunData(
            experiment_name="experiment_b",
            run_id="experiment_b_run1",
            configs={"group": "b"},
            string_sets={"tags": ["group_b"]},
        ),
        RunData(
            experiment_name="experiment_b",
            run_id="experiment_b_run2",
            configs={"group": "b"},
        ),
    ]

    return ensure_project(ProjectData(runs=runs_data))


@pytest.mark.parametrize(
    "arg_runs",
    [
        ".*",
        None,
        ALL_RUN_IDS,
        Filter.name(["experiment_a", "experiment_b"]),
        Filter.name("experiment_a | experiment_b"),
        Filter.eq("group", "a") | Filter.eq("group", "b"),
    ],
)
def test_list_all_runs(project: IngestedProjectData, arg_runs):
    result = runs.list_runs(
        project=project.project_identifier,
        runs=arg_runs,
    )
    assert len(result) == len(ALL_RUN_IDS)
    assert set(result) == set(ALL_RUN_IDS)


@pytest.mark.parametrize(
    "arg_runs",
    [
        "experiment_a.*",
        RUNS_IN_EXPERIMENT_A,
        Filter.name(["experiment_a"]),
        Filter.name("experiment_a"),
        Filter.eq("group", "a"),
        Filter.eq(Attribute(name="group", type="string"), "a"),
        Filter.eq(Attribute(name="group"), "a"),
        Filter.contains_all(Attribute(name="tags", type="string_set"), ["group_a"]),
    ],
)
def test_list_experiment_a_runs(project: IngestedProjectData, arg_runs):
    result = runs.list_runs(
        project=project.project_identifier,
        runs=arg_runs,
    )
    assert len(result) == len(RUNS_IN_EXPERIMENT_A)
    assert set(result) == set(RUNS_IN_EXPERIMENT_A)


@pytest.mark.parametrize(
    "arg_runs",
    [
        "abc",
        ["abc"],
        Filter.eq(Attribute(name="non-existent", type="bool"), True),
    ],
)
def test_list_runs_empty_filter(project: IngestedProjectData, arg_runs):
    result = runs.list_runs(
        project=project.project_identifier,
        runs=arg_runs,
    )

    assert set(result) == set()
