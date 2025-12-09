from datetime import (
    datetime,
    timezone,
)

import pandas as pd
import pytest

import neptune_query.runs as runs
from neptune_query.filters import (
    Attribute,
    AttributeFilter,
    Filter,
)
from tests.e2e.conftest import EnsureProjectFunction
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    ProjectData,
    RunData,
)


# TODO: remove once all e2e tests use the ensure_project framework
@pytest.fixture(scope="module", autouse=True)
def run_with_attributes_autouse():
    # Override autouse ingestion from shared v1 fixtures; this module ingests its own data.
    return None


@pytest.fixture(scope="module")
def project(ensure_project: EnsureProjectFunction) -> IngestedProjectData:
    # root (level: None)
    #   └── fork1 (level: 1, fork_point: 4)
    #         └── fork2 (level: 2, fork_point: 8)
    linear_history_tree = [
        RunData(
            experiment_name="exp_with_linear_history",
            run_id="linear_history_root",
            float_series={
                "foo0": {float(step): float(step * 0.1) for step in range(10)},
                "foo1": {float(step): float(step * 0.2) for step in range(10)},
            },
            configs={
                "int-value": 1,
                "float-value": 1.0,
                "str-value": "hello_1",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
            },
        ),
        RunData(
            experiment_name="exp_with_linear_history",
            run_id="linear_history_fork1",
            fork_point=("linear_history_root", 4.0),
            configs={
                "int-value": 2,
                "float-value": 2.0,
                "str-value": "hello_2",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
            },
        ),
        RunData(
            experiment_name="exp_with_linear_history",
            run_id="linear_history_fork2",
            fork_point=("linear_history_fork1", 8.0),
            float_series={
                "foo0": {float(step): float(step * 0.7) for step in range(9, 20)},
            },
            configs={
                "int-value": 3,
                "float-value": 3.0,
                "str-value": "hello_3",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
            },
        ),
    ]

    # forked_history_tree:
    # root (level: None)
    #   ├── fork1 (level: 1, fork_point: 4)
    #   └── fork2 (level: 1, fork_point: 8)
    forked_history_tree = [
        RunData(
            experiment_name="epx_with_forked_history",
            run_id="forked_history_root",
            configs={
                "int-value": 1,
                "float-value": 1.0,
                "str-value": "hello_1",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
            },
        ),
        RunData(
            experiment_name="epx_with_forked_history",
            run_id="forked_history_fork1",
            fork_point=("forked_history_root", 4.0),
            configs={
                "int-value": 2,
                "float-value": 2.0,
                "str-value": "hello_2",
                "bool-value": True,
                "datetime-value": datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
            },
        ),
        RunData(
            experiment_name="epx_with_forked_history",
            run_id="forked_history_fork2",
            fork_point=("forked_history_root", 8.0),
            configs={
                "int-value": 3,
                "float-value": 3.0,
                "str-value": "hello_3",
                "bool-value": False,
                "datetime-value": datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
            },
        ),
    ]

    return ensure_project(ProjectData(runs=linear_history_tree + forked_history_tree))


@pytest.mark.parametrize(
    "runs_filter, attributes_filter, expected_attributes",
    [
        (
            r"^linear_history_root$",
            r".*-value$",
            {
                "run": ["linear_history_root"],
                "int-value:int": [1],
                "float-value:float": [1.0],
                "str-value:string": ["hello_1"],
                "bool-value:bool": [False],
                "datetime-value:datetime": [datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc)],
            },
        ),
        (
            r"^linear_history_root$",
            [],
            {
                "run": ["linear_history_root"],
            },
        ),
        (
            "^non_exist$",
            "^foo0$",
            {
                "run": [],
            },
        ),
        (
            r"^linear_history_root$",
            r"^foo.*$",
            {
                "run": ["linear_history_root"],
                "foo0:float_series": [0.1 * 9],
                "foo1:float_series": [0.2 * 9],
            },
        ),
        (
            r"^linear_history_root$",
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root"],
                "foo0:float_series": [0.1 * 9],
            },
        ),
        (
            "^linear_history_root$",
            AttributeFilter(name="foo0$") | AttributeFilter(name=".*-value$"),
            {
                "run": ["linear_history_root"],
                "int-value:int": [1],
                "float-value:float": [1.0],
                "str-value:string": ["hello_1"],
                "bool-value:bool": [False],
                "datetime-value:datetime": [datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc)],
                "foo0:float_series": [0.1 * 9],
            },
        ),
        (
            r"^linear_history_root$|^linear_history_fork2$",
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root", "linear_history_fork2"],
                "foo0:float_series": [0.1 * 9, 0.7 * 19],
            },
        ),
        (
            ["linear_history_root", "linear_history_fork2"],
            AttributeFilter(name=r"foo0$"),
            {
                "run": ["linear_history_root", "linear_history_fork2"],
                "foo0:float_series": [0.1 * 9, 0.7 * 19],
            },
        ),
        (
            r"forked_history_root|forked_history_fork1",
            r".*-value$",
            {
                "run": ["forked_history_root", "forked_history_fork1"],
                "int-value:int": [1, 2],
                "float-value:float": [1.0, 2.0],
                "str-value:string": ["hello_1", "hello_2"],
                "bool-value:bool": [False, True],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.matches("sys/custom_run_id", r"forked_history_root|forked_history_fork1"),
            r".*-value$",
            {
                "run": ["forked_history_root", "forked_history_fork1"],
                "int-value:int": [1, 2],
                "float-value:float": [1.0, 2.0],
                "str-value:string": ["hello_1", "hello_2"],
                "bool-value:bool": [False, True],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.eq("sys/name", "exp_with_linear_history"),
            # matches runs with experiment_name 'exp_with_linear_history'
            r".*-value$",
            {
                "run": ["linear_history_fork1", "linear_history_fork2", "linear_history_root"],
                "int-value:int": [2, 3, 1],
                "float-value:float": [2.0, 3.0, 1.0],
                "str-value:string": ["hello_2", "hello_3", "hello_1"],
                "bool-value:bool": [True, False, False],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                ],
            },
        ),
        (
            Filter.exists(Attribute("str-value", type="string")),  # matches runs that have config 'str-value'
            r".*-value$",
            {
                "run": [
                    "forked_history_fork1",
                    "forked_history_fork2",
                    "forked_history_root",
                    "linear_history_fork1",
                    "linear_history_fork2",
                    "linear_history_root",
                ],
                "int-value:int": [2, 3, 1, 2, 3, 1],
                "float-value:float": [2.0, 3.0, 1.0, 2.0, 3.0, 1.0],
                "str-value:string": ["hello_2", "hello_3", "hello_1", "hello_2", "hello_3", "hello_1"],
                "bool-value:bool": [True, False, False, True, False, False],
                "datetime-value:datetime": [
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 2, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 3, 0, 0, 0, timezone.utc),
                    datetime(2025, 1, 1, 1, 0, 0, 0, timezone.utc),
                ],
            },
        ),
    ],
)
@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
def test_fetch_runs_table(
    project: IngestedProjectData,
    runs_filter,
    attributes_filter,
    expected_attributes,
    type_suffix_in_column_names: bool,
):
    df = runs.fetch_runs_table(
        project=project.project_identifier,
        runs=runs_filter,
        attributes=attributes_filter,
        sort_by=Attribute("sys/custom_run_id", type="string"),
        sort_direction="desc",
        type_suffix_in_column_names=type_suffix_in_column_names,
    )

    expected_data = {
        trim_suffix(k, type_suffix_in_column_names): v
        for k, v in sorted(expected_attributes.items(), key=lambda x: (x[0], "") if isinstance(x[0], str) else x[0])
    }
    expected = pd.DataFrame(expected_data).sort_values("run", ascending=False)
    expected["run"] = expected["run"].astype(object)
    expected.set_index("run", drop=True, inplace=True)
    expected.columns.name = "attribute"

    pd.testing.assert_frame_equal(df, expected)


def trim_suffix(name, type_suffix_in_column_names):
    if type_suffix_in_column_names:
        return name
    else:
        if isinstance(name, tuple):
            return name[0].split(":")[0], name[1]
        else:
            return name.split(":")[0]
