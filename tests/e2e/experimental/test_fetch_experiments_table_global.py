from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
import pytest

from neptune_query import filters
from neptune_query.exceptions import ConflictingAttributeTypes
from neptune_query.experimental import fetch_experiments_table_global
from tests.e2e.data_ingestion import (
    IngestedProjectData,
    IngestedRunData,
    ProjectData,
    RunData,
)

pytestmark = pytest.mark.filterwarnings("ignore:.*fetch_.*_table.*:neptune_query.warnings.ExperimentalWarning")

CONFLICTING_ATTRIBUTE_PATH = "conflict/shared"


@dataclass(frozen=True)
class Column:
    name: str
    type: Literal["config", "float_series"]


@pytest.fixture(scope="session")
def unique_execution_module_key(test_execution_id) -> str:
    """
    This fixture provides a key that is unique for this module in the current test execution.
    It can be used to create unique project names, experiment names, etc.
    """
    return f"{test_execution_id}_{__name__}"


def test_fetch_experiments_table_returns_all_experiments(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_experiments_table_global(
        experiments=f"{unique_execution_module_key}",
        attributes=["config/int", "config/string", "metrics/loss"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_experiments = _experiment_heads_sorted_by_name([project_1, project_2])
    expected_dataframe = _expected_dataframe(
        experiments=expected_experiments,
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
            Column(name="metrics/loss", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test_fetch_experiments_table_respects_sort_direction(
    project_1, project_2, unique_execution_module_key, sort_direction: Literal["asc", "desc"]
):
    dataframe = fetch_experiments_table_global(
        experiments=f"{unique_execution_module_key}",
        attributes=["config/int"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
    )

    expected_experiments = sorted(
        _experiment_heads_sorted_by_name([project_1, project_2]),
        key=lambda e: e.experiment_name,
        reverse=(sort_direction == "desc"),
    )
    expected_dataframe = _expected_dataframe(
        experiments=expected_experiments,
        columns=[Column(name="config/int", type="config")],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_filters_by_regex(project_1, unique_execution_module_key):
    dataframe = fetch_experiments_table_global(
        experiments=rf"^exp_project_1 & {unique_execution_module_key}",
        attributes=["config/int", "metrics/loss"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        experiments=_experiment_heads_sorted_by_name([project_1]),
        columns=[
            Column(name="config/int", type="config"),
            Column(name="metrics/loss", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_filters_by_name_list(project_1, project_2, unique_execution_module_key):
    selected_experiments = [
        _experiment_head_by_name(project_1, "exp_project_1_alt"),
        _experiment_head_by_name(project_2, "exp_project_2_alt"),
    ]

    dataframe = fetch_experiments_table_global(
        experiments=[e.experiment_name for e in selected_experiments],
        attributes=["config/int", "metrics/loss"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        experiments=sorted(selected_experiments, key=lambda e: e.experiment_name),
        columns=[
            Column(name="config/int", type="config"),
            Column(name="metrics/loss", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_filters_by_attribute_filter(project_2):
    target_experiment = _experiment_head_by_name(project_2, "exp_project_2")

    dataframe = fetch_experiments_table_global(
        experiments=filters.Filter.eq(filters.Attribute("sys/name", type="string"), target_experiment.experiment_name),
        attributes=["config/int", "metrics/loss"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        experiments=[target_experiment],
        columns=[
            Column(name="config/int", type="config"),
            Column(name="metrics/loss", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_applies_limit(project_1, project_2, unique_execution_module_key):
    limit = 2
    dataframe = fetch_experiments_table_global(
        experiments=f"{unique_execution_module_key}",
        attributes=["config/int", "config/string", "metrics/loss"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        limit=limit,
    )

    expected_experiments = _experiment_heads_sorted_by_name([project_1, project_2])[:limit]
    expected_dataframe = _expected_dataframe(
        experiments=expected_experiments,
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
            Column(name="metrics/loss", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_with_type_suffix(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_experiments_table_global(
        experiments=f"{unique_execution_module_key}",
        attributes=r"(config/(int|string)|metrics/loss)",
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        type_suffix_in_column_names=True,
    )

    expected_experiments = _experiment_heads_sorted_by_name([project_1, project_2])
    expected_dataframe = _expected_dataframe(
        experiments=expected_experiments,
        columns=[
            Column(name="config/int:int", type="config"),
            Column(name="config/string:string", type="config"),
            Column(name="metrics/loss:float_series", type="float_series"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_conflicting_attributes_with_type_suffix(project_1, project_2):
    experiments_with_conflict = [
        _experiment_head_by_name(project_1, "exp_project_1"),
        _experiment_head_by_name(project_2, "exp_project_2"),
    ]

    dataframe = fetch_experiments_table_global(
        experiments=[e.experiment_name for e in experiments_with_conflict],
        attributes=[CONFLICTING_ATTRIBUTE_PATH],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        type_suffix_in_column_names=True,
    )

    expected_dataframe = _expected_dataframe(
        experiments=sorted(experiments_with_conflict, key=lambda e: e.experiment_name),
        columns=[
            Column(name=f"{CONFLICTING_ATTRIBUTE_PATH}:float_series", type="float_series"),
            Column(name=f"{CONFLICTING_ATTRIBUTE_PATH}:string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_experiments_table_conflicting_attributes_without_type_suffix(project_1, project_2):
    experiments_with_conflict = [
        _experiment_head_by_name(project_1, "exp_project_1"),
        _experiment_head_by_name(project_2, "exp_project_2"),
    ]

    with pytest.raises(ConflictingAttributeTypes):
        fetch_experiments_table_global(
            experiments=[e.experiment_name for e in experiments_with_conflict],
            attributes=[CONFLICTING_ATTRIBUTE_PATH],
            sort_by=filters.Attribute("sys/name", type="string"),
            sort_direction="asc",
        )


def test_fetch_experiments_table_with_empty_attributes(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_experiments_table_global(
        experiments=f"{unique_execution_module_key}",
        attributes=[],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_experiments = _experiment_heads_sorted_by_name([project_1, project_2])
    expected_dataframe = _expected_dataframe(experiments=expected_experiments, columns=[])

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


@pytest.fixture(scope="session")
def project_1(ensure_project, unique_execution_module_key) -> IngestedProjectData:
    exp_project_1_root = RunData(
        experiment_name_base="exp_project_1",
        run_id_base="run_project_1_root",
        fork_point=None,
        configs={
            "config/int": 1,
            "config/string": "project-1-root",
        },
        float_series={
            "metrics/loss": {0: 0.5, 1: 0.25, 2: 0.125},
            "metrics/accuracy": {0: 0.1, 1: 0.2, 2: 0.3},
        },
    )
    exp_project_1_branch_a = RunData(
        experiment_name_base="exp_project_1",
        run_id_base="run_project_1_branch_a",
        fork_point=("run_project_1_root", 1.0),
        configs={
            "config/int": 3,
            "config/string": "project-1-branch-a",
        },
        float_series={
            "metrics/loss": {1: 0.24, 2: 0.12, 3: 0.06},
            "metrics/accuracy": {1: 0.25, 2: 0.35, 3: 0.45},
        },
    )
    exp_project_1_branch_b = RunData(
        experiment_name_base="exp_project_1",
        run_id_base="run_project_1_branch_b",
        fork_point=("run_project_1_branch_a", 2.0),
        configs={
            "config/int": 5,
            "config/string": "project-1-branch-b",
            CONFLICTING_ATTRIBUTE_PATH: "config-exp-branch-b",
        },
        float_series={
            "metrics/loss": {2: 0.18, 3: 0.09, 4: 0.045},
            "metrics/accuracy": {2: 0.4, 3: 0.5, 4: 0.6},
        },
    )
    exp_project_1_alt_root = RunData(
        experiment_name_base="exp_project_1_alt",
        run_id_base="run_project_1_alt_root",
        fork_point=None,
        configs={
            "config/int": 7,
            "config/string": "project-1-alt-root",
        },
        float_series={
            "metrics/loss": {0: 0.65, 1: 0.33},
            "metrics/accuracy": {0: 0.2, 1: 0.35},
        },
    )
    exp_project_1_alt_branch = RunData(
        experiment_name_base="exp_project_1_alt",
        run_id_base="run_project_1_alt_branch",
        fork_point=("run_project_1_alt_root", 1.0),
        configs={
            "config/int": 9,
            "config/string": "project-1-alt-branch",
        },
        float_series={
            "metrics/loss": {1: 0.32, 2: 0.16},
            "metrics/accuracy": {1: 0.4, 2: 0.55},
        },
    )

    return ensure_project(
        unique_key=unique_execution_module_key,
        project_data=ProjectData(
            project_name_base="global_fetch_experiments_table_project_1",
            runs=[
                exp_project_1_root,
                exp_project_1_branch_a,
                exp_project_1_branch_b,
                exp_project_1_alt_root,
                exp_project_1_alt_branch,
            ],
        ),
    )


@pytest.fixture(scope="session")
def project_2(ensure_project, unique_execution_module_key) -> IngestedProjectData:
    exp_project_2_root = RunData(
        experiment_name_base="exp_project_2",
        run_id_base="run_project_2_root",
        fork_point=None,
        configs={
            "config/int": 2,
            "config/string": "project-2-root",
        },
        float_series={
            "metrics/loss": {0: 1.0, 1: 0.8, 2: 0.6},
            "metrics/accuracy": {0: 0.4, 1: 0.5, 2: 0.6},
        },
    )
    exp_project_2_branch = RunData(
        experiment_name_base="exp_project_2",
        run_id_base="run_project_2_branch",
        fork_point=("run_project_2_root", 1.0),
        configs={
            "config/int": 4,
            "config/string": "project-2-branch",
        },
        float_series={
            "metrics/loss": {1: 0.75, 2: 0.55, 3: 0.45},
            "metrics/accuracy": {1: 0.52, 2: 0.62, 3: 0.72},
        },
    )
    exp_project_2_branch_deep = RunData(
        experiment_name_base="exp_project_2",
        run_id_base="run_project_2_branch_deep",
        fork_point=("run_project_2_branch", 2.0),
        configs={
            "config/int": 6,
            "config/string": "project-2-branch-deep",
        },
        float_series={
            "metrics/loss": {2: 0.58, 3: 0.4},
            "metrics/accuracy": {2: 0.65, 3: 0.75},
            CONFLICTING_ATTRIBUTE_PATH: {2: 1.1, 3: 1.2},
        },
    )
    exp_project_2_alt_root = RunData(
        experiment_name_base="exp_project_2_alt",
        run_id_base="run_project_2_alt_root",
        fork_point=None,
        configs={
            "config/int": 8,
            "config/string": "project-2-alt-root",
        },
        float_series={
            "metrics/loss": {0: 1.1, 1: 0.95},
            "metrics/accuracy": {0: 0.45, 1: 0.55},
        },
    )
    exp_project_2_aardvark = RunData(
        experiment_name_base="exp_aardvark",  # Named to sort first alphabetically across projects
        run_id_base="run_project_2_aardvark",
        fork_point=None,
        configs={
            "config/int": 0,
            "config/string": "project-2-aardvark",
        },
        float_series={
            "metrics/loss": {0: 0.9, 1: 0.7},
            "metrics/accuracy": {0: 0.5, 1: 0.6},
        },
    )

    return ensure_project(
        unique_key=unique_execution_module_key,
        project_data=ProjectData(
            project_name_base="global_fetch_experiments_table_project_2",
            runs=[
                exp_project_2_root,
                exp_project_2_branch,
                exp_project_2_branch_deep,
                exp_project_2_alt_root,
                exp_project_2_aardvark,
            ],
        ),
    )


def _experiment_heads_sorted_by_name(
    projects: Sequence[IngestedProjectData],
) -> list[IngestedRunData]:
    experiment_heads: list[IngestedRunData] = []
    for project in projects:
        experiment_heads_in_project: dict[str, IngestedRunData] = {}
        for run in project.ingested_runs:
            experiment_heads_in_project[run.experiment_name] = run
        experiment_heads.extend(experiment_heads_in_project.values())

    return sorted(experiment_heads, key=lambda head: head.experiment_name)


def _experiment_base_name(experiment_name: str) -> str:
    return experiment_name.rsplit("__", 1)[0]


def _experiment_head_by_name(project: IngestedProjectData, experiment_base_name: str) -> IngestedRunData:
    head: IngestedRunData | None = None
    for run in project.ingested_runs:
        if _experiment_base_name(run.experiment_name) == experiment_base_name:
            head = run
    if head is None:
        raise ValueError(
            f"Experiment head '{experiment_base_name}' not found in project '{project.project_identifier}'."
        )
    return head


def _expected_dataframe(
    *,
    experiments: Sequence[IngestedRunData],
    columns: Sequence[Column],
) -> pd.DataFrame:
    if columns:
        ordered_columns = sorted(columns, key=lambda item: item.name)
        data = {
            column.name: [_extract_column_value(head, column) for head in experiments] for column in ordered_columns
        }
        columns_index = pd.Index([column.name for column in ordered_columns], name="attribute")
    else:
        data = {}
        columns_index = pd.Index([], name="attribute")

    index = pd.MultiIndex.from_tuples(
        [(run.project_identifier, run.experiment_name) for run in experiments],
        names=["project", "experiment"],
    )

    return pd.DataFrame(data=data, index=index, columns=columns_index)


def _extract_column_value(run: IngestedRunData, column: Column) -> str | int | float:
    attribute_path = column.name.split(":", 1)[0]  # Remove type suffix if present
    if column.type == "config":
        return run.configs.get(attribute_path, np.nan)
    if column.type == "float_series":
        if (series := run.float_series.get(attribute_path, None)) is not None:
            return series[max(series)]  # Return the last value in the float series
        return np.nan
    raise ValueError(f"Unsupported column type '{column.type}'.")
