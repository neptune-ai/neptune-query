from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
import pytest

from neptune_query import filters
from neptune_query.exceptions import ConflictingAttributeTypes
from neptune_query.experimental import fetch_runs_table_global
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


def test_fetch_runs_table_returns_all_runs(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_runs_table_global(
        runs=f"{unique_execution_module_key}",
        attributes=["config/int", "config/string"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        runs=_all_runs_sorted_by_name([project_1, project_2]),
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test_fetch_runs_table_respects_sort_direction(
    project_1, project_2, unique_execution_module_key, sort_direction: Literal["asc", "desc"]
):
    dataframe = fetch_runs_table_global(
        runs=f"{unique_execution_module_key}",
        attributes=["config/int"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction=sort_direction,
    )

    expected_runs = sorted(
        _all_runs_sorted_by_name([project_1, project_2]),
        key=lambda run: run.experiment_name,
        reverse=(sort_direction == "desc"),
    )

    expected_dataframe = _expected_dataframe(
        runs=expected_runs,
        columns=[Column(name="config/int", type="config")],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_filters_by_regex(project_1, project_2, unique_execution_module_key):
    pattern = rf"run_project_1_.*__{unique_execution_module_key}"
    dataframe = fetch_runs_table_global(
        runs=pattern,
        attributes=["config/int", "config/string"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        runs=project_1.ingested_runs,
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_filters_by_name_list(project_1, project_2, unique_execution_module_key):
    selected_runs = [project_1.ingested_runs[1], project_2.ingested_runs[2]]
    dataframe = fetch_runs_table_global(
        runs=[run.run_id for run in selected_runs],
        attributes=["config/int", "config/string"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        runs=selected_runs,
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_filters_by_attribute_filter(project_1, project_2):
    target_run = project_2.ingested_runs[0]
    dataframe = fetch_runs_table_global(
        runs=filters.Filter.eq(filters.Attribute("sys/custom_run_id", type="string"), target_run.run_id),
        attributes=["config/int", "config/string"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
    )

    expected_dataframe = _expected_dataframe(
        runs=[target_run],
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_applies_limit(project_1, project_2, unique_execution_module_key):
    limit = 4
    dataframe = fetch_runs_table_global(
        runs=f"{unique_execution_module_key}",
        attributes=["config/int", "config/string"],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        limit=limit,
    )

    expected_dataframe = _expected_dataframe(
        runs=_all_runs_sorted_by_name([project_1, project_2])[:limit],
        columns=[
            Column(name="config/int", type="config"),
            Column(name="config/string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_with_type_suffix(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_runs_table_global(
        runs=f"{unique_execution_module_key}",
        attributes=r"config/(int|string)",
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        limit=None,
        type_suffix_in_column_names=True,
    )

    expected_dataframe = _expected_dataframe(
        runs=_all_runs_sorted_by_name([project_1, project_2]),
        columns=[
            Column(name="config/int:int", type="config"),
            Column(name="config/string:string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_conflicting_attributes_with_type_suffix(project_1, project_2):
    runs_with_conflict = [
        project_1.ingested_runs[0],
        project_2.ingested_runs[0],
    ]

    dataframe = fetch_runs_table_global(
        runs=[run.run_id for run in runs_with_conflict],
        attributes=[CONFLICTING_ATTRIBUTE_PATH],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        type_suffix_in_column_names=True,
    )

    expected_dataframe = _expected_dataframe(
        runs=sorted(runs_with_conflict, key=lambda run: run.experiment_name),
        columns=[
            Column(name=f"{CONFLICTING_ATTRIBUTE_PATH}:float_series", type="float_series"),
            Column(name=f"{CONFLICTING_ATTRIBUTE_PATH}:string", type="config"),
        ],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_fetch_runs_table_conflicting_attributes_without_type_suffix(project_1, project_2):
    runs_with_conflict = [
        project_1.ingested_runs[0],
        project_2.ingested_runs[0],
    ]

    with pytest.raises(ConflictingAttributeTypes):
        fetch_runs_table_global(
            runs=[run.run_id for run in runs_with_conflict],
            attributes=[CONFLICTING_ATTRIBUTE_PATH],
            sort_by=filters.Attribute("sys/name", type="string"),
            sort_direction="asc",
        )


def test_fetch_runs_table_with_empty_attributes(project_1, project_2, unique_execution_module_key):
    dataframe = fetch_runs_table_global(
        runs=f"{unique_execution_module_key}",
        attributes=[],
        sort_by=filters.Attribute("sys/name", type="string"),
        sort_direction="asc",
        limit=None,
    )

    expected_dataframe = _expected_dataframe(
        runs=_all_runs_sorted_by_name([project_1, project_2]),
        columns=[],
    )

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


@pytest.fixture(scope="session")
def project_1(ensure_project, unique_execution_module_key) -> IngestedProjectData:
    return ensure_project(
        unique_key=unique_execution_module_key,
        project_data=ProjectData(
            project_name_base="project_1",
            runs=[
                RunData(
                    experiment_name_base="exp_project_1_alpha",
                    run_id_base="run_project_1_alpha",
                    fork_point=None,
                    configs={
                        "config/int": 1,
                        "config/string": "project-1-alpha",
                        CONFLICTING_ATTRIBUTE_PATH: "config-alpha",
                    },
                    float_series={
                        "metrics/loss": {0: 0.5, 1: 0.25, 2: 0.125},
                        "metrics/accuracy": {0: 0.1, 1: 0.2, 2: 0.3},
                    },
                ),
                RunData(
                    experiment_name_base="exp_project_1_beta",
                    run_id_base="run_project_1_beta",
                    fork_point=None,
                    configs={
                        "config/int": 3,
                        "config/string": "project-1-beta",
                    },
                    float_series={
                        "metrics/loss": {0: 0.6, 1: 0.3, 2: 0.15},
                        "metrics/accuracy": {0: 0.15, 1: 0.25, 2: 0.35},
                    },
                ),
                RunData(
                    experiment_name_base="exp_project_1_gamma",
                    run_id_base="run_project_1_gamma",
                    fork_point=None,
                    configs={
                        "config/int": 5,
                        "config/string": "project-1-gamma",
                    },
                    float_series={
                        "metrics/loss": {0: 0.7, 1: 0.35, 2: 0.175},
                        "metrics/accuracy": {0: 0.2, 1: 0.3, 2: 0.4},
                    },
                ),
            ],
        ),
    )


@pytest.fixture(scope="session")
def project_2(ensure_project, unique_execution_module_key) -> IngestedProjectData:
    return ensure_project(
        unique_key=unique_execution_module_key,
        project_data=ProjectData(
            project_name_base="project_2",
            runs=[
                RunData(
                    experiment_name_base="exp_project_2_alpha",
                    run_id_base="run_project_2_alpha",
                    fork_point=None,
                    configs={
                        "config/int": 2,
                        "config/string": "project-2-alpha",
                    },
                    float_series={
                        "metrics/loss": {0: 1.0, 1: 0.8, 2: 0.6},
                        "metrics/accuracy": {0: 0.4, 1: 0.5, 2: 0.6},
                        CONFLICTING_ATTRIBUTE_PATH: {0: 0.1, 1: 0.2},
                    },
                ),
                RunData(
                    experiment_name_base="exp_project_2_beta",
                    run_id_base="run_project_2_beta",
                    fork_point=None,
                    configs={
                        "config/int": 4,
                        "config/string": "project-2-beta",
                    },
                    float_series={
                        "metrics/loss": {0: 1.1, 1: 0.9, 2: 0.7},
                        "metrics/accuracy": {0: 0.45, 1: 0.55, 2: 0.65},
                    },
                ),
                RunData(
                    experiment_name_base="exp_project_2_gamma",
                    run_id_base="run_project_2_gamma",
                    fork_point=None,
                    configs={
                        "config/int": 6,
                        "config/string": "project-2-gamma",
                    },
                    float_series={
                        "metrics/loss": {0: 1.2, 1: 1.0, 2: 0.8},
                        "metrics/accuracy": {0: 0.5, 1: 0.6, 2: 0.7},
                    },
                ),
                RunData(
                    experiment_name_base="exp_aardvark",  # Named to sort first alphabetically across projects
                    run_id_base="run_project_2_aardvark",
                    fork_point=None,
                    configs={
                        "config/int": 0,
                        "config/string": "project-2-aardvark",
                    },
                    float_series={
                        "metrics/loss": {0: 0.95, 1: 0.75},
                        "metrics/accuracy": {0: 0.55, 1: 0.65},
                    },
                ),
            ],
        ),
    )


def _all_runs_sorted_by_name(projects: Sequence[IngestedProjectData]) -> list[IngestedRunData]:
    return sorted([run for project in projects for run in project.ingested_runs], key=lambda run: run.experiment_name)


def _expected_dataframe(
    *,
    runs: Sequence[IngestedRunData],
    columns: Sequence[Column],
) -> pd.DataFrame:
    if columns:
        ordered_columns = sorted(columns, key=lambda item: item.name)
        data = {column.name: [_extract_column_value(run, column) for run in runs] for column in ordered_columns}
        columns_index = pd.Index([column.name for column in ordered_columns], name="attribute")
    else:
        data = {}
        columns_index = pd.Index([], name="attribute")

    return pd.DataFrame(
        data=data,
        index=pd.MultiIndex.from_tuples(
            [(run.project_identifier, run.run_id) for run in runs],
            names=["project", "run"],
        ),
        columns=columns_index,
    )


def _extract_column_value(run: IngestedRunData, column: Column) -> str | int | float:
    attribute_path = column.name.split(":", 1)[0]  # Remove type suffix if present
    if column.type == "config":
        return run.configs.get(attribute_path, np.nan)
    if column.type == "float_series":
        if (series := run.float_series.get(attribute_path, None)) is not None:
            return series[max(series)]  # Return the last value in the float series
        return np.nan
    raise ValueError(f"Unsupported column type '{column.type}'.")
