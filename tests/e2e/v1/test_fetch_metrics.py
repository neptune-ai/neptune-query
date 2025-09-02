from datetime import timedelta
from itertools import chain
from typing import (
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from neptune_query import fetch_metrics
from neptune_query.filters import (
    AttributeFilter,
    Filter,
)
from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.metrics import FloatPointValue
from tests.e2e.conftest import Project
from tests.e2e.data import (
    NOW,
    PATH,
    TEST_DATA,
    ExperimentData,
)
from tests.e2e.v1.generator import (
    EXP_NAME_INF_NAN_RUN,
    MULT_EXPERIMENT_HISTORY_EXP_2,
    RUN_BY_ID,
    RUN_ID_INF_NAN_RUN,
    timestamp_for_step,
)


def _to_run_attribute_definition(project, run, metric_name):
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier(project), SysId(run)),
        AttributeDefinition(metric_name, "float_series"),
    )


def _to_float_point_value(step, value):
    return int(timestamp_for_step(step).timestamp() * 1000), step, value, False, 1.0


def _sys_id_label_mapping(experiments: list[ExperimentData]) -> dict[SysId, str]:
    return {SysId(experiment.name): experiment.name for experiment in experiments}


def _run_attribute_definition(
    project: Project,
    experiment_name: str,
    float_series_path: str,
) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier(project.project_identifier), SysId(experiment_name)),
        AttributeDefinition(float_series_path, "float_series"),
    )


def _float_point_value(step, value) -> FloatPointValue:
    return (
        int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000,
        step,
        value,
        False,
        1.0,
    )


def _metrics_data(
    project: Project,
    experiment_name_to_metrics: dict[str, dict[str, list[float]]],
    steps: list[float],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> dict[RunAttributeDefinition, list[FloatPointValue]]:
    step_filter = (
        step_range[0] if step_range[0] is not None else -np.inf,
        step_range[1] if step_range[1] is not None else np.inf,
    )

    apply_tail_limit = lambda points: points[-tail_limit:] if tail_limit is not None else points  # noqa: E731

    return {
        _run_attribute_definition(project, experiment_name, path): apply_tail_limit(
            [_float_point_value(step, values[int(step)]) for step in steps if step_filter[0] <= step <= step_filter[1]]
        )
        for experiment_name, metrics in experiment_name_to_metrics.items()
        for path, values in metrics.items()
    }


def create_expected_data(
    project: Project,
    experiments: list[ExperimentData],
    type_suffix_in_column_names: bool,
    include_time: Union[Literal["absolute"], None],
    step_range: Tuple[Optional[int], Optional[int]],
    tail_limit: Optional[int],
) -> Tuple[pd.DataFrame, List[str], set[str]]:
    metrics_data: dict[RunAttributeDefinition, list[FloatPointValue]] = {}

    columns = set()
    filtered_experiments = set()

    step_filter = (
        step_range[0] if step_range[0] is not None else -np.inf,
        step_range[1] if step_range[1] is not None else np.inf,
    )
    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]

        for path, series in experiment.float_series.items():
            filtered = []
            for step in steps:
                if step_filter[0] <= step <= step_filter[1]:
                    columns.add(f"{path}:float_series" if type_suffix_in_column_names else path)
                    filtered_experiments.add(experiment.name)
                    filtered.append(
                        (
                            int((NOW + timedelta(seconds=int(step))).timestamp()) * 1000,
                            step,
                            series[int(step)],
                            False,
                            1.0,
                        )
                    )
            limited = filtered[-tail_limit:] if tail_limit is not None else filtered

            attribute_run = RunAttributeDefinition(
                RunIdentifier(ProjectIdentifier(project.project_identifier), SysId(experiment.name)),
                AttributeDefinition(path, "float_series"),
            )
            metrics_data.setdefault(attribute_run, []).extend(limited)

    df = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=_sys_id_label_mapping(experiments),
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=False,
        timestamp_column_name="absolute_time" if include_time == "absolute" else None,
        index_column_name="experiment",
    )

    sorted_columns = list(sorted(columns))
    if include_time == "absolute":
        absolute_columns = [[(c, "absolute_time"), (c, "value")] for c in sorted_columns]
        return df, list(chain.from_iterable(absolute_columns)), filtered_experiments
    else:
        return df, sorted_columns, filtered_experiments


@pytest.mark.parametrize(
    "arg_attributes",
    [
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ".*/metrics/.*",
        # Alternative should work too, see bug PY-137
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
        | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
    ],
)
@pytest.mark.parametrize(
    "arg_experiments",
    [
        Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
        f"{TEST_DATA.exp_name(0)}|{TEST_DATA.exp_name(1)}|{TEST_DATA.exp_name(2)}",
        f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",  # ERS
        [exp.name for exp in TEST_DATA.experiments[:3]],
    ],
)
@pytest.mark.parametrize(
    "step_range,tail_limit,page_point_limit,type_suffix_in_column_names,include_time",
    [
        (
            (0, 5),
            None,
            50,
            True,
            None,
        ),
        (
            (0, None),
            3,
            1_000_000,
            False,
            "absolute",
        ),
        (
            (None, 5),
            5,
            50,
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__filter_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize("step_range", [(0, 5), (0, None), (None, 5), (None, None), (100, 200)])
@pytest.mark.parametrize("tail_limit", [None, 3, 5])
@pytest.mark.parametrize("page_point_limit", [50, 1_000_000])
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,type_suffix_in_column_names,include_time",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            None,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",
            ".*/metrics/.*",
            False,
            "absolute",
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            True,
            "absolute",
        ),
    ],
)
def test__fetch_metrics_unique__step_variants(
    project,
    arg_experiments,
    arg_attributes,
    step_range,
    tail_limit,
    page_point_limit,
    type_suffix_in_column_names,
    include_time,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize("type_suffix_in_column_names", [True, False])
@pytest.mark.parametrize("include_time", [None, "absolute"])  # "relative",
@pytest.mark.parametrize(
    "arg_experiments,arg_attributes,step_range,tail_limit,page_point_limit",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (0, 5),
            None,
            50,
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",
            ".*/metrics/.*",
            (0, None),
            3,
            1_000_000,
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
            (None, 5),
            5,
            50,
        ),
    ],
)
def test__fetch_metrics_unique__output_format_variants(
    project,
    arg_experiments,
    arg_attributes,
    type_suffix_in_column_names,
    include_time,
    step_range,
    tail_limit,
    page_point_limit,
):
    experiments = TEST_DATA.experiments[:3]

    with patch("neptune_query.internal.retrieval.metrics.TOTAL_POINT_LIMIT", page_point_limit):
        result = fetch_metrics(
            experiments=arg_experiments,
            attributes=arg_attributes,
            type_suffix_in_column_names=type_suffix_in_column_names,
            step_range=step_range,
            tail_limit=tail_limit,
            include_time=include_time,
            project=project.project_identifier,
        )

    expected, columns, filtered_experiments = create_expected_data(
        project, experiments, type_suffix_in_column_names, include_time, step_range, tail_limit
    )

    pd.testing.assert_frame_equal(result, expected)
    assert result.columns.tolist() == columns
    assert result.index.names == ["experiment", "step"]
    assert {t[0] for t in result.index.tolist()} == filtered_experiments


@pytest.mark.parametrize(
    "lineage_to_the_root,expected_values",
    [
        (
            True,
            [(step, step * 0.1) for step in range(0, 5)]
            + [(step, step * 0.2) for step in range(5, 9)]
            + [(step, step * 0.3) for step in range(9, 12)],
        ),
        (False, [(step, step * 0.2) for step in range(5, 9)] + [(step, step * 0.3) for step in range(9, 12)]),
    ],
)
def test__fetch_metrics__lineage(new_project_id, lineage_to_the_root, expected_values):
    df = fetch_metrics(
        project=new_project_id,
        experiments=[MULT_EXPERIMENT_HISTORY_EXP_2],
        attributes=r"metrics/m1",
        lineage_to_the_root=lineage_to_the_root,
    )

    expected = create_metrics_dataframe(
        metrics_data={
            _to_run_attribute_definition(new_project_id, MULT_EXPERIMENT_HISTORY_EXP_2, "metrics/m1"): [
                _to_float_point_value(step, value) for step, value in expected_values
            ]
        },
        sys_id_label_mapping={SysId(MULT_EXPERIMENT_HISTORY_EXP_2): MULT_EXPERIMENT_HISTORY_EXP_2},
        type_suffix_in_column_names=False,
        include_point_previews=False,
        timestamp_column_name=None,
        index_column_name="experiment",
    )

    pd.testing.assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "series_name,expected_values",
    [
        ("series-containing-inf", RUN_BY_ID[RUN_ID_INF_NAN_RUN].metrics_values("series-containing-inf")),
        ("series-containing-nan", RUN_BY_ID[RUN_ID_INF_NAN_RUN].metrics_values("series-containing-nan")),
    ],
)
@pytest.mark.skip(reason="Skipped until inf/nan handling is enabled in the backend")
def test__fetch_metrics_nan_inf(new_project_id, series_name, expected_values):
    df = fetch_metrics(
        project=new_project_id,
        experiments=[EXP_NAME_INF_NAN_RUN],
        attributes=[series_name],
    )

    expected = create_metrics_dataframe(
        metrics_data={
            _to_run_attribute_definition(new_project_id, EXP_NAME_INF_NAN_RUN, series_name): [
                _to_float_point_value(step, value) for step, value in expected_values
            ]
        },
        sys_id_label_mapping={SysId(EXP_NAME_INF_NAN_RUN): EXP_NAME_INF_NAN_RUN},
        type_suffix_in_column_names=False,
        include_point_previews=False,
        timestamp_column_name=None,
        index_column_name="experiment",
    )

    pd.testing.assert_frame_equal(df, expected)
