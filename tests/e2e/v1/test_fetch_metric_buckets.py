from typing import (
    Iterable,
    Literal,
    Optional,
    Union,
)

import pandas as pd
import pytest

from neptune_query import fetch_metric_buckets
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
from neptune_query.internal.output_format import create_metric_buckets_dataframe
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket
from tests.e2e.data import (
    NUMBER_OF_STEPS,
    PATH,
    TEST_DATA,
    ExperimentData,
)
from tests.e2e.metric_buckets import (
    aggregate_metric_buckets,
    calculate_global_range,
    calculate_metric_bucket_ranges,
)
from tests.e2e.v1.generator import (
    EXP_NAME_INF_NAN_RUN,
    RUN_BY_ID,
    RUN_ID_INF_NAN_RUN,
)

EXPERIMENT = TEST_DATA.experiments[0]


def _to_run_attribute_definition(project, run, path):
    return RunAttributeDefinition(
        RunIdentifier(project, SysId(run)),
        AttributeDefinition(path, "float_series"),
    )


def _sys_id_label_mapping(experiments: Iterable[str]) -> dict[SysId, str]:
    return {SysId(name): name for name in experiments}  # we just use the id as the label in tests


def create_expected_data_experiments(
    project_identifier: ProjectIdentifier,
    experiments: list[ExperimentData],
    x: Union[Literal["step"]],  # TODO - only option
    limit: int,
    include_point_previews: bool,  # TODO - add to the test data?
) -> pd.DataFrame:
    data = {
        exp.name: {
            path: list(zip(exp.float_series[f"{PATH}/metrics/step"], ys)) for path, ys in exp.float_series.items()
        }
        for exp in experiments
    }
    return _create_expected_data_metric_buckets_dataframe(
        data,
        project_identifier,
        x,
        limit,
        include_point_previews,
    )


def _calculate_expected_data_global_range(
    data: dict[str, dict[str, list[tuple[float, float]]]],
) -> tuple[float, float]:
    global_range: Optional[tuple[float, float]] = None
    for experiment_data in data.values():
        for series_data in experiment_data.values():
            series_range = calculate_global_range(series_data, x_range=None)
            if global_range is None:
                global_range = series_range
            else:
                global_range = (
                    min(global_range[0], series_range[0]),
                    max(global_range[1], series_range[1]),
                )
    assert global_range is not None
    return global_range


def _create_expected_data_metric_buckets_dataframe(
    data: dict[str, dict[str, list[tuple[float, float]]]],
    project_identifier: ProjectIdentifier,
    x: Union[Literal["step"]],  # TODO - only option
    limit: int,
    include_point_previews: bool,  # TODO - add to the test data?
) -> pd.DataFrame:
    global_from, global_to = _calculate_expected_data_global_range(data)
    bucket_ranges = calculate_metric_bucket_ranges(global_from, global_to, limit=limit + 1)

    bucket_data: dict[RunAttributeDefinition, list[TimeseriesBucket]] = {}
    for experiment_name, experiment_data in data.items():
        for path, series in experiment_data.items():
            buckets = aggregate_metric_buckets(series, bucket_ranges)
            attribute_run = _to_run_attribute_definition(project_identifier, experiment_name, path)
            bucket_data.setdefault(attribute_run, []).extend(buckets)

    return create_metric_buckets_dataframe(
        buckets_data=bucket_data,
        sys_id_label_mapping=_sys_id_label_mapping(data.keys()),
        container_column_name="experiment",
    )


@pytest.mark.parametrize(
    "y",
    [
        AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ".*/metrics/.*",
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
    "x,limit,include_point_previews",
    [
        (
            "step",
            2,
            True,
        ),
        (
            "step",
            10,
            False,
        ),
    ],
)
def test__fetch_metric_buckets__experiment_attribute_filter_variants(
    project,
    arg_experiments,
    x,
    y,
    limit,
    include_point_previews,
):
    experiments = TEST_DATA.experiments[:3]

    result_df = fetch_metric_buckets(
        project=project.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_df = create_expected_data_experiments(
        project_identifier=project.project_identifier,
        experiments=experiments,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "limit",
    [1, 2, 3, 10, NUMBER_OF_STEPS + 10, 1000],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(  # Not sure where to test variants of preview. They are ignored for now anyway...
    "include_point_previews",
    [True, False],
)
@pytest.mark.parametrize(
    "arg_experiments,y",
    [
        (
            Filter.name([exp.name for exp in TEST_DATA.experiments[:3]]),
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ),
        (
            f"{TEST_DATA.exp_name(0)} | {TEST_DATA.exp_name(1)} | {TEST_DATA.exp_name(2)}",
            ".*/metrics/.*",
        ),
        (
            [exp.name for exp in TEST_DATA.experiments[:3]],
            AttributeFilter(name=r".*/metrics/.*", type=["float_series"])
            | AttributeFilter(name=r".*/metrics/.*", type=["float_series"]),
        ),
    ],
)
def test__fetch_metric_buckets__bucketing_x_limit_variants(
    project,
    arg_experiments,
    x,
    y,
    limit,
    include_point_previews,
):
    experiments = TEST_DATA.experiments[:3]

    result_df = fetch_metric_buckets(
        project=project.project_identifier,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_df = create_expected_data_experiments(
        project_identifier=project.project_identifier,
        experiments=experiments,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "arg_experiments",
    [TEST_DATA.experiments[:1], TEST_DATA.experiments[:3]],
)
@pytest.mark.parametrize(
    "y",
    [[path] for path in EXPERIMENT.unique_length_float_series.keys()]
    + [list(EXPERIMENT.unique_length_float_series.keys())],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(
    "limit",
    [2, 3, 10],
)
@pytest.mark.parametrize(
    "include_point_previews",
    [True],
)
def test__fetch_metric_buckets__handles_misaligned_steps_in_metrics(
    arg_experiments,
    y,
    project,
    x,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=project.project_identifier,
        experiments=[exp.name for exp in arg_experiments],
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_data = {
        experiment.name: {path: experiment.unique_length_float_series[path] for path in y}
        for experiment in arg_experiments
    }
    expected_df = _create_expected_data_metric_buckets_dataframe(
        data=expected_data,
        project_identifier=project.project_identifier,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize(
    "arg_experiments,run_id,y",
    [
        (EXP_NAME_INF_NAN_RUN, RUN_ID_INF_NAN_RUN, "series-containing-inf"),
        (EXP_NAME_INF_NAN_RUN, RUN_ID_INF_NAN_RUN, "series-containing-nan"),
        (EXP_NAME_INF_NAN_RUN, RUN_ID_INF_NAN_RUN, "series-ending-with-inf"),
        (EXP_NAME_INF_NAN_RUN, RUN_ID_INF_NAN_RUN, "series-ending-with-nan"),
    ],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
)
@pytest.mark.parametrize(
    "limit",
    [2, 3, 10, NUMBER_OF_STEPS + 10],
)
@pytest.mark.parametrize(
    "include_point_previews",
    [True],
)
@pytest.mark.skip(reason="Skipped until inf/nan handling is enabled in the backend")
def test__fetch_metric_buckets__inf_nan(
    new_project_id,
    arg_experiments,
    run_id,
    x,
    y,
    limit,
    include_point_previews,
):
    result_df = fetch_metric_buckets(
        project=new_project_id,
        experiments=arg_experiments,
        x=x,
        y=y,
        limit=limit,
        include_point_previews=include_point_previews,
        lineage_to_the_root=True,
    )

    expected_data = {arg_experiments: {y: RUN_BY_ID[run_id].metrics_values(y)}}
    expected_df = _create_expected_data_metric_buckets_dataframe(
        data=expected_data,
        project_identifier=new_project_id,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)
