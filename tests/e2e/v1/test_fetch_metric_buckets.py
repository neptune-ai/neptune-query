from typing import (
    Iterable,
    Literal,
    Optional,
    Union,
)

import numpy as np
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
    return create_expected_data_dict(
        data,
        project_identifier,
        x,
        limit,
        include_point_previews,
    )


def _calculate_ranges_x(
    data: dict[str, dict[str, list[tuple[float, float]]]],
    limit: int,
) -> list[tuple[float, float]]:
    global_range_x: Optional[tuple[float, float]] = None
    for experiment_data in data.values():
        for series in experiment_data.values():
            xs = [x for x, _ in series]
            if global_range_x is None:
                global_range_x = min(xs), max(xs)
            else:
                global_min_x, global_max_x = global_range_x
                global_range_x = min(global_min_x, min(xs)), max(global_max_x, max(xs))
    assert global_range_x is not None
    global_min_x, global_max_x = global_range_x

    if global_min_x == global_max_x:
        return [(global_min_x, float("inf"))]  # TODO: bug on the backend side...

    bucket_ranges_x = []
    bucket_width = (global_max_x - global_min_x) / (limit - 1)
    for bucket_i in range(limit):
        if bucket_i == 0:
            from_x = float("-inf")
        else:
            from_x = global_min_x + bucket_width * (bucket_i - 1)

        if bucket_i == limit:
            to_x = float("inf")
        else:
            to_x = global_min_x + bucket_width * bucket_i
        bucket_ranges_x.append((from_x, to_x))

    return bucket_ranges_x


def create_expected_data_dict(
    data: dict[str, dict[str, list[tuple[float, float]]]],
    project_identifier: ProjectIdentifier,
    x: Union[Literal["step"]],  # TODO - only option
    limit: int,
    include_point_previews: bool,  # TODO - add to the test data?
) -> pd.DataFrame:
    bucket_ranges_x = _calculate_ranges_x(data, limit)

    bucket_data: dict[RunAttributeDefinition, list[TimeseriesBucket]] = {}
    for experiment_name, experiment_data in data.items():
        for path, series in experiment_data.items():
            buckets = []
            for bucket_i, bucket_range_x in enumerate(bucket_ranges_x):
                from_x, to_x = bucket_range_x

                positive_inf_count = 0
                negative_inf_count = 0
                nan_count = 0
                xs = []
                ys = []
                for x, y in series:
                    if from_x < x <= to_x or (
                        bucket_i == 0 and x == from_x
                    ):  # TODO: remove the 2nd case after bug is fixed
                        # TODO: these counts are not checked yet bc they are not in the final df
                        if np.isposinf(y):
                            positive_inf_count += 1
                        elif np.isneginf(y):
                            negative_inf_count += 1
                        elif np.isnan(y):
                            nan_count += 1
                        else:
                            xs.append(x)
                            ys.append(y)

                bucket = TimeseriesBucket(
                    index=bucket_i,
                    from_x=from_x,
                    to_x=to_x,
                    first_x=xs[0] if xs else float("nan"),
                    first_y=ys[0] if ys else float("nan"),
                    last_x=xs[-1] if xs else float("nan"),
                    last_y=ys[-1] if ys else float("nan"),
                    # statistics:
                    y_min=float(np.min(ys)) if ys else float("nan"),
                    y_max=float(np.max(ys)) if ys else float("nan"),
                    finite_point_count=len(ys),
                    nan_count=nan_count,
                    positive_inf_count=positive_inf_count,
                    negative_inf_count=negative_inf_count,
                    finite_points_sum=float(np.sum(ys)) if ys else 0.0,
                )
                buckets.append(bucket)

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
def test__fetch_metric_buckets__filter_variants(
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
    [2, 3, 10, NUMBER_OF_STEPS + 10],
)
@pytest.mark.parametrize(
    "include_point_previews",
    [True, False],
)
@pytest.mark.parametrize(
    "x",
    ["step"],
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
def test__fetch_metric_buckets__bucket_variants(
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
def test__fetch_metric_buckets__different_steps(
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
    expected_df = create_expected_data_dict(
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
    expected_df = create_expected_data_dict(
        data=expected_data,
        project_identifier=new_project_id,
        x=x,
        limit=limit,
        include_point_previews=include_point_previews,
    )

    pd.testing.assert_frame_equal(result_df, expected_df)
