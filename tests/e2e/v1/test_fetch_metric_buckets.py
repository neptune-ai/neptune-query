from typing import (
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
from tests.e2e.conftest import Project
from tests.e2e.data import (
    NUMBER_OF_STEPS,
    PATH,
    TEST_DATA,
    ExperimentData,
)


def _to_run_attribute_definition(project, run, path):
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier(project), SysId(run)),
        AttributeDefinition(path, "float_series"),
    )


def _sys_id_label_mapping(experiments: list[ExperimentData]) -> dict[SysId, str]:
    return {SysId(experiment.name): experiment.name for experiment in experiments}


def create_expected_data(
    project: Project,
    experiments: list[ExperimentData],
    x: Union[Literal["step"]],  # TODO - only option
    limit: int,
    include_point_previews: bool,  # TODO - add to the test data?
) -> pd.DataFrame:
    bucket_data: dict[RunAttributeDefinition, list[TimeseriesBucket]] = {}

    global_range_x: Optional[tuple[float, float]] = None
    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]
        if global_range_x is None:
            global_range_x = min(steps), max(steps)
        else:
            global_min_x, global_max_x = global_range_x
            global_range_x = min(global_min_x, min(steps)), max(global_max_x, max(steps))
    assert global_range_x is not None
    global_min_x, global_max_x = global_range_x

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

    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]

        for path, series in experiment.float_series.items():
            buckets = []
            for bucket_i, bucket_range_x in enumerate(bucket_ranges_x):
                from_x, to_x = bucket_range_x

                xs = []
                ys = []
                for x, y in zip(steps, series):
                    if from_x < x <= to_x:
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
                    y_min=float(np.nanmin(ys)) if ys else float("nan"),
                    y_max=float(np.nanmax(ys)) if ys else float("nan"),
                    finite_point_count=int(np.isfinite(ys).sum()) if ys else 0,
                    nan_count=int(np.isnan(ys).sum()) if ys else 0,
                    positive_inf_count=0,
                    negative_inf_count=0,
                    finite_points_sum=float(np.nansum(ys)) if ys else 0.0,
                )
                buckets.append(bucket)

            attribute_run = _to_run_attribute_definition(project.project_identifier, experiment.name, path)
            bucket_data.setdefault(attribute_run, []).extend(buckets)

    return create_metric_buckets_dataframe(
        buckets_data=bucket_data,
        sys_id_label_mapping=_sys_id_label_mapping(experiments),
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

    expected_df = create_expected_data(
        project=project, experiments=experiments, x=x, limit=limit, include_point_previews=include_point_previews
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

    expected_df = create_expected_data(
        project=project, experiments=experiments, x=x, limit=limit, include_point_previews=include_point_previews
    )

    pd.testing.assert_frame_equal(result_df, expected_df)
