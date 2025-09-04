import itertools
from typing import (
    Literal,
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

    for experiment in experiments:
        steps = experiment.float_series[f"{PATH}/metrics/step"]
        for path, series in experiment.float_series.items():
            buckets = []
            bucket_width = max(1, len(steps) // (limit - 1))
            from_i = float("-inf")
            for bucket_i, to_i in enumerate(
                itertools.chain([1], range(bucket_width, len(steps) + bucket_width, bucket_width))
            ):
                if from_i == float("-inf"):
                    xs = steps[:to_i]
                    ys = series[:to_i]
                else:
                    xs = steps[from_i:to_i]
                    ys = series[from_i:to_i]

                bucket = TimeseriesBucket(
                    index=bucket_i,
                    from_x=float("-inf") if from_i == float("-inf") else steps[from_i],
                    to_x=steps[to_i - 1] if to_i - 1 < len(steps) else float("inf"),
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
                from_i = to_i

            attribute_run = _to_run_attribute_definition(project.project_identifier, experiment.name, path)
            bucket_data.setdefault(attribute_run, []).extend(buckets)

    return create_metric_buckets_dataframe(
        buckets_data=bucket_data,
        sys_id_label_mapping=_sys_id_label_mapping(experiments),
        container_column_name="experiment",
    )


@pytest.mark.parametrize(
    "x",
    ["step"],
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
    "limit,include_point_previews",
    [
        (
            2,
            True,
        ),
        (
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
