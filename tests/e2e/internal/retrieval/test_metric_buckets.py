import numpy as np
import pytest

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    RunAttributeDefinition,
)
from neptune_query.internal.retrieval.metric_buckets import (
    TimeseriesBucket,
    fetch_time_series_buckets,
)
from neptune_query.internal.retrieval.search import ContainerType
from tests.e2e.data import (
    FLOAT_SERIES_PATHS,
    PATH,
    TEST_DATA,
)

EXPERIMENT = TEST_DATA.experiments[0]


def test_fetch_time_series_buckets_does_not_exist(client, project, experiment_identifier):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition("does-not-exist", "string"))

    # when
    result = fetch_time_series_buckets(
        client,
        run_attribute_definitions=[run_definition],
        container_type=ContainerType.EXPERIMENT,
        x="step",
        lineage_to_the_root=False,
        include_point_previews=False,
        limit=10,
        x_range=None,
    )

    # then
    assert result == {run_definition: []}


def _calculate_bucket_ranges(
    series: list[tuple[float, float]], limit: int, x_range: tuple[float, float] | None
) -> list[TimeseriesBucket]:
    if x_range is not None:
        range_from, range_to = x_range
    else:
        xs = [x for x, y in series]
        range_from, range_to = min(xs), max(xs)

    bucket_ranges = []
    bucket_width = (range_to - range_from) / (limit - 1)
    for bucket_i in range(limit + 1):
        if bucket_i == 0:
            from_x = float("-inf")
        else:
            from_x = range_from + bucket_width * (bucket_i - 1)

        if bucket_i == limit:
            to_x = float("inf")
        else:
            to_x = range_from + bucket_width * bucket_i
        bucket_ranges.append((from_x, to_x))
    return bucket_ranges


def _aggregate_buckets(
    series: list[tuple[float, float]], limit: int, x_range: tuple[float, float] | None
) -> list[TimeseriesBucket]:
    bucket_ranges = _calculate_bucket_ranges(series, limit, x_range)

    buckets = []
    for bucket_i, bucket_x_range in enumerate(bucket_ranges):
        from_x, to_x = bucket_x_range

        count = 0
        positive_inf_count = 0
        negative_inf_count = 0
        nan_count = 0
        xs = []
        ys = []
        for x, y in series:
            if from_x < x <= to_x or (bucket_i == 0 and x == from_x):
                count += 1
                if np.isposinf(y):
                    positive_inf_count += 1
                elif np.isneginf(y):
                    negative_inf_count += 1
                elif np.isnan(y):
                    nan_count += 1
                else:
                    xs.append(x)
                    ys.append(y)
        if count == 0:
            continue

        bucket = TimeseriesBucket(
            index=bucket_i,
            from_x=from_x,
            to_x=to_x,
            first_x=xs[0] if xs else float("nan"),
            first_y=ys[0] if ys else float("nan"),
            last_x=xs[-1] if xs else float("nan"),
            last_y=ys[-1] if ys else float("nan"),
            y_min=float(np.min(ys)) if ys else float("nan"),
            y_max=float(np.max(ys)) if ys else float("nan"),
            finite_point_count=len(ys),
            nan_count=nan_count,
            positive_inf_count=positive_inf_count,
            negative_inf_count=negative_inf_count,
            finite_points_sum=float(np.sum(ys)) if ys else 0.0,
        )
        buckets.append(bucket)
    return buckets


@pytest.mark.parametrize(
    "attribute_name, expected_values",
    [
        (
            FLOAT_SERIES_PATHS[0],
            list(zip(EXPERIMENT.float_series[f"{PATH}/metrics/step"], EXPERIMENT.float_series[FLOAT_SERIES_PATHS[0]])),
        ),
    ],
)
@pytest.mark.parametrize(
    "limit",
    [2, 10, 100],
)
@pytest.mark.parametrize(
    "x_range",
    [None, (1, 2), (-100, 100)],
)
def test_fetch_time_series_buckets_single_series(
    client, project, experiment_identifier, attribute_name, expected_values, limit, x_range
):
    # given
    run_definition = RunAttributeDefinition(experiment_identifier, AttributeDefinition(attribute_name, "float-series"))

    # when
    result = fetch_time_series_buckets(
        client,
        run_attribute_definitions=[run_definition],
        container_type=ContainerType.EXPERIMENT,
        x="step",
        lineage_to_the_root=False,
        include_point_previews=False,
        limit=limit,
        x_range=x_range,
    )

    print()
    print(f"{limit=}, {x_range=}:")
    print("; ".join([f"({b.from_x},{b.to_x}] count={b.finite_point_count}" for b in result[run_definition]]))

    # then
    expected_buckets = _aggregate_buckets(expected_values, limit, x_range)
    assert result == {run_definition: expected_buckets}
