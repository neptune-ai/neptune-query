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
from tests.e2e.metric_buckets import (
    aggregate_metric_buckets,
    calculate_global_range,
    calculate_metric_bucket_ranges,
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

    # then
    expected_buckets = _aggregate_metric_buckets(expected_values, limit, x_range)
    assert result == {run_definition: expected_buckets}


def _aggregate_metric_buckets(
    series: list[tuple[float, float]], limit: int, x_range: tuple[float, float] | None
) -> list[TimeseriesBucket]:
    global_from, global_to = calculate_global_range(series, x_range)
    bucket_ranges = calculate_metric_bucket_ranges(global_from, global_to, limit)
    return aggregate_metric_buckets(series, bucket_ranges)
