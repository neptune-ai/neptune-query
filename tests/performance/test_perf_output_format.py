from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metric_buckets_dataframe
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket

from .util import expected_benchmark

EXPERIMENTS = 500
PATHS = 5
STEPS = 100
BUCKETS = 5


def _generate_bucket_metrics(
    experiments: int, paths: int, buckets: int
) -> dict[RunAttributeDefinition, list[TimeseriesBucket]]:
    return {
        _generate_run_attribute_definition(experiment, path): [_generate_bucket_metric(index=i) for i in range(buckets)]
        for experiment in range(experiments)
        for path in range(paths)
    }


def _generate_run_attribute_definition(
    experiment: int, path: int, attribute_type="float_series"
) -> RunAttributeDefinition:
    return RunAttributeDefinition(
        RunIdentifier(ProjectIdentifier("foo/bar"), SysId(f"sysid{experiment}")),
        AttributeDefinition(f"path{path}", attribute_type),
    )


def _generate_bucket_metric(index: int) -> TimeseriesBucket:
    if index > 0:
        return TimeseriesBucket(
            index=index,
            from_x=20.0 * index,
            to_x=20.0 * (index + 1),
            first_x=20.0 * index + 2,
            first_y=100.0 * (index - 1) + 90.0,
            last_x=20.0 * (index + 1) - 2,
            last_y=100.0 * index,
            y_min=80.0 * index,
            y_max=110.0 * index,
            finite_point_count=10 + index,
            nan_count=5 - index,
            positive_inf_count=2 * index,
            negative_inf_count=index,
            finite_points_sum=950.0 * index,
        )
    else:
        return TimeseriesBucket(
            index=index,
            from_x=float("-inf"),
            to_x=20.0,
            first_x=20.0,
            first_y=0.0,
            last_x=20.0,
            last_y=0.0,
            y_min=0.0,
            y_max=0.0,
            finite_point_count=1,
            nan_count=0,
            positive_inf_count=0,
            negative_inf_count=0,
            finite_points_sum=0.0,
        )


@expected_benchmark(min_p0=0.400, max_p80=0.500, max_p100=1.000)
def test_perf_create_metric_buckets_dataframe(benchmark):
    """Test the creation of a flat DataFrame from float point values."""

    buckets_data = _generate_bucket_metrics(EXPERIMENTS, PATHS, BUCKETS)
    sys_id_label_mapping = {SysId(f"sysid{experiment}"): f"exp{experiment}" for experiment in range(EXPERIMENTS)}
    benchmark(
        create_metric_buckets_dataframe,
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )
