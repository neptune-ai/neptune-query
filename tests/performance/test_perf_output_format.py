from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import (
    convert_table_to_dataframe,
    create_metric_buckets_dataframe,
    create_metrics_dataframe,
    create_series_dataframe,
)
from neptune_query.internal.retrieval.attribute_types import FloatSeriesAggregations
from neptune_query.internal.retrieval.attribute_values import AttributeValue
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket
from neptune_query.internal.retrieval.series import SeriesValue

from .decorator import expected_benchmark

EXPERIMENT_IDENTIFIER = RunIdentifier(ProjectIdentifier("project/abc"), SysId("XXX-1"))


def _create_float_series_value(path: str, exp: int):
    """Helper to create a float series value for testing."""
    return AttributeValue(
        attribute_definition=AttributeDefinition(path, "float_series"),
        value=FloatSeriesAggregations(last=float(exp), min=0.0, max=float(exp), average=float(exp) / 2, variance=0.0),
        run_identifier=EXPERIMENT_IDENTIFIER,
    )


def _create_string_value(path: str, exp: int):
    """Helper to create a string value for testing."""
    return AttributeValue(
        attribute_definition=AttributeDefinition(path, "string"),
        value=f"value_{exp}",
        run_identifier=EXPERIMENT_IDENTIFIER,
    )


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


@expected_benchmark(num_experiments=5, num_paths=500, num_buckets=50, min_p0=0.500, max_p80=0.650, max_p100=1.000)
@expected_benchmark(num_experiments=50, num_paths=50, num_buckets=50, min_p0=0.500, max_p80=0.650, max_p100=1.000)
@expected_benchmark(num_experiments=500, num_paths=5, num_buckets=50, min_p0=0.500, max_p80=0.650, max_p100=1.000)
def test_perf_create_metric_buckets_dataframe(benchmark, num_experiments, num_paths, num_buckets):
    """Test the creation of a flat DataFrame from float point values."""

    buckets_data = _generate_bucket_metrics(num_experiments, num_paths, num_buckets)
    sys_id_label_mapping = {SysId(f"sysid{experiment}"): f"exp{experiment}" for experiment in range(num_experiments)}
    benchmark(
        create_metric_buckets_dataframe,
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )


@expected_benchmark(num_experiments=50, num_paths=50, num_steps=500, min_p0=0.500, max_p80=0.600, max_p100=0.800)
@expected_benchmark(num_experiments=50, num_paths=500, num_steps=50, min_p0=0.500, max_p80=0.600, max_p100=0.800)
@expected_benchmark(num_experiments=500, num_paths=50, num_steps=50, min_p0=0.500, max_p80=0.660, max_p100=0.800)
def test_perf_create_metrics_dataframe(benchmark, num_experiments, num_steps, num_paths):
    """Test the performance of creating metrics DataFrame with many experiments and steps."""
    metrics_data = {}
    for exp in range(num_experiments):
        for path in range(num_paths):
            run_attr_def = _generate_run_attribute_definition(exp, path)
            metrics_data[run_attr_def] = [
                (None, float(step), float(step * exp), False, 1.0)
                for step in range(num_steps)  # FloatPointValue as tuple
            ]

    sys_id_label_mapping = {SysId(f"sysid{exp}"): f"exp{exp}" for exp in range(num_experiments)}

    benchmark(
        create_metrics_dataframe,
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=True,
        include_point_previews=False,
        index_column_name="experiment",
    )


@expected_benchmark(num_experiments=200, num_paths=50, num_steps=100, min_p0=0.600, max_p80=1.000, max_p100=1.500)
@expected_benchmark(num_experiments=50, num_paths=200, num_steps=100, min_p0=0.600, max_p80=1.000, max_p100=1.500)
def test_perf_create_series_dataframe(benchmark, num_experiments, num_paths, num_steps):
    """Test the performance of creating series DataFrame with string values."""
    series_data = {}
    for exp in range(num_experiments):
        for path in range(num_paths):
            run_attr_def = _generate_run_attribute_definition(exp, path, attribute_type="string_series")
            series_data[run_attr_def] = [
                SeriesValue(step=float(step), value=f"value_{exp}_{step}", timestamp_millis=None)
                for step in range(num_steps)
            ]

    sys_id_label_mapping = {SysId(f"sysid{exp}"): f"exp{exp}" for exp in range(num_experiments)}

    benchmark(
        create_series_dataframe,
        series_data=series_data,
        project_identifier="foo/bar",
        sys_id_label_mapping=sys_id_label_mapping,
        index_column_name="experiment",
        timestamp_column_name=None,
    )


@expected_benchmark(num_experiments=100, num_paths=4000, min_p0=0.500, max_p80=1.000, max_p100=1.500)
@expected_benchmark(num_experiments=500, num_paths=1000, min_p0=0.500, max_p80=0.800, max_p100=1.200)
@expected_benchmark(num_experiments=5000, num_paths=100, min_p0=0.500, max_p80=0.800, max_p100=1.200)
def test_perf_convert_table_to_dataframe(benchmark, num_experiments, num_paths):
    """Test performance of converting a large table with multiple value types to DataFrame."""
    table_data = {}
    for exp in range(num_experiments):
        values = []
        for path in range(num_paths):
            # Add float series values
            values.append(_create_float_series_value(f"metric{path}", exp))
            # Add string values
            values.append(_create_string_value(f"param{path}", exp))
        table_data[f"exp{exp}"] = values

    benchmark(
        convert_table_to_dataframe,
        table_data=table_data,
        project_identifier="foo/bar",
        selected_aggregations={},
        type_suffix_in_column_names=True,
        index_column_name="experiment",
    )
