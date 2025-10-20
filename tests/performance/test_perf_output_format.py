from neptune_query.internal.identifiers import SysId
from neptune_query.internal.output_format import (
    convert_table_to_dataframe,
    create_metric_buckets_dataframe,
    create_metrics_dataframe,
    create_series_dataframe,
)
from neptune_query.internal.retrieval.series import SeriesValue
from tests.helpers.metrics import (
    FloatPointValue,
    normalize_metrics_data,
)

from . import generate
from .decorator import expected_benchmark


@expected_benchmark(
    dict(num_experiments=5, num_paths=500, num_buckets=50, min_p0=0.500, max_p80=0.700, max_p100=1.000),
    dict(num_experiments=50, num_paths=50, num_buckets=50, min_p0=0.500, max_p80=0.700, max_p100=1.000),
    dict(num_experiments=500, num_paths=5, num_buckets=50, min_p0=0.500, max_p80=0.700, max_p100=1.000),
)
def test_perf_create_metric_buckets_dataframe(benchmark, num_experiments, num_paths, num_buckets):
    """Test the performance of create_metric_buckets_dataframe"""

    buckets_data = generate.bucket_metrics(num_experiments, num_paths, num_buckets)
    sys_id_label_mapping = {SysId(f"sysid{experiment}"): f"exp{experiment}" for experiment in range(num_experiments)}
    benchmark(
        create_metric_buckets_dataframe,
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name="experiment",
    )


@expected_benchmark(
    dict(num_experiments=50, num_paths=50, num_steps=500, min_p0=0.030, max_p80=0.050, max_p100=0.060),
    dict(num_experiments=50, num_paths=500, num_steps=50, min_p0=0.070, max_p80=0.080, max_p100=0.090),
    dict(num_experiments=500, num_paths=50, num_steps=50, min_p0=0.070, max_p80=0.090, max_p100=0.100),
)
def test_perf_create_metrics_dataframe(benchmark, num_experiments, num_steps, num_paths):
    """Test the performance of create_metrics_dataframe"""
    metrics_data = {}
    for exp in range(num_experiments):
        for path in range(num_paths):
            run_attr_def = generate.run_attribute_definition(exp, path)
            metrics_data[run_attr_def] = [
                FloatPointValue.create(
                    step=float(step),
                    value=float(step * exp),
                    timestamp_ms=None,
                    is_preview=False,
                    completion_ratio=1.0,
                )
                for step in range(num_steps)
            ]

    sys_id_label_mapping = {SysId(f"sysid{exp}"): f"exp{exp}" for exp in range(num_experiments)}

    benchmark(
        create_metrics_dataframe,
        metrics_data=normalize_metrics_data(metrics_data),
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=True,
        include_point_previews=False,
        index_column_name="experiment",
    )


# NOTE: This test gets surprisingly big variance in timing, probably due to memory usage?
# Example from 2025-10-01: p0: 0.772, p80: 0.941, p100: 0.946
@expected_benchmark(
    num_experiments=100, num_attributes=10, num_points_per_attribute=1500, min_p0=0.070, max_p80=0.110, max_p100=0.120
)
def test_perf_create_metrics_dataframe_with_random_data(
    benchmark, num_experiments, num_attributes, num_points_per_attribute
):
    """Test the performance of create_metrics_dataframe with random data"""
    sys_ids = generate.random_alnum_strings(count=num_experiments, length=50)
    metric_names = generate.random_alnum_strings(count=num_attributes, length=10)
    metrics_data = {
        generate.run_attribute_definition(sys_id, metric): [
            generate.float_point_value(i, sys_ids.index(sys_id) * 0.321) for i in range(num_points_per_attribute)
        ]
        for sys_id in sys_ids
        for metric in metric_names
    }

    sys_id_label_mapping = {r.run_identifier.sys_id: r.run_identifier.sys_id for r in metrics_data.keys()}
    benchmark(
        create_metrics_dataframe,
        metrics_data=normalize_metrics_data(metrics_data),
        sys_id_label_mapping=sys_id_label_mapping,
        type_suffix_in_column_names=False,
        include_point_previews=False,
        index_column_name="experiment",
        timestamp_column_name=None,
    )


@expected_benchmark(
    dict(num_experiments=200, num_paths=50, num_steps=100, min_p0=0.160, max_p80=0.200, max_p100=0.300),
    dict(num_experiments=50, num_paths=200, num_steps=100, min_p0=0.160, max_p80=0.200, max_p100=0.300),
)
def test_perf_create_series_dataframe(benchmark, num_experiments, num_paths, num_steps):
    """Test the performance of create_series_dataframe"""
    series_data = {}
    for exp in range(num_experiments):
        for path in range(num_paths):
            run_attr_def = generate.run_attribute_definition(exp, path, attribute_type="string_series")
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


@expected_benchmark(
    dict(num_experiments=100, num_paths=5000, min_p0=0.500, max_p80=0.750, max_p100=1.000),
    dict(num_experiments=800, num_paths=1000, min_p0=0.500, max_p80=0.750, max_p100=1.000),
    dict(num_experiments=5000, num_paths=150, min_p0=0.500, max_p80=0.700, max_p100=1.000),
)
def test_perf_convert_table_to_dataframe(benchmark, num_experiments, num_paths):
    """Test the performance of convert_table_to_dataframe"""
    table_data = {}
    for exp in range(num_experiments):
        values = []
        for path in range(num_paths):
            # Add float series values
            values.append(generate.float_series_value(f"metric{path}", exp))
            # Add string values
            values.append(generate.string_value(f"param{path}", exp))
        table_data[f"exp{exp}"] = values

    benchmark(
        convert_table_to_dataframe,
        table_data=table_data,
        project_identifier="foo/bar",
        type_suffix_in_column_names=True,
        index_column_name="experiment",
    )
