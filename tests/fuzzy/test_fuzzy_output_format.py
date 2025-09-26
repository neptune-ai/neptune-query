import numpy as np
import pandas as pd
from hypothesis import (
    given,
    note,
)
from hypothesis import strategies as st

from neptune_query.internal.output_format import (
    create_metric_buckets_dataframe,
    create_metrics_dataframe,
    create_series_dataframe,
)
from neptune_query.internal.retrieval.metrics import (
    StepIndex,
    ValueIndex,
)
from tests.fuzzy.data_generators import (
    metric_buckets_datasets,
    metric_datasets,
    series_datasets,
)

EXPECTED_COLUMN_ORDER = ["value", "absolute_time", "is_preview", "preview_completion"]


@given(
    metric_dataset=metric_datasets(),
    timestamp_column_name=st.one_of(st.just(None), st.sampled_from(["absolute_time"])),
    type_suffix_in_column_names=st.booleans(),
    include_point_previews=st.booleans(),
    index_column_name=st.sampled_from(["experiment", "run"]),
)
def test_create_metrics_dataframe(
    metric_dataset, timestamp_column_name, type_suffix_in_column_names, include_point_previews, index_column_name
):
    metrics_data, sys_id_label_mapping = metric_dataset

    df = create_metrics_dataframe(
        metrics_data=metrics_data,
        sys_id_label_mapping=sys_id_label_mapping,
        timestamp_column_name=timestamp_column_name,
        type_suffix_in_column_names=type_suffix_in_column_names,
        include_point_previews=include_point_previews,
        index_column_name=index_column_name,
    )
    note(f"index: {df.index}")
    note(f"columns: {df.columns}")
    note(f"df: {df}")

    # validate index
    expected_experiment_steps = sorted(
        {
            (sys_id_label_mapping[run_attributes.run_identifier.sys_id], point[StepIndex])
            for run_attributes, points in metrics_data.items()
            for point in points
        }
    )
    assert df.index.tolist() == expected_experiment_steps

    # validate columns
    expected_attributes = sorted(
        {
            f"{run_attributes.attribute_definition.name}:{run_attributes.attribute_definition.type}"
            if type_suffix_in_column_names
            else run_attributes.attribute_definition.name
            for run_attributes, points in metrics_data.items()
            if points
        }
    )
    expected_subcolumns = ["value"]
    if timestamp_column_name:
        expected_subcolumns.append(timestamp_column_name)
    if include_point_previews:
        expected_subcolumns.extend(["is_preview", "preview_completion"])
    if len(expected_subcolumns) > 1:
        expected_columns = sorted(
            [(attr, subcol) for attr in expected_attributes for subcol in expected_subcolumns],
            key=lambda x: (x[0], EXPECTED_COLUMN_ORDER.index(x[1])),
        )
    else:
        expected_columns = expected_attributes
    assert df.columns.tolist() == expected_columns

    # validate actual values using mean comparison (finite values only)
    expected_values = [
        point[ValueIndex] for points in metrics_data.values() for point in points if np.isfinite(point[ValueIndex])
    ]
    value_columns = [
        col for col in df.columns if (isinstance(col, tuple) and col[1] == "value") or (not isinstance(col, tuple))
    ]

    if expected_values and value_columns:
        # sorting is actually important to ensure that we get consistent results for large floats
        expected_mean = np.mean(sorted(expected_values))
        actual_mean = np.mean(sorted(n for n in df[value_columns].values.flatten() if np.isfinite(n)))
        assert np.isclose(
            expected_mean, actual_mean, equal_nan=True
        ), f"Mean mismatch: expected {expected_mean}, got {actual_mean}"
    elif not expected_values and not value_columns:
        pass  # Both empty, which is correct
    else:
        assert False, f"Mismatch: expected_values={len(expected_values)}, value_columns={len(value_columns)}"


@given(
    buckets_dataset=metric_buckets_datasets(),
    container_column_name=st.sampled_from(["experiment", "run"]),
)
def test_create_metric_buckets_dataframe(buckets_dataset, container_column_name):
    buckets_data, sys_id_label_mapping = buckets_dataset

    df = create_metric_buckets_dataframe(
        buckets_data=buckets_data,
        sys_id_label_mapping=sys_id_label_mapping,
        container_column_name=container_column_name,
    )
    note(f"index: {df.index}")
    note(f"columns: {df.columns}")
    note(f"df: {df}")

    # validate index (buckets)
    all_buckets = set()
    for buckets in buckets_data.values():
        all_buckets.update((bucket.from_x, bucket.to_x) for bucket in buckets if bucket.finite_point_count > 0)
    expected_buckets = [pd.Interval(left=bucket[0], right=bucket[1], closed="right") for bucket in sorted(all_buckets)]

    # _collapse_open_buckets merges 1st and 2nd bucket
    if len(expected_buckets) == 1:
        first_bucket = expected_buckets[0]
        if first_bucket.left == float("-inf"):
            expected_buckets = [pd.Interval(left=first_bucket.right, right=first_bucket.right, closed="both")]
        elif first_bucket.right == float("inf"):
            expected_buckets = [pd.Interval(left=first_bucket.left, right=first_bucket.left, closed="both")]
    elif len(expected_buckets) > 1:
        first_bucket, second_bucket = expected_buckets[0], expected_buckets[1]
        bucket_length = second_bucket.length
        if first_bucket.right >= second_bucket.left - bucket_length * 0.5:
            expected_buckets = [
                pd.Interval(left=first_bucket.right, right=second_bucket.right, closed="both")
            ] + expected_buckets[2:]
        else:
            expected_buckets = [
                pd.Interval(left=first_bucket.right, right=first_bucket.right + bucket_length, closed="both")
            ] + expected_buckets[1:]
    assert df.index.tolist() == expected_buckets

    # validate columns (run, metric, value)
    expected_columns = set()
    for run_attributes, buckets in buckets_data.items():
        label = sys_id_label_mapping[run_attributes.run_identifier.sys_id]
        metric_name = run_attributes.attribute_definition.name
        expected_columns.add((label, metric_name, "x"))
        expected_columns.add((label, metric_name, "y"))

    expected_columns = sorted(expected_columns)
    assert df.columns.tolist() == expected_columns


@given(
    series_dataset=series_datasets(),
    timestamp_column_name=st.one_of(st.just(None), st.sampled_from(["absolute_time"])),
    index_column_name=st.sampled_from(["experiment", "run"]),
)
def test_create_series_dataframe(series_dataset, timestamp_column_name, index_column_name):
    series_data, sys_id_label_mapping = series_dataset

    # Use a dummy project identifier for the test
    project_identifier = "test-org/test-project"

    df = create_series_dataframe(
        series_data=series_data,
        project_identifier=project_identifier,
        sys_id_label_mapping=sys_id_label_mapping,
        index_column_name=index_column_name,
        timestamp_column_name=timestamp_column_name,
    )
    note(f"index: {df.index}")
    note(f"columns: {df.columns}")
    note(f"df: {df}")

    # validate index
    expected_experiment_steps = sorted(
        {
            (sys_id_label_mapping[run_attributes.run_identifier.sys_id], point.step)
            for run_attributes, points in series_data.items()
            for point in points
        }
    )
    assert df.index.tolist() == expected_experiment_steps

    # validate columns
    expected_attributes = sorted(
        list({run_attributes.attribute_definition.name for run_attributes, points in series_data.items() if points})
    )
    expected_subcolumns = ["value"]
    if timestamp_column_name:
        expected_subcolumns.append(timestamp_column_name)
    if len(expected_subcolumns) > 1:
        expected_columns = sorted(
            [(attr, subcol) for attr in expected_attributes for subcol in expected_subcolumns],
            key=lambda x: (x[0], EXPECTED_COLUMN_ORDER.index(x[1])),
        )
    else:
        expected_columns = expected_attributes
    assert df.columns.tolist() == expected_columns

    # validate actual values using count comparison (since values can be complex objects)
    expected_count = sum(len(points) for points in series_data.values())
    value_columns = [
        col for col in df.columns if (isinstance(col, tuple) and col[1] == "value") or (not isinstance(col, tuple))
    ]
    actual_count = sum(df[col].count() for col in value_columns)

    assert expected_count == actual_count, f"Value count mismatch: expected {expected_count}, got {actual_count}"
