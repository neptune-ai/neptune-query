from datetime import timedelta

from hypothesis import (
    given,
    note,
    settings,
)
from hypothesis import strategies as st

from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.metrics import StepIndex
from tests.fuzzy.data_generators import metric_datasets


@settings(max_examples=1000, deadline=timedelta(minutes=1))
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

    # validate index
    expected_experiment_steps = sorted(
        list(
            {
                (sys_id_label_mapping[run_attributes.run_identifier.sys_id], point[StepIndex])
                for run_attributes, points in metrics_data.items()
                for point in points
            }
        )
    )
    note(f"index: {df.index}")
    assert df.index.tolist() == expected_experiment_steps

    # validate columns
    expected_attributes = sorted(
        list(
            {
                f"{run_attributes.attribute_definition.name}:{run_attributes.attribute_definition.type}"
                if type_suffix_in_column_names
                else run_attributes.attribute_definition.name
                for run_attributes, points in metrics_data.items()
                if points
            }
        )
    )
    expected_subcolumns = ["value"]
    if timestamp_column_name:
        expected_subcolumns.append(timestamp_column_name)
    if include_point_previews:
        expected_subcolumns.extend(["is_preview", "preview_completion"])
    if len(expected_subcolumns) > 1:
        expected_columns = sorted([(attr, subcol) for attr in expected_attributes for subcol in expected_subcolumns])
    else:
        expected_columns = expected_attributes
    note(f"columns: {df.columns}")
    assert df.columns.tolist() == expected_columns
