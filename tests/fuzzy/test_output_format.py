import datetime
from datetime import timedelta
from typing import (
    Optional,
    Sequence,
    Tuple,
)

import pandas as pd
from hypothesis import (
    given,
    note,
    settings,
)
from hypothesis import strategies as st

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.output_format import create_metrics_dataframe
from neptune_query.internal.retrieval.attribute_types import ALL_TYPES
from neptune_query.internal.retrieval.metrics import (
    FloatPointValue,
    StepIndex,
)


@st.composite
def metric_datasets(draw) -> Tuple[dict[RunAttributeDefinition, list[FloatPointValue]], dict[SysId, str]]:
    project_identifier = draw(project_identifiers())
    run_attribute_definition_list = draw(
        st.lists(run_attribute_definitions(project_identifier=project_identifier, types=["float_series"]))
    )
    metrics_data = {
        run_attribute_definition: draw(float_point_values())
        for run_attribute_definition in run_attribute_definition_list
    }

    labels = draw(
        st.lists(
            st.text(min_size=1, max_size=1024),
            min_size=len(run_attribute_definition_list),
            max_size=len(run_attribute_definition_list),
            unique=True,
        )
    )
    label_mapping = {
        run_attribute_definition.run_identifier.sys_id: label
        for run_attribute_definition, label in zip(run_attribute_definition_list, labels)
    }

    return metrics_data, label_mapping


@st.composite
def float_point_values(draw, *, min_size: int = 0, max_size: Optional[int] = None) -> list[FloatPointValue]:
    step_list = draw(
        st.lists(
            st.floats(allow_nan=False, allow_infinity=False, min_value=0),
            min_size=min_size,
            max_size=max_size,
            unique=True,
        ).map(sorted)
    )
    return [draw(float_point_value(step=step)) for step in step_list]


@st.composite
def float_point_value(draw, *, step: float) -> FloatPointValue:
    timestamp_millis = draw(
        st.datetimes(
            min_value=pd.Timestamp.min,
            max_value=pd.Timestamp.max,
            allow_imaginary=False,
            timezones=st.just(datetime.timezone.utc),
        ).map(lambda dt: int(dt.timestamp() * 1000))
    )
    value = draw(st.floats(allow_nan=True, allow_infinity=True))
    preview = draw(st.booleans())
    preview_completion = draw(st.floats(min_value=0.0, max_value=1.0))

    return (
        timestamp_millis,
        step,
        value,
        preview,
        preview_completion,
    )


@st.composite
def run_attribute_definitions(
    draw, *, project_identifier: ProjectIdentifier, types: Sequence[str] = ALL_TYPES
) -> RunAttributeDefinition:
    run_identifier = draw(run_identifiers(project_identifier=project_identifier))
    attribute_definition = draw(attribute_definitions(types=types))

    return RunAttributeDefinition(run_identifier=run_identifier, attribute_definition=attribute_definition)


@st.composite
def attribute_definitions(draw, *, types: Sequence[str] = ALL_TYPES) -> AttributeDefinition:
    name = draw(st.text(min_size=1, max_size=1024))
    attribute_type = draw(st.sampled_from(types))
    return AttributeDefinition(name=name, type=attribute_type)


@st.composite
def run_identifiers(draw, *, project_identifier: ProjectIdentifier) -> RunIdentifier:
    sys_id = draw(st.text(min_size=1, max_size=1024))
    return RunIdentifier(project_identifier=project_identifier, sys_id=SysId(sys_id))


@st.composite
def project_identifiers(draw) -> RunIdentifier:
    organization = draw(
        st.text(alphabet=st.characters(codec="utf-8", exclude_characters="/"), min_size=1, max_size=255)
    )
    project = draw(st.text(alphabet=st.characters(codec="utf-8", exclude_characters="/"), min_size=1, max_size=128))
    return organization + "/" + project


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
