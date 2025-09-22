import datetime
from typing import (
    Optional,
    Sequence,
    Tuple,
)

import pandas as pd
from hypothesis import strategies as st

from neptune_query.internal.identifiers import (
    AttributeDefinition,
    ProjectIdentifier,
    RunAttributeDefinition,
    RunIdentifier,
    SysId,
)
from neptune_query.internal.retrieval.attribute_types import ALL_TYPES
from neptune_query.internal.retrieval.metrics import FloatPointValue


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
