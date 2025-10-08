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
from neptune_query.internal.retrieval.attribute_types import (
    ALL_TYPES,
    File,
    Histogram,
)
from neptune_query.internal.retrieval.metric_buckets import TimeseriesBucket
from neptune_query.internal.retrieval.metrics import FloatPointValue
from neptune_query.internal.retrieval.series import SeriesValue


@st.composite
def metric_datasets(draw) -> Tuple[dict[RunAttributeDefinition, list[FloatPointValue]], dict[SysId, str]]:
    project_identifier = draw(project_identifiers())
    run_attribute_definition_list = draw(
        st.lists(run_attribute_definitions(project_identifier=project_identifier, types=["float_series"]))
    )
    metrics_data = {
        run_attribute_definition: draw(float_point_values(max_size=1024))
        for run_attribute_definition in run_attribute_definition_list
    }

    label_mapping = draw(generate_label_mapping(run_attribute_definition_list))

    return metrics_data, label_mapping


@st.composite
def generate_label_mapping(draw, run_attribute_definition_list: list[RunAttributeDefinition]) -> dict[SysId, str]:
    """Generate a mapping from run sys_id to unique labels."""
    labels = draw(
        st.lists(
            st.text(min_size=1, max_size=1024),
            min_size=len(run_attribute_definition_list),
            max_size=len(run_attribute_definition_list),
            unique=True,
        )
    )
    return {
        run_attribute_definition.run_identifier.sys_id: label
        for run_attribute_definition, label in zip(run_attribute_definition_list, labels)
    }


@st.composite
def float_point_values(draw, *, min_size: int = 0, max_size: Optional[int] = None) -> list[FloatPointValue]:
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    step_list = draw(
        st.lists(
            st.floats(allow_nan=False, allow_infinity=False, min_value=0),
            min_size=size,
            max_size=size,
            unique=True,
        ).map(sorted)
    )
    values_list = draw(st.lists(st.floats(allow_nan=True, allow_infinity=True), min_size=size, max_size=size))
    timestamp_millis_list = draw(
        st.lists(
            st.datetimes(
                min_value=pd.Timestamp.min,
                max_value=pd.Timestamp.max,
                allow_imaginary=False,
                timezones=st.just(datetime.timezone.utc),
            ).map(lambda dt: int(dt.timestamp() * 1000)),
            min_size=size,
            max_size=size,
        )
    )
    has_previews = draw(st.booleans())
    if has_previews:
        preview_list = draw(st.lists(st.booleans(), min_size=size, max_size=size))
        preview_completion_list = [
            draw(st.floats(min_value=0.0, max_value=1.0)) if preview else 1.0 for preview in preview_list
        ]
    else:
        preview_list = [False] * size
        preview_completion_list = [1.0] * size

    return [
        (
            timestamp_millis,
            step,
            value,
            preview,
            preview_completion,
        )
        for timestamp_millis, step, value, preview, preview_completion in zip(
            timestamp_millis_list, step_list, values_list, preview_list, preview_completion_list
        )
    ]


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


@st.composite
def metric_buckets_datasets(draw) -> Tuple[dict[RunAttributeDefinition, list[TimeseriesBucket]], dict[SysId, str]]:
    project_identifier = draw(project_identifiers())
    run_attribute_definition_list = draw(
        st.lists(run_attribute_definitions(project_identifier=project_identifier, types=["float_series"]))
    )

    bucket_ranges_list = draw(bucket_ranges_lists(max_size=10))

    buckets_data = {
        run_attribute_definition: draw(timeseries_buckets(bucket_ranges=bucket_ranges_list))
        for run_attribute_definition in run_attribute_definition_list
    }

    label_mapping = draw(generate_label_mapping(run_attribute_definition_list))

    return buckets_data, label_mapping


@st.composite
def bucket_ranges_lists(draw, *, min_size: int = 1, max_size: Optional[int] = None) -> list[tuple[float, float]]:
    bucket_count = draw(st.integers(min_value=min_size, max_value=max_size))
    range_from = draw(st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False))
    range_to = draw(
        st.floats(min_value=range_from + 1e-6, max_value=range_from + 1e6, allow_nan=False, allow_infinity=False)
    )

    if bucket_count == 1:
        return [(range_from, float("inf"))]

    bucket_ranges = []
    bucket_width = (range_to - range_from) / (bucket_count - 1)
    for bucket_i in range(bucket_count + 1):
        if bucket_i == 0:
            from_x = float("-inf")
        else:
            from_x = min(range_from + bucket_width * (bucket_i - 1), range_to)

        if bucket_i == bucket_count:
            to_x = float("inf")
        else:
            to_x = min(range_from + bucket_width * bucket_i, range_to)
        bucket_ranges.append((from_x, to_x))
    return bucket_ranges


@st.composite
def timeseries_buckets(draw, *, bucket_ranges: list[tuple[float, float]]) -> list[TimeseriesBucket]:
    buckets_ranges_subseq = draw(
        st.lists(
            st.integers(min_value=0, max_value=len(bucket_ranges) - 1),
            min_size=0,
            max_size=len(bucket_ranges),
            unique=True,
        ).map(lambda idxs: [bucket_ranges[i] for i in sorted(idxs)])
        if bucket_ranges
        else st.just([])
    )

    buckets = [
        draw(timeseries_bucket(index=i, from_x=from_x, to_x=to_x))
        for i, (from_x, to_x) in enumerate(buckets_ranges_subseq)
    ]

    return buckets


@st.composite
def timeseries_bucket(draw, *, index: int, from_x: float, to_x: float) -> TimeseriesBucket:
    if from_x == float("-inf"):
        finite_point_count = 1
        finite_point_xs = [to_x]
        finite_point_ys = [draw(st.floats(allow_nan=False, allow_infinity=False))]
    else:
        finite_point_count = draw(st.integers(min_value=0, max_value=1024))
        finite_point_xs = [
            draw(st.floats(min_value=from_x, max_value=to_x, allow_nan=False, allow_infinity=False))
            for _ in range(finite_point_count)
        ]
        finite_point_ys = [draw(st.floats(allow_nan=False, allow_infinity=False)) for _ in range(finite_point_count)]

    if finite_point_count > 0:
        finite_points = list(zip(finite_point_xs, finite_point_ys))
        finite_points.sort()
        first_x, first_y = finite_points[0]
        last_x, last_y = finite_points[-1]
        # y_min = min(finite_point_ys)
        # y_max = max(finite_point_ys)
        # finite_points_sum = sum(finite_point_ys)
    else:
        finite_point_count = 0
        first_x = float("nan")
        first_y = float("nan")
        last_x = float("nan")
        last_y = float("nan")
        # y_min = float("nan")
        # y_max = float("nan")
        # finite_points_sum = 0

    # nan_count = draw(st.integers(min_value=0))
    # positive_inf_count = draw(st.integers(min_value=0))
    # negative_inf_count = draw(st.integers(min_value=0))

    return TimeseriesBucket(
        index=index,
        from_x=from_x,
        to_x=to_x,
        first_x=first_x,
        first_y=first_y,
        last_x=last_x,
        last_y=last_y,
        # y_min=y_min,
        # y_max=y_max,
        # finite_point_count=finite_point_count,
        # nan_count=nan_count,
        # positive_inf_count=positive_inf_count,
        # negative_inf_count=negative_inf_count,
        # finite_points_sum=finite_points_sum,
    )


@st.composite
def series_datasets(draw) -> Tuple[dict[RunAttributeDefinition, list[SeriesValue]], dict[SysId, str]]:
    project_identifier = draw(project_identifiers())
    run_attribute_definition_list = draw(
        st.lists(
            run_attribute_definitions(
                project_identifier=project_identifier, types=["string_series", "file_series", "histogram_series"]
            )
        )
    )
    series_data = {
        run_attribute_definition: draw(
            series_values(max_size=1024, series_type=run_attribute_definition.attribute_definition.type)
        )
        for run_attribute_definition in run_attribute_definition_list
    }

    label_mapping = draw(generate_label_mapping(run_attribute_definition_list))

    return series_data, label_mapping


@st.composite
def series_values(
    draw, *, min_size: int = 0, max_size: Optional[int] = None, series_type: str = "string_series"
) -> list[SeriesValue]:
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    step_list = draw(
        st.lists(
            st.floats(allow_nan=False, allow_infinity=False, min_value=0),
            min_size=size,
            max_size=size,
            unique=True,
        ).map(sorted)
    )
    timestamp_millis_list = draw(
        st.lists(
            st.datetimes(
                min_value=pd.Timestamp.min,
                max_value=pd.Timestamp.max,
                allow_imaginary=False,
                timezones=st.just(datetime.timezone.utc),
            ).map(lambda dt: int(dt.timestamp() * 1000)),
            min_size=size,
            max_size=size,
        )
    )

    # Generate values based on the specific series type
    values_list = []
    for _ in range(size):
        if series_type == "string_series":
            value = draw(st.text(min_size=0, max_size=1000))
        elif series_type == "file_series":
            value = File(
                path=draw(st.text(min_size=1, max_size=200)),
                size_bytes=draw(st.integers(min_value=0, max_value=1000000)),
                mime_type=draw(st.sampled_from(["text/plain", "image/png", "application/json", "text/csv"])),
            )
        elif series_type == "histogram_series":
            bin_count = draw(st.integers(min_value=2, max_value=20))
            edges = draw(
                st.lists(
                    st.floats(allow_nan=False, allow_infinity=False), min_size=bin_count + 1, max_size=bin_count + 1
                )
            )
            edges.sort()
            values = draw(
                st.lists(
                    st.floats(allow_nan=False, allow_infinity=False, min_value=0),
                    min_size=bin_count,
                    max_size=bin_count,
                )
            )
            value = Histogram(
                type=draw(st.sampled_from(["linear", "logarithmic"])),
                edges=edges,
                values=values,
            )
        else:
            raise ValueError(f"Unsupported series type: {series_type}")
        values_list.append(value)

    return [
        SeriesValue(step=step, value=value, timestamp_millis=timestamp_millis)
        for step, value, timestamp_millis in zip(step_list, values_list, timestamp_millis_list)
    ]
