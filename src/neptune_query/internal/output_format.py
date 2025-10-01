#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pathlib
from typing import (
    Any,
    Generator,
    Optional,
    Tuple,
)

import numpy as np
import pandas as pd

from .. import types
from ..exceptions import ConflictingAttributeTypes
from . import identifiers
from .retrieval import (
    metric_buckets,
    metrics,
    series,
)
from .retrieval.attribute_types import (
    TYPE_AGGREGATIONS,
    File,
    Histogram,
)
from .retrieval.attribute_values import AttributeValue
from .retrieval.metrics import (
    IsPreviewIndex,
    PreviewCompletionIndex,
    StepIndex,
    TimestampIndex,
    ValueIndex,
)
from .retrieval.search import ContainerType

__all__ = (
    "convert_table_to_dataframe",
    "create_metrics_dataframe",
    "create_series_dataframe",
    "create_files_dataframe",
    "create_metric_buckets_dataframe",
)


def convert_table_to_dataframe(
    table_data: dict[str, list[AttributeValue]],
    project_identifier: str,
    type_suffix_in_column_names: bool,
    # TODO: accept container_type as an argument instead of index_column_name
    # see https://github.com/neptune-ai/neptune-fetcher/pull/402/files#r2260012199
    index_column_name: str = "experiment",
) -> pd.DataFrame:

    if not table_data:
        return pd.DataFrame(
            index=pd.Index([], name=index_column_name),
            columns=pd.Index([], name="attribute"),
        )

    def convert_row(label: str, values: list[AttributeValue]) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for value in values:
            column_name = f"{value.attribute_definition.name}:{value.attribute_definition.type}"
            attribute_type = value.attribute_definition.type
            if attribute_type in TYPE_AGGREGATIONS:
                aggregation_value = value.value
                element_value = getattr(aggregation_value, "last")
                step = getattr(aggregation_value, "last_step", None)
            else:
                element_value = value.value
                step = None

            if attribute_type == "file" or attribute_type == "file_series":
                row[column_name] = _create_output_file(
                    project_identifier=project_identifier,
                    file=element_value,
                    label=label,
                    index_column_name=index_column_name,
                    attribute_path=value.attribute_definition.name,
                    step=step,
                )
            elif attribute_type == "histogram" or attribute_type == "histogram_series":
                row[column_name] = _create_output_histogram(element_value)
            else:
                row[column_name] = element_value
        return row

    def transform_column_names(df: pd.DataFrame) -> pd.DataFrame:
        if type_suffix_in_column_names:
            return df

        # Transform the column by removing the type
        original_columns = df.columns
        df.columns = pd.Index([col.rsplit(":", 1)[0] for col in df.columns])

        # Check for duplicate names
        duplicated = df.columns.duplicated(keep=False)
        if duplicated.any():
            duplicated_names = df.columns[duplicated]
            duplicated_names_set = set(duplicated_names)
            conflicting_types: dict[str, set[str]] = {}
            for original_col, new_col in zip(original_columns, df.columns):
                if new_col in duplicated_names_set:
                    conflicting_types.setdefault(new_col, set()).add(original_col.rsplit(":", 1)[1])

            raise ConflictingAttributeTypes(conflicting_types.keys())  # TODO: add conflicting types to the exception

        return df

    rows = []
    for label, values in table_data.items():
        row: Any = convert_row(label, values)
        row[index_column_name] = label
        rows.append(row)

    dataframe = pd.DataFrame(rows)
    dataframe = transform_column_names(dataframe)
    dataframe.set_index(index_column_name, drop=True, inplace=True)

    dataframe.columns.name = "attribute"
    sorted_columns = sorted(dataframe.columns)
    dataframe = dataframe[sorted_columns]

    return dataframe


def create_metrics_dataframe(
    metrics_data: dict[identifiers.RunAttributeDefinition, list[metrics.FloatPointValue]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
    *,
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    index_column_name: str,
    timestamp_column_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Creates a memory-efficient DataFrame directly from FloatPointValue tuples.

    Note that `data_points` must be sorted by (experiment name, path) to ensure correct
    categorical codes.

    There is an intermediate processing step where we represent paths as categorical codes
    Example:

    Assuming there are 2 user columns called "foo" and "bar", 2 steps each. The intermediate
    DF will have this shape:

            experiment  path   step  value
        0     exp-name     0    1.0    0.0
        1     exp-name     0    2.0    0.5
        1     exp-name     1    1.0    1.5
        1     exp-name     1    2.0    2.5

    `path_mapping` would contain {"foo": 0, "bar": 1}. The column names will be restored before returning the
    DF, which then can be sorted based on its columns.

    The reason for the intermediate representation is that logging a metric called eg. "step" would conflict
    with our "step" column during df.reset_index(), and we would crash.
    Operating on integer codes is safe, as they can never appear as valid metric names.

    If `timestamp_column_name` is provided, timestamp will be included in the DataFrame under the
    specified column.
    """

    path_mapping: dict[str, int] = {}
    sys_id_mapping: dict[str, int] = {}
    label_mapping: list[str] = []

    for run_attr_definition in metrics_data:
        if run_attr_definition.run_identifier.sys_id not in sys_id_mapping:
            sys_id_mapping[run_attr_definition.run_identifier.sys_id] = len(sys_id_mapping)
            label_mapping.append(sys_id_label_mapping[run_attr_definition.run_identifier.sys_id])

        if run_attr_definition.attribute_definition.name not in path_mapping:
            path_mapping[run_attr_definition.attribute_definition.name] = len(path_mapping)

    def generate_categorized_rows() -> Generator[Tuple, None, None]:
        for attribute, points in metrics_data.items():
            exp_category = sys_id_mapping[attribute.run_identifier.sys_id]
            path_category = path_mapping[attribute.attribute_definition.name]

            for point in points:
                # Only include columns that we know we need. Note that the list of columns must match the
                # the list of `types` below.
                head = (
                    exp_category,
                    path_category,
                    point[StepIndex],
                    point[ValueIndex],
                )
                if include_point_previews and timestamp_column_name:
                    tail: Tuple[Any, ...] = (
                        point[TimestampIndex],
                        point[IsPreviewIndex],
                        point[PreviewCompletionIndex],
                    )
                elif timestamp_column_name:
                    tail = (point[TimestampIndex],)
                elif include_point_previews:
                    tail = (point[IsPreviewIndex], point[PreviewCompletionIndex])
                else:
                    tail = ()

                yield head + tail

    types = [
        (index_column_name, "uint32"),
        ("path", "uint32"),
        ("step", "float64"),
        ("value", "float64"),
    ]

    if timestamp_column_name:
        types.append((timestamp_column_name, "int64"))

    if include_point_previews:
        types.append(("is_preview", "bool"))
        types.append(("preview_completion", "float64"))

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(), dtype=types),
    )

    if timestamp_column_name:
        df[timestamp_column_name] = pd.to_datetime(df[timestamp_column_name], unit="ms", origin="unix", utc=True)

    df = _pivot_df(df, index_column_name, timestamp_column_name, extra_value_columns=types[4:])
    df = _restore_labels_in_index(df, index_column_name, label_mapping)
    df = _restore_path_column_names(df, path_mapping, "float_series" if type_suffix_in_column_names else None)
    df = _sort_index_and_columns(df, index_column_name)

    return df


def create_series_dataframe(
    series_data: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]],
    project_identifier: str,
    sys_id_label_mapping: dict[identifiers.SysId, str],
    index_column_name: str,
    timestamp_column_name: Optional[str],
) -> pd.DataFrame:
    experiment_mapping: dict[identifiers.SysId, int] = {}
    path_mapping: dict[str, int] = {}
    label_mapping: list[str] = []

    for run_attr_definition in series_data.keys():
        if run_attr_definition.run_identifier.sys_id not in experiment_mapping:
            experiment_mapping[run_attr_definition.run_identifier.sys_id] = len(experiment_mapping)
            label_mapping.append(sys_id_label_mapping[run_attr_definition.run_identifier.sys_id])

        if run_attr_definition.attribute_definition.name not in path_mapping:
            path_mapping[run_attr_definition.attribute_definition.name] = len(path_mapping)

    def convert_values(
        run_attribute_definition: identifiers.RunAttributeDefinition, values: list[series.SeriesValue]
    ) -> list[series.SeriesValue]:
        if run_attribute_definition.attribute_definition.type == "file_series":
            label = sys_id_label_mapping[run_attribute_definition.run_identifier.sys_id]
            return [
                series.SeriesValue(
                    step=point.step,
                    value=_create_output_file(
                        project_identifier=project_identifier,
                        file=point.value,
                        label=label,
                        index_column_name=index_column_name,
                        attribute_path=run_attribute_definition.attribute_definition.name,
                        step=point.step,
                    ),
                    timestamp_millis=point.timestamp_millis,
                )
                for point in values
            ]
        elif run_attribute_definition.attribute_definition.type == "histogram_series":
            return [
                series.SeriesValue(
                    step=point.step,
                    value=_create_output_histogram(point.value),
                    timestamp_millis=point.timestamp_millis,
                )
                for point in values
            ]
        else:
            return values

    def generate_categorized_rows() -> Generator[Tuple, None, None]:
        for attribute, values in series_data.items():
            exp_category = experiment_mapping[attribute.run_identifier.sys_id]
            path_category = path_mapping[attribute.attribute_definition.name]
            converted_values = convert_values(attribute, values)

            if timestamp_column_name:
                for point in converted_values:
                    yield exp_category, path_category, point.step, point.value, point.timestamp_millis
            else:
                for point in converted_values:
                    yield exp_category, path_category, point.step, point.value

    types = [
        (index_column_name, "uint32"),
        ("path", "uint32"),
        ("step", "float64"),
        ("value", "object"),
    ]
    if timestamp_column_name:
        types.append((timestamp_column_name, "int64"))

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(), dtype=types),
    )

    if timestamp_column_name:
        df[timestamp_column_name] = pd.to_datetime(df[timestamp_column_name], unit="ms", origin="unix", utc=True)

    df = _pivot_df(df, index_column_name, timestamp_column_name, extra_value_columns=types[4:])
    df = _restore_labels_in_index(df, index_column_name, label_mapping)
    df = _restore_path_column_names(df, path_mapping, None)
    df = _sort_index_and_columns(df, index_column_name)

    return df


def create_metric_buckets_dataframe(
    buckets_data: dict[identifiers.RunAttributeDefinition, list[metric_buckets.TimeseriesBucket]],
    sys_id_label_mapping: dict[identifiers.SysId, str],
    *,
    container_column_name: str,
) -> pd.DataFrame:
    """
    Output Example:

    experiment    experiment_1                                        experiment_2
    series        metrics/loss            metrics/accuracy            metrics/loss            metrics/accuracy
    bucket                   x          y                x          y            x          y               x          y
    (0.0, 20.0]       0.766337  46.899769         0.629231  29.418603     0.793347   3.618248        0.445641  16.923348
    (20.0, 40.0]     20.435899  42.001229        20.825488  11.989595    20.151307  21.244816       20.720397  20.515981
    (40.0, 60.0]     40.798869  10.429626        40.640794  10.276835    40.338434  33.692977       40.381568  15.954130
    (60.0, 80.0]     60.856616  20.633254        60.033832   0.927636    60.002655  37.048722       60.713322  49.537098
    (80.0, 100.0]    80.522183   6.084259        80.019450  39.666397    80.003379  22.569435       80.745987  42.658697
    """

    path_mapping: dict[str, int] = {}
    sys_id_mapping: dict[str, int] = {}
    label_mapping: list[str] = []

    for run_attr_definition in buckets_data:
        if run_attr_definition.run_identifier.sys_id not in sys_id_mapping:
            sys_id_mapping[run_attr_definition.run_identifier.sys_id] = len(sys_id_mapping)
            label_mapping.append(sys_id_label_mapping[run_attr_definition.run_identifier.sys_id])

        if run_attr_definition.attribute_definition.name not in path_mapping:
            path_mapping[run_attr_definition.attribute_definition.name] = len(path_mapping)

    def generate_categorized_rows() -> Generator[Tuple, None, None]:
        for attribute, buckets in buckets_data.items():
            exp_category = sys_id_mapping[attribute.run_identifier.sys_id]
            path_category = path_mapping[attribute.attribute_definition.name]

            buckets.sort(key=lambda b: (b.from_x, b.to_x))
            for ix, bucket in enumerate(buckets):
                yield (
                    exp_category,
                    path_category,
                    bucket.from_x,
                    bucket.to_x,
                    bucket.first_x if ix == 0 else bucket.last_x,
                    bucket.first_y if ix == 0 else bucket.last_y,
                )

    types = [
        (container_column_name, "uint32"),
        ("path", "uint32"),
        ("from_x", "float64"),
        ("to_x", "float64"),
        ("x", "float64"),
        ("y", "float64"),
    ]

    df = pd.DataFrame(
        np.fromiter(generate_categorized_rows(), dtype=types),
    )

    df["bucket"] = pd.IntervalIndex.from_arrays(df["from_x"], df["to_x"], closed="right")
    df = df.drop(columns=["from_x", "to_x"])

    df = df.pivot_table(
        index="bucket",
        columns=[container_column_name, "path"],
        values=["x", "y"],
        observed=True,
        dropna=True,
        sort=False,
    )

    df = _restore_labels_in_columns(df, container_column_name, label_mapping)
    df = _restore_path_column_names(df, path_mapping, None)

    # Add back any columns that were removed because they were all NaN
    if buckets_data:
        desired_columns = pd.MultiIndex.from_tuples(
            [
                (
                    dim,
                    sys_id_label_mapping[run_attr_definition.run_identifier.sys_id],
                    run_attr_definition.attribute_definition.name,
                )
                for run_attr_definition in buckets_data.keys()
                for dim in ("x", "y")
            ],
            names=["bucket", container_column_name, "metric"],
        )
        df = df.reindex(columns=desired_columns)
    else:
        # Handle empty case - create expected column structure
        df.columns = pd.MultiIndex.from_product([["x", "y"], [], []], names=["bucket", container_column_name, "metric"])

    df = df.reorder_levels([1, 2, 0], axis="columns")
    df = df.sort_index(axis="columns", level=[0, 1])
    df = df.sort_index()
    df.index.name = None
    df.columns.names = (container_column_name, "metric", "bucket")

    df = _collapse_open_buckets(df)

    return df


def _collapse_open_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """
    1st returned bucket is (-inf, first_point], which we merge with the 2nd bucket (first_point, end],
    resulting in a new bucket [first_point, end].
    If there's only one bucket, it should have form (first_point, inf). We transform it to [first_point, first_point].
    """
    df.index = df.index.astype(object)  # IntervalIndex cannot mix Intervals closed from different sides

    if df.index.empty:
        return df

    if len(df.index) == 1:
        finite_value = None
        if np.isfinite(df.index[0].right) and not np.isfinite(df.index[0].left):
            finite_value = df.index[0].right
        elif np.isfinite(df.index[0].left) and not np.isfinite(df.index[0].right):
            finite_value = df.index[0].left

        if finite_value is not None:
            new_interval = pd.Interval(left=finite_value, right=finite_value, closed="both")
            df.index = pd.Index([new_interval], dtype=object)
        return df

    col_funcs = {
        "x": lambda s: s[s.first_valid_index()] if s.first_valid_index() is not None else np.nan,
        "y": lambda s: s[s.first_valid_index()] if s.first_valid_index() is not None else np.nan,
    }

    first, second = df.index[0], df.index[1]
    if first.right >= second.left - second.length * 0.5:  # floats can be imprecise, we use bucket length as a tolerance
        new_interval = pd.Interval(left=first.right, right=second.right, closed="both")
        new_row = df.iloc[0:2].apply(axis="index", func=lambda col: col_funcs[col.name[-1]](col))
        df = df.drop(index=[first, second])
        df.loc[new_interval] = new_row
        df = df.sort_index()
    else:
        new_interval = pd.Interval(left=first.right, right=first.right + second.length, closed="both")
        df.index = [new_interval] + list(df.index[1:])

    return df


def _pivot_df(
    df: pd.DataFrame,
    index_column_name: str,
    timestamp_column_name: Optional[str],
    extra_value_columns: list[tuple[str, str]],
) -> pd.DataFrame:
    # Holds all existing (experiment, step) pairs
    # This is needed because pivot_table will remove rows if they are all NaN
    observed_idx = pd.MultiIndex.from_frame(
        df[[index_column_name, "step"]]
        .astype(
            {
                index_column_name: "uint32",
                "step": "float64",
            }
        )
        .drop_duplicates()
    )

    if df.empty and timestamp_column_name:
        # Handle empty DataFrame case to avoid pandas dtype errors
        df[timestamp_column_name] = pd.Series(dtype="datetime64[ns]")

    if extra_value_columns:
        # Holds all existing columns
        # This is needed because pivot_table will remove columns if they are all NaN
        value_columns = ["value"] + [col[0] for col in extra_value_columns]
        observed_columns = pd.MultiIndex.from_tuples(
            [(value, path) for path in df["path"].unique() for value in value_columns], names=[None, "path"]
        )

        # if there are multiple value columns, don't specify them and rely on pandas to create the column multi-index
        df = df.pivot_table(
            index=[index_column_name, "step"], columns="path", aggfunc="first", observed=True, dropna=True, sort=False
        )
    else:
        observed_columns = df["path"].unique()

        # when there's only "value", define values explicitly, to make pandas generate a flat index
        df = df.pivot_table(
            index=[index_column_name, "step"],
            columns="path",
            values="value",
            aggfunc="first",
            observed=True,
            dropna=True,
            sort=False,
        )

    # Add back any columns that were removed because they were all NaN
    return df.reindex(index=observed_idx, columns=observed_columns)


def _restore_labels_in_index(
    df: pd.DataFrame,
    column_name: str,
    label_mapping: list[str],
) -> pd.DataFrame:
    if df.index.empty:
        df.index = df.index.set_levels(df.index.get_level_values(column_name).astype(str), level=column_name)
        return df

    return df.rename(index={i: label for i, label in enumerate(label_mapping)}, level=column_name)


def _restore_labels_in_columns(
    df: pd.DataFrame,
    column_name: str,
    label_mapping: list[str],
) -> pd.DataFrame:
    if df.index.empty:
        df.columns = df.columns.set_levels(df.columns.get_level_values(column_name).astype(str), level=column_name)
        return df

    return df.rename(columns={i: label for i, label in enumerate(label_mapping)}, level=column_name)


def _restore_path_column_names(
    df: pd.DataFrame, path_mapping: dict[str, int], type_suffix: Optional[str]
) -> pd.DataFrame:
    """
    Accepts an DF in an intermediate format in _create_dataframe, and the mapping of column names.
    Restores colum names in the DF based on the mapping.
    """
    # No columns to rename, simply ensure the dtype of the path column changes from categorical int to str
    if df.columns.empty:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.set_levels(df.columns.get_level_values("path").astype(str), level="path")
        else:
            df.columns = df.columns.astype(str)
        return df

    # We need to reverse the mapping to index -> column name
    if type_suffix:
        reverse_mapping = {index: f"{path}:{type_suffix}" for path, index in path_mapping.items()}
    else:
        reverse_mapping = {index: path for path, index in path_mapping.items()}
    return df.rename(columns=reverse_mapping)


def _sort_index_and_columns(df: pd.DataFrame, index_column_name: str) -> pd.DataFrame:
    # MultiIndex DFs need to have column index order swapped: value/metric_name -> metric_name/value.
    # We also sort columns, but only after the original names have been restored.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns.names = (None, None)
        df = df.swaplevel(axis="columns")
        df = df.sort_index(axis="columns", level=0, kind="stable", sort_remaining=False)
    else:
        df.columns.name = None
        df = df.sort_index(axis="columns")

    return df.sort_index(axis="index", level=[index_column_name, "step"])


def create_files_dataframe(
    file_data: dict[types.File, Optional[pathlib.Path]],
    container_type: "ContainerType",
) -> pd.DataFrame:
    index_column_name = "experiment" if container_type == ContainerType.EXPERIMENT else "run"

    if not file_data:
        return pd.DataFrame(
            index=pd.MultiIndex.from_tuples([], names=[index_column_name, "step"]),
            columns=pd.Index([], name="attribute"),
        )

    rows: list[dict[str, Any]] = []
    for file, path in file_data.items():
        row = {
            index_column_name: file.container_identifier,
            "attribute": file.attribute_path,
            "step": file.step,
            "path": str(path) if path else None,
        }
        rows.append(row)

    dataframe = pd.DataFrame(rows)
    dataframe = dataframe.pivot(index=[index_column_name, "step"], columns="attribute", values="path")

    dataframe = dataframe.sort_index()
    sorted_columns = sorted(dataframe.columns)
    return dataframe[sorted_columns]


def _create_output_file(
    project_identifier: str,
    file: File,
    label: str,
    index_column_name: str,
    attribute_path: str,
    step: Optional[float] = None,
) -> types.File:
    run_id = label if index_column_name == "run" else None
    experiment_name = label if index_column_name == "experiment" else None
    return types.File(
        project_identifier=project_identifier,
        experiment_name=experiment_name,
        run_id=run_id,
        attribute_path=attribute_path,
        step=step,
        path=file.path,
        size_bytes=file.size_bytes,
        mime_type=file.mime_type,
    )


def _create_output_histogram(
    histogram: Histogram,
) -> types.Histogram:
    return types.Histogram(
        type=histogram.type,
        edges=histogram.edges,
        values=histogram.values,
    )
