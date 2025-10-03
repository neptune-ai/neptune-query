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
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
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
    """Create a wide metrics DataFrame while keeping peak allocations low.

    Data is materialized directly into columnar numpy arrays keyed by integer
    codes (both for experiments and metric paths). This avoids the
    pivot_table-based reshaping that used to fan out the intermediate
    DataFrame and spike memory usage.

    If `timestamp_column_name` is provided, absolute timestamps will be added
    under the requested column name (converted to UTC aware datetimes). When
    `include_point_previews` is enabled the preview flags are added alongside
    the metric values.
    """

    mappings = _build_run_and_path_mappings(metrics_data.keys(), sys_id_label_mapping)
    display_names = _resolve_path_display_names(
        mappings.path_to_index, type_suffix="float_series" if type_suffix_in_column_names else None
    )

    run_steps: dict[int, set[float]] = {}
    paths_with_data: set[int] = set()

    for attribute, points in metrics_data.items():
        if not points:
            continue

        exp_index = mappings.sys_id_to_index[attribute.run_identifier.sys_id]
        path_index = mappings.path_to_index[attribute.attribute_definition.name]
        paths_with_data.add(path_index)

        step_set = run_steps.setdefault(exp_index, set())
        for point in points:
            step_set.add(point[StepIndex])

    row_lookup = _build_row_lookup(run_steps, mappings.labels)

    num_rows = len(row_lookup.experiment_codes)
    path_order = sorted(paths_with_data, key=display_names.__getitem__)
    use_multi_columns = include_point_previews or timestamp_column_name is not None

    value_initializers: dict[str, Callable[[int], np.ndarray]] = {
        "value": _create_nan_float_array,
    }
    if timestamp_column_name:
        value_initializers[timestamp_column_name] = _create_nan_float_array
    if include_point_previews:
        value_initializers["is_preview"] = _create_nan_object_array
        value_initializers["preview_completion"] = _create_nan_float_array

    path_buffers = _initialize_path_buffers(num_rows, path_order, value_initializers)

    for attribute, points in metrics_data.items():
        if not points:
            continue

        exp_index = mappings.sys_id_to_index[attribute.run_identifier.sys_id]
        step_to_row_index = row_lookup.by_experiment.get(exp_index, {})
        if not step_to_row_index:
            continue

        path_index = mappings.path_to_index[attribute.attribute_definition.name]
        buffer = path_buffers.get(path_index)
        if buffer is None:
            continue

        columns = buffer.columns
        for point in points:
            row_idx = step_to_row_index.get(point[StepIndex])
            if row_idx is None:
                continue

            columns["value"][row_idx] = point[ValueIndex]

            if timestamp_column_name:
                timestamp_val = point[TimestampIndex]
                columns[timestamp_column_name][row_idx] = np.nan if timestamp_val is None else float(timestamp_val)

            if include_point_previews:
                columns["is_preview"][row_idx] = point[IsPreviewIndex]
                columns["preview_completion"][row_idx] = point[PreviewCompletionIndex]

    index = pd.MultiIndex.from_arrays(
        [row_lookup.experiment_codes, row_lookup.step_values], names=[index_column_name, "step"]
    )

    value_order = ["value"]
    if timestamp_column_name:
        value_order.append(timestamp_column_name)
    if include_point_previews:
        value_order.extend(["is_preview", "preview_completion"])

    df = _assemble_wide_dataframe(
        index=index,
        path_buffers=path_buffers,
        path_order=path_order,
        value_order=value_order,
        multiindex_columns=use_multi_columns,
    )

    if use_multi_columns and timestamp_column_name is not None:
        for column in df.columns:
            if column[1] == timestamp_column_name:
                df[column] = pd.to_datetime(df[column], unit="ms", origin="unix", utc=True)

    if use_multi_columns and include_point_previews:
        for column in df.columns:
            if column[1] != "is_preview":
                continue
            series = df[column]
            if series.isna().any():
                continue
            df[column] = series.astype(bool)

    df = _label_dataframe_index(df, index_column_name, mappings.labels)
    df = _label_dataframe_columns(df, display_names)

    return df


def create_series_dataframe(
    series_data: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]],
    project_identifier: str,
    sys_id_label_mapping: dict[identifiers.SysId, str],
    index_column_name: str,
    timestamp_column_name: Optional[str],
) -> pd.DataFrame:
    mappings = _build_run_and_path_mappings(series_data.keys(), sys_id_label_mapping)
    display_names = _resolve_path_display_names(mappings.path_to_index, None)

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
        if run_attribute_definition.attribute_definition.type == "histogram_series":
            return [
                series.SeriesValue(
                    step=point.step,
                    value=_create_output_histogram(point.value),
                    timestamp_millis=point.timestamp_millis,
                )
                for point in values
            ]
        return values

    converted_series: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]] = {}
    run_steps: dict[int, set[float]] = {}
    paths_with_data: set[int] = set()

    for attribute, values in series_data.items():
        converted_values = convert_values(attribute, values)
        converted_series[attribute] = converted_values

        if not converted_values:
            continue

        exp_index = mappings.sys_id_to_index[attribute.run_identifier.sys_id]
        path_index = mappings.path_to_index[attribute.attribute_definition.name]
        paths_with_data.add(path_index)

        step_set = run_steps.setdefault(exp_index, set())
        for point in converted_values:
            step_set.add(point.step)

    lookup = _build_row_lookup(run_steps, mappings.labels)

    num_rows = len(lookup.experiment_codes)
    path_order = sorted(paths_with_data, key=display_names.__getitem__)
    use_multi_columns = timestamp_column_name is not None

    value_initializers: dict[str, Callable[[int], np.ndarray]] = {
        "value": _create_nan_object_array,
    }
    if timestamp_column_name:
        value_initializers[timestamp_column_name] = _create_nan_float_array

    path_buffers = _initialize_path_buffers(num_rows, path_order, value_initializers)

    for attribute, converted_values in converted_series.items():
        if not converted_values:
            continue

        exp_index = mappings.sys_id_to_index[attribute.run_identifier.sys_id]
        row_map = lookup.by_experiment.get(exp_index, {})
        if not row_map:
            continue

        path_index = mappings.path_to_index[attribute.attribute_definition.name]
        buffer = path_buffers.get(path_index)
        if buffer is None:
            continue

        columns = buffer.columns
        for point in converted_values:
            row_idx = row_map.get(point.step)
            if row_idx is None:
                continue

            columns["value"][row_idx] = point.value

            if timestamp_column_name:
                timestamp_val = point.timestamp_millis
                columns[timestamp_column_name][row_idx] = np.nan if timestamp_val is None else float(timestamp_val)

    index = pd.MultiIndex.from_arrays([lookup.experiment_codes, lookup.step_values], names=[index_column_name, "step"])

    value_order = ["value"]
    if timestamp_column_name:
        value_order.append(timestamp_column_name)

    df = _assemble_wide_dataframe(
        index=index,
        path_buffers=path_buffers,
        path_order=path_order,
        value_order=value_order,
        multiindex_columns=use_multi_columns,
    )

    if use_multi_columns and timestamp_column_name:
        for column in df.columns:
            if column[1] == timestamp_column_name:
                df[column] = pd.to_datetime(df[column], unit="ms", origin="unix", utc=True)

    df = _label_dataframe_index(df, index_column_name, mappings.labels)
    df = _label_dataframe_columns(df, display_names)

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
    df = _restore_path_column_names(df, display_names=_resolve_path_display_names(path_mapping, None))

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


@dataclass(slots=True)
class RunPathMappings:
    """Precomputed indices for experiment identifiers and metric paths.

    The DataFrame assembly logic works with integer codes for both runs and
    attribute paths to minimise repeated dictionary lookups. This structure
    bundles those mappings together with the ordered list of human-readable
    labels that will be used when restoring the index.
    """

    sys_id_to_index: dict[identifiers.SysId, int]
    path_to_index: dict[str, int]
    labels: list[str]


@dataclass(slots=True)
class RowLookup:
    """Dense and sparse views of the target row positions.

    When converting point values into columnar buffers we must repeatedly ask
    "where should this (experiment, step) pair be written?"  The ``by_experiment``
    mapping provides O(1) access for that question, while ``experiment_codes`` and
    ``step_values`` retain the deterministic ordering needed to instantiate the
    final MultiIndex.
    """

    by_experiment: dict[int, dict[float, int]]
    experiment_codes: np.ndarray
    step_values: np.ndarray


@dataclass(slots=True)
class PathBuffer:
    """Container for all column arrays associated with a single path.

    Each metric path may expose several logical columns (value, timestamp,
    preview flags, ...). ``PathBuffer`` ensures the arrays for those columns are
    created exactly once and then shared when the DataFrame is materialized.
    """

    columns: dict[str, np.ndarray]


def _build_run_and_path_mappings(
    definitions: Iterable[identifiers.RunAttributeDefinition],
    sys_id_label_mapping: dict[identifiers.SysId, str],
) -> RunPathMappings:
    """Assign dense integer identifiers to experiments and metric paths.

    Parameters
    ----------
    definitions
        Iterable of attribute definitions that will appear in the final
        DataFrame. Duplicates are ignored while preserving deterministic index
        assignments.
    sys_id_label_mapping
        Resolver from ``SysId`` to the label requested by the caller. The order
        in which definitions are processed determines the order of ``labels``.

    Returns
    -------
    RunPathMappings
        Dataclass bundling compact indices for experiments and paths together
        with the ordered label list referenced later during index restoration.
    """
    path_mapping: dict[str, int] = {}

    for definition in definitions:
        path_name = definition.attribute_definition.name
        path_mapping.setdefault(path_name, len(path_mapping))

    sys_ids_sorted_by_label = sorted(sys_id_label_mapping.items(), key=lambda item: item[1])
    sys_id_mapping = {sys_id: index for index, (sys_id, _) in enumerate(sys_ids_sorted_by_label)}
    label_mapping = [label for _, label in sys_ids_sorted_by_label]

    return RunPathMappings(sys_id_mapping, path_mapping, label_mapping)


def _build_row_lookup(
    run_steps: dict[int, set[float]],
    label_mapping: list[str],
) -> RowLookup:
    """Translate the collected step sets into row-position lookups.

    Parameters
    ----------
    run_steps
        Mapping from experiment index to the set of steps observed for that
        experiment while traversing the raw points.
    label_mapping
        Ordered list of labels for each experiment. Only the length is required
        to size the outer lookup, but the reference clarifies intent.

    Returns
    -------
    RowLookup
        Dataclass exposing both a nested dictionary for fast writes and NumPy
        arrays that define the MultiIndex ordering.
    """
    row_lookup: dict[int, dict[float, int]] = {idx: {} for idx in range(len(label_mapping))}
    experiment_codes: list[int] = []
    step_values: list[float] = []

    for exp_index in range(len(label_mapping)):
        for step in sorted(run_steps.get(exp_index, ())):
            row_lookup[exp_index][step] = len(experiment_codes)
            experiment_codes.append(exp_index)
            step_values.append(step)

    experiment_codes_array = np.array(experiment_codes, dtype=np.uint32)
    step_values_array = np.array(step_values, dtype=np.float64)

    return RowLookup(row_lookup, experiment_codes_array, step_values_array)


def _resolve_path_display_names(path_mapping: dict[str, int], type_suffix: Optional[str]) -> dict[int, str]:
    """Map internal path indices to their human-readable column names."""

    if type_suffix:
        return {idx: f"{path}:{type_suffix}" for path, idx in path_mapping.items()}
    return {idx: path for path, idx in path_mapping.items()}


def _initialize_path_buffers(
    num_rows: int,
    path_order: list[int],
    value_initializers: dict[str, Callable[[int], np.ndarray]],
) -> dict[int, PathBuffer]:
    """Allocate column buffers for each metric path in advance.

    Parameters
    ----------
    num_rows
        Number of rows expected in the final DataFrame. Zero short-circuits the
        allocation and yields an empty dict.
    path_order
        Ordered list of path indices that should produce columns.
    value_initializers
        Mapping from column label to a callable returning an appropriately
        initialized NumPy array for ``num_rows`` entries.

    Returns
    -------
    dict[int, PathBuffer]
        Mapping from path index to ``PathBuffer`` cells containing the allocated
        arrays ready for in-place writes.
    """
    if num_rows == 0:
        return {}

    return {
        path_index: PathBuffer({key: initialize(num_rows) for key, initialize in value_initializers.items()})
        for path_index in path_order
    }


def _assemble_wide_dataframe(
    *,
    index: pd.MultiIndex,
    path_buffers: dict[int, PathBuffer],
    path_order: list[int],
    value_order: list[str],
    multiindex_columns: bool,
) -> pd.DataFrame:
    """Materialize the final DataFrame from the pre-allocated column buffers.

    Parameters
    ----------
    index
        MultiIndex representing the desired row structure; reused directly.
    path_buffers
        Mapping from path index to the ``PathBuffer`` containing column arrays.
    path_order
        Sequence dictating column ordering across metric paths.
    value_order
        Ordered list of value labels (e.g. ``"value"``, ``"absolute_time"``)
        that should appear for each path.
    multiindex_columns
        When ``True`` the resulting columns form a MultiIndex of ``(value, path)``;
        otherwise only the path dimension is used and the values remain flat.

    Returns
    -------
    pandas.DataFrame
        DataFrame whose columns share references with the provided buffers,
        ensuring no additional copies are made during construction.
    """
    data: dict[Any, np.ndarray] = {}
    column_keys: list[Any] = []

    if multiindex_columns:
        for path_index in path_order:
            buffer = path_buffers.get(path_index)
            if buffer is None:
                continue
            columns = buffer.columns
            for value_key in value_order:
                if value_key not in columns:
                    continue
                column_key = (path_index, value_key)
                column_keys.append(column_key)
                data[column_key] = columns[value_key]

        df = pd.DataFrame(data, index=index)
        if column_keys:
            df = df[column_keys]
            df.columns = pd.MultiIndex.from_tuples(column_keys, names=["path", None])
        else:
            df = pd.DataFrame(index=index)
            df.columns = pd.MultiIndex(
                levels=[pd.Index([], dtype=object), pd.Index(value_order, dtype=object)],
                codes=[[], []],
                names=["path", None],
            )
    else:
        for path_index in path_order:
            buffer = path_buffers.get(path_index)
            if buffer is None:
                continue
            column_keys.append(path_index)
            data[path_index] = buffer.columns["value"]

        df = pd.DataFrame(data, index=index)
        if column_keys:
            df = df[column_keys]
        else:
            df.columns = pd.Index([], dtype=str)

    return df


def _create_nan_float_array(rows: int) -> np.ndarray:
    """Return a float64 vector prefilled with ``np.nan`` values.

    Parameters
    ----------
    rows
        Desired length of the array.

    Returns
    -------
    numpy.ndarray
        One-dimensional float array initialized to NaN, suitable for in-place
        assignment of numeric values.
    """
    return np.full(rows, np.nan, dtype=np.float64)


def _create_nan_object_array(rows: int) -> np.ndarray:
    """Return an object vector prefilled with ``np.nan`` values.

    Parameters
    ----------
    rows
        Desired length of the array.

    Returns
    -------
    numpy.ndarray
        One-dimensional object array initialized to NaN. This is primarily used
        for columns storing Python objects (files, histograms, booleans with
        nullable semantics).
    """
    array = np.empty(rows, dtype=object)
    array[:] = np.nan
    return array


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


def _restore_labels_in_columns(
    df: pd.DataFrame,
    column_name: str,
    label_mapping: list[str],
) -> pd.DataFrame:
    if df.columns.empty:
        df.columns = df.columns.set_levels(df.columns.get_level_values(column_name).astype(str), level=column_name)
        return df

    return df.rename(columns={i: label for i, label in enumerate(label_mapping)}, level=column_name)


def _restore_path_column_names(df: pd.DataFrame, display_names: dict[int, str]) -> pd.DataFrame:
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

    level = "path" if isinstance(df.columns, pd.MultiIndex) else None
    return df.rename(columns=display_names, level=level)


def _label_dataframe_index(df: pd.DataFrame, index_name: str, labels: list[str]) -> pd.DataFrame:
    if df.index.empty:
        df.index = df.index.set_levels(df.index.levels[0].astype(str), level=0)
        return df
    return df.rename(index={i: label for i, label in enumerate(labels)}, level=index_name)


def _label_dataframe_columns(df: pd.DataFrame, display_names: dict[int, str]) -> pd.DataFrame:
    if df.columns.empty:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns.names = (None, None)
        else:
            df.columns.name = None
        return df

    level = "path" if isinstance(df.columns, pd.MultiIndex) else None
    df = df.rename(columns=display_names, level=level)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns.names = (None, None)
    else:
        df.columns.name = None
    return df


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
