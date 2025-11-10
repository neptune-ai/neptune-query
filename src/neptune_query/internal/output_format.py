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
import itertools
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generator,
    Literal,
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
    series,
)
from .retrieval.attribute_types import (
    TYPE_AGGREGATIONS,
    File,
    Histogram,
)
from .retrieval.attribute_values import AttributeValue
from .retrieval.metrics import MetricDatapoints
from .retrieval.search import ContainerType
from .util import _validate_allowed_value

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
    metrics_data: dict[identifiers.RunAttributeDefinition, MetricDatapoints],
    sys_id_label_mapping: dict[identifiers.SysId, str],
    *,
    type_suffix_in_column_names: bool,
    include_point_previews: bool,
    index_column_name: str,
    timestamp_column_name: Optional[Literal["absolute_time"]] = None,
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

    _validate_allowed_value(timestamp_column_name, ["absolute_time"], "timestamp_column_name")

    # TODO - to remove, as not needed and takes time
    # Ensure that all MetricDatapoints in metrics_data are sorted by their step values.
    # for _, datapoints in metrics_data.items():
    #     if datapoints.length <= 1:
    #         continue
    #     if np.any( datapoints.steps[1:] <  datapoints.steps[:-1]):
    #         raise ValueError("MetricDatapoints.steps must be sorted in non-decreasing order")



    def path_display_name(attr_def: identifiers.RunAttributeDefinition) -> str:
        return (
            f"{attr_def.attribute_definition.name}:float_series"
            if type_suffix_in_column_names
            else attr_def.attribute_definition.name
        )
    # TODO - I would recommend to sort it by name to avoid random order of columns
    paths_with_data: set[str] = {
        path_display_name(definition) for definition, metric_values in metrics_data.items() if metric_values.length > 0
    }

    sys_id_to_steps = defaultdict(list)
    for definition, metric_values in metrics_data.items():
        sys_id_to_steps[definition.run_identifier.sys_id].append(metric_values.steps)

    # More time-efficient implemention of np.unique(np.concatenate(step_arrays)
    def sort_and_unique(arrays, kind='mergesort'):
        emperical_threshold = 32 # optimization - number of arrays to split into two for recursion
        if len(arrays) > emperical_threshold:
            left = sort_and_unique(arrays[:len(arrays)//2])
            right = sort_and_unique(arrays[len(arrays)//2:])
            arrays = [left, right]
        c = np.concatenate((arrays))
        c.sort(kind=kind)
        flag = np.ones(len(c), dtype=bool)
        np.not_equal(c[1:], c[:-1], out=flag[1:])
        return c[flag]

    run_to_observed_steps: dict[str, np.ndarray] = {}
    for sys_id, step_arrays in sys_id_to_steps.items():
        run_to_observed_steps[sys_id_label_mapping[sys_id]] = sort_and_unique(step_arrays)

    del sys_id_to_steps

    index_data = IndexData.from_observed_steps(
        observed_steps=run_to_observed_steps,
        display_name_to_sys_id={v: k for k, v in sys_id_label_mapping.items()},
        index_level_labels=(index_column_name, "step"),
        access="vector",
    )

    del run_to_observed_steps


    # It is time to construct the dataframe
    rows_count = index_data.row_count()
    columns_per_attribute = 1 + (1 if timestamp_column_name else 0) + (2 if include_point_previews else 0)
    columns_count = columns_per_attribute * len(paths_with_data)
    
    column_suffixes_and_types = [("", "float64"),]
    if timestamp_column_name:
        column_suffixes_and_types.append((":absolute_time", np.float64))
    if include_point_previews:
        column_suffixes_and_types.append((":is_preview", bool))
        
    column_names = [
        name + suffix
        for name in paths_with_data
        for suffix, _ in column_suffixes_and_types
    ]

    # Prepare numpy_array mapping and dataframe
    is_uniform_dtype = len({type for _, type in column_suffixes_and_types}) == 1

    if is_uniform_dtype:
        # TODO:
        # np.empty is more appropriate here, but it is not supported by pandas
        raw_data = np.empty((rows_count, columns_count), dtype=column_suffixes_and_types[0][1])
        # TODO - works only with float64
        raw_data.fill(np.nan)
        attribute_to_ndarray = {}
        index = 0
        for name in paths_with_data:
            for suffix, _ in column_suffixes_and_types:
                attribute_to_ndarray[(name, suffix)] = raw_data[:, index]
                index += 1
        dataframe = pd.DataFrame(raw_data, columns=column_names, index=index_data.display_names, copy=False)
    else:
        # TODO - implement non-uniform column dtypes
        raise NotImplementedError("Non-uniform column dtypes are not supported yet")

    SUFFIX_TO_ATTRIBUTE = {
        "": "values",
        ":absolute_time": "timestamps",
        ":is_preview": "is_preview",
        ":preview_completion": "completion_ratio",
    }
    # Fill dataframe with data
    for definition, metric_values in metrics_data.items():
        if metric_values.length == 0:
            continue

        rows_indexes = index_data.lookup_row_vector(sys_id=definition.run_identifier.sys_id, steps=metric_values.steps)
        attribute_name = path_display_name(definition)
        for suffix, _ in column_suffixes_and_types:
            attribute_to_ndarray[(attribute_name, suffix)][rows_indexes] = getattr(metric_values, SUFFIX_TO_ATTRIBUTE[suffix])


    # # Preallocate column vectors for every logical value we might emit.
    # path_buffers: dict[str, PathBuffer] = _initialize_path_buffers(
    #     num_rows=index_data.row_count(),
    #     paths_with_data=paths_with_data,
    #     value_initializers=ValueInitializers(
    #         value=_create_nan_float_array,
    #         absolute_time=_create_nan_float_array if timestamp_column_name else None,
    #         is_preview=_create_nan_object_array if include_point_previews else None,
    #         preview_completion=_create_nan_float_array if include_point_previews else None,
    #     ),
    # )

    # # Write every metric point directly into the pre-allocated buffers.
    # for definition, metric_values in metrics_data.items():
    #     if metric_values.length == 0:
    #         continue

    #     rows = index_data.lookup_row_vector(sys_id=definition.run_identifier.sys_id, steps=metric_values.steps)

    #     buffer: PathBuffer = path_buffers[path_display_name(definition)]
    #     buffer.value[rows] = metric_values.values
    #     if buffer.absolute_time is not None:
    #         buffer.absolute_time[rows] = metric_values.timestamps
    #     if buffer.is_preview is not None:
    #         buffer.is_preview[rows] = metric_values.is_preview
    #     if buffer.preview_completion is not None:
    #         buffer.preview_completion[rows] = metric_values.completion_ratio

    # dataframe = _assemble_wide_dataframe(
    #     index_data=index_data,
    #     path_buffers=path_buffers,
    #     sub_columns=(
    #         ["value"]
    #         + ([timestamp_column_name] if timestamp_column_name else [])
    #         + (["is_preview", "preview_completion"] if include_point_previews else [])
    #     ),
    # )
    return dataframe


def create_series_dataframe(
    series_data: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]],
    project_identifier: str,
    sys_id_label_mapping: dict[identifiers.SysId, str],
    index_column_name: str,
    timestamp_column_name: Optional[Literal["absolute_time"]] = None,
) -> pd.DataFrame:

    _validate_allowed_value(timestamp_column_name, ["absolute_time"], "timestamp_column_name")

    def convert_values(
        run_attribute_definition: identifiers.RunAttributeDefinition, values: list[series.SeriesValue]
    ) -> list[series.SeriesValue]:
        if run_attribute_definition.attribute_definition.type == "file_series":
            label = sys_id_label_mapping[run_attribute_definition.run_identifier.sys_id]
            return [
                series.SeriesValue(
                    step=v.step,
                    value=_create_output_file(
                        project_identifier=project_identifier,
                        file=v.value,
                        label=label,
                        index_column_name=index_column_name,
                        attribute_path=run_attribute_definition.attribute_definition.name,
                        step=v.step,
                    ),
                    timestamp_millis=v.timestamp_millis,
                )
                for v in values
            ]
        if run_attribute_definition.attribute_definition.type == "histogram_series":
            return [
                series.SeriesValue(
                    step=v.step,
                    value=_create_output_histogram(v.value),
                    timestamp_millis=v.timestamp_millis,
                )
                for v in values
            ]
        return values

    converted_series: dict[identifiers.RunAttributeDefinition, list[series.SeriesValue]] = {}
    run_to_observed_steps: dict[str, set[float]] = dict()
    paths_with_data: set[str] = set()

    # Normalize raw payloads (files, histograms) while tracking observed dimensions.
    for definition, values in series_data.items():
        converted_values = convert_values(definition, values)
        converted_series[definition] = converted_values
        if not converted_values:
            continue

        paths_with_data.add(definition.attribute_definition.name)

        step_set = run_to_observed_steps.setdefault(sys_id_label_mapping[definition.run_identifier.sys_id], set())
        for point in converted_values:
            step_set.add(point.step)

    index_data = IndexData.from_observed_steps(
        observed_steps={run: np.fromiter(steps, dtype=np.float64) for run, steps in run_to_observed_steps.items()},
        display_name_to_sys_id={v: k for k, v in sys_id_label_mapping.items()},
        index_level_labels=(index_column_name, "step"),
        access="dict",
    )

    # Allocate column storage ahead of time for each path/value pair.
    path_buffers = _initialize_path_buffers(
        num_rows=index_data.row_count(),
        paths_with_data=paths_with_data,
        value_initializers=ValueInitializers(
            value=_create_nan_object_array,
            absolute_time=_create_nan_float_array if timestamp_column_name else None,
        ),
    )

    # Fill buffers row-by-row using the dense row lookup.
    for definition, converted_values in converted_series.items():
        if not converted_values:
            continue

        step_to_row_index: dict[float, int] = index_data.lookup_row_dict(sys_id=definition.run_identifier.sys_id)
        buffer = path_buffers[definition.attribute_definition.name]
        for point in converted_values:
            row_idx: int = step_to_row_index[point.step]
            buffer.value[row_idx] = point.value
            if buffer.absolute_time is not None:
                buffer.absolute_time[row_idx] = point.timestamp_millis

    return _assemble_wide_dataframe(
        index_data=index_data,
        path_buffers=path_buffers,
        sub_columns=["value"] + ([timestamp_column_name] if timestamp_column_name else []),
    )


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
    df = _restore_path_column_names(df, display_names={idx: path for path, idx in path_mapping.items()})

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
class IndexData:
    """Precomputed pieces needed to rebuild the MultiIndex for the result.

    ``display_names`` and ``step_values`` hold the ordered coordinates, while
    ``names`` stores the index level labels that should be applied to the
    finished DataFrame.

    The ``sys_id_offsets`` mapping stores, for every run, the contiguous slice
    of ``display_names``/``step_values`` that corresponds to that run. This
    allows efficient lookup of multiple row indices for a given run and a
    vector of steps without maintaining a secondary array of per-run vectors.
    """

    display_names: list[str]
    step_values: np.ndarray
    index_level_labels: tuple[str, str]
    sys_id_ranges: Optional[dict[identifiers.SysId, tuple[int, int]]]
    row_dict_lookup: Optional[dict[identifiers.SysId, dict[float, int]]]

    @classmethod
    def from_observed_steps(
        cls,
        observed_steps: dict[str, np.ndarray],
        display_name_to_sys_id: dict[str, identifiers.SysId],
        index_level_labels: tuple[str, str],
        access: Literal["vector", "dict"],
    ) -> "IndexData":
        """
        Importantly, this is where the sorting order of the index is defined.
        """

        sys_id_ranges: Optional[dict[identifiers.SysId, tuple[int, int]]] = {} if access == "vector" else None
        row_dict_lookup: Optional[dict[identifiers.SysId, dict[float, int]]] = {} if access == "dict" else None

        total_rows_count = sum(len(steps) for steps in observed_steps.values())
        display_names: list[str] = [""] * total_rows_count
        step_values: np.ndarray = np.empty(shape=(total_rows_count,), dtype=np.float64)
        row_num: int = 0

        for display_name, steps in sorted(observed_steps.items(), key=lambda x: x[0]):
            sys_id = display_name_to_sys_id[display_name]

            step_values[row_num:row_num + steps.size] = steps
            display_names[row_num:row_num + steps.size] = [display_name] * steps.size
            if sys_id_ranges is not None:
                sys_id_ranges[sys_id] = (row_num, row_num + steps.size)
            if row_dict_lookup is not None:
                row_dict_lookup[sys_id] = {float(step): idx for idx, step in enumerate(steps, start=row_num)}
            row_num += steps.size


        return cls(
            display_names=display_names,
            step_values=np.array(step_values, dtype=np.float64),
            index_level_labels=index_level_labels,
            sys_id_ranges=sys_id_ranges,
            row_dict_lookup=row_dict_lookup,
        )

    def row_count(self) -> int:
        """Return how many rows the index description represents."""

        return len(self.display_names)

    def lookup_row_vector(self, sys_id: identifiers.SysId, steps: np.ndarray) -> np.ndarray:
        if self.sys_id_ranges is None:
            raise RuntimeError("IndexData was not initialized with vector lookup support.")
        start, end = self.sys_id_ranges[sys_id]
        run_steps = self.step_values[start:end]
        
        # TO-OPTIMIZE: there is space for optimization, we can use fact that both are sorted and have O(n+m) complexity, but it is hard
        # to beat numpy implementation without going to C code.
        relative_rows = run_steps.searchsorted(steps)
        relative_rows += start
        return relative_rows

    def lookup_row_dict(self, sys_id: identifiers.SysId) -> dict[float, int]:
        if self.row_dict_lookup is None:
            raise RuntimeError("IndexData was not initialized with dict lookup support.")
        return self.row_dict_lookup[sys_id]


@dataclass(slots=True)
class ValueInitializers:
    """Factory callables for each logical sub-column emitted for a path."""

    value: Callable[[int], np.ndarray]
    absolute_time: Optional[Callable[[int], np.ndarray]] = None
    is_preview: Optional[Callable[[int], np.ndarray]] = None
    preview_completion: Optional[Callable[[int], np.ndarray]] = None


@dataclass(slots=True)
class PathBuffer:
    """Concrete NumPy arrays for the logical sub-columns of a path."""

    value: np.ndarray
    absolute_time: Optional[np.ndarray] = None
    is_preview: Optional[np.ndarray] = None
    preview_completion: Optional[np.ndarray] = None

    def get_data(self, subcolumn: str) -> Optional[np.ndarray | pd.Series | pd.api.extensions.ExtensionArray]:
        if subcolumn == "absolute_time":
            return pd.to_datetime(self.absolute_time, unit="ms", utc=True) if self.absolute_time is not None else None
        if subcolumn == "is_preview":
            return pd.array(self.is_preview, dtype="boolean") if self.is_preview is not None else None

        return getattr(self, subcolumn, None)


def _initialize_path_buffers(
    num_rows: int,
    paths_with_data: set[str],
    value_initializers: ValueInitializers,
) -> dict[str, PathBuffer]:
    """Allocate column arrays for each path using the provided factories."""
    if num_rows == 0:
        return {}

    def _maybe_initialize(initializer: Optional[Callable[[int], np.ndarray]]) -> Optional[np.ndarray]:
        return initializer(num_rows) if initializer is not None else None

    return {
        path: PathBuffer(
            value=value_initializers.value(num_rows),
            absolute_time=_maybe_initialize(value_initializers.absolute_time),
            is_preview=_maybe_initialize(value_initializers.is_preview),
            preview_completion=_maybe_initialize(value_initializers.preview_completion),
        )
        for path in paths_with_data
    }


def _assemble_wide_dataframe(
    *,
    index_data: IndexData,
    path_buffers: dict[str, PathBuffer],
    sub_columns: list[str],
) -> pd.DataFrame:
    """Attach each path buffer to the target index without copying data."""
    data: dict[Any, np.ndarray] = {}

    multiindex_columns = len(sub_columns) > 1
    if multiindex_columns:
        multi_column_keys: list[tuple[str, str]] = []
        for path in sorted(path_buffers.keys()):
            buffer: PathBuffer = path_buffers[path]
            for sub_column in sub_columns:
                multi_column_key: tuple[str, str] = (path, sub_column)
                data[multi_column_key] = buffer.get_data(sub_column)
                multi_column_keys.append(multi_column_key)

        columns = (
            pd.MultiIndex.from_tuples(multi_column_keys, names=[None, None])
            if multi_column_keys
            # The only significant difference between empty MultiIndex.from_tuples and from_product
            # is that the latter can better infer type of the 2nd level.
            else pd.MultiIndex.from_product([[], sub_columns], names=[None, None])
        )

    else:
        column_keys: list[str] = []
        for path in sorted(path_buffers.keys()):
            column_keys.append(path)
            data[path] = path_buffers[path].value

        columns = pd.Index(data=column_keys, dtype=object, name=None)

    index = pd.MultiIndex.from_arrays(
        arrays=[index_data.display_names, index_data.step_values],
        names=index_data.index_level_labels,
    )
    return pd.DataFrame(data=data, index=index, columns=columns, copy=True)


def _create_nan_float_array(size: int) -> np.ndarray:
    """Return a ``float64`` vector pre-filled with ``NaN`` values."""

    return np.full(size, np.nan, dtype=np.float64)


def _create_nan_object_array(size: int) -> np.ndarray:
    """Return an ``object`` vector pre-filled with ``NaN`` placeholders."""

    array = np.empty(size, dtype=object)
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
