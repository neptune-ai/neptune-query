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
#

__all__ = [
    "fetch_experiments_table_global",
    "fetch_runs_table_global",
]

from typing import (
    Literal,
    Optional,
    Union,
)

import pandas as _pandas

from neptune_query import filters
from neptune_query._internal import (
    resolve_attributes_filter,
    resolve_experiments_filter,
    resolve_runs_filter,
    resolve_sort_by,
)
from neptune_query.internal.composition import fetch_table as _fetch_table
from neptune_query.internal.experimental import experimental as experimental_api
from neptune_query.internal.query_metadata_context import use_query_metadata
from neptune_query.internal.retrieval import search as _search


@use_query_metadata(api_function="fetch_experiments_table_global")
@experimental_api
def fetch_experiments_table_global(
    *,
    experiments: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Union[str, list[str], filters.AttributeFilter] = [],
    sort_by: filters.Attribute = filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
) -> _pandas.DataFrame:
    """Fetches a table of experiment metadata, with runs as rows and attributes as columns.

    To narrow the results, define filters for experiments to search or attributes to include.

    Returns a DataFrame similar to the runs table in the web app.
    For series attributes, the last logged value is returned.

    Args:
        experiments: Filter specifying which experiments to include.
            If a string is provided, it's treated as a regex pattern that the names must match.
            If a list of strings is provided, it's treated as exact experiment names to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
            The filter must use only selected attributes from the sys/ and env/ namespaces. Attribute types
            must be provided explicitly in the filter (use filters.Attribute with a type).
            Ask your administrator for a list of supported attributes.
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        sort_by: Attribute to sort the table by. If specified, needs to specify the attribute type.
        sort_direction: The direction to sort columns by: `"desc"` (default) or `"asc"`.
        limit: Maximum number of experiments to return. By default, all experiments are included.
        type_suffix_in_column_names: If True, columns of the returned DataFrame
            are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
            If False (default), the method throws an exception if there are multiple types under one path.

    Example:
        Fetch attributes matching `loss` or `configs` from two specific experiments:
        ```
        import neptune_query.experimental as nq_experimental


        nq_experimental.fetch_experiments_table_global(
            experiments=["seagull-week1", "seagull-week2"],
            attributes=r"loss | configs",
        )
        ```
    """
    experiments_filter = resolve_experiments_filter(experiments)
    attributes_filter = resolve_attributes_filter(attributes)
    resolved_sort_by = resolve_sort_by(sort_by)

    return _fetch_table.fetch_table_global(
        filter_=experiments_filter,
        attributes=attributes_filter,
        sort_by=resolved_sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=_search.ContainerType.EXPERIMENT,
    )


@use_query_metadata(api_function="fetch_runs_table_global")
@experimental_api
def fetch_runs_table_global(
    *,
    runs: Optional[Union[str, list[str], filters.Filter]] = None,
    attributes: Union[str, list[str], filters.AttributeFilter] = [],
    sort_by: filters.Attribute = filters.Attribute("sys/creation_time", type="datetime"),
    sort_direction: Literal["asc", "desc"] = "desc",
    limit: Optional[int] = None,
    type_suffix_in_column_names: bool = False,
) -> _pandas.DataFrame:
    """Fetches a table of run metadata, with runs as rows and attributes as columns.

    To narrow the results, define filters for runs to search or attributes to include.

    Returns a DataFrame similar to the runs table in the web app.
    For series attributes, the last logged value is returned.

    Args:
        runs: Filter specifying which runs to include.
            If a string is provided, it's treated as a regex pattern that the run IDs must match.
            If a list of strings is provided, it's treated as exact run IDs to match.
            To provide a more complex condition on an arbitrary attribute value, pass a Filter object.
            The filter must use only attributes from the sys/ and env/ namespaces. Attribute types
            must be provided explicitly in the filter (use filters.Attribute with a type).
        attributes: Filter specifying which attributes to include.
            If a string is provided, it's treated as a regex pattern that the attribute names must match.
            If a list of strings is provided, it's treated as exact attribute names to match.
            To provide a more complex condition, pass an AttributeFilter object.
        sort_by: Attribute to sort the table by. If specified, needs to specify the attribute type.
        sort_direction: The direction to sort columns by: `"desc"` (default) or `"asc"`.
        limit: Maximum number of runs to return. By default, all runs are included.
        type_suffix_in_column_names: If True, columns of the returned DataFrame
            are suffixed with ":<type>", e.g. "attribute1:float_series", "attribute1:string".
            If False (default), the method throws an exception if there are multiple types under one path.

    Example:
        Fetch constituent runs of an experiment, with attributes matching `loss` or `configs` as columns:
        ```
        import neptune_query.experimental as nq_experimental
        from neptune_query.filters import Filter


        nq_experimental.fetch_runs_table_global(
            runs=Filter.eq("sys/name", "exp-week9"),
            attributes=r"loss | configs",
        )
        ```
    """
    runs_filter = resolve_runs_filter(runs)
    attributes_filter = resolve_attributes_filter(attributes)
    resolved_sort_by = resolve_sort_by(sort_by)

    return _fetch_table.fetch_table_global(
        filter_=runs_filter,
        attributes=attributes_filter,
        sort_by=resolved_sort_by,
        sort_direction=sort_direction,
        limit=limit,
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=_search.ContainerType.RUN,
    )
