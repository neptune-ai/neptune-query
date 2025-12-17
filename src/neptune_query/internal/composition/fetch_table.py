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
from collections import defaultdict
from typing import (
    Generator,
    Literal,
    Optional,
)

import pandas as pd

from ...exceptions import NeptuneUserError
from .. import client as _client
from .. import context as _context
from .. import identifiers
from ..composition import attribute_components as _components
from ..composition import (
    concurrency,
    type_inference,
    validation,
)
from ..filters import (
    _Attribute,
    _BaseAttributeFilter,
    _Filter,
)
from ..identifiers import ProjectIdentifier
from ..output_format import (
    TableRow,
    create_runs_table,
    create_runs_table_multiproject,
)
from ..retrieval import attribute_values as att_vals
from ..retrieval import (
    global_search,
    search,
    util,
)

__all__ = ("fetch_table", "fetch_table_global")


def fetch_table(
    *,
    project_identifier: ProjectIdentifier,
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    sort_by: _Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: Optional[int],
    type_suffix_in_column_names: bool,
    context: Optional[_context.Context] = None,
    container_type: search.ContainerType,
) -> pd.DataFrame:
    validation.validate_limit(limit)
    _sort_direction = validation.validate_sort_direction(sort_direction)

    valid_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(context=valid_context)

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):

        inference_result = type_inference.infer_attribute_types_in_filter(
            client=client,
            project_identifier=project_identifier,
            filter_=filter_,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )
        filter_ = inference_result.get_result_or_raise()
        inference_result.emit_warnings()

        sort_by_inference_result = type_inference.infer_attribute_types_in_sort_by(
            client=client,
            project_identifier=project_identifier,
            sort_by=sort_by,
            fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
        )
        sort_by = sort_by_inference_result.result
        sort_by_inference_result.emit_warnings()

        sys_id_label_mapping: dict[identifiers.SysId, str] = {}
        result_by_id: dict[identifiers.SysId, list[att_vals.AttributeValue]] = {}

        def go_fetch_sys_attrs() -> Generator[list[identifiers.SysId], None, None]:
            for page in search.fetch_sys_id_labels(container_type)(
                client=client,
                project_identifier=project_identifier,
                filter_=filter_,
                sort_by=sort_by,
                sort_direction=_sort_direction,
                limit=limit,
            ):
                sys_ids = []
                for item in page.items:
                    result_by_id[item.sys_id] = []  # I assume that dict preserves the order set here
                    sys_id_label_mapping[item.sys_id] = item.label
                    sys_ids.append(item.sys_id)
                yield sys_ids

        output = concurrency.generate_concurrently(
            items=go_fetch_sys_attrs(),
            executor=executor,
            downstream=lambda sys_ids: _components.fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attributes,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids,
                downstream=lambda sys_ids_split, definitions_page: _components.fetch_attribute_values_split(
                    client=client,
                    project_identifier=project_identifier,
                    executor=executor,
                    sys_ids=sys_ids_split,
                    attribute_definitions=definitions_page.items,
                    downstream=concurrency.return_value,
                ),
            ),
        )

        attribute_value_pages: Generator[util.Page[att_vals.AttributeValue], None, None] = concurrency.gather_results(
            output
        )
        for attribute_values_page in attribute_value_pages:
            for attribute_value in attribute_values_page.items:
                result_by_id[attribute_value.run_identifier.sys_id].append(attribute_value)

    return create_runs_table(
        table_rows=[
            TableRow(values=values, label=sys_id_label_mapping[sys_id], project_identifier=project_identifier)
            for sys_id, values in result_by_id.items()
        ],
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=container_type,
    )


def fetch_table_global(
    *,
    filter_: Optional[_Filter],
    attributes: _BaseAttributeFilter,
    sort_by: _Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: Optional[int],
    type_suffix_in_column_names: bool,
    context: Optional[_context.Context] = None,
    container_type: search.ContainerType,
) -> pd.DataFrame:
    validation.validate_limit(limit)
    _sort_direction = validation.validate_sort_direction(sort_direction)

    if sort_by.type is None:
        raise NeptuneUserError(
            "Cannot resolve sort_by attribute type. Please pass filters.Attribute with an explicit type."
        )

    type_inference.ensure_attribute_types_provided_in_filter(filter_)

    valid_context = _context.validate_context(context or _context.get_context())
    client = _client.get_client(context=valid_context)

    # Order is preserved here
    table_rows: dict[identifiers.RunIdentifier, TableRow] = {}

    with (
        concurrency.create_thread_pool_executor() as executor,
        concurrency.create_thread_pool_executor() as fetch_attribute_definitions_executor,
    ):

        def go_fetch_entries() -> Generator[tuple[identifiers.ProjectIdentifier, list[identifiers.SysId]], None, None]:
            for page in global_search.fetch_global_entries(
                client=client,
                filter_=filter_,
                sort_by=sort_by,
                sort_direction=_sort_direction,
                limit=limit,
                container_type=container_type,
            ):
                entries_by_project: dict[identifiers.ProjectIdentifier, list[identifiers.SysId]] = defaultdict(list)
                for entry in page.items:
                    entries_by_project[entry.project_identifier].append(entry.sys_id)
                    table_rows[identifiers.RunIdentifier(entry.project_identifier, entry.sys_id)] = TableRow(
                        values=[],
                        label=entry.label,
                        project_identifier=entry.project_identifier,
                    )

                for project_identifier, sys_ids in entries_by_project.items():
                    yield project_identifier, sys_ids

        def _fetch_attribute_values(
            project_identifier: identifiers.ProjectIdentifier,
            sys_ids: list[identifiers.SysId],
        ) -> concurrency.OUT:
            return _components.fetch_attribute_values_by_filter_split(
                client=client,
                project_identifier=project_identifier,
                executor=executor,
                fetch_attribute_definitions_executor=fetch_attribute_definitions_executor,
                sys_ids=sys_ids,
                attribute_filter=attributes,
                downstream=concurrency.return_value,
            )

        output = concurrency.generate_concurrently(
            items=go_fetch_entries(),
            executor=executor,
            downstream=lambda item: _fetch_attribute_values(project_identifier=item[0], sys_ids=item[1]),
        )

        attribute_value_pages: Generator[util.Page[att_vals.AttributeValue], None, None] = concurrency.gather_results(
            output
        )

        for attribute_values_page in attribute_value_pages:
            for attribute_value in attribute_values_page.items:
                table_rows[attribute_value.run_identifier].values.append(attribute_value)

    return create_runs_table_multiproject(
        table_rows=list(table_rows.values()),
        type_suffix_in_column_names=type_suffix_in_column_names,
        container_type=container_type,
    )
