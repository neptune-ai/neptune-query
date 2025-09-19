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

from typing import (
    Optional, AsyncGenerator,
)

from neptune_api.client import AuthenticatedClient

from .. import (
    filters,
    identifiers,
)
from ..composition import concurrency
from ..composition.attributes import fetch_attribute_definitions
from ..retrieval import attribute_values as att_vals
from ..retrieval import (
    search,
    split,
    util,
)
from . import attributes


def fetch_attribute_definitions_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    attribute_filter: filters._BaseAttributeFilter,
    sys_ids: list[identifiers.SysId],
) -> AsyncGenerator[tuple[list[identifiers.SysId], util.Page[identifiers.AttributeDefinition]]]:
    return concurrency.flat_map_sync(
        items=split.split_sys_ids(sys_ids),
        downstream=lambda sys_ids_split: concurrency.map_async_generator(concurrency.flat_map_async_generator(
            items=fetch_attribute_definitions(
                client=client,
                project_identifiers=[project_identifier],
                run_identifiers=[identifiers.RunIdentifier(project_identifier, sys_id) for sys_id in sys_ids_split],
                attribute_filter=attribute_filter,
            ),
        ), lambda definitions: (sys_ids_split, definitions)),
    )


def fetch_attribute_definitions_complete(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    filter_: Optional[filters._Filter],
    attribute_filter: filters._BaseAttributeFilter,
    container_type: search.ContainerType,
) -> AsyncGenerator[util.Page[identifiers.AttributeDefinition]]:
    if container_type == search.ContainerType.RUN and filter_ is None:
        return fetch_attribute_definitions(
            client=client,
            project_identifiers=[project_identifier],
            run_identifiers=None,
            attribute_filter=attribute_filter,
        )
    else:
        return concurrency.flat_map_sync(
            items=search.fetch_sys_ids(
                client=client,
                project_identifier=project_identifier,
                filter_=filter_,
                container_type=container_type,
            ),
            downstream=lambda sys_ids_page: concurrency.map_async_generator(fetch_attribute_definitions_split(
                client=client,
                project_identifier=project_identifier,
                attribute_filter=attribute_filter,
                sys_ids=sys_ids_page.items,
            ), lambda pair: pair[1])
        )


def fetch_attribute_values_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    sys_ids: list[identifiers.SysId],
    attribute_definitions: list[identifiers.AttributeDefinition],
) -> AsyncGenerator[util.Page[att_vals.AttributeValue]]:
    return concurrency.flat_map_sync(
        items=split.split_sys_ids_attributes(sys_ids, attribute_definitions),
        downstream=lambda split_pair: att_vals.fetch_attribute_values(
            client=client,
            project_identifier=project_identifier,
            run_identifiers=[identifiers.RunIdentifier(project_identifier, s) for s in split_pair[0]],
            attribute_definitions=split_pair[1],
        ),
    )


def fetch_attribute_values_by_filter_split(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    sys_ids: list[identifiers.SysId],
    attribute_filter: filters._BaseAttributeFilter,
) -> AsyncGenerator[util.Page[att_vals.AttributeValue]]:
    return concurrency.flat_map_sync(
        items=split.split_sys_ids(sys_ids),
        downstream=lambda split: attributes.fetch_attribute_values(
            client=client,
            project_identifier=project_identifier,
            run_identifiers=[identifiers.RunIdentifier(project_identifier, s) for s in split],
            attribute_filter=attribute_filter,
        )
    )
