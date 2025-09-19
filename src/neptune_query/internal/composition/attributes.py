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
    Iterable,
    Optional,
    Tuple, AsyncGenerator,
)

from neptune_api.client import AuthenticatedClient

from .. import (
    env,
    filters,
    identifiers,
)
from ..composition import concurrency
from ..retrieval import attribute_definitions as att_defs
from ..retrieval import attribute_values as att_vals
from ..retrieval import util
from ..retrieval.attribute_filter import split_attribute_filters


async def fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._BaseAttributeFilter,
    batch_size: int = env.NEPTUNE_QUERY_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> AsyncGenerator[util.Page[identifiers.AttributeDefinition]]:
    pages_filters = _fetch_attribute_definitions(
        client, project_identifiers, run_identifiers, attribute_filter, batch_size
    )

    seen_items: set[identifiers.AttributeDefinition] = set()
    async for page, filter_ in pages_filters:
        new_items = [item for item in page.items if item not in seen_items]
        seen_items.update(new_items)
        yield util.Page(items=new_items)


async def _fetch_attribute_definitions(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._BaseAttributeFilter,
    batch_size: int,
) -> AsyncGenerator[tuple[util.Page[identifiers.AttributeDefinition], filters._AttributeFilter]]:
    def go_fetch_single(
        filter_: filters._AttributeFilter,
    ) -> AsyncGenerator[util.Page[identifiers.AttributeDefinition]]:
        return att_defs.fetch_attribute_definitions_single_filter(
            client=client,
            project_identifiers=project_identifiers,
            run_identifiers=run_identifiers,
            attribute_filter=filter_,
            batch_size=batch_size,
        )

    filters_ = split_attribute_filters(attribute_filter)

    items = concurrency.flat_map_sync(
        items=(filter_ for filter_ in filters_),
        downstream=lambda filter_: concurrency.flat_map_async_generator(
            items=go_fetch_single(filter_),
            downstream=lambda _page: concurrency.return_value((_page, filter_)),
        ),
    )

    async for item in items:
        yield item


async def fetch_attribute_values(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    run_identifiers: Iterable[identifiers.RunIdentifier],
    attribute_filter: filters._BaseAttributeFilter,
    batch_size: int = env.NEPTUNE_QUERY_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> AsyncGenerator[util.Page[att_vals.AttributeValue]]:
    pages_filters = _fetch_attribute_values(
        client, project_identifier, run_identifiers, attribute_filter, batch_size
    )

    seen_items: set[Tuple[identifiers.RunIdentifier, identifiers.AttributeDefinition]] = set()
    async for page in pages_filters:
        new_items = [item for item in page.items if (item.run_identifier, item.attribute_definition) not in seen_items]
        seen_items.update((item.run_identifier, item.attribute_definition) for item in new_items)
        yield util.Page(items=new_items)


async def _fetch_attribute_values(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    run_identifiers: Iterable[identifiers.RunIdentifier],
    attribute_filter: filters._BaseAttributeFilter,
    batch_size: int,
) -> AsyncGenerator[util.Page[att_vals.AttributeValue]]:
    def go_fetch_single(filter_: filters._AttributeFilter) -> AsyncGenerator[util.Page[att_vals.AttributeValue]]:
        return att_vals.fetch_attribute_values(
            client=client,
            project_identifier=project_identifier,
            run_identifiers=run_identifiers,
            attribute_definitions=filter_,
            batch_size=batch_size,
        )

    filters_ = split_attribute_filters(attribute_filter)

    items = concurrency.flat_map_sync(
        items=(filter_ for filter_ in filters_),
        downstream=go_fetch_single,
    )

    async for item in items:
        yield item
