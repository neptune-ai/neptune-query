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
import functools as ft
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
)

from neptune_api.api.retrieval import query_attribute_definitions_within_project
from neptune_api.client import AuthenticatedClient
from neptune_api.models import (
    QueryAttributeDefinitionsBodyDTO,
    QueryAttributeDefinitionsResultDTO,
)

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from .. import filters  # noqa: E402
from .. import (  # noqa: E402
    env,
    identifiers,
)
from ..logger import get_logger
from ..retrieval import attribute_types as types  # noqa: E402
from ..retrieval import util  # noqa: E402
from ..retrieval import retry
from .attribute_filter import transform_attribute_filter_into_params

logger = get_logger()


def fetch_attribute_definitions_single_filter(
    client: AuthenticatedClient,
    project_identifiers: Iterable[identifiers.ProjectIdentifier],
    run_identifiers: Optional[Iterable[identifiers.RunIdentifier]],
    attribute_filter: filters._AttributeFilter,
    batch_size: int = env.NEPTUNE_QUERY_ATTRIBUTE_DEFINITIONS_BATCH_SIZE.get(),
) -> Generator[util.Page[identifiers.AttributeDefinition], None, None]:
    params: dict[str, Any] = {
        "projectIdentifiers": list(project_identifiers),
        "attributeNameFilter": dict(),
        "nextPage": {"limit": batch_size},
    }

    if run_identifiers is not None:
        params["experimentIdsFilter"] = [str(e) for e in run_identifiers]

    attribute_filter_params = transform_attribute_filter_into_params(attribute_filter)
    params.update(attribute_filter_params)

    return util.fetch_pages(
        client=client,
        fetch_page=_fetch_attribute_definitions_page,
        process_page=_process_attribute_definitions_page,
        make_new_page_params=ft.partial(_make_new_attribute_definitions_page_params, batch_size=batch_size),
        params=params,
    )


def _fetch_attribute_definitions_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
) -> QueryAttributeDefinitionsResultDTO:
    logger.debug(f"Calling query_attribute_definitions_within_project with params: {params}")

    body = QueryAttributeDefinitionsBodyDTO.from_dict(params)
    call_api = retry.handle_errors_default(
        with_neptune_client_metadata(query_attribute_definitions_within_project.sync_detailed)
    )
    response = call_api(client=client, body=body)

    logger.debug(
        f"query_attribute_definitions_within_project response status: {response.status_code}, "
        f"content length: {len(response.content) if response.content else 'no content'}"
    )

    return response.parsed


def _process_attribute_definitions_page(
    data: QueryAttributeDefinitionsResultDTO,
) -> util.Page[identifiers.AttributeDefinition]:
    items = []
    for entry in data.entries:
        item = identifiers.AttributeDefinition(
            name=entry.name,
            type=types.map_attribute_type_backend_to_python(str(entry.type)),
        )
        items.append(item)
    return util.Page(items=items)


def _make_new_attribute_definitions_page_params(
    params: dict[str, Any],
    data: Optional[QueryAttributeDefinitionsResultDTO],
    batch_size: int,
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.next_page.next_page_token
    if not next_page_token or len(data.entries) < batch_size:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params
