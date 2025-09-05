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
from dataclasses import dataclass
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
    Union,
)

from neptune_api.api.retrieval import query_attributes_within_project_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import QueryAttributesBodyDTO
from neptune_api.proto.neptune_pb.api.v1.model.attributes_pb2 import ProtoQueryAttributesResultDTO

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from .. import (
    env,
    filters,
    identifiers,
)
from ..logger import get_logger
from ..retrieval import (
    retry,
    util,
)
from ..retrieval.attribute_types import (
    extract_value,
    map_attribute_type_backend_to_python,
)
from .attribute_filter import transform_attribute_filter_into_params

logger = get_logger()


@dataclass(frozen=True)
class AttributeValue:
    attribute_definition: identifiers.AttributeDefinition
    value: Any
    run_identifier: identifiers.RunIdentifier


def fetch_attribute_values(
    client: AuthenticatedClient,
    project_identifier: identifiers.ProjectIdentifier,
    run_identifiers: Iterable[identifiers.RunIdentifier],
    attribute_definitions: Union[Iterable[identifiers.AttributeDefinition], filters._AttributeFilter],
    batch_size: int = env.NEPTUNE_QUERY_ATTRIBUTE_VALUES_BATCH_SIZE.get(),
) -> Generator[util.Page[AttributeValue], None, None]:
    attribute_definitions_set: Optional[set[identifiers.AttributeDefinition]] = None

    params: dict[str, Any] = {
        "nextPage": {"limit": batch_size},
    }
    if run_identifiers is not None:
        params["experimentIdsFilter"] = [str(e) for e in run_identifiers]

    if isinstance(attribute_definitions, filters._AttributeFilter):
        attribute_filter_params = transform_attribute_filter_into_params(attribute_definitions)
        params.update(attribute_filter_params)
    else:
        attribute_definitions_set = set(attribute_definitions)
        if not attribute_definitions_set or not run_identifiers:
            yield from []
            return
        params["attributeNamesFilter"] = [ad.name for ad in attribute_definitions_set]

    yield from util.fetch_pages(
        client=client,
        fetch_page=ft.partial(_fetch_attribute_values_page, project_identifier=project_identifier),
        process_page=ft.partial(
            _process_attribute_values_page,
            attribute_definitions_set=attribute_definitions_set,
            project_identifier=project_identifier,
        ),
        make_new_page_params=_make_new_attribute_values_page_params,
        params=params,
    )


def _fetch_attribute_values_page(
    client: AuthenticatedClient,
    params: dict[str, Any],
    project_identifier: identifiers.ProjectIdentifier,
) -> ProtoQueryAttributesResultDTO:
    logger.debug(
        f"Calling query_attributes_within_project_proto with project_identifier: {project_identifier}, params: {params}"
    )

    body = QueryAttributesBodyDTO.from_dict(params)
    call_api = retry.handle_errors_default(
        with_neptune_client_metadata(query_attributes_within_project_proto.sync_detailed)
    )
    response = call_api(client=client, body=body, project_identifier=project_identifier)

    logger.debug(
        f"query_attributes_within_project_proto response status: {response.status_code}, "
        f"content length: {len(response.content) if response.content else 'no content'}"
    )
    return ProtoQueryAttributesResultDTO.FromString(response.content)


def _process_attribute_values_page(
    data: ProtoQueryAttributesResultDTO,
    attribute_definitions_set: Optional[set[identifiers.AttributeDefinition]],
    project_identifier: identifiers.ProjectIdentifier,
) -> util.Page[AttributeValue]:
    items = []
    for entry in data.entries:
        run_identifier = identifiers.RunIdentifier(
            project_identifier=project_identifier, sys_id=identifiers.SysId(entry.experimentShortId)
        )

        for attr in entry.attributes:
            attr_definition = identifiers.AttributeDefinition(
                name=attr.name, type=map_attribute_type_backend_to_python(attr.type)
            )
            if attribute_definitions_set is not None and attr_definition not in attribute_definitions_set:
                continue

            item_value = extract_value(attr)
            if item_value is None:
                continue

            attr_value = AttributeValue(
                attribute_definition=attr_definition,
                value=item_value,
                run_identifier=run_identifier,
            )
            items.append(attr_value)

    return util.Page(items=items)


def _make_new_attribute_values_page_params(
    params: dict[str, Any], data: Optional[ProtoQueryAttributesResultDTO]
) -> Optional[dict[str, Any]]:
    if data is None:
        if "nextPageToken" in params["nextPage"]:
            del params["nextPage"]["nextPageToken"]
        return params

    next_page_token = data.nextPage.nextPageToken
    if not next_page_token:
        return None

    params["nextPage"]["nextPageToken"] = next_page_token
    return params
