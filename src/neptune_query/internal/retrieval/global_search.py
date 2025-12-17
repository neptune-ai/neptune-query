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
from __future__ import annotations

import functools as ft
from dataclasses import dataclass
from typing import (
    Generator,
    Literal,
    Optional,
)

import attrs
from neptune_api.api.retrieval import search_global_leaderboard_entries_proto
from neptune_api.client import AuthenticatedClient
from neptune_api.models import (
    AttributeTypeDTO,
    GlobalSearchParamsDTO,
    NqlQueryParamsDTO,
    QueryLeaderboardParamsFieldDTO,
    QueryLeaderboardParamsFieldDTOAggregationMode,
    QueryLeaderboardParamsPaginationDTO,
    QueryLeaderboardParamsSortingParamsDTO,
    QueryLeaderboardParamsSortingParamsDTODir,
)
from neptune_api.proto.neptune_pb.api.v1.model.leaderboard_entries_pb2 import (
    ProtoAttributesDTO,
    ProtoLeaderboardEntriesSearchResultDTO,
)
from neptune_api.types import UNSET

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from .. import env
from ..filters import (
    _Attribute,
    _Filter,
)
from ..identifiers import (
    CustomRunId,
    ProjectIdentifier,
    SysId,
    SysName,
)
from ..logger import get_logger
from ..retrieval import (
    retry,
    util,
)
from ..retrieval.attribute_types import map_attribute_type_python_to_backend
from .search import ContainerType

logger = get_logger()

__all__ = ("GlobalRunSearchEntry", "fetch_global_entries")


@dataclass(frozen=True)
class GlobalRunSearchEntry:
    sys_id: SysId
    sys_name: Optional[SysName]
    sys_custom_run_id: Optional[CustomRunId]
    project_identifier: ProjectIdentifier
    container_type: ContainerType

    @property
    def label(self) -> str:
        return str(self.sys_name) if self.container_type == ContainerType.EXPERIMENT else str(self.sys_custom_run_id)

    def __post_init__(self) -> None:
        if self.sys_id is None:
            raise ValueError(f"sys_id cannot be None in a global search entry ({self})")
        if self.container_type == ContainerType.RUN and self.sys_custom_run_id is None:
            raise ValueError(f"sys/custom_run_id missing for a global search entry representing run ({self})")
        if self.container_type == ContainerType.EXPERIMENT and self.sys_name is None:
            raise ValueError(f"sys/name missing for a global search entry representing experiment ({self})")


def fetch_global_entries(
    *,
    client: AuthenticatedClient,
    filter_: Optional[_Filter],
    sort_by: _Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: Optional[int],
    container_type: ContainerType,
) -> Generator[util.Page[GlobalRunSearchEntry], None, None]:
    batch_size = env.NEPTUNE_QUERY_SYS_ATTRS_BATCH_SIZE.get()

    yield from util.fetch_pages(
        client=client,
        fetch_page=_fetch_entries_page,
        process_page=ft.partial(_process_entries_page, container_type=container_type),
        make_new_page_params=ft.partial(_make_next_page_params, limit=limit, batch_size=batch_size),
        initial_params=GlobalSearchParamsDTO(
            experiment_leader=container_type == ContainerType.EXPERIMENT,
            pagination=QueryLeaderboardParamsPaginationDTO(
                limit=min(batch_size, limit) if limit is not None else batch_size,
                offset=0,
            ),
            query=NqlQueryParamsDTO(query=filter_.to_query()) if filter_ is not None else UNSET,
            sorting=QueryLeaderboardParamsSortingParamsDTO(
                sort_by=QueryLeaderboardParamsFieldDTO(
                    name=sort_by.name,
                    type=AttributeTypeDTO(
                        map_attribute_type_python_to_backend(sort_by.type) if sort_by.type is not None else "string"
                    ),
                    aggregation_mode=QueryLeaderboardParamsFieldDTOAggregationMode(sort_by.aggregation)
                    if sort_by.aggregation is not None
                    else UNSET,
                ),
                dir_=QueryLeaderboardParamsSortingParamsDTODir.ASCENDING
                if sort_direction == "asc"
                else QueryLeaderboardParamsSortingParamsDTODir.DESCENDING,
            ),
        ),
    )


def _fetch_entries_page(
    client: AuthenticatedClient,
    params: GlobalSearchParamsDTO,
) -> ProtoLeaderboardEntriesSearchResultDTO:
    logger.debug(f"Calling search_l_global_entries_proto with params: {params}")

    call_api = retry.handle_errors_default(
        with_neptune_client_metadata(search_global_leaderboard_entries_proto.sync_detailed)
    )
    response = call_api(client=client, body=params)

    logger.debug(
        f"search_l_global_entries_proto response status: {response.status_code}, "
        f"content length: {len(response.content) if response.content else 'no content'}"
    )
    return ProtoLeaderboardEntriesSearchResultDTO.FromString(response.content)


def _make_next_page_params(
    current_params: GlobalSearchParamsDTO,
    data: Optional[ProtoLeaderboardEntriesSearchResultDTO],
    *,
    limit: Optional[int],
    batch_size: int,
) -> Optional[GlobalSearchParamsDTO]:
    if data is None:
        return current_params

    # If the server returned fewer entries than we process in a single request, we've reached the end.
    if len(data.entries) < batch_size:
        return None

    retrieved_so_far = current_params.pagination.offset + batch_size
    if limit is not None and retrieved_so_far >= limit:
        return None

    return attrs.evolve(
        current_params,
        pagination=attrs.evolve(
            current_params.pagination,
            limit=min(limit - retrieved_so_far, batch_size) if limit is not None else batch_size,
            offset=retrieved_so_far,
        ),
    )


def _process_entries_page(
    data: ProtoLeaderboardEntriesSearchResultDTO,
    *,
    container_type: ContainerType,
) -> util.Page[GlobalRunSearchEntry]:
    def to_global_search_entry(entry: ProtoAttributesDTO) -> GlobalRunSearchEntry:
        attributes: dict[str, str] = {
            attribute.name: attribute.string_properties.value
            for attribute in entry.attributes
            if attribute.HasField("string_properties")
        }

        return GlobalRunSearchEntry(
            sys_id=SysId(attributes["sys/id"]),
            sys_name=SysName(attributes["sys/name"]) if attributes.get("sys/name") is not None else None,
            sys_custom_run_id=(
                CustomRunId(attributes["sys/custom_run_id"])
                if attributes.get("sys/custom_run_id") is not None
                else None
            ),
            project_identifier=ProjectIdentifier(f"{entry.organization_name}/{entry.project_name}"),
            container_type=container_type,
        )

    return util.Page(items=[to_global_search_entry(entry) for entry in data.entries])
