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

from dataclasses import dataclass
from http import HTTPStatus
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Literal,
    Optional,
    Union,
)

import httpx
from neptune_api import errors
from neptune_api.client import AuthenticatedClient
from neptune_api.types import (
    UNSET,
    Response,
    Unset,
)

from neptune_query.internal.query_metadata_context import with_neptune_client_metadata

from .. import (
    env,
    identifiers,
)
from ..filters import (
    _Attribute,
    _Filter,
)
from ..logger import get_logger
from ..retrieval import (
    retry,
    util,
)
from ..retrieval.attribute_types import map_attribute_type_python_to_backend
from . import search

logger = get_logger()

__all__ = ("GlobalRunSearchEntry", "fetch_global_entries")

_AGGREGATION_MAP: Dict[str, str] = {
    "last": "last",
    "min": "min",
    "max": "max",
    "average": "average",
    "variance": "variance",
}


@dataclass(frozen=True)
class GlobalRunSearchEntry:
    container_type: search.ContainerType
    project_identifier: identifiers.ProjectIdentifier
    sys_id: identifiers.SysId
    sys_name: Optional[identifiers.SysName]
    sys_custom_run_id: Optional[identifiers.CustomRunId]

    @property
    def label(self) -> str:
        if self.container_type == search.ContainerType.RUN:
            if self.sys_custom_run_id is None:
                raise RuntimeError("Expected sys/custom_run_id for run entry.")
            return self.sys_custom_run_id
        if self.sys_name is None:
            raise RuntimeError("Expected sys/name for experiment entry.")
        return self.sys_name


def fetch_global_entries(
    *,
    client: AuthenticatedClient,
    filter_: Optional[_Filter],
    sort_by: _Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: Optional[int],
    container_type: search.ContainerType,
    batch_size: int = env.NEPTUNE_QUERY_SYS_ATTRS_BATCH_SIZE.get(),
) -> Generator[util.Page[GlobalRunSearchEntry], None, None]:
    remaining = limit
    offset = 0

    while remaining is None or remaining > 0:
        current_limit = batch_size if remaining is None else min(batch_size, remaining)
        params = _build_params(
            filter_=filter_,
            sort_by=sort_by,
            sort_direction=sort_direction,
            limit=current_limit,
            offset=offset,
            container_type=container_type,
        )
        response = _fetch_entries_page(client=client, params=params)
        raw_entries = response.get("entries", []) if response else []
        entries = _convert_entries(raw_entries, container_type=container_type)
        fetched_count = len(raw_entries)

        if remaining is not None and len(entries) > remaining:
            entries = entries[:remaining]

        if entries:
            yield util.Page(items=entries)

        if fetched_count < current_limit:
            break

        if remaining is not None:
            remaining -= len(entries)
            if remaining <= 0:
                break

        offset += fetched_count


def _build_params(
    *,
    filter_: Optional[_Filter],
    sort_by: _Attribute,
    sort_direction: Literal["asc", "desc"],
    limit: int,
    offset: int,
    container_type: search.ContainerType,
) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "experimentLeader": container_type == search.ContainerType.EXPERIMENT,
        "pagination": {"limit": limit, "offset": offset},
        "sorting": {
            "sortBy": _build_sort_field(sort_by),
            "dir": _map_direction(sort_direction),
        },
    }

    if filter_ is not None:
        body["query"] = {"query": str(filter_)}

    return body


def _build_sort_field(sort_by: _Attribute) -> Dict[str, Any]:
    attribute_type_backend = (
        map_attribute_type_python_to_backend(sort_by.type) if sort_by.type is not None else "string"
    )

    field: Dict[str, Any] = {
        "name": sort_by.name,
        "type": attribute_type_backend,
    }

    if sort_by.aggregation is not None:
        aggregation = _AGGREGATION_MAP.get(sort_by.aggregation)
        if aggregation is not None:
            field["aggregationMode"] = aggregation

    return field


def _fetch_entries_page(
    *, client: AuthenticatedClient, params: Any  #: SearchUserRunsParamsDTO,  # type: ignore
) -> Any:  # LeaderboardEntriesSearchResultDTO:  # type: ignore
    call_api = retry.handle_errors_default(with_neptune_client_metadata(_call_search_user_runs))
    response = call_api(client=client, body=params)
    parsed = response.parsed

    if not isinstance(parsed, dict):
        logger.debug("searchUserRuns returned no parsed payload; defaulting to empty result.")
        return {"entries": [], "matchingItemCount": 0}

    return parsed


def _convert_entries(
    entries: Iterable[Dict[str, Any]],
    *,
    container_type: search.ContainerType,
) -> list[GlobalRunSearchEntry]:
    converted: list[GlobalRunSearchEntry] = []
    for entry in entries:
        entry_container_type = _map_container_type(str(entry.get("type", "")))
        if entry_container_type != container_type:
            continue

        attributes = _attributes_to_dict(entry.get("attributes", []))
        sys_id_value = attributes.get("sys/id") or entry.get("experimentId")
        if not sys_id_value:
            raise RuntimeError("Expected sys/id in global search response.")
        sys_id = identifiers.SysId(sys_id_value)
        organization_name = entry.get("organizationName", "")
        project_name = entry.get("projectName", "")
        project_identifier = identifiers.ProjectIdentifier(f"{organization_name}/{project_name}")

        sys_name = attributes.get("sys/name")
        sys_custom_run_id = attributes.get("sys/custom_run_id")

        global_entry = GlobalRunSearchEntry(
            sys_id=sys_id,
            sys_name=identifiers.SysName(sys_name) if sys_name is not None else None,
            sys_custom_run_id=identifiers.CustomRunId(sys_custom_run_id) if sys_custom_run_id is not None else None,
            project_identifier=project_identifier,
            container_type=container_type,
        )

        if container_type == search.ContainerType.RUN and global_entry.sys_custom_run_id is None:
            raise RuntimeError("Expected sys/custom_run_id for run entry in global search response.")
        if container_type == search.ContainerType.EXPERIMENT and global_entry.sys_name is None:
            raise RuntimeError("Expected sys/name for experiment entry in global search response.")

        converted.append(global_entry)

    return converted


def _map_container_type(entry_type: str) -> search.ContainerType:
    if entry_type.lower() == "run":
        return search.ContainerType.RUN
    return search.ContainerType.EXPERIMENT


def _attributes_to_dict(attributes: Iterable[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {}
    for attribute in attributes:
        name = attribute.get("name")
        if not name:
            continue
        value = _extract_string_attribute(attribute)
        if value is not None:
            result[name] = value
    return result


def _extract_string_attribute(attribute: Dict[str, Any]) -> Optional[str]:
    string_properties = attribute.get("stringProperties")
    if isinstance(string_properties, dict):
        value = string_properties.get("value")
        if value is not None:
            return str(value)
    return None


def _map_direction(direction: Literal["asc", "desc"]) -> str:
    return "ascending" if direction == "asc" else "descending"


def _call_search_user_runs(
    *,
    client: AuthenticatedClient,
    body: Dict[str, Any],
    x_neptune_client_metadata: Union[Unset, str] = UNSET,
) -> Response[Union[Any, Dict[str, Any]]]:
    kwargs = _get_search_user_runs_kwargs(body=body, x_neptune_client_metadata=x_neptune_client_metadata)
    response = client.get_httpx_client().request(**kwargs)
    return _build_search_user_runs_response(client=client, response=response)


def _get_search_user_runs_kwargs(
    *,
    body: Dict[str, Any],
    x_neptune_client_metadata: Union[Unset, str],
) -> Dict[str, Any]:
    headers: Dict[str, Any] = {}
    if not isinstance(x_neptune_client_metadata, Unset):
        headers["X-Neptune-Client-Metadata"] = x_neptune_client_metadata
    headers["Content-Type"] = "application/json"

    return {
        "method": "post",
        "url": "/api/leaderboard/v1/leaderboard/entries/searchUserRuns",
        "headers": headers,
        "json": body,
    }


def _build_search_user_runs_response(
    *,
    client: AuthenticatedClient,
    response: httpx.Response,
) -> Response[Union[Any, Dict[str, Any]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_search_user_runs_response(client=client, response=response),
    )


def _parse_search_user_runs_response(
    *,
    client: AuthenticatedClient,
    response: httpx.Response,
) -> Optional[Union[Any, Dict[str, Any]]]:
    try:
        if response.status_code == HTTPStatus.OK:
            return response.json()
        if response.status_code == HTTPStatus.BAD_REQUEST:
            return None
        if response.status_code == HTTPStatus.UNAUTHORIZED:
            return None
        if response.status_code == HTTPStatus.FORBIDDEN:
            return None
        if response.status_code == HTTPStatus.NOT_FOUND:
            return None
        if response.status_code == HTTPStatus.REQUEST_TIMEOUT:
            return None
        if response.status_code == HTTPStatus.CONFLICT:
            return None
        if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
            return None
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            return None
    except Exception as exc:
        raise errors.UnableToParseResponse(exc, response) from exc

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    return None
