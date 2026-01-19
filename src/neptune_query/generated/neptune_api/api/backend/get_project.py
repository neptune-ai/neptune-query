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

from http import HTTPStatus
from typing import (
    Any,
    cast,
)

import httpx

from ... import errors
from ...client import (
    AuthenticatedClient,
    Client,
)
from ...models.error import Error
from ...models.project_dto import ProjectDTO
from ...types import (
    UNSET,
    Response,
)


def _get_kwargs(
    *,
    project_identifier: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["projectIdentifier"] = project_identifier

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/backend/v1/projects/get",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | ProjectDTO | None:
    try:
        if response.status_code == 200:
            response_200 = ProjectDTO.from_dict(response.json())

            return response_200

        if response.status_code == 400:
            response_400 = Error.from_dict(response.json())

            return response_400

        if response.status_code == 401:
            response_401 = cast(Any, None)
            return response_401

        if response.status_code == 403:
            response_403 = cast(Any, None)
            return response_403

        if response.status_code == 404:
            response_404 = cast(Any, None)
            return response_404

        if response.status_code == 408:
            response_408 = cast(Any, None)
            return response_408

        if response.status_code == 422:
            response_422 = cast(Any, None)
            return response_422

    except Exception as e:
        raise errors.UnableToParseResponse(e, response) from e

    if True:
        if client.raise_on_unexpected_status:
            raise errors.UnexpectedStatus(response.status_code, response.content)
        else:
            return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | ProjectDTO]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    project_identifier: str,
) -> Response[Any | Error | ProjectDTO]:
    """
    Args:
        project_identifier (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ProjectDTO]
    """

    kwargs = _get_kwargs(
        project_identifier=project_identifier,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    project_identifier: str,
) -> Any | Error | ProjectDTO | None:
    """
    Args:
        project_identifier (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ProjectDTO
    """

    return sync_detailed(
        client=client,
        project_identifier=project_identifier,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    project_identifier: str,
) -> Response[Any | Error | ProjectDTO]:
    """
    Args:
        project_identifier (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ProjectDTO]
    """

    kwargs = _get_kwargs(
        project_identifier=project_identifier,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    project_identifier: str,
) -> Any | Error | ProjectDTO | None:
    """
    Args:
        project_identifier (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ProjectDTO
    """

    return (
        await asyncio_detailed(
            client=client,
            project_identifier=project_identifier,
        )
    ).parsed
