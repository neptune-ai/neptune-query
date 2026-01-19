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
from io import BytesIO
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
from ...models.query_attributes_body_dto import QueryAttributesBodyDTO
from ...types import (
    UNSET,
    File,
    Response,
    Unset,
)


def _get_kwargs(
    *,
    body: QueryAttributesBodyDTO,
    project_identifier: str,
    x_neptune_client_metadata: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(x_neptune_client_metadata, Unset):
        headers["X-Neptune-Client-Metadata"] = x_neptune_client_metadata

    params: dict[str, Any] = {}

    params["projectIdentifier"] = project_identifier

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/leaderboard/v1/proto/leaderboard/attributes/query",
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | File | None:
    try:
        if response.status_code == 200:
            response_200 = File(payload=BytesIO(response.content))

            return response_200

        if response.status_code == 400:
            response_400 = cast(Any, None)
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

        if response.status_code == 409:
            response_409 = cast(Any, None)
            return response_409

        if response.status_code == 422:
            response_422 = cast(Any, None)
            return response_422

        if response.status_code == 429:
            response_429 = cast(Any, None)
            return response_429

    except Exception as e:
        raise errors.UnableToParseResponse(e, response) from e

    if True:
        if client.raise_on_unexpected_status:
            raise errors.UnexpectedStatus(response.status_code, response.content)
        else:
            return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | File]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryAttributesBodyDTO,
    project_identifier: str,
    x_neptune_client_metadata: str | Unset = UNSET,
) -> Response[Any | File]:
    """Queries attributes

    Args:
        project_identifier (str):
        x_neptune_client_metadata (str | Unset):
        body (QueryAttributesBodyDTO):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | File]
    """

    kwargs = _get_kwargs(
        body=body,
        project_identifier=project_identifier,
        x_neptune_client_metadata=x_neptune_client_metadata,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: QueryAttributesBodyDTO,
    project_identifier: str,
    x_neptune_client_metadata: str | Unset = UNSET,
) -> Any | File | None:
    """Queries attributes

    Args:
        project_identifier (str):
        x_neptune_client_metadata (str | Unset):
        body (QueryAttributesBodyDTO):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | File
    """

    return sync_detailed(
        client=client,
        body=body,
        project_identifier=project_identifier,
        x_neptune_client_metadata=x_neptune_client_metadata,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: QueryAttributesBodyDTO,
    project_identifier: str,
    x_neptune_client_metadata: str | Unset = UNSET,
) -> Response[Any | File]:
    """Queries attributes

    Args:
        project_identifier (str):
        x_neptune_client_metadata (str | Unset):
        body (QueryAttributesBodyDTO):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | File]
    """

    kwargs = _get_kwargs(
        body=body,
        project_identifier=project_identifier,
        x_neptune_client_metadata=x_neptune_client_metadata,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: QueryAttributesBodyDTO,
    project_identifier: str,
    x_neptune_client_metadata: str | Unset = UNSET,
) -> Any | File | None:
    """Queries attributes

    Args:
        project_identifier (str):
        x_neptune_client_metadata (str | Unset):
        body (QueryAttributesBodyDTO):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | File
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            project_identifier=project_identifier,
            x_neptune_client_metadata=x_neptune_client_metadata,
        )
    ).parsed
