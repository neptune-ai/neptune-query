from http import HTTPStatus
from unittest.mock import Mock

import httpx
import pytest

from neptune_query.generated.neptune_api.api.backend import (
    exchange_api_token,
    get_client_config,
    get_project,
)
from neptune_query.generated.neptune_api.api.retrieval import query_attribute_definitions_within_project
from neptune_query.generated.neptune_api.errors import UnableToParseResponse
from neptune_query.generated.neptune_api.models import QueryAttributeDefinitionsBodyDTO


# Test for invalid JSON as well as empty JSON responses -> missing fields
@pytest.mark.parametrize("content", (b"not-json", b"{}"))
@pytest.mark.parametrize(
    "endpoint_module, kwargs",
    (
        (query_attribute_definitions_within_project, {"body": QueryAttributeDefinitionsBodyDTO()}),
        (get_project, {"project_identifier": "some/project"}),
        (get_client_config, {}),
        (exchange_api_token, {"x_neptune_api_token": "some-token"}),
    ),
)
def test_error_while_parsing_200_response(endpoint_module, kwargs, content):
    httpx_client = Mock()
    httpx_client.request.return_value = httpx.Response(
        request=httpx.Request("GET", "https://example.com/api/projects/some/project"),
        status_code=HTTPStatus(200),
        content=content,
        headers={},
    )

    client = Mock()
    client.get_httpx_client = Mock(return_value=httpx_client)

    kwargs["client"] = client

    with pytest.raises(UnableToParseResponse):
        endpoint_module.sync_detailed(**kwargs)


@pytest.mark.parametrize("content", (b"not-json", b"{}"))
@pytest.mark.parametrize(
    "endpoint_module, kwargs",
    (
        (get_project, {"project_identifier": ""}),
        (get_client_config, {}),
        (exchange_api_token, {"x_neptune_api_token": "some-token"}),
    ),
)
def test_error_while_parsing_400_response(endpoint_module, kwargs, content):
    """Test errors in parsing error responses, specifically 400 Bad Request -> the Error model."""
    httpx_client = Mock()
    httpx_client.request.return_value = httpx.Response(
        request=httpx.Request("GET", "https://example.com/api/projects/some/project"),
        status_code=HTTPStatus(400),
        content=content,
        headers={},
    )

    client = Mock()
    client.get_httpx_client = Mock(return_value=httpx_client)

    kwargs["client"] = client

    with pytest.raises(UnableToParseResponse):
        endpoint_module.sync_detailed(**kwargs)
