import pytest

from neptune_query.generated.neptune_api import Client
from neptune_query.generated.neptune_api.auth_helpers import exchange_api_key
from neptune_query.generated.neptune_api.errors import (
    ApiKeyRejectedError,
    UnableToExchangeApiKeyError,
    UnexpectedStatus,
)


def test_exchange_api_called(mocker, credentials, oauth_token):
    # given
    access_token, refresh_token = oauth_token.access_token, oauth_token.refresh_token

    client_mock = mocker.MagicMock(spec=Client)
    request_mock = mocker.MagicMock()
    client_mock.get_httpx_client.return_value = request_mock
    request_mock.request.return_value = mocker.MagicMock()
    request_mock.request.return_value.status_code = 200
    request_mock.request.return_value.json.return_value = {
        "accessToken": access_token,
        "refreshToken": refresh_token,
        "username": "henry",
    }

    # when
    token = exchange_api_key(client_mock, credentials)

    # then
    assert token.access_token == access_token
    assert token.refresh_token == refresh_token


@pytest.mark.parametrize(
    "status_code, expect_exception",
    (
        (400, UnableToExchangeApiKeyError),
        (401, ApiKeyRejectedError),
        (403, UnableToExchangeApiKeyError),
        (500, UnexpectedStatus),
    ),
)
def test_exchange_key_error(mocker, credentials, status_code, expect_exception):
    # given
    client_mock = mocker.MagicMock(spec=Client)
    request_mock = mocker.MagicMock()
    client_mock.get_httpx_client.return_value = request_mock
    request_mock.request.return_value = mocker.MagicMock()
    request_mock.request.return_value.status_code = status_code
    request_mock.request.return_value.json.return_value = {"code": status_code, "message": "Unauthorized"}

    # when
    with pytest.raises(expect_exception):
        exchange_api_key(client_mock, credentials)
