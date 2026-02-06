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

import base64
import json
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from unittest.mock import patch

import jwt
import pytest

from neptune_query.exceptions import (
    NeptuneFailedToFetchClientConfig,
    NeptuneInvalidCredentialsError,
)
from neptune_query.generated.neptune_api.credentials import Credentials
from neptune_query.generated.neptune_api.errors import ApiKeyRejectedError
from neptune_query.generated.neptune_api.types import OAuthToken
from neptune_query.internal.api_utils import create_auth_api_client

_JWT_TEST_SECRET = "test-secret-with-at-least-thirty-two-bytes"


def _make_credentials(base_url: str = "https://dev.neptune.internal.openai.org") -> Credentials:
    serialized = base64.b64encode(json.dumps({"api_address": base_url, "api_url": base_url}).encode("utf-8")).decode(
        "utf-8"
    )
    return Credentials.from_api_key(serialized)


def _make_oauth_token(
    iss: str = "https://issuer.example/auth/realms/neptune", azp: str = "neptune-cli", expires_in_seconds: int = 3600
) -> OAuthToken:
    expiration = int((datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)).timestamp())
    access_token = jwt.encode({"exp": expiration, "iss": iss, "azp": azp}, _JWT_TEST_SECRET, algorithm="HS256")
    refresh_token = jwt.encode({"exp": expiration}, _JWT_TEST_SECRET, algorithm="HS256")
    return OAuthToken.from_tokens(access=access_token, refresh=refresh_token)


def test_create_auth_api_client_uses_token_claims():
    # given
    credentials = _make_credentials(base_url="https://server.from-api-token.example")

    # when
    with patch("neptune_query.internal.api_utils.exchange_api_key") as exchange_mock:
        client = create_auth_api_client(credentials=credentials)

    # then
    exchange_mock.assert_not_called()
    assert client.client_id == ""
    assert (
        client.token_refreshing_endpoint == f"{credentials.base_url}/auth/realms/neptune/protocol/openid-connect/token"
    )
    assert client.initial_oauth_token is None


def test_create_auth_api_client_uses_token_claims_during_lazy_exchange():
    # given
    issuer = "https://issuer.example/auth/realms/another-realm"
    credentials = _make_credentials(base_url="https://server.from-api-token.example")
    exchanged_token = _make_oauth_token(iss=issuer, azp="neptune-cli")

    # when
    with patch("neptune_query.internal.api_utils.exchange_api_key", return_value=exchanged_token) as exchange_mock:
        client = create_auth_api_client(credentials=credentials)
        token = client.exchange_token()

    # then
    assert token is exchanged_token
    assert client.client_id == "neptune-cli"
    exchange_mock.assert_called_once_with(client=client, credentials=credentials)


def test_create_auth_api_client_raises_on_missing_claim_during_lazy_exchange():
    # given
    issuer = "https://dev.neptune.internal.openai.org/auth/realms/neptune"
    expiration = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    token_without_azp = OAuthToken.from_tokens(
        access=jwt.encode({"exp": expiration, "iss": issuer}, _JWT_TEST_SECRET, algorithm="HS256"),
        refresh=jwt.encode({"exp": expiration}, _JWT_TEST_SECRET, algorithm="HS256"),
    )
    credentials = _make_credentials()

    # when/then
    with patch("neptune_query.internal.api_utils.exchange_api_key", return_value=token_without_azp):
        client = create_auth_api_client(credentials=credentials)
        with pytest.raises(RuntimeError, match="Expected token claim 'azp'"):
            client.exchange_token()


def test_create_auth_api_client_raises_invalid_credentials_on_rejected_api_key_during_lazy_exchange():
    # given
    credentials = _make_credentials()

    # when/then
    with patch("neptune_query.internal.api_utils.exchange_api_key", side_effect=ApiKeyRejectedError):
        with pytest.raises(NeptuneInvalidCredentialsError):
            create_auth_api_client(credentials=credentials).exchange_token()


def test_create_auth_api_client_wraps_unexpected_exchange_errors_during_lazy_exchange():
    # given
    credentials = _make_credentials()

    # when/then
    with patch("neptune_query.internal.api_utils.exchange_api_key", side_effect=RuntimeError("boom")):
        with pytest.raises(NeptuneFailedToFetchClientConfig):
            create_auth_api_client(credentials=credentials).exchange_token()


def test_create_auth_api_client_uses_exchange_callback():
    # given
    issuer = "https://dev.neptune.internal.openai.org/auth/realms/neptune"
    credentials = _make_credentials()
    first_token = _make_oauth_token(iss=issuer, azp="neptune-cli")

    # when
    with patch("neptune_query.internal.api_utils.exchange_api_key", return_value=first_token) as exchange_mock:
        client = create_auth_api_client(
            credentials=credentials,
        )
        token = client.api_key_exchange_callback(client, credentials)

    # then
    assert token is first_token
    exchange_mock.assert_called_once_with(client=client, credentials=credentials)


def test_create_auth_api_client_preserves_exchanged_expired_token_during_lazy_exchange():
    # given
    issuer = "https://dev.neptune.internal.openai.org/auth/realms/neptune"
    credentials = _make_credentials()
    expired_first_token = _make_oauth_token(iss=issuer, azp="neptune-cli", expires_in_seconds=-1)

    # when
    with patch("neptune_query.internal.api_utils.exchange_api_key", return_value=expired_first_token):
        client = create_auth_api_client(
            credentials=credentials,
        )
        token = client.exchange_token()

    # then
    assert token is expired_first_token
    assert client.client_id == "neptune-cli"
