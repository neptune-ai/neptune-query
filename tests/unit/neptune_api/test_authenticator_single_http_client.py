from datetime import (
    datetime,
    timedelta,
    timezone,
)
from urllib.parse import parse_qs

import httpx
import jwt

from neptune_query.generated.neptune_api import AuthenticatedClient
from neptune_query.generated.neptune_api.auth_helpers import exchange_api_key
from neptune_query.generated.neptune_api.credentials import Credentials
from neptune_query.generated.neptune_api.types import OAuthToken

_JWT_TEST_SECRET = "test-secret-with-at-least-thirty-two-bytes"


def _make_token_pair(*, expires_in_seconds: int = 3600) -> tuple[str, str]:
    expiration = int((datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)).timestamp())
    access_token = jwt.encode({"exp": expiration}, _JWT_TEST_SECRET, algorithm="HS256")
    refresh_token = jwt.encode({"exp": expiration}, _JWT_TEST_SECRET, algorithm="HS256")
    return access_token, refresh_token


def test_single_http_client_handles_api_key_exchange(credentials):
    valid_credentials = Credentials(api_key=credentials.api_key, base_url="https://api.neptune.ai")
    access_token, refresh_token = _make_token_pair()
    captured_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_paths.append(request.url.path)

        if request.url.path == "/api/backend/v1/authorization/oauth-token":
            assert request.headers["X-Neptune-Api-Token"] == valid_credentials.api_key
            assert "Authorization" not in request.headers
            return httpx.Response(
                status_code=200,
                json={
                    "accessToken": access_token,
                    "refreshToken": refresh_token,
                    "username": "test-user",
                },
            )

        if request.url.path == "/api/backend/v1/projects":
            assert request.headers["Authorization"] == f"Bearer {access_token}"
            return httpx.Response(status_code=200, json={})

        return httpx.Response(status_code=404, json={})

    client = AuthenticatedClient(
        base_url=valid_credentials.base_url,
        credentials=valid_credentials,
        client_id="neptune-cli",
        token_refreshing_endpoint=f"{valid_credentials.base_url}/auth/realms/neptune/protocol/openid-connect/token",
        api_key_exchange_callback=exchange_api_key,
        httpx_args={"transport": httpx.MockTransport(handler)},
    )

    response = client.get_httpx_client().get("/api/backend/v1/projects")

    assert response.status_code == 200
    assert captured_paths == ["/api/backend/v1/authorization/oauth-token", "/api/backend/v1/projects"]


def test_single_http_client_uses_initial_token_without_api_key_exchange(credentials):
    valid_credentials = Credentials(api_key=credentials.api_key, base_url="https://api.neptune.ai")
    initial_access, initial_refresh = _make_token_pair()
    initial_token = OAuthToken.from_tokens(access=initial_access, refresh=initial_refresh)
    captured_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_paths.append(request.url.path)

        if request.url.path == "/api/backend/v1/authorization/oauth-token":
            raise AssertionError("Did not expect API key exchange when initial token is present")

        if request.url.path == "/api/backend/v1/projects":
            assert request.headers["Authorization"] == f"Bearer {initial_access}"
            return httpx.Response(status_code=200, json={})

        return httpx.Response(status_code=404, json={})

    client = AuthenticatedClient(
        base_url=valid_credentials.base_url,
        credentials=valid_credentials,
        client_id="neptune-cli",
        token_refreshing_endpoint=f"{valid_credentials.base_url}/auth/realms/neptune/protocol/openid-connect/token",
        initial_oauth_token=initial_token,
        api_key_exchange_callback=exchange_api_key,
        httpx_args={"transport": httpx.MockTransport(handler)},
    )

    response = client.get_httpx_client().get("/api/backend/v1/projects")

    assert response.status_code == 200
    assert captured_paths == ["/api/backend/v1/projects"]


def test_single_http_client_does_not_reuse_expired_initial_token(credentials):
    valid_credentials = Credentials(api_key=credentials.api_key, base_url="https://api.neptune.ai")
    expired_access, expired_refresh = _make_token_pair(expires_in_seconds=-1)
    refreshed_access, refreshed_refresh = _make_token_pair()
    expired_token = OAuthToken.from_tokens(access=expired_access, refresh=expired_refresh)
    captured_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_paths.append(request.url.path)

        if request.url.path == "/api/backend/v1/authorization/oauth-token":
            assert request.headers["X-Neptune-Api-Token"] == valid_credentials.api_key
            assert "Authorization" not in request.headers
            return httpx.Response(
                status_code=200,
                json={
                    "accessToken": refreshed_access,
                    "refreshToken": refreshed_refresh,
                    "username": "test-user",
                },
            )

        if request.url.path == "/api/backend/v1/projects":
            assert request.headers["Authorization"] == f"Bearer {refreshed_access}"
            return httpx.Response(status_code=200, json={})

        return httpx.Response(status_code=404, json={})

    client = AuthenticatedClient(
        base_url=valid_credentials.base_url,
        credentials=valid_credentials,
        client_id="neptune-cli",
        token_refreshing_endpoint=f"{valid_credentials.base_url}/auth/realms/neptune/protocol/openid-connect/token",
        initial_oauth_token=expired_token,
        api_key_exchange_callback=exchange_api_key,
        httpx_args={"transport": httpx.MockTransport(handler)},
    )

    response = client.get_httpx_client().get("/api/backend/v1/projects")

    assert response.status_code == 200
    assert captured_paths == ["/api/backend/v1/authorization/oauth-token", "/api/backend/v1/projects"]


def test_single_http_client_refreshes_without_bearer_on_refresh_request(credentials):
    valid_credentials = Credentials(api_key=credentials.api_key, base_url="https://api.neptune.ai")
    initial_access, initial_refresh = _make_token_pair(expires_in_seconds=3600)
    refreshed_access, refreshed_refresh = _make_token_pair(expires_in_seconds=3600)
    token_endpoint_path = "/auth/realms/neptune/protocol/openid-connect/token"
    captured_paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_paths.append(request.url.path)

        if request.url.path == token_endpoint_path:
            assert "Authorization" not in request.headers
            assert "X-Neptune-Internal-Auth-Request" not in request.headers
            parsed_data = parse_qs(request.content.decode("utf-8"))
            assert parsed_data.get("grant_type") == ["refresh_token"]
            assert parsed_data.get("client_id") == ["neptune-cli"]
            assert parsed_data.get("refresh_token") == [initial_refresh]
            return httpx.Response(
                status_code=200,
                json={
                    "access_token": refreshed_access,
                    "refresh_token": refreshed_refresh,
                },
            )

        if request.url.path == "/api/backend/v1/projects":
            assert request.headers["Authorization"] == f"Bearer {refreshed_access}"
            return httpx.Response(status_code=200, json={})

        return httpx.Response(status_code=404, json={})

    client = AuthenticatedClient(
        base_url=valid_credentials.base_url,
        credentials=valid_credentials,
        client_id="neptune-cli",
        token_refreshing_endpoint=f"{valid_credentials.base_url}{token_endpoint_path}",
        api_key_exchange_callback=exchange_api_key,
        httpx_args={"transport": httpx.MockTransport(handler)},
    )

    authenticator = client.get_httpx_client()._auth
    assert authenticator is not None
    authenticator._token = OAuthToken.from_tokens(access=initial_access, refresh=initial_refresh)
    authenticator._token._expiration_time = 0  # force refresh path without relying on wall clock

    response = client.get_httpx_client().get("/api/backend/v1/projects")

    assert response.status_code == 200
    assert captured_paths == [token_endpoint_path, "/api/backend/v1/projects"]
