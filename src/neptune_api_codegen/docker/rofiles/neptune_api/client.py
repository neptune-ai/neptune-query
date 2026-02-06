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

__all__ = ["Client", "AuthenticatedClient", "NeptuneAuthenticator"]

import logging
import ssl
import threading
import typing
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Optional,
    Union,
)

import httpx
from attrs import (
    define,
    evolve,
    field,
)

from .credentials import Credentials
from .errors import UnableToRefreshTokenError
from .types import (
    OAuthToken,
    decode_token_without_verification,
)

# Disable httpx logging, httpx logs requests at INFO level
logging.getLogger("httpx").setLevel(logging.WARN)


@define
class Client:
    """A class for keeping track of data related to the API

    The following are accepted as keyword arguments and will be used to construct httpx Clients internally:

        ``base_url``: The base URL for the API, all requests are made to a relative path to this URL

        ``cookies``: A dictionary of cookies to be sent with every request

        ``headers``: A dictionary of headers to be sent with every request

        ``timeout``: The maximum amount of a time a request can take. API functions will raise
        httpx.TimeoutException if this is exceeded.

        ``verify_ssl``: Whether or not to verify the SSL certificate of the API server. This should be True in production,
        but can be set to False for testing purposes.

        ``follow_redirects``: Whether or not to follow redirects. Default value is False.

        ``httpx_args``: A dictionary of additional arguments to be passed to the ``httpx.Client`` and ``httpx.AsyncClient`` constructor.


    Attributes:
        raise_on_unexpected_status: Whether or not to raise an errors.UnexpectedStatus if the API returns a
            status code that was not documented in the source OpenAPI document. Can also be provided as a keyword
            argument to the constructor.
    """

    raise_on_unexpected_status: bool = field(default=False, kw_only=True)
    _base_url: str = field(alias="base_url")
    _cookies: Dict[str, str] = field(factory=dict, kw_only=True, alias="cookies")
    _headers: Dict[str, str] = field(factory=dict, kw_only=True, alias="headers")
    _timeout: Optional[httpx.Timeout] = field(default=None, kw_only=True, alias="timeout")
    _verify_ssl: Union[str, bool, ssl.SSLContext] = field(default=True, kw_only=True, alias="verify_ssl")
    _follow_redirects: bool = field(default=False, kw_only=True, alias="follow_redirects")
    _httpx_args: Dict[str, Any] = field(factory=dict, kw_only=True, alias="httpx_args")
    _client: Optional[httpx.Client] = field(default=None, init=False)
    _async_client: Optional[httpx.AsyncClient] = field(default=None, init=False)

    def with_headers(self, headers: Dict[str, str]) -> "Client":
        """Get a new client matching this one with additional headers"""
        if self._client is not None:
            self._client.headers.update(headers)
        if self._async_client is not None:
            self._async_client.headers.update(headers)
        return evolve(self, headers={**self._headers, **headers})

    def with_cookies(self, cookies: Dict[str, str]) -> "Client":
        """Get a new client matching this one with additional cookies"""
        if self._client is not None:
            self._client.cookies.update(cookies)
        if self._async_client is not None:
            self._async_client.cookies.update(cookies)
        return evolve(self, cookies={**self._cookies, **cookies})

    def with_timeout(self, timeout: httpx.Timeout) -> "Client":
        """Get a new client matching this one with a new timeout (in seconds)"""
        if self._client is not None:
            self._client.timeout = timeout
        if self._async_client is not None:
            self._async_client.timeout = timeout
        return evolve(self, timeout=timeout)

    def set_httpx_client(self, client: httpx.Client) -> "Client":
        """Manually the underlying httpx.Client

        **NOTE**: This will override any other settings on the client, including cookies, headers, and timeout.
        """
        self._client = client
        return self

    def get_httpx_client(self) -> httpx.Client:
        """Get the underlying httpx.Client, constructing a new one if not previously set"""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self._base_url,
                cookies=self._cookies,
                headers=self._headers,
                timeout=self._timeout,
                verify=self._verify_ssl,
                follow_redirects=self._follow_redirects,
                **self._httpx_args,
            )
        return self._client

    def __enter__(self) -> "Client":
        """Enter a context manager for self.client—you cannot enter twice (see httpx docs)"""
        self.get_httpx_client().__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """Exit a context manager for internal httpx.Client (see httpx docs)"""
        self.get_httpx_client().__exit__(*args, **kwargs)

    def set_async_httpx_client(self, async_client: httpx.AsyncClient) -> "Client":
        """Manually the underlying httpx.AsyncClient

        **NOTE**: This will override any other settings on the client, including cookies, headers, and timeout.
        """
        self._async_client = async_client
        return self

    def get_async_httpx_client(self) -> httpx.AsyncClient:
        """Get the underlying httpx.AsyncClient, constructing a new one if not previously set"""
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(
                base_url=self._base_url,
                cookies=self._cookies,
                headers=self._headers,
                timeout=self._timeout,
                verify=self._verify_ssl,
                follow_redirects=self._follow_redirects,
                **self._httpx_args,
            )
        return self._async_client

    async def __aenter__(self) -> "Client":
        """Enter a context manager for underlying httpx.AsyncClient—you cannot enter twice (see httpx docs)"""
        await self.get_async_httpx_client().__aenter__()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        """Exit a context manager for underlying httpx.AsyncClient (see httpx docs)"""
        await self.get_async_httpx_client().__aexit__(*args, **kwargs)


@define
class AuthenticatedClient:
    """A Client which has been authenticated for use on secured endpoints

    The following are accepted as keyword arguments and will be used to construct httpx Clients internally:

        ``base_url``: The base URL for the API, all requests are made to a relative path to this URL

        ``cookies``: A dictionary of cookies to be sent with every request

        ``headers``: A dictionary of headers to be sent with every request

        ``timeout``: The maximum amount of a time a request can take. API functions will raise
        httpx.TimeoutException if this is exceeded.

        ``verify_ssl``: Whether or not to verify the SSL certificate of the API server. This should be True in production,
        but can be set to False for testing purposes.

        ``follow_redirects``: Whether or not to follow redirects. Default value is False.

        ``httpx_args``: A dictionary of additional arguments to be passed to the ``httpx.Client`` and ``httpx.AsyncClient`` constructor.


    Attributes:
        raise_on_unexpected_status: Whether or not to raise an errors.UnexpectedStatus if the API returns a
            status code that was not documented in the source OpenAPI document. Can also be provided as a keyword
            argument to the constructor.
        credentials: User credentials for authentication.
        token_refreshing_endpoint: Token refreshing endpoint url
        client_id: Client identifier for the OAuth application. If omitted and
            initial_oauth_token is provided, it is derived from token claim `azp`.
        initial_oauth_token: Initial OAuth token exchanged during client bootstrap.
        api_key_exchange_callback: The Neptune API Token exchange function
        prefix: The prefix to use for the Authorization header
        auth_header_name: The name of the Authorization header
    """

    raise_on_unexpected_status: bool = field(default=False, kw_only=True)
    _base_url: str = field(alias="base_url")
    _cookies: Dict[str, str] = field(factory=dict, kw_only=True, alias="cookies")
    _headers: Dict[str, str] = field(factory=dict, kw_only=True, alias="headers")
    _timeout: Optional[httpx.Timeout] = field(default=None, kw_only=True, alias="timeout")
    _verify_ssl: Union[str, bool, ssl.SSLContext] = field(default=True, kw_only=True, alias="verify_ssl")
    _follow_redirects: bool = field(default=False, kw_only=True, alias="follow_redirects")
    _httpx_args: Dict[str, Any] = field(factory=dict, kw_only=True, alias="httpx_args")
    _client: Optional[httpx.Client] = field(default=None, init=False)
    _async_client: Optional[httpx.AsyncClient] = field(default=None, init=False)

    credentials: Credentials
    token_refreshing_endpoint: str
    api_key_exchange_callback: Callable[[Union["AuthenticatedClient", Client], Credentials], OAuthToken]
    client_id: str = ""
    initial_oauth_token: Optional[OAuthToken] = None
    prefix: str = "Bearer"
    auth_header_name: str = "Authorization"

    def __attrs_post_init__(self) -> None:
        if self.client_id or self.initial_oauth_token is None:
            return
        self.client_id = _extract_client_id_from_access_token(self.initial_oauth_token.access_token)

    def with_headers(self, headers: Dict[str, str]) -> "AuthenticatedClient":
        """Get a new client matching this one with additional headers"""
        if self._client is not None:
            self._client.headers.update(headers)

        if self._async_client is not None:
            self._async_client.headers.update(headers)

        return evolve(self, headers={**self._headers, **headers})

    def with_cookies(self, cookies: Dict[str, str]) -> "AuthenticatedClient":
        """Get a new client matching this one with additional cookies"""
        if self._client is not None:
            self._client.cookies.update(cookies)

        if self._async_client is not None:
            self._async_client.cookies.update(cookies)

        return evolve(self, cookies={**self._cookies, **cookies})

    def with_timeout(self, timeout: httpx.Timeout) -> "AuthenticatedClient":
        """Get a new client matching this one with a new timeout (in seconds)"""
        if self._client is not None:
            self._client.timeout = timeout

        if self._async_client is not None:
            self._async_client.timeout = timeout

        return evolve(self, timeout=timeout)

    def set_httpx_client(self, client: httpx.Client) -> "AuthenticatedClient":
        """Manually the underlying httpx.Client

        **NOTE**: This will override any other settings on the client, including cookies, headers, and timeout.
        """
        self._client = client
        return self

    def exchange_token(self) -> OAuthToken:
        token = self.api_key_exchange_callback(self, self.credentials)
        if not self.client_id:
            self.client_id = _extract_client_id_from_access_token(token.access_token)
        return token

    def get_httpx_client(self) -> httpx.Client:
        """Get the underlying httpx.Client, constructing a new one if not previously set"""
        if self._client is None:
            self._client = httpx.Client(
                auth=NeptuneAuthenticator(
                    client_id=self.client_id,
                    token_refreshing_endpoint=self.token_refreshing_endpoint,
                    client=self,
                    initial_token=self.initial_oauth_token,
                ),
                base_url=self._base_url,
                cookies=self._cookies,
                headers=self._headers,
                timeout=self._timeout,
                verify=self._verify_ssl,
                follow_redirects=self._follow_redirects,
                **self._httpx_args,
            )
        return self._client

    def __enter__(self) -> "AuthenticatedClient":
        """Enter a context manager for self.client—you cannot enter twice (see httpx docs)"""
        self.get_httpx_client().__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """Exit a context manager for internal httpx.Client (see httpx docs)"""
        self.get_httpx_client().__exit__(*args, **kwargs)

    def set_async_httpx_client(self, async_client: httpx.AsyncClient) -> "AuthenticatedClient":
        """Manually the underlying httpx.AsyncClient

        **NOTE**: This will override any other settings on the client, including cookies, headers, and timeout.
        """
        # TODO: Missing implementation
        raise NotImplementedError

    def get_async_httpx_client(self) -> httpx.AsyncClient:
        """Get the underlying httpx.AsyncClient, constructing a new one if not previously set"""
        # TODO: Missing implementation
        raise NotImplementedError

    async def __aenter__(self) -> "AuthenticatedClient":
        """Enter a context manager for underlying httpx.AsyncClient—you cannot enter twice (see httpx docs)"""
        # TODO: Missing implementation
        raise NotImplementedError

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        """Exit a context manager for underlying httpx.AsyncClient (see httpx docs)"""
        # TODO: Missing implementation
        raise NotImplementedError


class NeptuneAuthenticator(httpx.Auth):
    __LOCK = threading.RLock()

    def __init__(
        self,
        client_id: str,
        token_refreshing_endpoint: str,
        client: AuthenticatedClient,
        initial_token: Optional[OAuthToken] = None,
    ):
        self._client_id: str = client_id
        self._token_refreshing_endpoint: str = token_refreshing_endpoint

        self._client = client
        self._token = initial_token if initial_token and not initial_token.is_expired else None

    def _refresh_existing_token(self) -> OAuthToken:
        if self._token is None:
            raise ValueError("Cannot refresh an empty token")
        try:
            response = self._client.get_httpx_client().post(
                url=self._token_refreshing_endpoint,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._token.refresh_token,
                    "client_id": self._client_id,
                    "expires_in": self._token.seconds_left,
                },
            )
            data = response.json()
            return OAuthToken.from_tokens(access=data["access_token"], refresh=data["refresh_token"])
        except Exception as e:
            raise UnableToRefreshTokenError("Unable to refresh token") from e

    def _refresh_token(self) -> None:
        with self.__LOCK:
            if self._token is not None:
                self._token = self._refresh_existing_token()

            if self._token is None:
                self._token = self._client.exchange_token()
                self._client_id = self._client.client_id

    def _refresh_token_if_expired(self) -> None:
        try:
            if self._token is None or self._token.is_expired:
                self._refresh_token()
        # Don't reset the token on network errors. Raise them immediately for the user to retry.
        except httpx.RequestError:
            raise
        # On any other error reset the token to None to force a new token retrieval
        except Exception:
            self._token = None
            self._refresh_token()

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if self._is_internal_auth_request(request):
            yield request
            return

        self._refresh_token_if_expired()

        if self._token is not None:
            request.headers["Authorization"] = f"Bearer {self._token.access_token}"

        yield request

    async def async_auth_flow(self, request: httpx.Request) -> typing.AsyncGenerator[httpx.Request, httpx.Response]:
        # TODO: Missing implementation
        yield request

    def _is_internal_auth_request(self, request: httpx.Request) -> bool:
        # API key exchange endpoint request carries this header.
        if "X-Neptune-Api-Token" in request.headers:
            return True

        request_url = str(request.url).rstrip("/")
        return request_url == self._token_refreshing_endpoint.rstrip("/")


def _extract_client_id_from_access_token(access_token: str) -> str:
    claims = decode_token_without_verification(access_token)
    client_id = claims.get("azp")
    if not isinstance(client_id, str) or not client_id:
        raise RuntimeError("Expected token claim 'azp' to be a non-empty string")
    return client_id
