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

from typing import (
    Callable,
    Dict,
    Optional,
)

import httpx

from neptune_query.generated.neptune_api import AuthenticatedClient
from neptune_query.generated.neptune_api.auth_helpers import exchange_api_key
from neptune_query.generated.neptune_api.credentials import Credentials

from .env import (
    NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS,
    NEPTUNE_VERIFY_SSL,
)


def create_auth_api_client(
    *,
    credentials: Credentials,
    proxies: Optional[Dict[str, str]] = None,
) -> AuthenticatedClient:
    return AuthenticatedClient(
        base_url=credentials.base_url,
        credentials=credentials,
        api_key_exchange_callback=exchange_api_key,
        verify_ssl=NEPTUNE_VERIFY_SSL.get(),
        httpx_args={"mounts": proxies, "http2": False},
        timeout=httpx.Timeout(NEPTUNE_HTTP_REQUEST_TIMEOUT_SECONDS.get()),
        headers={"User-Agent": _generate_user_agent()},
    )


_ILLEGAL_CHARS = str.maketrans({c: "_" for c in " ();/"})


def _generate_user_agent() -> str:
    import platform
    from importlib.metadata import version

    def sanitize(value: Callable[[], str]) -> str:
        try:
            result = value()
            return result.translate(_ILLEGAL_CHARS)
        except Exception:
            return "unknown"

    package_name = "neptune-query"
    package_version = sanitize(lambda: version(package_name))
    additional_metadata = {
        "neptune-api": sanitize(lambda: version("neptune-api")),
        "python": sanitize(platform.python_version),
        "os": sanitize(platform.system),
    }

    additional_metadata_str = "; ".join(f"{k}={v}" for k, v in additional_metadata.items())
    return f"{package_name}/{package_version} ({additional_metadata_str})"
