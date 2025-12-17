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
#
from __future__ import annotations

import base64
import contextlib
import functools
import json
import logging
import secrets
from importlib.metadata import (
    PackageNotFoundError,
    version,
)
from typing import (
    Callable,
    Generator,
    Optional,
    ParamSpec,
    TypeVar,
)

from neptune_api.types import Response

from neptune_query.internal import env
from neptune_query.internal.composition import concurrency

# This flag is used to control whether query metadata should be added to the request headers
ADD_QUERY_METADATA = True

logger = logging.getLogger(__name__)


class QueryMetadata:
    def __init__(
        self,
        api_function: str,
        client_version: str,
        nq_query_id: str,
        user_data: str | None = None,
    ) -> None:

        # longest observed: "fetch_experiments_table_global" - 30 characters
        self.api_function = api_function[:32]

        # longest observed: "nq/1.8.0b1.post8+cea1d73" - 24 characters
        self.client_version = client_version[:24]

        # nq_query_id should be exactly 8 characters
        self.nq_query_id = nq_query_id[:8]

        # user_data may contribute at most 80 characters when JSON-encoded
        self.user_data: str | dict | None = _process_user_data(user_data)

    def to_json(self) -> str:
        return json.dumps(
            {
                "fn": self.api_function,
                "v": self.client_version,
                "qid": self.nq_query_id,
                "ud": self.user_data,
            }
        )


@contextlib.contextmanager
def use_query_metadata(api_function: str) -> Generator[None, None, None]:
    user_data = env.NEPTUNE_QUERY_METADATA.get()

    query_metadata = QueryMetadata(
        api_function=api_function,
        client_version=_get_client_version(),
        nq_query_id=_generate_nq_query_id(),
        user_data=user_data if user_data else None,
    )
    with concurrency.use_thread_local({"query_metadata": query_metadata}):
        yield


T = ParamSpec("T")
R = TypeVar("R")


def with_neptune_client_metadata(func: Callable[T, Response[R]]) -> Callable[T, Response[R]]:
    @functools.wraps(func)
    def wrapper(*args: T.args, **kwargs: T.kwargs) -> Response[R]:
        query_metadata: Optional[QueryMetadata] = concurrency.get_thread_local(
            "query_metadata", expected_type=QueryMetadata
        )
        if ADD_QUERY_METADATA and query_metadata:
            kwargs["x_neptune_client_metadata"] = query_metadata.to_json()
        return func(*args, **kwargs)

    return wrapper


@functools.cache
def _get_client_version() -> str:
    try:
        package_version = version("neptune-query")
    except PackageNotFoundError:
        package_version = "unknown"
    return f"nq/{package_version}"


@functools.cache
def _process_user_data(user_data: str | None) -> str | dict | None:
    if not user_data:
        return None

    try:
        user_data = json.loads(user_data)
        user_data_is_proper_json = True
    except json.JSONDecodeError:
        user_data_is_proper_json = False

    if len(json.dumps(user_data)) > 82:
        user_data = "NEPTUNE_QUERY_METADATA too long"
        if user_data_is_proper_json:
            logger.debug("User data in NEPTUNE_QUERY_METADATA env too long. Skipping user data in query metadata.")
        else:
            logger.debug(
                "User data in NEPTUNE_QUERY_METADATA env too long (and not JSON-encoded). "
                "Skipping user data in query metadata."
            )

    return user_data


def _generate_nq_query_id() -> str:
    # Generate a random string of exactly 8 URL-safe base64 characters (a-z, A-Z, 0-9, -, _):
    nq_query_id_bytes = secrets.token_bytes(6)
    nq_query_id = base64.urlsafe_b64encode(nq_query_id_bytes).decode("ascii")
    return nq_query_id
