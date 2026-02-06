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

"""Contains some shared types for properties"""

__all__ = ["File", "Response", "FileJsonType", "Unset", "UNSET", "OAuthToken", "decode_token_without_verification"]

import time
from http import HTTPStatus
from typing import (
    Any,
    BinaryIO,
    Generic,
    Literal,
    MutableMapping,
    Optional,
    Tuple,
    TypeVar,
)

import jwt
from attrs import (
    define,
    field,
)

from .errors import InvalidApiTokenException

MINIMAL_EXPIRATION_SECONDS = 30
DECODING_OPTIONS = {
    "verify_signature": False,
    "verify_exp": False,
    "verify_nbf": False,
    "verify_iat": False,
    "verify_aud": False,
    "verify_iss": False,
}


def decode_token_without_verification(token: str) -> dict[str, Any]:
    try:
        decoded_token = jwt.decode(token, options=DECODING_OPTIONS)
    except jwt.InvalidTokenError as e:
        raise InvalidApiTokenException("Cannot decode the access token") from e

    if not isinstance(decoded_token, dict):
        raise InvalidApiTokenException("Cannot decode the access token")

    return decoded_token


class Unset:
    def __bool__(self) -> Literal[False]:
        return False


UNSET: Unset = Unset()

FileJsonType = Tuple[Optional[str], BinaryIO, Optional[str]]


@define
class File:
    """Contains information for file uploads"""

    payload: BinaryIO
    file_name: Optional[str] = None
    mime_type: Optional[str] = None

    def to_tuple(self) -> FileJsonType:
        """Return a tuple representation that httpx will accept for multipart/form-data"""
        return self.file_name, self.payload, self.mime_type


T = TypeVar("T")


@define
class Response(Generic[T]):
    """A response from an endpoint"""

    url: str
    status_code: HTTPStatus
    content: bytes
    headers: MutableMapping[str, str]
    parsed: Optional[T]


@define
class OAuthToken:
    _expiration_time: float = field(default=0.0, alias="expiration_time", kw_only=True)
    access_token: str
    refresh_token: str

    @classmethod
    def from_tokens(cls, access: str, refresh: str) -> "OAuthToken":
        # Decode the JWT to get expiration time
        try:
            decoded_token = decode_token_without_verification(access)
            expiration_time = float(decoded_token["exp"])
        except (KeyError, TypeError, ValueError) as e:
            raise InvalidApiTokenException("Cannot decode the access token") from e

        return OAuthToken(access_token=access, refresh_token=refresh, expiration_time=expiration_time)

    @property
    def seconds_left(self) -> float:
        return self._expiration_time - time.time() - MINIMAL_EXPIRATION_SECONDS

    @property
    def is_expired(self) -> bool:
        return self.seconds_left <= 0
