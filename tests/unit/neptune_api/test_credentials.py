import base64
import json
import uuid

import pytest

from neptune_query.generated.neptune_api.credentials import Credentials
from neptune_query.generated.neptune_api.errors import InvalidApiTokenException


def test_valid():
    # given
    data = {"api_address": "host", "api_url": "host", "api_key": str(uuid.uuid4())}
    api_key = base64.b64encode(json.dumps(data).encode("utf-8")).decode("utf-8")

    # when
    credentials = Credentials.from_api_key(api_key=api_key)

    # then
    assert credentials.base_url == "host"
    assert credentials.api_key == api_key


def test_invalid_api_key():
    # given
    api_key = "eyJ..."

    # then
    with pytest.raises(InvalidApiTokenException):
        Credentials.from_api_key(api_key=api_key)
