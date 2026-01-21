import base64
import json
import uuid
from datetime import (
    datetime,
    timedelta,
    timezone,
)

import jwt
import pytest

from neptune_query.generated.neptune_api.credentials import Credentials
from neptune_query.generated.neptune_api.types import (
    MINIMAL_EXPIRATION_SECONDS,
    OAuthToken,
)

FIXED_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
EXPIRATION_TIME = FIXED_TIME + timedelta(seconds=MINIMAL_EXPIRATION_SECONDS + 1)


@pytest.fixture
def credentials() -> Credentials:
    data = {"api_address": "host", "api_url": "host", "api_key": str(uuid.uuid4())}
    base64_encoded = base64.b64encode(json.dumps(data).encode("utf-8")).decode("utf-8")
    return Credentials.from_api_key(base64_encoded)


@pytest.fixture
def oauth_token() -> OAuthToken:
    return OAuthToken.from_tokens(
        access=jwt.encode({"exp": datetime.timestamp(FIXED_TIME)}, "secret", algorithm="HS256"),
        refresh=jwt.encode({"exp": datetime.timestamp(FIXED_TIME)}, "secret", algorithm="HS256"),
    )


@pytest.fixture
def expired_oauth_token() -> OAuthToken:
    return OAuthToken.from_tokens(
        access=jwt.encode({"exp": datetime.timestamp(EXPIRATION_TIME)}, "secret", algorithm="HS256"),
        refresh=jwt.encode({"exp": datetime.timestamp(EXPIRATION_TIME)}, "secret", algorithm="HS256"),
    )
