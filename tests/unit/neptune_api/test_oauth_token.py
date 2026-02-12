import pytest
from freezegun import freeze_time

from neptune_query.generated.neptune_api.errors import InvalidApiTokenException
from neptune_query.generated.neptune_api.types import OAuthToken


def test_invalid():
    # given
    access_token = "eyJ"
    refresh_token = ""

    # then
    with pytest.raises(InvalidApiTokenException):
        OAuthToken.from_tokens(access=access_token, refresh=refresh_token)


@freeze_time("2024-01-02 03:03:34 UTC")
def test_almost_expired(oauth_token):
    # given
    access_token, refresh_token = oauth_token.access_token, oauth_token.refresh_token

    # when
    token = OAuthToken.from_tokens(access=access_token, refresh=refresh_token)

    # then
    assert token.seconds_left == 1
    assert token.is_expired is False
    assert token.token_endpoint.endswith("/protocol/openid-connect/token")


@freeze_time("2024-01-02 03:03:35 UTC")
def test_expired_equal(oauth_token):
    # given
    access_token, refresh_token = oauth_token.access_token, oauth_token.refresh_token

    # when
    token = OAuthToken.from_tokens(access=access_token, refresh=refresh_token)

    # then
    assert token.seconds_left == 0.0
    assert token.is_expired is True


@freeze_time("2024-01-02 03:03:36 UTC")
def test_expired(oauth_token):
    # given
    access_token, refresh_token = oauth_token.access_token, oauth_token.refresh_token

    # when
    token = OAuthToken.from_tokens(access=access_token, refresh=refresh_token)

    # then
    assert token.seconds_left == -1.0
    assert token.is_expired is True
