from freezegun import freeze_time

from neptune_query.generated.neptune_api.client import NeptuneAuthenticator


def test_use_token_factory(mocker, credentials, oauth_token):
    # given
    client = mocker.MagicMock()
    client.exchange_token.return_value = oauth_token
    authenticator = NeptuneAuthenticator(
        client_id="client_id",
        token_refreshing_endpoint="https://api.neptune.ai/oauth/token",
        client=client,
    )
    request = mocker.MagicMock()
    request.headers = {}

    # when
    request = next(authenticator.sync_auth_flow(request=request))

    # then
    assert request.headers["Authorization"] == f"Bearer {oauth_token.access_token}"


@freeze_time("2024-01-02 03:04:35")
def test_refresh(mocker, credentials, expired_oauth_token, oauth_token):
    # given
    client = mocker.MagicMock()
    authenticator = NeptuneAuthenticator(
        client_id="client_id",
        token_refreshing_endpoint="https://api.neptune.ai/oauth/token",
        client=client,
    )
    request = mocker.MagicMock()
    request.headers = {}

    # and
    authenticator._token = expired_oauth_token

    # and
    client.get_httpx_client().post.return_value = mocker.MagicMock()
    client.get_async_httpx_client().post.return_value.status_code = 200
    client.get_httpx_client().post.return_value.json.return_value = {
        "access_token": oauth_token.access_token,
        "refresh_token": oauth_token.refresh_token,
    }

    # when
    request = next(authenticator.sync_auth_flow(request=request))

    # then
    assert request.headers["Authorization"] == f"Bearer {oauth_token.access_token}"
