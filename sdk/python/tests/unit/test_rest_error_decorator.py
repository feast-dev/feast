from unittest.mock import Mock, patch

import assertpy
import pytest
import requests

from feast import RepoConfig
from feast.errors import PermissionNotFoundException
from feast.infra.online_stores.remote import (
    RemoteOnlineStoreConfig,
    get_remote_online_features,
)
from feast.permissions.client.http_auth_requests_wrapper import HttpSessionManager


@pytest.fixture
def feast_exception() -> PermissionNotFoundException:
    return PermissionNotFoundException("dummy_name", "dummy_project")


@pytest.fixture
def none_feast_exception() -> RuntimeError:
    return RuntimeError("dummy_name", "dummy_project")


@patch("feast.infra.online_stores.remote.requests.sessions.Session.post")
def test_rest_error_handling_with_feast_exception(
    mock_post, environment, feast_exception
):
    # Close any cached session to ensure mock is applied to fresh session
    HttpSessionManager.close_session()

    # Create a mock response object
    mock_response = Mock()
    mock_response.status_code = feast_exception.http_status_code()
    mock_response.json.return_value = feast_exception.to_error_detail()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()

    # Configure the mock to return the mock response
    mock_post.return_value = mock_response

    store = environment.feature_store
    online_config = RemoteOnlineStoreConfig(type="remote", path="dummy")

    with pytest.raises(
        PermissionNotFoundException,
        match="Permission dummy_name does not exist in project dummy_project",
    ):
        get_remote_online_features(
            config=RepoConfig(
                project="test", online_store=online_config, registry=store.registry
            ),
            req_body="{test:test}",
        )


@patch("feast.infra.online_stores.remote.requests.sessions.Session.post")
def test_rest_error_handling_with_none_feast_exception(
    mock_post, environment, none_feast_exception
):
    # Close any cached session to ensure mock is applied to fresh session
    HttpSessionManager.close_session()

    # Create a mock response object
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.json.return_value = str(none_feast_exception)
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()

    # Configure the mock to return the mock response
    mock_post.return_value = mock_response

    store = environment.feature_store
    online_config = RemoteOnlineStoreConfig(type="remote", path="dummy")

    response = get_remote_online_features(
        config=RepoConfig(
            project="test", online_store=online_config, registry=store.registry
        ),
        req_body="{test:test}",
    )

    assertpy.assert_that(response).is_not_none()
    assertpy.assert_that(response.status_code).is_equal_to(500)
    assertpy.assert_that(response.json()).is_equal_to("('dummy_name', 'dummy_project')")
