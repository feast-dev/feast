import uuid
import pytest
from feast import Client

PROJECT_USER = "user_" + uuid.uuid4().hex.upper()[0:6]
PROJECT_NAME = "basic_" + uuid.uuid4().hex.upper()[0:6]

@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope="module")
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture(scope="module")
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )
    return client

@pytest.mark.integration
def test_client_project(client):
    # Create new project
    client.create_project(PROJECT_NAME)

    # List project and check if the new project is available
    assert PROJECT_NAME in client.list_projects();

    # Archive the project
    client.set_project(project=PROJECT_NAME)
    client.archive_project(PROJECT_NAME)

    # Check that the project is already archived
    assert PROJECT_NAME not in client.list_projects();

@pytest.mark.integration
def test_client_project_members(client):
    # Create new project
    client.create_project(PROJECT_NAME)
    client.set_project(project=PROJECT_NAME)

    # Add member to the new project
    client.add_member(user=PROJECT_USER, project=PROJECT_NAME)
    assert PROJECT_USER in client.list_members(project=PROJECT_NAME)

    # Remove member from the project
    client.remove_member(user=PROJECT_USER, project=PROJECT_NAME)
    assert PROJECT_USER not in client.list_members(project=PROJECT_NAME)


@pytest.mark.integration
def test_client_non_existent_project_and_member(client):
    # List member from the non existent project
    with pytest.raises(Exception):
        client.list_members(PROJECT_NAME)

    # Create new project
    client.create_project(project=PROJECT_NAME)
    client.set_project(project=PROJECT_NAME)

    # Remove member that is not registered in the project
    with pytest.raises(Exception):
        client.remove_member(user=PROJECT_USER, project=PROJECT_NAME)

@pytest.mark.integration
def test_client_add_same_member_multiple_times(client):
    # Create new project
    client.create_project(PROJECT_NAME)
    client.set_project(PROJECT_NAME)

    # Add the same member to the project multiple times
    client.add_member(user=PROJECT_USER, project=PROJECT_NAME)
    with pytest.raises(Exception):
        client.add_member(user=PROJECT_USER, project=PROJECT_NAME)
