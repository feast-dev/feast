import uuid
import pytest
from feast import Client


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
def test_client_add_list_member(client):
    client.create_project(project="customer_satisfaction")
    client.set_project(project="customer_satisfaction")

    # User does not have access to project: customer_satisfaction
    with pytest.raises(Exception):
      assert client.list_members(project="customer_satisfaction")

    # User is granted access to project: customer_satisfaction
    client.add_member(user="kelly", project="customer_satisfaction")
    assert client.list_members() is not None

    # Check that the user is added to the members's list of the active project
    members_list = client.list_members("customer_satisfaction")
    assert "kelly" in members_list

@pytest.mark.integration
def test_client_remove_list_member(client):
    client.set_project(project="customer_satisfaction")

    # User has access to list members in the project: customer_satisfaction
    assert client.list_members(project="customer_satisfaction") is not None

    # User does not have access to project: customer_satisfaction
    client.remove_member(user="kelly", project="customer_satisfaction")
    with pytest.raises(Exception):
      assert client.list_members()

    # Check that the user is removed from the members's list of the active project
    members_list_updated = client.list_members("customer_satisfaction")
    assert "kelly" not in members_list_updated

@pytest.mark.integration
def test_client_list_projects(client):
    # User does not have access to list active projects
    with pytest.raises(Exception):
        client.list_projects()

    # User has access to archive project: customer_satisfaction
    client.add_member(user="kelly", project="customer_satisfaction")
    assert client.list_projects()


@pytest.mark.integration
def test_client_archive_project(client):
    client.set_project(project="customer_satisfaction")

    # User does not have access to archive project: customer_satisfaction
    client.remove_member(user="kelly", project="customer_satisfaction")
    with pytest.raises(Exception):
        client.archive_project("customer_satisfaction")

    # User has access to archive project: customer_satisfaction
    client.add_member(user="kelly", project="customer_satisfaction")
    client.archive_project("customer_satisfaction")

    # Check that the project is removed from the active Feast projects list
    projects_list_updated = client.list_projects()
    assert "customer_satisfaction" not in projects_list_updated


@pytest.mark.integration
def test_client_set_project(client):
    # TODO: Set project to the non-existent project
    pass

@pytest.mark.integration
def test_client_create_project(client):
    pass
