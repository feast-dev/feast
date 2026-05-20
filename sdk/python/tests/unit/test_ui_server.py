import contextlib
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import assertpy
import pytest
from fastapi.testclient import TestClient

from feast.ui_server import get_app

# Test constants
EXPECTED_SUCCESS_STATUS = 200
EXPECTED_ERROR_STATUS = 503
TEST_PROJECT_NAME = "test_project"


def _create_mock_ui_files(temp_dir):
    """Helper function to create required UI files structure"""
    ui_dir = os.path.join(temp_dir, "ui", "build")
    os.makedirs(ui_dir, exist_ok=True)

    projects_file = os.path.join(ui_dir, "projects-list.json")
    with open(projects_file, "w") as f:
        json.dump({"projects": []}, f)

    index_file = os.path.join(ui_dir, "index.html")
    with open(index_file, "w") as f:
        f.write("<html><body>Test UI</body></html>")


@contextlib.contextmanager
def _setup_importlib_mocks(temp_dir):
    """Helper function to setup importlib resource mocks."""
    mock_path = Path(temp_dir)

    mock_context_manager = MagicMock()
    mock_context_manager.__enter__.return_value = mock_path
    mock_context_manager.__exit__.return_value = None

    mock_file_ref = MagicMock()
    mock_file_ref.__truediv__.return_value = MagicMock()

    with (
        patch("feast.ui_server.importlib_resources.files") as mock_files,
        patch("feast.ui_server.importlib_resources.as_file") as mock_as_file,
    ):
        mock_files.return_value = mock_file_ref
        mock_as_file.return_value = mock_context_manager

        yield mock_files, mock_as_file


def _make_project_mock(name, description=""):
    """Create a mock project object with the given name and description."""
    proj = MagicMock()
    proj.name = name
    proj.description = description
    return proj


@pytest.fixture
def mock_feature_store():
    """Fixture for creating a mock feature store"""
    mock_store = MagicMock()
    return mock_store


@pytest.fixture
def ui_app_with_registry(mock_feature_store):
    """Fixture for UI app with valid registry data."""
    mock_registry = MagicMock()
    mock_registry.list_projects.return_value = []
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME)
            yield app


@pytest.fixture
def ui_app_without_registry(mock_feature_store):
    """Fixture for UI app where registry raises on list_projects."""
    mock_registry = MagicMock()
    mock_registry.list_projects.side_effect = Exception("registry unavailable")
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME)
            yield app


def test_ui_server_health_endpoint(ui_app_with_registry):
    """Health endpoint returns 200 when registry is available."""
    client = TestClient(ui_app_with_registry)
    response = client.get("/health")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)


def test_ui_server_health_endpoint_with_unavailable_registry(ui_app_without_registry):
    """Health endpoint returns 503 when registry is unavailable."""
    client = TestClient(ui_app_without_registry)
    response = client.get("/health")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_ERROR_STATUS)


@pytest.mark.parametrize(
    "registry_available,expected_status",
    [(True, EXPECTED_SUCCESS_STATUS), (False, EXPECTED_ERROR_STATUS)],
)
def test_health_endpoint_status(
    registry_available, expected_status, mock_feature_store
):
    """Health endpoint returns correct status based on registry availability."""
    mock_registry = MagicMock()
    if registry_available:
        mock_registry.list_projects.return_value = []
    else:
        mock_registry.list_projects.side_effect = Exception("unavailable")
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME)
            client = TestClient(app)
            response = client.get("/health")
            assertpy.assert_that(response.status_code).is_equal_to(expected_status)


def test_catch_all_route(ui_app_with_registry):
    """Test the catch-all route for React router paths."""
    client = TestClient(ui_app_with_registry)

    with pytest.raises(Exception):
        client.get("/p/some/react/path")


# ---------- projects-list.json tests ----------


def _read_projects_list(temp_dir):
    """Read the projects-list.json written by get_app via the mock (ui_dir = temp_dir)."""
    projects_file = os.path.join(temp_dir, "projects-list.json")
    with open(projects_file) as f:
        return json.load(f)


def test_projects_list_registry_path(mock_feature_store):
    """projects-list.json uses /api/v1 as registryPath."""
    mock_registry = MagicMock()
    mock_registry.list_projects.return_value = []
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(data["projects"][0]["registryPath"]).is_equal_to("/api/v1")


def test_projects_list_with_root_path(mock_feature_store):
    """root_path prefix is included in registryPath."""
    mock_registry = MagicMock()
    mock_registry.list_projects.return_value = []
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(
                mock_feature_store,
                TEST_PROJECT_NAME,
                root_path="/feast",
            )

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(data["projects"][0]["registryPath"]).is_equal_to(
            "/feast/api/v1"
        )


def test_projects_list_multiple_projects(mock_feature_store):
    """Multiple projects get an 'All Projects' entry prepended."""
    mock_registry = MagicMock()
    mock_registry.list_projects.return_value = [
        _make_project_mock("project_alpha", "Alpha project"),
        _make_project_mock("project_beta", "Beta project"),
    ]
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(len(data["projects"])).is_equal_to(3)
        assertpy.assert_that(data["projects"][0]["id"]).is_equal_to("all")
        assertpy.assert_that(data["projects"][1]["id"]).is_equal_to("project_alpha")
        assertpy.assert_that(data["projects"][2]["id"]).is_equal_to("project_beta")


def test_projects_list_fallback_on_empty(mock_feature_store):
    """When list_projects returns empty, fallback project is used."""
    mock_registry = MagicMock()
    mock_registry.list_projects.return_value = []
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(len(data["projects"])).is_equal_to(1)
        assertpy.assert_that(data["projects"][0]["id"]).is_equal_to(TEST_PROJECT_NAME)


def test_projects_list_fallback_on_exception(mock_feature_store):
    """When list_projects raises, fallback project is used."""
    mock_registry = MagicMock()
    mock_registry.list_projects.side_effect = Exception("not implemented")
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(len(data["projects"])).is_equal_to(1)
        assertpy.assert_that(data["projects"][0]["id"]).is_equal_to(TEST_PROJECT_NAME)
