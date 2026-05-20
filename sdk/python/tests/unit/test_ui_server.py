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
REGISTRY_TTL_SECS = 60


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


@pytest.fixture
def mock_feature_store():
    """Fixture for creating a mock feature store"""
    mock_store = MagicMock()
    mock_store.refresh_registry = MagicMock()
    return mock_store


@pytest.fixture
def ui_app_with_registry(mock_feature_store):
    """Fixture for UI app with valid registry data."""
    mock_registry = MagicMock()
    mock_proto = MagicMock()
    mock_proto.SerializeToString.return_value = b"mock_proto_data"
    mock_proto.projects = []
    mock_registry.proto.return_value = mock_proto
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
            yield app


@pytest.fixture
def ui_app_without_registry(mock_feature_store):
    """Fixture for UI app with None registry data."""
    mock_registry = MagicMock()
    mock_registry.proto.return_value = None
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
            yield app


def test_ui_server_health_endpoint(ui_app_with_registry):
    """Health endpoint returns 200 when registry is available."""
    client = TestClient(ui_app_with_registry)
    response = client.get("/health")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)


def test_ui_server_health_endpoint_with_none_registry(ui_app_without_registry):
    """Health endpoint returns 503 when registry is None."""
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
    if registry_available:
        mock_registry = MagicMock()
        mock_proto = MagicMock()
        mock_proto.SerializeToString.return_value = b"mock_proto_data"
        mock_proto.projects = []
        mock_registry.proto.return_value = mock_proto
        mock_feature_store.registry = mock_registry
    else:
        mock_registry = MagicMock()
        mock_registry.proto.return_value = None
        mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
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
    mock_proto = MagicMock()
    mock_proto.SerializeToString.return_value = b"data"
    mock_proto.projects = []
    mock_registry.proto.return_value = mock_proto
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(data["projects"][0]["registryPath"]).is_equal_to("/api/v1")


def test_projects_list_with_root_path(mock_feature_store):
    """root_path prefix is included in registryPath."""
    mock_registry = MagicMock()
    mock_proto = MagicMock()
    mock_proto.SerializeToString.return_value = b"data"
    mock_proto.projects = []
    mock_registry.proto.return_value = mock_proto
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(
                mock_feature_store,
                TEST_PROJECT_NAME,
                REGISTRY_TTL_SECS,
                root_path="/feast",
            )

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(data["projects"][0]["registryPath"]).is_equal_to(
            "/feast/api/v1"
        )


def test_projects_list_multiple_projects(mock_feature_store):
    """Multiple projects get an 'All Projects' entry prepended."""
    mock_registry = MagicMock()
    mock_proto = MagicMock()
    mock_proto.SerializeToString.return_value = b"data"

    proj1 = MagicMock()
    proj1.spec.name = "project_alpha"
    proj1.spec.description = "Alpha project"
    proj2 = MagicMock()
    proj2.spec.name = "project_beta"
    proj2.spec.description = "Beta project"
    mock_proto.projects = [proj1, proj2]

    mock_registry.proto.return_value = mock_proto
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)

        data = _read_projects_list(temp_dir)
        assertpy.assert_that(len(data["projects"])).is_equal_to(3)
        assertpy.assert_that(data["projects"][0]["id"]).is_equal_to("all")
        assertpy.assert_that(data["projects"][1]["id"]).is_equal_to("project_alpha")
        assertpy.assert_that(data["projects"][2]["id"]).is_equal_to("project_beta")
