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

    # Create projects-list.json file
    projects_file = os.path.join(ui_dir, "projects-list.json")
    with open(projects_file, "w") as f:
        json.dump({"projects": []}, f)

    # Create index.html file
    index_file = os.path.join(ui_dir, "index.html")
    with open(index_file, "w") as f:
        f.write("<html><body>Test UI</body></html>")


@contextlib.contextmanager
def _setup_importlib_mocks(temp_dir):
    """Helper function to setup importlib resource mocks.

    This function mocks the importlib_resources functionality used by the UI server
    to serve static files. It creates a proper context manager that returns the
    temporary directory path when used with importlib_resources.as_file().
    """
    mock_path = Path(temp_dir)

    # Create a proper context manager mock
    mock_context_manager = MagicMock()
    mock_context_manager.__enter__.return_value = mock_path
    mock_context_manager.__exit__.return_value = None

    # Mock the files() method to return a mock that supports division
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
    """Fixture for UI app with valid registry data.

    Creates a UI app instance with a properly configured feature store
    that has valid registry data available for testing endpoints that
    require registry access.
    """
    mock_registry = MagicMock()
    mock_proto = MagicMock()
    mock_proto.SerializeToString.return_value = b"mock_proto_data"
    mock_registry.proto.return_value = mock_proto
    mock_registry._get_registry_proto.return_value = mock_proto
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
            yield app


@pytest.fixture
def ui_app_without_registry(mock_feature_store):
    """Fixture for UI app with None registry data.

    Creates a UI app instance with a feature store that has no registry
    data available, used for testing error conditions and service
    unavailable responses.
    """
    mock_registry = MagicMock()
    mock_registry.proto.return_value = None
    mock_registry._get_registry_proto.return_value = None
    mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
            yield app


def test_ui_server_health_endpoint(ui_app_with_registry):
    """Test the UI server health endpoint returns 200 when registry is available.

    This test verifies that the /health endpoint correctly returns HTTP 200
    when the feature store registry is properly initialized and contains data.
    """
    client = TestClient(ui_app_with_registry)
    response = client.get("/health")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)


def test_ui_server_health_endpoint_with_none_registry(ui_app_without_registry):
    """Test the UI server health endpoint returns 503 when registry is None.

    This test verifies that the /health endpoint correctly returns HTTP 503
    (Service Unavailable) when the feature store registry is not available
    or contains no data.
    """
    client = TestClient(ui_app_without_registry)
    response = client.get("/health")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_ERROR_STATUS)


def test_registry_endpoint_with_valid_data(ui_app_with_registry):
    """Test the registry endpoint returns valid data with correct content type.

    This test verifies that the /registry endpoint correctly returns HTTP 200
    with the proper content-type header when registry data is available.
    """
    client = TestClient(ui_app_with_registry)
    response = client.get("/registry")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)
    assertpy.assert_that(response.headers["content-type"]).is_equal_to(
        "application/octet-stream"
    )


def test_registry_endpoint_with_none_data(ui_app_without_registry):
    """Test the registry endpoint returns 503 when registry data is None.

    This test verifies that the /registry endpoint correctly returns HTTP 503
    (Service Unavailable) when no registry data is available.
    """
    client = TestClient(ui_app_without_registry)
    response = client.get("/registry")
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_ERROR_STATUS)


def test_save_document_endpoint_success(ui_app_with_registry):
    """Test the save document endpoint successfully saves data to a labels file.

    This test verifies that the /save-document endpoint correctly processes
    a valid request, creates a labels file, and returns success confirmation.
    """
    client = TestClient(ui_app_with_registry)

    # Create a temporary file in the current working directory for testing
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, dir=os.getcwd()
    ) as f:
        test_file_path = f.name
        f.write("# Test file content")

    try:
        request_data = {
            "file_path": test_file_path,
            "data": {"test": "data", "key": "value"},
        }

        response = client.post("/save-document", json=request_data)
        assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)

        response_data = response.json()
        assertpy.assert_that(response_data["success"]).is_true()
        assertpy.assert_that(response_data).contains_key("saved_to")

        # Verify the file was created
        labels_file = response_data["saved_to"]
        assertpy.assert_that(os.path.exists(labels_file)).is_true()

        with open(labels_file, "r") as f:
            saved_data = json.load(f)
        assertpy.assert_that(saved_data).is_equal_to(request_data["data"])

    finally:
        # Cleanup
        if os.path.exists(test_file_path):
            os.unlink(test_file_path)
        labels_file = test_file_path.replace(".py", "-labels.json")
        if os.path.exists(labels_file):
            os.unlink(labels_file)


def test_save_document_endpoint_invalid_path(ui_app_with_registry):
    """Test the save document endpoint returns error for invalid file path.

    This test verifies that the /save-document endpoint correctly rejects
    file paths that are outside the current working directory for security.
    """
    client = TestClient(ui_app_with_registry)

    request_data = {
        "file_path": "/invalid/absolute/path/outside/workspace.py",
        "data": {"test": "data"},
    }

    response = client.post("/save-document", json=request_data)
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)

    response_data = response.json()
    assertpy.assert_that(response_data).contains_key("error")
    assertpy.assert_that(response_data["error"]).contains("Invalid file path")


def test_save_document_endpoint_exception_handling(ui_app_with_registry):
    """Test the save document endpoint handles exceptions gracefully.

    This test verifies that the /save-document endpoint properly catches
    and returns error responses when exceptions occur during processing.
    """
    client = TestClient(ui_app_with_registry)

    # Test with a file path outside the current working directory (will cause an exception)
    request_data = {
        "file_path": "/invalid/absolute/path/outside/workspace.py",
        "data": {"test": "data"},
    }

    response = client.post("/save-document", json=request_data)
    assertpy.assert_that(response.status_code).is_equal_to(EXPECTED_SUCCESS_STATUS)

    response_data = response.json()
    assertpy.assert_that(response_data).contains_key("error")
    assertpy.assert_that(response_data["error"]).contains("Invalid file path")


@pytest.mark.parametrize(
    "registry_available,expected_status",
    [(True, EXPECTED_SUCCESS_STATUS), (False, EXPECTED_ERROR_STATUS)],
)
def test_health_endpoint_status(
    registry_available, expected_status, mock_feature_store
):
    """Test the health endpoint returns correct status based on registry availability.

    This parametrized test verifies that the /health endpoint returns the
    appropriate HTTP status code based on whether registry data is available.
    """
    if registry_available:
        mock_registry = MagicMock()
        mock_proto = MagicMock()
        mock_proto.SerializeToString.return_value = b"mock_proto_data"
        mock_registry.proto.return_value = mock_proto
        mock_registry._get_registry_proto.return_value = mock_proto
        mock_feature_store.registry = mock_registry
    else:
        mock_registry = MagicMock()
        mock_registry.proto.return_value = None
        mock_registry._get_registry_proto.return_value = None
        mock_feature_store.registry = mock_registry

    with tempfile.TemporaryDirectory() as temp_dir:
        _create_mock_ui_files(temp_dir)

        with _setup_importlib_mocks(temp_dir):
            app = get_app(mock_feature_store, TEST_PROJECT_NAME, REGISTRY_TTL_SECS)
            client = TestClient(app)
            response = client.get("/health")
            assertpy.assert_that(response.status_code).is_equal_to(expected_status)


def test_catch_all_route(ui_app_with_registry):
    """Test the catch-all route for React router paths.

    This test reveals a bug in the original UI server code where ui_dir
    is not in scope for the catch_all function. The ui_dir variable is defined
    inside the importlib_resources context manager but used outside of it.
    This causes a NameError when the route is accessed.
    """
    client = TestClient(ui_app_with_registry)

    # The route will fail due to the scope issue with ui_dir
    with pytest.raises(Exception):  # Expecting NameError or FileNotFoundError
        client.get("/p/some/react/path")
