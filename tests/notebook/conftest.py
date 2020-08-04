import pytest


@pytest.fixture(scope="module")
def test_run_id(pytestconfig):
    return pytestconfig.getoption("test_run_id")


@pytest.fixture(scope="module")
def output_bucket_name(pytestconfig):
    return pytestconfig.getoption("output_bucket_name")


def pytest_addoption(parser):
    parser.addoption(
        "--output_bucket_name", action="store", default="feast-test-notebook-artifacts"
    )
    parser.addoption("--test_run_id", action="store")
