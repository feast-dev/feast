import pytest
import os


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "databricks_skip: marks tests as skipped on Databricks"
    )


def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default=os.getenv("FEAST_CORE_URL", "localhost:6565"))
    parser.addoption("--serving_url", action="store", default=os.getenv("FEAST_ONLINE_SERVING_URL", "localhost:6566"))
    parser.addoption("--batch_serving_url", action="store", default=os.getenv("FEAST_BATCH_SERVING_URL", "localhost:6567"))
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption(
        "--gcs_path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--enable_auth", action="store", default="False")