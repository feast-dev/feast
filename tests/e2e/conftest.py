import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "databricks_skip: marks tests as skipped on Databricks"
    )


def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6566")
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption("--gcs_path", action="store", default="gs://feast-templocation-kf-feast/")
    parser.addoption("--databricks", action="store_true", default=False, help="skip tests not supported with Databricks runner")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--databricks"):
        # --databricks not given in cli: don't skip tests not supported with Databricks runner
        return
    skip_databricks = pytest.mark.skip(reason="skipped when --databricks option is passed")
    for item in items:
        if "databricks_skip" in item.keywords:
            item.add_marker(skip_databricks)
