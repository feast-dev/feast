import pytest


def pytest_addoption(parser):
    parser.addoption("--core_url", action="store", default="localhost:6565")
    parser.addoption("--serving_url", action="store", default="localhost:6566")
    parser.addoption("--allow_dirty", action="store", default="False")
    parser.addoption(
        "--gcs_path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--enable_auth", action="store", default="False")
    parser.addoption("--kafka_brokers", action="store", default="localhost:9092")

    parser.addoption("--env", action="store", help="local|aws|gcloud", default="local")
    parser.addoption(
        "--staging-path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--dataproc-cluster-name", action="store")
    parser.addoption("--dataproc-region", action="store")
    parser.addoption("--dataproc-project", action="store")
    parser.addoption("--ingestion-jar", action="store")


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed (%s)" % previousfailed.name)
