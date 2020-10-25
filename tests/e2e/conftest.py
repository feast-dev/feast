import os

import pytest


def pytest_addoption(parser):
    parser.addoption("--core-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption(
        "--gcs_path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--kafka-brokers", action="store", default="localhost:9092")

    parser.addoption("--env", action="store", help="local|aws|gcloud", default="local")
    parser.addoption(
        "--staging-path", action="store", default="gs://feast-templocation-kf-feast/"
    )
    parser.addoption("--dataproc-cluster-name", action="store")
    parser.addoption("--dataproc-region", action="store")
    parser.addoption("--dataproc-project", action="store")
    parser.addoption("--ingestion-jar", action="store")
    parser.addoption("--redis-url", action="store", default="localhost:6379")
    parser.addoption("--feast-version", action="store")


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


from .fixtures.base import project_root, project_version  # noqa
from .fixtures.client import (  # noqa
    feast_client,
    global_staging_path,
    ingestion_job_jar,
    local_staging_path,
)

if not os.environ.get("DISABLE_SERVICE_FIXTURES"):
    from .fixtures.services import *  # noqa
    from .fixtures.feast_services import *  # type: ignore # noqa
else:
    from .fixtures.external_services import *  # type: ignore # noqa

