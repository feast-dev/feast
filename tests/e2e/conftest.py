import os

import pytest


def pytest_addoption(parser):
    parser.addoption("--core-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption("--job-service-url", action="store", default="localhost:6568")
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
    parser.addoption("--redis-cluster", action="store_true")
    parser.addoption("--feast-version", action="store")
    parser.addoption("--bq-project", action="store")
    parser.addoption("--feast-project", action="store", default="default")


def pytest_runtest_setup(item):
    env_names = [mark.args[0] for mark in item.iter_markers(name="env")]
    if env_names:
        if item.config.getoption("env") not in env_names:
            pytest.skip(f"test requires env in {env_names}")


from .fixtures.base import project_root, project_version  # noqa
from .fixtures.client import (  # noqa
    feast_client,
    global_staging_path,
    ingestion_job_jar,
    local_staging_path,
)

if not os.environ.get("DISABLE_SERVICE_FIXTURES"):
    from .fixtures.services import (  # noqa
        kafka_port,
        kafka_server,
        redis_server,
        zookeeper_server,
    )
else:
    from .fixtures.external_services import kafka_server, redis_server  # noqa

if not os.environ.get("DISABLE_FEAST_SERVICE_FIXTURES"):
    from .fixtures.feast_services import *  # type: ignore # noqa
    from .fixtures.services import postgres_server  # noqa
else:
    from .fixtures.external_services import (  # type: ignore # noqa
        feast_core,
        feast_serving,
        feast_jobservice,
        enable_auth,
    )

from .fixtures.data import *  # noqa
