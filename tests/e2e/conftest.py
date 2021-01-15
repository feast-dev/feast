import os

import pytest


def pytest_addoption(parser):
    parser.addoption("--core-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption("--job-service-url", action="store", default="localhost:6568")
    parser.addoption("--kafka-brokers", action="store", default="localhost:9092")

    parser.addoption(
        "--env", action="store", help="local|aws|gcloud|k8s", default="local"
    )
    parser.addoption("--with-job-service", action="store_true")
    parser.addoption("--staging-path", action="store")
    parser.addoption("--dataproc-cluster-name", action="store")
    parser.addoption("--dataproc-region", action="store")
    parser.addoption("--emr-cluster-id", action="store")
    parser.addoption("--emr-region", action="store")
    parser.addoption("--dataproc-project", action="store")
    parser.addoption("--dataproc-executor-instances", action="store", default="2")
    parser.addoption("--dataproc-executor-cores", action="store", default="2")
    parser.addoption("--dataproc-executor-memory", action="store", default="2g")
    parser.addoption("--ingestion-jar", action="store")
    parser.addoption("--redis-url", action="store", default="localhost:6379")
    parser.addoption("--redis-cluster", action="store_true")
    parser.addoption("--feast-version", action="store")
    parser.addoption("--bq-project", action="store")
    parser.addoption("--feast-project", action="store", default="default")
    parser.addoption("--statsd-url", action="store", default="localhost:8125")
    parser.addoption("--prometheus-url", action="store", default="localhost:9102")
    parser.addoption(
        "--scheduled-streaming-job",
        action="store_true",
        help="When set tests won't manually start streaming jobs,"
        " instead jobservice's loop is responsible for that",
    )


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
    tfrecord_feast_client,
)

if not os.environ.get("DISABLE_SERVICE_FIXTURES"):
    from .fixtures.services import (  # noqa
        kafka_port,
        kafka_server,
        redis_server,
        statsd_server,
        zookeeper_server,
    )
else:
    from .fixtures.external_services import (  # type: ignore # noqa
        kafka_server,
        redis_server,
        statsd_server,
    )

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
