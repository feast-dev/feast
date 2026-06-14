"""
Fixtures for universal online-store tests.
"""

from typing import Dict

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class _SharedDbDynamoDBOnlineStoreCreator(OnlineStoreCreator):
    """DynamoDB Local container started with ``-sharedDb -inMemory``."""

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            DockerContainer("amazon/dynamodb-local:latest")
            .with_exposed_ports("8000")
            .with_command("-jar DynamoDBLocal.jar -sharedDb -inMemory")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        wait_for_logs(
            container=self.container,
            predicate="Initializing DynamoDB Local with the following configuration:",
            timeout=10,
        )
        exposed_port = self.container.get_exposed_port("8000")
        return {
            "type": "dynamodb",
            "endpoint_url": f"http://localhost:{exposed_port}",
            "region": "us-west-2",
        }

    def teardown(self):
        self.container.stop()


@pytest.fixture
async def dynamodb_local_environment(monkeypatch, worker_id):
    """Isolated, self-contained Environment for DynamoDB async tests."""
    from feast.infra.online_stores.dynamodb import DynamoDBOnlineStore
    from tests.universal.feature_repos.integration_test_repo_config import (
        IntegrationTestRepoConfig,
    )
    from tests.universal.feature_repos.repo_configuration import (
        construct_test_environment,
        construct_universal_test_data,
    )
    from tests.universal.feature_repos.universal.data_sources.file import (
        FileDataSourceCreator,
    )

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fakeaccesskey000000")
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", "fakesecretkey0000000000000000000000000000"
    )
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_SECURITY_TOKEN", raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", raising=False)
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)

    DynamoDBOnlineStore._dynamodb_client = None
    DynamoDBOnlineStore._dynamodb_resource = None

    config = IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=FileDataSourceCreator,
        online_store_creator=_SharedDbDynamoDBOnlineStoreCreator,
        online_store=None,
    )

    environment = construct_test_environment(
        config, fixture_request=None, worker_id=worker_id
    )
    environment.setup()
    universal_test_data = construct_universal_test_data(environment)
    await environment.feature_store.initialize()

    yield environment, universal_test_data

    await environment.feature_store.close()
    environment.teardown()
    DynamoDBOnlineStore._dynamodb_client = None
    DynamoDBOnlineStore._dynamodb_resource = None
