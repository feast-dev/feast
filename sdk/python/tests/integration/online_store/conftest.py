"""
Fixtures for online-store integration tests.
"""

from typing import Dict

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.universal.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class _SharedDbDynamoDBOnlineStoreCreator(OnlineStoreCreator):
    """DynamoDB Local container started with ``-sharedDb -inMemory``.

    Why ``-sharedDb``
    -----------------
    DynamoDB Local 2.x namespaces tables by the **access key ID** in the
    request signature.  In CI, the sync ``boto3`` client and the async
    ``aiobotocore`` client can resolve credentials from *different* sources
    (env vars, credential file, ``credential_process``, container IAM role,
    etc.) even after ``monkeypatch.setenv`` has set fake keys—because the
    credential chain is evaluated lazily and various caches may hold stale
    values.

    When the two clients end up using *different* access keys, the sync
    client creates tables in namespace A while the async client queries
    namespace B, which is empty → ``ResourceNotFoundException``.

    ``-sharedDb`` collapses all namespaces into a single in-memory database,
    making table visibility completely independent of which credentials each
    client uses.  This is the correct setting for integration tests that want
    to verify async read/write behaviour without caring about credential
    isolation.
    """

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
    """Isolated, self-contained Environment for DynamoDB async tests.

    Root cause of the async credential failures
    -------------------------------------------
    DynamoDB Local 2.x isolates tables **per access key ID**.  In CI,
    ``boto3`` (sync, used to provision tables via ``store.apply()``) and
    ``aiobotocore`` (async, used for reads/writes in the test body) may
    resolve credentials from *different* sources even when ``monkeypatch``
    has set fake static keys—the credential chain is evaluated lazily and
    caches may hold stale values from a real AWS session configured in the
    runner environment.

    When the two clients end up using different access key IDs they land in
    different DynamoDB Local namespaces:

    * sync client  → namespace ``KEY_A`` → tables exist ✓
    * async client → namespace ``KEY_B`` → tables not found → ``ResourceNotFoundException``

    Fix: ``_SharedDbDynamoDBOnlineStoreCreator``
    --------------------------------------------
    The isolated container is started with ``-sharedDb -inMemory``.  In
    shared-DB mode DynamoDB Local stores *all* tables in a single namespace
    regardless of the access key, so sync and async clients always see the
    same tables.

    Why async + ``await fs.initialize()`` before yielding
    -----------------------------------------------------
    Calling ``await fs.initialize()`` eagerly creates the ``aiobotocore``
    client inside this fixture's event loop (the *same* loop the test will
    run in).  This pre-caches:

    1. ``FeatureStore._provider`` so the identical ``DynamoDBOnlineStore``
       instance is reused for the entire test.
    2. The aiobotocore client, which is now unambiguously pointed at our
       isolated container's ``endpoint_url``.

    Yields
    ------
    tuple[Environment, TestData]
        ``(environment, (entities, datasets, data_sources))``
    """
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

    # Set fake static credentials before any boto client is created.
    # These are accepted by DynamoDB Local regardless of validity.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fakeaccesskey000000")
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", "fakesecretkey0000000000000000000000000000"
    )
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_SECURITY_TOKEN", raising=False)
    # Prevent IMDS from injecting real session tokens on EC2-backed runners.
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    # Disable the container credentials provider (ECS/EKS IAM roles).
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", raising=False)
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", raising=False)
    # Ensure no profile redirects boto to a different credential source.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)

    # Reset class-level boto3 client caches so that no stale client from a
    # previous test in this worker bleeds into our isolated environment.
    DynamoDBOnlineStore._dynamodb_client = None
    DynamoDBOnlineStore._dynamodb_resource = None

    config = IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=FileDataSourceCreator,
        online_store_creator=_SharedDbDynamoDBOnlineStoreCreator,
        online_store=None,
    )

    environment = construct_test_environment(
        config,
        fixture_request=None,
        worker_id=worker_id,
    )
    environment.setup()

    # FileDataSourceCreator writes only local Parquet files — no AWS calls.
    universal_test_data = construct_universal_test_data(environment)

    # Eagerly initialise the aiobotocore client in *this* event loop so it
    # is guaranteed to point at our container and is reused throughout the
    # test body without lazy-init surprises.
    await environment.feature_store.initialize()

    yield environment, universal_test_data

    # Cleanly shut down the async client before the container disappears.
    await environment.feature_store.close()
    environment.teardown()

    # Flush class-level caches so the next test starts completely fresh.
    DynamoDBOnlineStore._dynamodb_client = None
    DynamoDBOnlineStore._dynamodb_resource = None
