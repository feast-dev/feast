import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from attr import dataclass

from feast import FeatureStore, RepoConfig
from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStore,
    BigQueryOfflineStoreConfig,
)
from feast.infra.online_stores.datastore import DatastoreOnlineStoreConfig
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import (
    correctness_feature_view,
)


@dataclass
class TestRepoConfig:
    """
    This class should hold all possible parameters that may need to be varied by individual tests.
    """

    provider: str = "local"
    offline_store: str = "file"
    online_store: str = "sqlite"

    full_feature_names: bool = True


@contextmanager
def construct_feature_store(test_repo_config: TestRepoConfig) -> FeatureStore:
    """
    This method should take in the parameters from the test repo config and created a feature repo, apply it,
    and return the constructed feature store object to callers.

    This feature store object can be interacted for the purposes of tests.
    The user is *not* expected to perform any clean up actions.

    :param test_repo_config: configuration
    :return: A feature store built using the supplied configuration.
    """
    df = create_dataset()

    project = f"test_correctness_{str(uuid.uuid4()).replace('-', '')}"

    # TODO: Parameterize over data sources, by pulling this into individual data_source classes behind
    # TODO: an appropriate interface.
    ds = BigQueryDataSourceCreator().create_data_source(project, df)
    offline_store = BigQueryOfflineStoreConfig()
    online_store = DatastoreOnlineStoreConfig(namespace="integration_test")

    with tempfile.TemporaryDirectory() as repo_dir_name:
        config = RepoConfig(
            registry=str(Path(repo_dir_name) / "registry.db"),
            project=project,
            provider=test_repo_config.provider,
            offline_store=offline_store,
            online_store=online_store,
        )
    fs = FeatureStore(config=config)
    fv = correctness_feature_view(ds)
    entity = driver
    fs.apply([fv, entity])
    yield fs

    fs.teardown()
    BigQueryDataSourceCreator().teardown(project)
