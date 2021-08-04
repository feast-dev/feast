import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Union

import pytest
from attr import dataclass

from feast import FeatureStore, RepoConfig, importer
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
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
    online_store: Union[str, Dict] = "sqlite"

    offline_store_creator: str = "tests.integration.feature_repos.universal.data_sources.file.FileDataSourceCreator"

    full_feature_names: bool = True


FULL_REPO_CONFIGS: List[TestRepoConfig] = [
    TestRepoConfig(),  # Local
    TestRepoConfig(
        provider="aws",
        offline_store_creator="tests.integration.feature_repos.universal.data_sources.redshift.RedshiftDataSourceCreator",
        online_store={"type": "dynamodb", "region": "us-west-2"},
    ),
    TestRepoConfig(
        provider="gcp",
        offline_store_creator="tests.integration.feature_repos.universal.data_sources.bigquery.BigQueryDataSourceCreator",
        online_store="datastore",
    ),
]


OFFLINE_STORES: List[str] = []
ONLINE_STORES: List[str] = []
PROVIDERS: List[str] = []


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

    project = f"test_correctness_{str(uuid.uuid4()).replace('-', '')[:8]}"

    module_name, config_class_name = test_repo_config.offline_store_creator.rsplit(
        ".", 1
    )

    offline_creator: DataSourceCreator = importer.get_class_from_type(
        module_name, config_class_name, "DataSourceCreator"
    )()
    ds = offline_creator.create_data_source(project, df)
    offline_store = offline_creator.create_offline_store_config()
    online_store = test_repo_config.online_store

    with tempfile.TemporaryDirectory() as repo_dir_name:
        config = RepoConfig(
            registry=str(Path(repo_dir_name) / "registry.db"),
            project=project,
            provider=test_repo_config.provider,
            offline_store=offline_store,
            online_store=online_store,
            repo_path=repo_dir_name,
        )
        fs = FeatureStore(config=config)
        fv = correctness_feature_view(ds)
        entity = driver()
        fs.apply([fv, entity])

        yield fs

        fs.teardown()
        offline_creator.teardown(project)


def parametrize_e2e_test(e2e_test):
    @pytest.mark.integration
    @pytest.mark.parametrize("config", FULL_REPO_CONFIGS, ids=lambda v: v.provider)
    def inner_test(config):
        with construct_feature_store(config) as fs:
            e2e_test(fs)

    return inner_test
