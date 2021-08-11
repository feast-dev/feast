import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import pytest
from attr import dataclass

from feast import FeatureStore, FeatureView, RepoConfig, driver_test_data, importer
from feast.data_source import DataSource
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.entities import customer, driver


@dataclass(frozen=True, str=True)
class TestRepoConfig:
    """
    This class should hold all possible parameters that may need to be varied by individual tests.
    """

    provider: str = "local"
    online_store: Union[str, Dict] = "sqlite"

    offline_store_creator: str = "tests.integration.feature_repos.universal.data_sources.file.FileDataSourceCreator"

    full_feature_names: bool = True
    infer_event_timestamp_col: bool = True


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


@dataclass
class Environment:
    name: str
    test_repo_config: TestRepoConfig
    feature_store: FeatureStore
    data_source: DataSource
    data_source_creator: DataSourceCreator

    customer_entities = list(range(1001, 1110))
    driver_entities = list(range(5001, 5110))

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=7)

    def customer_fixtures(self) -> Tuple[pd.DataFrame, FeatureView]:
        customer_df = driver_test_data.create_customer_daily_profile_df(
            self.customer_entities, self.start_date, self.end_date
        )
        customer_table_id = self.data_source_creator.get_prefixed_table_name(
            self.name, "customer_profile"
        )
        # self.data_source_creator.upload_df(customer_df, customer_table_id)

    def driver_stats_fixtures(self) -> Tuple[pd.DataFrame, FeatureView]:
        pass

    def order_fixtures(self) -> Tuple[pd.DataFrame, FeatureView]:
        pass

    def orders_sql_fixtures(self) -> Optional[str]:
        pass


def vary_full_feature_names(configs: List[TestRepoConfig]) -> List[TestRepoConfig]:
    new_configs = []
    for c in configs:
        true_c = replace(c, full_feature_names=True)
        false_c = replace(c, full_feature_names=False)
        new_configs.extend([true_c, false_c])
    return new_configs


def vary_infer_event_timestamp_col(
    configs: List[TestRepoConfig],
) -> List[TestRepoConfig]:
    new_configs = []
    for c in configs:
        true_c = replace(c, infer_event_timestamp_col=True)
        false_c = replace(c, infer_event_timestamp_col=False)
        new_configs.extend([true_c, false_c])
    return new_configs


def vary_providers_for_offline_stores(
    configs: List[TestRepoConfig],
) -> List[TestRepoConfig]:
    new_configs = []
    for c in configs:
        if "FileDataSourceCreator" in c.offline_store_creator:
            new_configs.append(c)
        elif "RedshiftDataSourceCreator" in c.offline_store_creator:
            for p in ["local", "aws"]:
                new_configs.append(replace(c, provider=p))
        elif "BigQueryDataSourceCreator" in c.offline_store_creator:
            for p in ["local", "gcp", "gcp_custom_offline_config"]:
                new_configs.append(replace(c, provider=p))
    return new_configs


@contextmanager
def construct_test_environment(
    test_repo_config: TestRepoConfig, create_and_apply: bool = False
) -> Environment:
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
    )(project)
    ds = offline_creator.create_data_sources(project, df)
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

        try:
            if create_and_apply:
                fvs = []
                fvs.extend([driver(), customer()])

                fs.apply(fvs)

            environment = Environment(
                name=project,
                test_repo_config=test_repo_config,
                feature_store=fs,
                data_source=ds,
                data_source_creator=offline_creator,
            )

            yield environment
        finally:
            fs.teardown()
            offline_creator.teardown(project)


def parametrize_e2e_test(e2e_test):
    """
    This decorator should be used for end-to-end tests. These tests are expected to be parameterized,
    and receive an empty feature repo created for all supported configurations.

    The decorator also ensures that sample data needed for the test is available in the relevant offline store.

    Decorated tests should create and apply the objects needed by the tests, and perform any operations needed
    (such as materialization and looking up feature values).

    The decorator takes care of tearing down the feature store, as well as the sample data.
    """

    @pytest.mark.integration
    @pytest.mark.parametrize("config", FULL_REPO_CONFIGS, ids=lambda v: str(v))
    def inner_test(config):
        with construct_test_environment(config) as environment:
            e2e_test(environment)

    return inner_test


def parametrize_offline_retrieval_test(offline_retrieval_test):
    """
    This decorator should be used for end-to-end tests. These tests are expected to be parameterized,
    and receive an empty feature repo created for all supported configurations.

    The decorator also ensures that sample data needed for the test is available in the relevant offline store.

    Decorated tests should create and apply the objects needed by the tests, and perform any operations needed
    (such as materialization and looking up feature values).

    The decorator takes care of tearing down the feature store, as well as the sample data.
    """

    configs = vary_providers_for_offline_stores(FULL_REPO_CONFIGS)
    configs = vary_full_feature_names(configs)
    configs = vary_infer_event_timestamp_col(configs)

    @pytest.mark.integration
    @pytest.mark.parametrize("config", configs, ids=lambda v: str(v))
    def inner_test(config):
        with construct_test_environment(config, create_and_apply=True) as environment:
            offline_retrieval_test(environment)

    return inner_test
