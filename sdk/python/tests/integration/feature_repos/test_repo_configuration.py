import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union

import pytest

from feast import FeatureStore, FeatureView, RepoConfig, driver_test_data, importer
from feast.data_source import DataSource
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.entities import customer, driver
from tests.integration.feature_repos.universal.feature_views import (
    create_customer_daily_profile_feature_view,
    create_driver_hourly_stats_feature_view,
)


@dataclass(frozen=True, repr=True)
class TestRepoConfig:
    """
    This class should hold all possible parameters that may need to be varied by individual tests.
    """

    provider: str = "local"
    online_store: Union[str, Dict] = "sqlite"

    offline_store_creator: str = "tests.integration.feature_repos.universal.data_sources.file.FileDataSourceCreator"

    full_feature_names: bool = True
    infer_event_timestamp_col: bool = True


def ds_creator_path(cls: str):
    return f"tests.integration.feature_repos.universal.data_sources.{cls}"


DYNAMO_CONFIG = {"type": "dynamodb", "region": "us-west-2"}
REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}
ASTRA_CONFIG = {"type": "astra", "secure_connect_bundle": "./secure-connect-sample-ml-data.zip",
                "client_id": "client_id_place_holder", "secret_key": "secret_key_place_holder",
                "keyspace": "keyspace_place_holder"}
FULL_REPO_CONFIGS: List[TestRepoConfig] = [
    # Local configurations
    TestRepoConfig(),
    TestRepoConfig(online_store=REDIS_CONFIG),
    # GCP configurations
    TestRepoConfig(
        provider="gcp",
        offline_store_creator=ds_creator_path("bigquery.BigQueryDataSourceCreator"),
        online_store="datastore",
    ),
    TestRepoConfig(
        provider="gcp",
        offline_store_creator=ds_creator_path("bigquery.BigQueryDataSourceCreator"),
        online_store=REDIS_CONFIG,
    ),
    # AWS configurations
    TestRepoConfig(
        provider="aws",
        offline_store_creator=ds_creator_path("redshift.RedshiftDataSourceCreator"),
        online_store=DYNAMO_CONFIG,
    ),
    TestRepoConfig(
        provider="aws",
        offline_store_creator=ds_creator_path("redshift.RedshiftDataSourceCreator"),
        online_store=REDIS_CONFIG,
    ),
    TestRepoConfig(
        provider="astra",
        offline_store_creator=ds_creator_path("redshift.RedshiftDataSourceCreator"),
        online_store=ASTRA_CONFIG
    )
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

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=7)
    before_start_date = end_date - timedelta(days=365)
    after_end_date = end_date + timedelta(days=365)

    customer_entities = list(range(1001, 1110))
    customer_df = driver_test_data.create_customer_daily_profile_df(
        customer_entities, start_date, end_date
    )
    _customer_feature_view: Optional[FeatureView] = None

    driver_entities = list(range(5001, 5110))
    driver_df = driver_test_data.create_driver_hourly_stats_df(
        driver_entities, start_date, end_date
    )
    _driver_stats_feature_view: Optional[FeatureView] = None

    orders_df = driver_test_data.create_orders_df(
        customers=customer_entities,
        drivers=driver_entities,
        start_date=before_start_date,
        end_date=after_end_date,
        order_count=1000,
    )
    _orders_table: Optional[str] = None

    def customer_feature_view(self) -> FeatureView:
        if self._customer_feature_view is None:
            customer_table_id = self.data_source_creator.get_prefixed_table_name(
                self.name, "customer_profile"
            )
            ds = self.data_source_creator.create_data_source(
                customer_table_id,
                self.customer_df,
                event_timestamp_column="event_timestamp",
                created_timestamp_column="created",
            )
            self._customer_feature_view = create_customer_daily_profile_feature_view(ds)
        return self._customer_feature_view

    def driver_stats_feature_view(self) -> FeatureView:
        if self._driver_stats_feature_view is None:
            driver_table_id = self.data_source_creator.get_prefixed_table_name(
                self.name, "driver_hourly"
            )
            ds = self.data_source_creator.create_data_source(
                driver_table_id,
                self.driver_df,
                event_timestamp_column="event_timestamp",
                created_timestamp_column="created",
            )
            self._driver_stats_feature_view = create_driver_hourly_stats_feature_view(
                ds
            )
        return self._driver_stats_feature_view

    def orders_table(self) -> Optional[str]:
        if self._orders_table is None:
            orders_table_id = self.data_source_creator.get_prefixed_table_name(
                self.name, "orders"
            )
            ds = self.data_source_creator.create_data_source(
                orders_table_id,
                self.orders_df,
                event_timestamp_column="event_timestamp",
                created_timestamp_column="created",
            )
            if hasattr(ds, "table_ref"):
                self._orders_table = ds.table_ref
            elif hasattr(ds, "table"):
                self._orders_table = ds.table
        return self._orders_table


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
            for p in ["local", "gcp"]:
                new_configs.append(replace(c, provider=p))
    return new_configs


@contextmanager
def construct_test_environment(
    test_repo_config: TestRepoConfig,
    create_and_apply: bool = False,
    materialize: bool = False,
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
    ds = offline_creator.create_data_source(
        project, df, field_mapping={"ts_1": "ts", "id": "driver_id"}
    )
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
        environment = Environment(
            name=project,
            test_repo_config=test_repo_config,
            feature_store=fs,
            data_source=ds,
            data_source_creator=offline_creator,
        )

        fvs = []
        entities = []
        try:
            if create_and_apply:
                entities.extend([driver(), customer()])
                fvs.extend(
                    [
                        environment.driver_stats_feature_view(),
                        environment.customer_feature_view(),
                    ]
                )
                fs.apply(fvs + entities)

            if materialize:
                fs.materialize(environment.start_date, environment.end_date)

            yield environment
        finally:
            offline_creator.teardown()
            fs.teardown()


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
    This decorator should be used by tests that rely on the offline store. These tests are expected to be parameterized,
    and receive an Environment object that contains a reference to a Feature Store with pre-applied
    entities and feature views.

    The decorator also ensures that sample data needed for the test is available in the relevant offline store.

    Decorated tests should interact with the offline store, via the FeatureStore.get_historical_features method. They
    may perform more operations as needed.

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


def parametrize_online_test(online_test):
    """
    This decorator should be used by tests that rely on the offline store. These tests are expected to be parameterized,
    and receive an Environment object that contains a reference to a Feature Store with pre-applied
    entities and feature views.

    The decorator also ensures that sample data needed for the test is available in the relevant offline store. This
    data is also materialized into the online store.

    The decorator takes care of tearing down the feature store, as well as the sample data.
    """

    configs = vary_providers_for_offline_stores(FULL_REPO_CONFIGS)
    configs = vary_full_feature_names(configs)
    configs = vary_infer_event_timestamp_col(configs)

    @pytest.mark.integration
    @pytest.mark.parametrize("config", configs, ids=lambda v: str(v))
    def inner_test(config):
        with construct_test_environment(
            config, create_and_apply=True, materialize=True
        ) as environment:
            online_test(environment)

    return inner_test
