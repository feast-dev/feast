import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
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
]


def construct_universal_entities() -> Dict[str, List[Any]]:
    return {"customer": list(range(1001, 1110)), "driver": list(range(5001, 5110))}


def construct_universal_datasets(
    entities: Dict[str, List[Any]], start_time: datetime, end_time: datetime
) -> Dict[str, pd.DataFrame]:
    customer_df = driver_test_data.create_customer_daily_profile_df(
        entities["customer"], start_time, end_time
    )
    driver_df = driver_test_data.create_driver_hourly_stats_df(
        entities["driver"], start_time, end_time
    )
    orders_df = driver_test_data.create_orders_df(
        customers=entities["customer"],
        drivers=entities["driver"],
        start_date=end_time - timedelta(days=365),
        end_date=end_time + timedelta(days=365),
        order_count=1000,
    )

    return {"customer": customer_df, "driver": driver_df, "orders": orders_df}


def construct_universal_data_sources(
    datasets: Dict[str, pd.DataFrame], data_source_creator: DataSourceCreator
) -> Dict[str, DataSource]:
    customer_ds = data_source_creator.create_data_sources(
        datasets["customer"],
        suffix="customer_profile",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    driver_ds = data_source_creator.create_data_sources(
        datasets["driver"],
        suffix="driver_hourly",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    orders_ds = data_source_creator.create_data_sources(
        datasets["orders"],
        suffix="orders",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    return {"customer": customer_ds, "driver": driver_ds, "orders": orders_ds}


def construct_universal_feature_views(
    data_sources: Dict[str, DataSource]
) -> Dict[str, FeatureView]:
    return {
        "customer": create_customer_daily_profile_feature_view(
            data_sources["customer"]
        ),
        "driver": create_driver_hourly_stats_feature_view(data_sources["driver"]),
    }


def setup_entities(
    environment: "Environment", entities_override: Optional[Dict[str, List[Any]]] = None
) -> "Environment":
    environment.entities = (
        entities_override if entities_override else construct_universal_entities()
    )
    return environment


def setup_datasets(
    environment: "Environment",
    datasets_override: Optional[Dict[str, pd.DataFrame]] = None,
) -> "Environment":
    environment.datasets = (
        datasets_override
        if datasets_override
        else construct_universal_datasets(
            environment.entities, environment.start_date, environment.end_date
        )
    )
    return environment


def setup_data_sources(
    environment: "Environment",
    data_sources_override: Optional[Dict[str, DataSource]] = None,
) -> "Environment":
    environment.datasources = (
        data_sources_override
        if data_sources_override
        else construct_universal_data_sources(
            environment.datasets, environment.data_source_creator
        )
    )
    return environment


def setup_feature_views(
    environment: "Environment",
    feature_views_override: Optional[Dict[str, FeatureView]] = None,
) -> "Environment":
    environment.feature_views = (
        feature_views_override
        if feature_views_override
        else construct_universal_feature_views(environment.datasources)
    )
    return environment


@dataclass
class Environment:
    name: str
    test_repo_config: TestRepoConfig
    feature_store: FeatureStore
    data_source: DataSource
    data_source_creator: DataSourceCreator

    entities_creator: Callable[[], Dict[str, List[Any]]] = field(
        default=construct_universal_entities
    )

    entities: Dict[str, List[Any]] = field(default_factory=dict)
    datasets: Dict[str, pd.DataFrame] = field(default_factory=dict)
    datasources: Dict[str, DataSource] = field(default_factory=dict)
    feature_views: Dict[str, FeatureView] = field(default_factory=list)

    end_date: datetime = field(
        default=datetime.now().replace(microsecond=0, second=0, minute=0)
    )

    def __post_init__(self):
        self.start_date: datetime = self.end_date - timedelta(days=7)


def table_name_from_data_source(ds: DataSource) -> Optional[str]:
    if hasattr(ds, "table_ref"):
        return ds.table_ref
    elif hasattr(ds, "table"):
        return ds.table
    return None


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
def construct_universal_test_environment(
    test_repo_config: TestRepoConfig,
    create_and_apply: bool = False,
    materialize: bool = False,
    data_source_cache=None,
) -> Environment:
    """
    This method should take in the parameters from the test repo config and created a feature repo, apply it,
    and return the constructed feature store object to callers.

    This feature store object can be interacted for the purposes of tests.
    The user is *not* expected to perform any clean up actions.

    :param test_repo_config: configuration
    :return: A feature store built using the supplied configuration.
    """

    project = f"test_correctness_{str(uuid.uuid4()).replace('-', '')[:8]}"

    module_name, config_class_name = test_repo_config.offline_store_creator.rsplit(
        ".", 1
    )

    offline_creator: DataSourceCreator = importer.get_class_from_type(
        module_name, config_class_name, "DataSourceCreator"
    )(project)

    # This needs to be abstracted away for test_e2e_universal which uses a different dataset.
    df = create_dataset()
    ds = offline_creator.create_data_sources(
        df, destination=project, field_mapping={"ts_1": "ts", "id": "driver_id"}
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
        print(f"Data Source Cache: {data_source_cache}")
        if data_source_cache:
            fixtures = data_source_cache.get(test_repo_config)
            print(f"Fixtures: {fixtures}")
            if fixtures:
                environment = setup_entities(environment, entities_override=fixtures[0])
                environment = setup_datasets(environment, datasets_override=fixtures[1])
                environment = setup_data_sources(
                    environment, data_sources_override=fixtures[2]
                )
            else:
                environment = setup_entities(environment)
                environment = setup_datasets(environment)
                environment = setup_data_sources(environment)
                data_source_cache.put(
                    test_repo_config,
                    environment.entities,
                    environment.datasets,
                    environment.datasources,
                    offline_creator,
                )
        else:
            environment = setup_entities(environment)
            environment = setup_datasets(environment)
            environment = setup_data_sources(environment)

        environment = setup_feature_views(environment)

        fvs = []
        entities = []
        try:
            if create_and_apply:
                entities.extend([driver(), customer()])
                fvs.extend(environment.feature_views.values())
                fs.apply(fvs + entities)

            if materialize:
                fs.materialize(environment.start_date, environment.end_date)

            yield environment
        finally:
            if not data_source_cache:
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
    def inner_test(config, data_source_cache):
        with construct_universal_test_environment(
            config, data_source_cache=data_source_cache
        ) as environment:
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

    configs = vary_full_feature_names(FULL_REPO_CONFIGS)

    @pytest.mark.integration
    @pytest.mark.parametrize("config", configs, ids=lambda v: str(v))
    def inner_test(config, data_source_cache):
        with construct_universal_test_environment(
            config, create_and_apply=True, data_source_cache=data_source_cache
        ) as environment:
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

    configs = vary_full_feature_names(FULL_REPO_CONFIGS)

    @pytest.mark.integration
    @pytest.mark.parametrize("config", configs, ids=lambda v: str(v))
    def inner_test(config, data_source_cache):
        with construct_universal_test_environment(
            config,
            create_and_apply=True,
            materialize=True,
            data_source_cache=data_source_cache,
        ) as environment:
            online_test(environment)

    return inner_test
