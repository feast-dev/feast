import dataclasses
import importlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pandas as pd
import pytest
import yaml

from feast import FeatureStore, FeatureView, OnDemandFeatureView, driver_test_data
from feast.constants import FULL_REPO_CONFIGS_MODULE_ENV_NAME
from feast.data_source import DataSource
from feast.errors import FeastModuleImportError
from feast.infra.feature_servers.base_config import FeatureLoggingConfig
from feast.infra.feature_servers.local_process.config import LocalFeatureServerConfig
from feast.repo_config import RegistryConfig, RepoConfig
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
    RegistryLocation,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)
from tests.integration.feature_repos.universal.feature_views import (
    conv_rate_plus_100_feature_view,
    create_conv_rate_request_source,
    create_customer_daily_profile_feature_view,
    create_driver_hourly_stats_batch_feature_view,
    create_driver_hourly_stats_feature_view,
    create_field_mapping_feature_view,
    create_global_stats_feature_view,
    create_location_stats_feature_view,
    create_order_feature_view,
    create_pushable_feature_view,
)
from tests.integration.feature_repos.universal.online_store.bigtable import (
    BigtableOnlineStoreCreator,
)
from tests.integration.feature_repos.universal.online_store.datastore import (
    DatastoreOnlineStoreCreator,
)
from tests.integration.feature_repos.universal.online_store.dynamodb import (
    DynamoDBOnlineStoreCreator,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

DYNAMO_CONFIG = {"type": "dynamodb", "region": "us-west-2"}
REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}
REDIS_CLUSTER_CONFIG = {
    "type": "redis",
    "redis_type": "redis_cluster",
    # Redis Cluster Port Forwarding is setup in "pr_integration_tests.yaml" under "Setup Redis Cluster".
    "connection_string": "127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003",
}

SNOWFLAKE_CONFIG = {
    "type": "snowflake.online",
    "account": os.getenv("SNOWFLAKE_CI_DEPLOYMENT", ""),
    "user": os.getenv("SNOWFLAKE_CI_USER", ""),
    "password": os.getenv("SNOWFLAKE_CI_PASSWORD", ""),
    "role": os.getenv("SNOWFLAKE_CI_ROLE", ""),
    "warehouse": os.getenv("SNOWFLAKE_CI_WAREHOUSE", ""),
    "database": "FEAST",
    "schema": "ONLINE",
}

BIGTABLE_CONFIG = {
    "type": "bigtable",
    "project_id": os.getenv("GCLOUD_PROJECT", "kf-feast"),
    "instance": os.getenv("BIGTABLE_INSTANCE_ID", "feast-integration-tests"),
}

ROCKSET_CONFIG = {
    "type": "rockset",
    "api_key": os.getenv("ROCKSET_APIKEY", ""),
    "host": os.getenv("ROCKSET_APISERVER", "api.rs2.usw2.rockset.com"),
}

OFFLINE_STORE_TO_PROVIDER_CONFIG: Dict[str, DataSourceCreator] = {
    "file": ("local", FileDataSourceCreator),
    "bigquery": ("gcp", BigQueryDataSourceCreator),
    "redshift": ("aws", RedshiftDataSourceCreator),
    "snowflake": ("aws", SnowflakeDataSourceCreator),
}

AVAILABLE_OFFLINE_STORES: List[Tuple[str, Type[DataSourceCreator]]] = [
    ("local", FileDataSourceCreator),
]

AVAILABLE_ONLINE_STORES: Dict[
    str, Tuple[Union[str, Dict[str, str]], Optional[Type[OnlineStoreCreator]]]
] = {
    "sqlite": ({"type": "sqlite"}, None),
}

# Only configure Cloud DWH if running full integration tests
if os.getenv("FEAST_IS_LOCAL_TEST", "False") != "True":
    AVAILABLE_OFFLINE_STORES.extend(
        [
            ("gcp", BigQueryDataSourceCreator),
            ("aws", RedshiftDataSourceCreator),
            ("aws", SnowflakeDataSourceCreator),
        ]
    )

    AVAILABLE_ONLINE_STORES["redis"] = (REDIS_CONFIG, None)
    AVAILABLE_ONLINE_STORES["dynamodb"] = (DYNAMO_CONFIG, None)
    AVAILABLE_ONLINE_STORES["datastore"] = ("datastore", None)
    AVAILABLE_ONLINE_STORES["snowflake"] = (SNOWFLAKE_CONFIG, None)
    AVAILABLE_ONLINE_STORES["bigtable"] = (BIGTABLE_CONFIG, None)

    # Uncomment to test using private Rockset account. Currently not enabled as
    # there is no dedicated Rockset instance for CI testing and there is no
    # containerized version of Rockset.
    # AVAILABLE_ONLINE_STORES["rockset"] = (ROCKSET_CONFIG, None)


full_repo_configs_module = os.environ.get(FULL_REPO_CONFIGS_MODULE_ENV_NAME)
if full_repo_configs_module is not None:
    try:
        module = importlib.import_module(full_repo_configs_module)
    except ImportError as e:
        raise FeastModuleImportError(
            full_repo_configs_module, "FULL_REPO_CONFIGS"
        ) from e

    try:
        AVAILABLE_ONLINE_STORES = getattr(module, "AVAILABLE_ONLINE_STORES")
        AVAILABLE_OFFLINE_STORES = getattr(module, "AVAILABLE_OFFLINE_STORES")
    except AttributeError:
        try:
            FULL_REPO_CONFIGS: List[IntegrationTestRepoConfig] = getattr(
                module, "FULL_REPO_CONFIGS"
            )
        except AttributeError as e:
            raise FeastModuleImportError(
                full_repo_configs_module, "FULL_REPO_CONFIGS"
            ) from e

        AVAILABLE_OFFLINE_STORES = [
            (config.provider, config.offline_store_creator)
            for config in FULL_REPO_CONFIGS
        ]
        AVAILABLE_OFFLINE_STORES = list(set(AVAILABLE_OFFLINE_STORES))  # unique only

        AVAILABLE_ONLINE_STORES = {
            c.online_store["type"]
            if isinstance(c.online_store, dict)
            else c.online_store: (c.online_store, c.online_store_creator)
            for c in FULL_REPO_CONFIGS
        }


# Replace online stores with emulated online stores if we're running local integration tests
if os.getenv("FEAST_LOCAL_ONLINE_CONTAINER", "False").lower() == "true":
    replacements: Dict[
        str, Tuple[Union[str, Dict[str, str]], Optional[Type[OnlineStoreCreator]]]
    ] = {
        "redis": (REDIS_CONFIG, RedisOnlineStoreCreator),
        "dynamodb": (DYNAMO_CONFIG, DynamoDBOnlineStoreCreator),
        "datastore": ("datastore", DatastoreOnlineStoreCreator),
        "bigtable": ("bigtable", BigtableOnlineStoreCreator),
    }

    for key, replacement in replacements.items():
        if key in AVAILABLE_ONLINE_STORES:
            AVAILABLE_ONLINE_STORES[key] = replacement


@dataclass
class UniversalEntities:
    customer_vals: List[Any]
    driver_vals: List[Any]
    location_vals: List[Any]


def construct_universal_entities() -> UniversalEntities:
    return UniversalEntities(
        customer_vals=list(range(1001, 1020)),
        driver_vals=list(range(5001, 5020)),
        location_vals=list(range(1, 50)),
    )


@dataclass
class UniversalDatasets:
    customer_df: pd.DataFrame
    driver_df: pd.DataFrame
    location_df: pd.DataFrame
    orders_df: pd.DataFrame
    global_df: pd.DataFrame
    field_mapping_df: pd.DataFrame
    entity_df: pd.DataFrame


def construct_universal_datasets(
    entities: UniversalEntities, start_time: datetime, end_time: datetime
) -> UniversalDatasets:
    customer_df = driver_test_data.create_customer_daily_profile_df(
        entities.customer_vals, start_time, end_time
    )
    driver_df = driver_test_data.create_driver_hourly_stats_df(
        entities.driver_vals, start_time, end_time
    )
    location_df = driver_test_data.create_location_stats_df(
        entities.location_vals, start_time, end_time
    )
    orders_df = driver_test_data.create_orders_df(
        customers=entities.customer_vals,
        drivers=entities.driver_vals,
        locations=entities.location_vals,
        start_date=start_time,
        end_date=end_time,
        order_count=20,
    )
    global_df = driver_test_data.create_global_daily_stats_df(start_time, end_time)
    field_mapping_df = driver_test_data.create_field_mapping_df(start_time, end_time)
    entity_df = orders_df[
        [
            "customer_id",
            "driver_id",
            "order_id",
            "origin_id",
            "destination_id",
            "event_timestamp",
        ]
    ]

    return UniversalDatasets(
        customer_df=customer_df,
        driver_df=driver_df,
        location_df=location_df,
        orders_df=orders_df,
        global_df=global_df,
        field_mapping_df=field_mapping_df,
        entity_df=entity_df,
    )


@dataclass
class UniversalDataSources:
    customer: DataSource
    driver: DataSource
    location: DataSource
    orders: DataSource
    global_ds: DataSource
    field_mapping: DataSource

    def values(self):
        return dataclasses.asdict(self).values()


def construct_universal_data_sources(
    datasets: UniversalDatasets, data_source_creator: DataSourceCreator
) -> UniversalDataSources:
    customer_ds = data_source_creator.create_data_source(
        datasets.customer_df,
        destination_name="customer_profile",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    driver_ds = data_source_creator.create_data_source(
        datasets.driver_df,
        destination_name="driver_hourly",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    location_ds = data_source_creator.create_data_source(
        datasets.location_df,
        destination_name="location_hourly",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    orders_ds = data_source_creator.create_data_source(
        datasets.orders_df,
        destination_name="orders",
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
    )
    global_ds = data_source_creator.create_data_source(
        datasets.global_df,
        destination_name="global",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    field_mapping_ds = data_source_creator.create_data_source(
        datasets.field_mapping_df,
        destination_name="field_mapping",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        field_mapping={"column_name": "feature_name"},
    )
    return UniversalDataSources(
        customer=customer_ds,
        driver=driver_ds,
        location=location_ds,
        orders=orders_ds,
        global_ds=global_ds,
        field_mapping=field_mapping_ds,
    )


@dataclass
class UniversalFeatureViews:
    customer: FeatureView
    global_fv: FeatureView
    driver: FeatureView
    driver_odfv: OnDemandFeatureView
    order: FeatureView
    location: FeatureView
    field_mapping: FeatureView
    pushed_locations: FeatureView

    def values(self):
        return dataclasses.asdict(self).values()


def construct_universal_feature_views(
    data_sources: UniversalDataSources,
    with_odfv: bool = True,
) -> UniversalFeatureViews:
    driver_hourly_stats = create_driver_hourly_stats_feature_view(data_sources.driver)
    driver_hourly_stats_base_feature_view = (
        create_driver_hourly_stats_batch_feature_view(data_sources.driver)
    )
    return UniversalFeatureViews(
        customer=create_customer_daily_profile_feature_view(data_sources.customer),
        global_fv=create_global_stats_feature_view(data_sources.global_ds),
        driver=driver_hourly_stats,
        driver_odfv=conv_rate_plus_100_feature_view(
            [driver_hourly_stats_base_feature_view, create_conv_rate_request_source()]
        )
        if with_odfv
        else None,
        order=create_order_feature_view(data_sources.orders),
        location=create_location_stats_feature_view(data_sources.location),
        field_mapping=create_field_mapping_feature_view(data_sources.field_mapping),
        pushed_locations=create_pushable_feature_view(data_sources.location),
    )


@dataclass
class Environment:
    name: str
    test_repo_config: IntegrationTestRepoConfig
    feature_store: FeatureStore
    data_source_creator: DataSourceCreator
    python_feature_server: bool
    worker_id: str
    online_store_creator: Optional[OnlineStoreCreator] = None
    fixture_request: Optional[pytest.FixtureRequest] = None

    def __post_init__(self):
        self.end_date = datetime.utcnow().replace(microsecond=0, second=0, minute=0)
        self.start_date: datetime = self.end_date - timedelta(days=3)


def table_name_from_data_source(ds: DataSource) -> Optional[str]:
    if hasattr(ds, "table_ref"):
        return ds.table_ref  # type: ignore
    elif hasattr(ds, "table"):
        return ds.table  # type: ignore
    return None


def construct_test_environment(
    test_repo_config: IntegrationTestRepoConfig,
    fixture_request: Optional[pytest.FixtureRequest],
    test_suite_name: str = "integration_test",
    worker_id: str = "worker_id",
    entity_key_serialization_version: int = 2,
) -> Environment:
    _uuid = str(uuid.uuid4()).replace("-", "")[:6]

    run_id = os.getenv("GITHUB_RUN_ID", default=None)
    run_id = f"gh_run_{run_id}_{_uuid}" if run_id else _uuid
    run_num = os.getenv("GITHUB_RUN_NUMBER", default=1)

    project = f"{test_suite_name}_{run_id}_{run_num}"

    offline_creator: DataSourceCreator = test_repo_config.offline_store_creator(
        project, fixture_request=fixture_request
    )
    offline_store_config = offline_creator.create_offline_store_config()

    if test_repo_config.online_store_creator:
        online_creator = test_repo_config.online_store_creator(
            project, fixture_request=fixture_request
        )
        online_store = (
            test_repo_config.online_store
        ) = online_creator.create_online_store()
    else:
        online_creator = None
        online_store = test_repo_config.online_store

    if test_repo_config.python_feature_server and test_repo_config.provider == "aws":
        from feast.infra.feature_servers.aws_lambda.config import (
            AwsLambdaFeatureServerConfig,
        )

        feature_server = AwsLambdaFeatureServerConfig(
            enabled=True,
            execution_role_name=os.getenv(
                "AWS_LAMBDA_ROLE",
                "arn:aws:iam::402087665549:role/lambda_execution_role",
            ),
        )

    else:
        feature_server = LocalFeatureServerConfig(
            feature_logging=FeatureLoggingConfig(enabled=True)
        )

    repo_dir_name = tempfile.mkdtemp()
    if (
        test_repo_config.python_feature_server and test_repo_config.provider == "aws"
    ) or test_repo_config.registry_location == RegistryLocation.S3:
        aws_registry_path = os.getenv(
            "AWS_REGISTRY_PATH", "s3://feast-integration-tests/registries"
        )
        registry: Union[
            str, RegistryConfig
        ] = f"{aws_registry_path}/{project}/registry.db"
    else:
        registry = RegistryConfig(
            path=str(Path(repo_dir_name) / "registry.db"),
            cache_ttl_seconds=1,
        )

    config = RepoConfig(
        registry=registry,
        project=project,
        provider=test_repo_config.provider,
        offline_store=offline_store_config,
        online_store=online_store,
        batch_engine=test_repo_config.batch_engine,
        repo_path=repo_dir_name,
        feature_server=feature_server,
        entity_key_serialization_version=entity_key_serialization_version,
    )

    # Create feature_store.yaml out of the config
    with open(Path(repo_dir_name) / "feature_store.yaml", "w") as f:
        yaml.safe_dump(json.loads(config.json()), f)

    fs = FeatureStore(repo_dir_name)
    # We need to initialize the registry, because if nothing is applied in the test before tearing down
    # the feature store, that will cause the teardown method to blow up.
    fs.registry._initialize_registry(project)
    environment = Environment(
        name=project,
        test_repo_config=test_repo_config,
        feature_store=fs,
        data_source_creator=offline_creator,
        python_feature_server=test_repo_config.python_feature_server,
        worker_id=worker_id,
        online_store_creator=online_creator,
        fixture_request=fixture_request,
    )

    return environment


TestData = Tuple[UniversalEntities, UniversalDatasets, UniversalDataSources]


def construct_universal_test_data(environment: Environment) -> TestData:
    entities = construct_universal_entities()
    datasets = construct_universal_datasets(
        entities, environment.start_date, environment.end_date
    )
    data_sources = construct_universal_data_sources(
        datasets, environment.data_source_creator
    )

    return entities, datasets, data_sources
