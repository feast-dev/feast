import dataclasses
import importlib
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import pandas as pd
import pytest

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
    driver_test_data,
)
from feast.constants import FULL_REPO_CONFIGS_MODULE_ENV_NAME
from feast.data_source import DataSource
from feast.errors import FeastModuleImportError
from feast.infra.feature_servers.base_config import (
    BaseFeatureServerConfig,
    FeatureLoggingConfig,
)
from feast.infra.feature_servers.local_process.config import LocalFeatureServerConfig
from feast.permissions.action import AuthzedAction
from feast.permissions.auth_model import OidcAuthConfig
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.repo_config import RegistryConfig, RepoConfig
from feast.utils import _utc_now
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
    DuckDBDataSourceCreator,
    DuckDBDeltaDataSourceCreator,
    FileDataSourceCreator,
    RemoteOfflineOidcAuthStoreDataSourceCreator,
    RemoteOfflineStoreDataSourceCreator,
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
    "database": os.getenv("SNOWFLAKE_CI_DATABASE", "FEAST"),
    "schema": os.getenv("SNOWFLAKE_CI_SCHEMA_ONLINE", "ONLINE"),
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

IKV_CONFIG = {
    "type": "ikv",
    "account_id": os.getenv("IKV_ACCOUNT_ID", ""),
    "account_passkey": os.getenv("IKV_ACCOUNT_PASSKEY", ""),
    "store_name": os.getenv("IKV_STORE_NAME", ""),
    "mount_directory": os.getenv("IKV_MOUNT_DIR", ""),
}

OFFLINE_STORE_TO_PROVIDER_CONFIG: Dict[str, Tuple[str, Type[DataSourceCreator]]] = {
    "file": ("local", FileDataSourceCreator),
    "bigquery": ("gcp", BigQueryDataSourceCreator),
    "redshift": ("aws", RedshiftDataSourceCreator),
    "snowflake": ("aws", SnowflakeDataSourceCreator),
}

AVAILABLE_OFFLINE_STORES: List[Tuple[str, Type[DataSourceCreator]]] = [
    ("local", FileDataSourceCreator),
    ("local", DuckDBDataSourceCreator),
    ("local", DuckDBDeltaDataSourceCreator),
    ("local", RemoteOfflineStoreDataSourceCreator),
    ("local", RemoteOfflineOidcAuthStoreDataSourceCreator),
]

if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
    AVAILABLE_OFFLINE_STORES.extend(
        [
            # todo: @tokoko to reenable
            # ("local", DuckDBDeltaS3DataSourceCreator),
        ]
    )

AVAILABLE_ONLINE_STORES: Dict[
    str, Tuple[Union[str, Dict[Any, Any]], Optional[Type[OnlineStoreCreator]]]
] = {"sqlite": ({"type": "sqlite"}, None)}

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

    # Uncomment to test using private IKV account. Currently not enabled as
    # there is no dedicated IKV instance for CI testing and there is no
    # containerized version of IKV.
    # AVAILABLE_ONLINE_STORES["ikv"] = (IKV_CONFIG, None)

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
            else c.online_store: (c.online_store, c.online_store_creator)  # type: ignore
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
    driver_odfv: Optional[OnDemandFeatureView]
    order: FeatureView
    location: FeatureView
    field_mapping: FeatureView
    pushed_locations: FeatureView

    def values(self):
        return dataclasses.asdict(self).values()


def construct_universal_feature_views(
    data_sources: UniversalDataSources,
    with_odfv: bool = True,
    use_substrait_odfv: bool = False,
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
            [
                driver_hourly_stats_base_feature_view[["conv_rate"]],
                create_conv_rate_request_source(),
            ],
            use_substrait_odfv=use_substrait_odfv,
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
    project: str
    provider: str
    registry: RegistryConfig
    data_source_creator: DataSourceCreator
    online_store_creator: Optional[OnlineStoreCreator]
    online_store: Optional[Union[str, Dict]]
    batch_engine: Optional[Union[str, Dict]]
    python_feature_server: bool
    worker_id: str
    feature_server: BaseFeatureServerConfig
    entity_key_serialization_version: int
    repo_dir_name: str
    fixture_request: Optional[pytest.FixtureRequest] = None

    def __post_init__(self):
        self.end_date = _utc_now().replace(microsecond=0, second=0, minute=0)
        self.start_date: datetime = self.end_date - timedelta(days=3)

    def setup(self):
        self.data_source_creator.setup(self.registry)

        self.config = RepoConfig(
            registry=self.registry,
            project=self.project,
            provider=self.provider,
            offline_store=self.data_source_creator.create_offline_store_config(),
            online_store=self.online_store_creator.create_online_store()
            if self.online_store_creator
            else self.online_store,
            batch_engine=self.batch_engine,
            repo_path=self.repo_dir_name,
            feature_server=self.feature_server,
            entity_key_serialization_version=self.entity_key_serialization_version,
        )

        self.feature_store = FeatureStore(config=self.config)

    def teardown(self):
        self.feature_store.teardown()
        self.data_source_creator.teardown()
        if self.online_store_creator:
            self.online_store_creator.teardown()


@dataclass
class OfflineServerPermissionsEnvironment(Environment):
    def setup(self):
        self.data_source_creator.setup(self.registry)
        keycloak_url = self.data_source_creator.get_keycloak_url()
        auth_config = OidcAuthConfig(
            client_id="feast-integration-client",
            client_secret="feast-integration-client-secret",
            username="reader_writer",
            password="password",
            realm="master",
            type="oidc",
            auth_server_url=keycloak_url,
            auth_discovery_url=f"{keycloak_url}/realms/master/.well-known"
            f"/openid-configuration",
        )
        self.config = RepoConfig(
            registry=self.registry,
            project=self.project,
            provider=self.provider,
            offline_store=self.data_source_creator.create_offline_store_config(),
            online_store=self.online_store_creator.create_online_store()
            if self.online_store_creator
            else self.online_store,
            batch_engine=self.batch_engine,
            repo_path=self.repo_dir_name,
            feature_server=self.feature_server,
            entity_key_serialization_version=self.entity_key_serialization_version,
            auth=auth_config,
        )

        self.feature_store = FeatureStore(config=self.config)
        permissions_list = [
            Permission(
                name="offline_permissions_perm",
                types=Permission,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="offline_entities_perm",
                types=Entity,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="offline_fv_perm",
                types=FeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="offline_odfv_perm",
                types=OnDemandFeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.QUERY_OFFLINE],
            ),
            Permission(
                name="offline_sfv_perm",
                types=StreamFeatureView,
                with_subclasses=False,
                policy=RoleBasedPolicy(roles=["reader"]),
                actions=[AuthzedAction.QUERY_OFFLINE],
            ),
        ]
        self.feature_store.apply(permissions_list)


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

    if test_repo_config.online_store_creator:
        online_creator = test_repo_config.online_store_creator(
            project, fixture_request=fixture_request
        )
    else:
        online_creator = None

    feature_server = LocalFeatureServerConfig(
        feature_logging=FeatureLoggingConfig(enabled=True)
    )

    repo_dir_name = tempfile.mkdtemp()
    if test_repo_config.registry_location == RegistryLocation.S3:
        aws_registry_path = os.getenv(
            "AWS_REGISTRY_PATH", "s3://feast-int-bucket/registries"
        )
        registry = RegistryConfig(path=f"{aws_registry_path}/{project}/registry.db")
    else:
        registry = RegistryConfig(
            path=str(Path(repo_dir_name) / "registry.db"),
            cache_ttl_seconds=1,
        )

    environment_params = {
        "name": project,
        "provider": test_repo_config.provider,
        "data_source_creator": offline_creator,
        "python_feature_server": test_repo_config.python_feature_server,
        "worker_id": worker_id,
        "online_store_creator": online_creator,
        "fixture_request": fixture_request,
        "project": project,
        "registry": registry,
        "feature_server": feature_server,
        "entity_key_serialization_version": entity_key_serialization_version,
        "repo_dir_name": repo_dir_name,
        "batch_engine": test_repo_config.batch_engine,
        "online_store": test_repo_config.online_store,
    }

    if not isinstance(offline_creator, RemoteOfflineOidcAuthStoreDataSourceCreator):
        environment = Environment(**environment_params)
    else:
        environment = OfflineServerPermissionsEnvironment(**environment_params)
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
