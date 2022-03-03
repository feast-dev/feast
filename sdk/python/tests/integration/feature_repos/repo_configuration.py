import dataclasses
import importlib
import json
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

import pandas as pd
import yaml

from feast import FeatureStore, FeatureView, OnDemandFeatureView, driver_test_data
from feast.constants import FULL_REPO_CONFIGS_MODULE_ENV_NAME
from feast.data_source import DataSource
from feast.errors import FeastModuleImportError
from feast.repo_config import RegistryConfig, RepoConfig
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)
from tests.integration.feature_repos.universal.feature_views import (
    conv_rate_plus_100_feature_view,
    create_conv_rate_request_data_source,
    create_customer_daily_profile_feature_view,
    create_driver_age_request_feature_view,
    create_driver_hourly_stats_feature_view,
    create_field_mapping_feature_view,
    create_global_stats_feature_view,
    create_location_stats_feature_view,
    create_order_feature_view,
)

DYNAMO_CONFIG = {"type": "dynamodb", "region": "us-west-2"}
# Port 12345 will chosen as default for redis node configuration because Redis Cluster is started off of nodes 6379 -> 6384. This causes conflicts in cli integration tests so we manually keep them separate.
REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:12345,db=0"}
REDIS_CLUSTER_CONFIG = {
    "type": "redis",
    "redis_type": "redis_cluster",
    # Redis Cluster Port Forwarding is setup in "pr_integration_tests.yaml" under "Setup Redis Cluster".
    "connection_string": "127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003",
}

# FULL_REPO_CONFIGS contains the repo configurations (e.g. provider, offline store,
# online store, test data, and more parameters) that most integration tests will test
# against. By default, FULL_REPO_CONFIGS uses the three providers (local, GCP, and AWS)
# with their default offline and online stores; it also tests the providers with the
# Redis online store. It can be overwritten by specifying a Python module through the
# FULL_REPO_CONFIGS_MODULE_ENV_NAME environment variable. In this case, that Python
# module will be imported and FULL_REPO_CONFIGS will be extracted from the file.
DEFAULT_FULL_REPO_CONFIGS: List[IntegrationTestRepoConfig] = [
    # Local configurations
    IntegrationTestRepoConfig(),
    IntegrationTestRepoConfig(python_feature_server=True),
]
if os.getenv("FEAST_IS_LOCAL_TEST", "False") != "True":
    DEFAULT_FULL_REPO_CONFIGS.extend(
        [
            # Redis configurations
            IntegrationTestRepoConfig(online_store=REDIS_CONFIG),
            IntegrationTestRepoConfig(online_store=REDIS_CLUSTER_CONFIG),
            # GCP configurations
            IntegrationTestRepoConfig(
                provider="gcp",
                offline_store_creator=BigQueryDataSourceCreator,
                online_store="datastore",
            ),
            IntegrationTestRepoConfig(
                provider="gcp",
                offline_store_creator=BigQueryDataSourceCreator,
                online_store=REDIS_CONFIG,
            ),
            # AWS configurations
            IntegrationTestRepoConfig(
                provider="aws",
                offline_store_creator=RedshiftDataSourceCreator,
                online_store=DYNAMO_CONFIG,
                python_feature_server=True,
            ),
            IntegrationTestRepoConfig(
                provider="aws",
                offline_store_creator=RedshiftDataSourceCreator,
                online_store=REDIS_CONFIG,
            ),
            # Snowflake configurations
            IntegrationTestRepoConfig(
                provider="aws",  # no list features, no feature server
                offline_store_creator=SnowflakeDataSourceCreator,
                online_store=REDIS_CONFIG,
            ),
        ]
    )
full_repo_configs_module = os.environ.get(FULL_REPO_CONFIGS_MODULE_ENV_NAME)
if full_repo_configs_module is not None:
    try:
        module = importlib.import_module(full_repo_configs_module)
        FULL_REPO_CONFIGS = getattr(module, "FULL_REPO_CONFIGS")
    except Exception as e:
        raise FeastModuleImportError(
            "FULL_REPO_CONFIGS", full_repo_configs_module
        ) from e
else:
    FULL_REPO_CONFIGS = DEFAULT_FULL_REPO_CONFIGS


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


def construct_universal_data_sources(
    datasets: UniversalDatasets, data_source_creator: DataSourceCreator
) -> UniversalDataSources:
    customer_ds = data_source_creator.create_data_source(
        datasets.customer_df,
        destination_name="customer_profile",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    driver_ds = data_source_creator.create_data_source(
        datasets.driver_df,
        destination_name="driver_hourly",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    location_ds = data_source_creator.create_data_source(
        datasets.location_df,
        destination_name="location_hourly",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    orders_ds = data_source_creator.create_data_source(
        datasets.orders_df,
        destination_name="orders",
        event_timestamp_column="event_timestamp",
        created_timestamp_column=None,
    )
    global_ds = data_source_creator.create_data_source(
        datasets.global_df,
        destination_name="global",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    field_mapping_ds = data_source_creator.create_data_source(
        datasets.field_mapping_df,
        destination_name="field_mapping",
        event_timestamp_column="event_timestamp",
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
    driver_age_request_fv: FeatureView
    order: FeatureView
    location: FeatureView
    field_mapping: FeatureView

    def values(self):
        return dataclasses.asdict(self).values()


def construct_universal_feature_views(
    data_sources: UniversalDataSources,
) -> UniversalFeatureViews:
    driver_hourly_stats = create_driver_hourly_stats_feature_view(data_sources.driver)
    return UniversalFeatureViews(
        customer=create_customer_daily_profile_feature_view(data_sources.customer),
        global_fv=create_global_stats_feature_view(data_sources.global_ds),
        driver=driver_hourly_stats,
        driver_odfv=conv_rate_plus_100_feature_view(
            {
                "driver": driver_hourly_stats,
                "input_request": create_conv_rate_request_data_source(),
            }
        ),
        driver_age_request_fv=create_driver_age_request_feature_view(),
        order=create_order_feature_view(data_sources.orders),
        location=create_location_stats_feature_view(data_sources.location),
        field_mapping=create_field_mapping_feature_view(data_sources.field_mapping),
    )


@dataclass
class Environment:
    name: str
    test_repo_config: IntegrationTestRepoConfig
    feature_store: FeatureStore
    data_source_creator: DataSourceCreator
    python_feature_server: bool
    worker_id: str

    def __post_init__(self):
        self.end_date = datetime.utcnow().replace(microsecond=0, second=0, minute=0)
        self.start_date: datetime = self.end_date - timedelta(days=3)

    def get_feature_server_endpoint(self) -> str:
        if self.python_feature_server and self.test_repo_config.provider == "local":
            return f"http://localhost:{self.get_local_server_port()}"
        return self.feature_store.get_feature_server_endpoint()

    def get_local_server_port(self) -> int:
        # Heuristic when running with xdist to extract unique ports for each worker
        parsed_worker_id = re.findall("gw(\\d+)", self.worker_id)
        if len(parsed_worker_id) != 0:
            worker_id_num = int(parsed_worker_id[0])
        else:
            worker_id_num = 0
        return 6566 + worker_id_num


def table_name_from_data_source(ds: DataSource) -> Optional[str]:
    if hasattr(ds, "table_ref"):
        return ds.table_ref  # type: ignore
    elif hasattr(ds, "table"):
        return ds.table  # type: ignore
    return None


def construct_test_environment(
    test_repo_config: IntegrationTestRepoConfig,
    test_suite_name: str = "integration_test",
    worker_id: str = "worker_id",
) -> Environment:
    _uuid = str(uuid.uuid4()).replace("-", "")[:6]

    run_id = os.getenv("GITHUB_RUN_ID", default=None)
    run_id = f"gh_run_{run_id}_{_uuid}" if run_id else _uuid
    run_num = os.getenv("GITHUB_RUN_NUMBER", default=1)

    project = f"{test_suite_name}_{run_id}_{run_num}"

    offline_creator: DataSourceCreator = test_repo_config.offline_store_creator(project)

    offline_store_config = offline_creator.create_offline_store_config()
    online_store = test_repo_config.online_store

    repo_dir_name = tempfile.mkdtemp()

    if test_repo_config.python_feature_server and test_repo_config.provider == "aws":
        from feast.infra.feature_servers.aws_lambda.config import (
            AwsLambdaFeatureServerConfig,
        )

        feature_server = AwsLambdaFeatureServerConfig(
            enabled=True,
            execution_role_name="arn:aws:iam::402087665549:role/lambda_execution_role",
        )

        registry = (
            f"s3://feast-integration-tests/registries/{project}/registry.db"
        )  # type: Union[str, RegistryConfig]
    else:
        # Note: even if it's a local feature server, the repo config does not have this configured
        feature_server = None
        registry = RegistryConfig(
            path=str(Path(repo_dir_name) / "registry.db"), cache_ttl_seconds=1,
        )

    config = RepoConfig(
        registry=registry,
        project=project,
        provider=test_repo_config.provider,
        offline_store=offline_store_config,
        online_store=online_store,
        repo_path=repo_dir_name,
        feature_server=feature_server,
    )

    # Create feature_store.yaml out of the config
    with open(Path(repo_dir_name) / "feature_store.yaml", "w") as f:
        yaml.safe_dump(json.loads(config.json()), f)

    fs = FeatureStore(repo_dir_name)
    # We need to initialize the registry, because if nothing is applied in the test before tearing down
    # the feature store, that will cause the teardown method to blow up.
    fs.registry._initialize_registry()
    environment = Environment(
        name=project,
        test_repo_config=test_repo_config,
        feature_store=fs,
        data_source_creator=offline_creator,
        python_feature_server=test_repo_config.python_feature_server,
        worker_id=worker_id,
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
