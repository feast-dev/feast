import random
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views, table_name_from_data_source)
from tests.integration.feature_repos.universal.data_sources.file import (
    RemoteOfflineOidcAuthStoreDataSourceCreator,
    RemoteOfflineStoreDataSourceCreator)
from tests.integration.feature_repos.universal.data_sources.snowflake import \
    SnowflakeDataSourceCreator
from tests.integration.feature_repos.universal.entities import (customer,
                                                                driver,
                                                                location)
from tests.utils.feature_records import (
    assert_feature_service_correctness,
    assert_feature_service_entity_mapping_correctness,
    get_expected_training_df, get_response_feature_name, validate_dataframes)

from feast.entity import Entity
from feast.errors import RequestDataNotFoundInEntityDfException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.offline_utils import \
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
from feast.types import Float32, Int32
from feast.utils import _utc_now

np.random.seed(0)


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: f"full:{v}")
@pytest.mark.parametrize(
    "use_substrait_odfv", [True, False], ids=lambda v: f"substrait:{v}"
)
def test_historical_features_main(
    environment, universal_data_sources, full_feature_names, use_substrait_odfv
):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources

    feature_views = construct_universal_feature_views(
        data_sources, use_substrait_odfv=use_substrait_odfv
    )

    entity_df_with_request_data = datasets.entity_df.copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]
    entity_df_with_request_data["driver_age"] = [
        i + 100 for i in range(len(entity_df_with_request_data))
    ]

    feature_service = FeatureService(
        name="convrate_plus100",
        features=[feature_views.driver[["conv_rate"]], feature_views.driver_odfv],
    )
    feature_service_entity_mapping = FeatureService(
        name="entity_mapping",
        features=[
            feature_views.location.with_name("origin").with_join_key_map(
                {"location_id": "origin_id"}
            ),
            feature_views.location.with_name("destination").with_join_key_map(
                {"location_id": "destination_id"}
            ),
        ],
    )

    store.apply(
        [
            driver(),
            customer(),
            location(),
            feature_service,
            feature_service_entity_mapping,
            *feature_views.values(),
        ]
    )

    event_timestamp = (
        DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in datasets.orders_df.columns
        else "e_ts"
    )
    full_expected_df = get_expected_training_df(
        datasets.customer_df,
        feature_views.customer,
        datasets.driver_df,
        feature_views.driver,
        datasets.orders_df,
        feature_views.order,
        datasets.location_df,
        feature_views.location,
        datasets.global_df,
        feature_views.global_fv,
        datasets.field_mapping_df,
        feature_views.field_mapping,
        entity_df_with_request_data,
        event_timestamp,
        full_feature_names,
    )

    # Only need the shadow entities features in the FeatureService test
    expected_df = full_expected_df.drop(
        columns=["origin__temperature", "destination__temperature"],
    )

    job_from_df = store.get_historical_features(
        entity_df=entity_df_with_request_data,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "conv_rate_plus_100:conv_rate_plus_100",
            "conv_rate_plus_100:conv_rate_plus_100_rounded",
            "conv_rate_plus_100:conv_rate_plus_val_to_add",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    if job_from_df.supports_remote_storage_export():
        files = job_from_df.to_remote_storage()
        assert len(files)  # 0  # This test should be way more detailed

    start_time = _utc_now()
    actual_df_from_df_entities = job_from_df.to_df()

    print(f"actual_df_from_df_entities shape: {actual_df_from_df_entities.shape}")
    end_time = _utc_now()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

    assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
    validate_dataframes(
        expected_df,
        actual_df_from_df_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )

    if not isinstance(
        environment.data_source_creator, (RemoteOfflineStoreDataSourceCreator, RemoteOfflineOidcAuthStoreDataSourceCreator)
    ):
        assert_feature_service_correctness(
            store,
            feature_service,
            full_feature_names,
            entity_df_with_request_data,
            expected_df,
            event_timestamp,
        )
        assert_feature_service_entity_mapping_correctness(
            store,
            feature_service_entity_mapping,
            full_feature_names,
            entity_df_with_request_data,
            full_expected_df,
            event_timestamp,
        )
    table_from_df_entities: pd.DataFrame = job_from_df.to_arrow().to_pandas()

    validate_dataframes(
        expected_df,
        table_from_df_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_with_shared_batch_source(
    environment, universal_data_sources, full_feature_names
):
    # Addresses https://github.com/feast-dev/feast/issues/2576

    store = environment.feature_store

    entities, datasets, data_sources = universal_data_sources
    driver_entity = driver()
    driver_stats_v1 = FeatureView(
        name="driver_stats_v1",
        entities=[driver_entity],
        schema=[Field(name="avg_daily_trips", dtype=Int32)],
        source=data_sources.driver,
    )
    driver_stats_v2 = FeatureView(
        name="driver_stats_v2",
        entities=[driver_entity],
        schema=[
            Field(name="avg_daily_trips", dtype=Int32),
            Field(name="conv_rate", dtype=Float32),
        ],
        source=data_sources.driver,
    )

    store.apply([driver_entity, driver_stats_v1, driver_stats_v2])

    with pytest.raises(KeyError):
        store.get_historical_features(
            entity_df=datasets.entity_df,
            features=[
                # `driver_stats_v1` does not have `conv_rate`
                "driver_stats_v1:conv_rate",
            ],
            full_feature_names=full_feature_names,
        ).to_df()


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_historical_features_with_missing_request_data(
    environment, universal_data_sources
):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    # If request data is missing that's needed for on demand transform, throw an error
    with pytest.raises(RequestDataNotFoundInEntityDfException):
        store.get_historical_features(
            entity_df=datasets.entity_df,
            features=[
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "conv_rate_plus_100:conv_rate_plus_100",
                "conv_rate_plus_100:conv_rate_plus_val_to_add",
                "global_stats:num_rides",
                "global_stats:avg_ride_length",
                "field_mapping:feature_name",
            ],
            full_feature_names=True,
        )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_with_entities_from_query(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store
    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    orders_table = table_name_from_data_source(data_sources.orders)
    if not orders_table:
        raise pytest.skip("Offline source is not sql-based")

    data_source_creator = environment.data_source_creator
    if isinstance(data_source_creator, SnowflakeDataSourceCreator):
        entity_df_query = f"""
        SELECT "customer_id", "driver_id", "order_id", "origin_id", "destination_id", "event_timestamp"
        FROM "{orders_table}"
        """
    else:
        entity_df_query = f"""
        SELECT customer_id, driver_id, order_id, origin_id, destination_id, event_timestamp
        FROM {orders_table}
        """

    store.apply([driver(), customer(), location(), *feature_views.values()])

    job_from_sql = store.get_historical_features(
        entity_df=entity_df_query,
        features=[
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    start_time = _utc_now()
    actual_df_from_sql_entities = job_from_sql.to_df()
    end_time = _utc_now()
    print(str(f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'"))

    event_timestamp = (
        DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in datasets.orders_df.columns
        else "e_ts"
    )
    full_expected_df = get_expected_training_df(
        datasets.customer_df,
        feature_views.customer,
        datasets.driver_df,
        feature_views.driver,
        datasets.orders_df,
        feature_views.order,
        datasets.location_df,
        feature_views.location,
        datasets.global_df,
        feature_views.global_fv,
        datasets.field_mapping_df,
        feature_views.field_mapping,
        datasets.entity_df,
        event_timestamp,
        full_feature_names,
    )

    # Not requesting the on demand transform with an entity_df query (can't add request data in them)
    expected_df_query = full_expected_df.drop(
        columns=[
            get_response_feature_name("conv_rate_plus_100", full_feature_names),
            get_response_feature_name("conv_rate_plus_100_rounded", full_feature_names),
            get_response_feature_name("avg_daily_trips", full_feature_names),
            get_response_feature_name("conv_rate", full_feature_names),
            "origin__temperature",
            "destination__temperature",
        ]
    )
    validate_dataframes(
        expected_df_query,
        actual_df_from_sql_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )

    table_from_sql_entities = job_from_sql.to_arrow().to_pandas()
    for col in table_from_sql_entities.columns:
        # check if col dtype is timezone naive
        if pd.api.types.is_datetime64_dtype(table_from_sql_entities[col]):
            table_from_sql_entities[col] = table_from_sql_entities[col].dt.tz_localize(
                "UTC"
            )
        expected_df_query[col] = expected_df_query[col].astype(
            table_from_sql_entities[col].dtype
        )

    validate_dataframes(
        expected_df_query,
        table_from_sql_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_persisting(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    storage = environment.data_source_creator.create_saved_dataset_destination()

    store.apply([driver(), customer(), location(), *feature_views.values()])

    # Added to handle the case that the offline store is remote
    store.registry.apply_data_source(storage.to_data_source(), store.config.project)

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    job = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    saved_dataset = store.create_saved_dataset(
        from_=job,
        name="saved_dataset",
        storage=storage,
        tags={"env": "test"},
        allow_overwrite=True,
    )

    event_timestamp = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    expected_df = get_expected_training_df(
        datasets.customer_df,
        feature_views.customer,
        datasets.driver_df,
        feature_views.driver,
        datasets.orders_df,
        feature_views.order,
        datasets.location_df,
        feature_views.location,
        datasets.global_df,
        feature_views.global_fv,
        datasets.field_mapping_df,
        feature_views.field_mapping,
        entity_df,
        event_timestamp,
        full_feature_names,
    ).drop(
        columns=[
            get_response_feature_name("conv_rate_plus_100", full_feature_names),
            get_response_feature_name("conv_rate_plus_100_rounded", full_feature_names),
            get_response_feature_name("avg_daily_trips", full_feature_names),
            get_response_feature_name("conv_rate", full_feature_names),
            "origin__temperature",
            "destination__temperature",
        ]
    )

    validate_dataframes(
        expected_df,
        saved_dataset.to_df(),
        sort_by=[event_timestamp, "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )

    validate_dataframes(
        job.to_df(),
        saved_dataset.to_df(),
        sort_by=[event_timestamp, "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_with_no_ttl(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    # Remove ttls.
    feature_views.customer.ttl = timedelta(seconds=0)
    feature_views.order.ttl = timedelta(seconds=0)
    feature_views.global_fv.ttl = timedelta(seconds=0)
    feature_views.field_mapping.ttl = timedelta(seconds=0)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    job = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    event_timestamp = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    expected_df = get_expected_training_df(
        datasets.customer_df,
        feature_views.customer,
        datasets.driver_df,
        feature_views.driver,
        datasets.orders_df,
        feature_views.order,
        datasets.location_df,
        feature_views.location,
        datasets.global_df,
        feature_views.global_fv,
        datasets.field_mapping_df,
        feature_views.field_mapping,
        entity_df,
        event_timestamp,
        full_feature_names,
    ).drop(
        columns=[
            get_response_feature_name("conv_rate_plus_100", full_feature_names),
            get_response_feature_name("conv_rate_plus_100_rounded", full_feature_names),
            get_response_feature_name("avg_daily_trips", full_feature_names),
            get_response_feature_name("conv_rate", full_feature_names),
            "origin__temperature",
            "destination__temperature",
        ]
    )

    validate_dataframes(
        expected_df,
        job.to_df(),
        sort_by=[event_timestamp, "driver_id", "customer_id"],
        event_timestamp_column=event_timestamp,
        timestamp_precision=timedelta(milliseconds=1),
    )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_historical_features_containing_backfills(environment):
    store = environment.feature_store

    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    tomorrow = now + timedelta(days=1)
    day_after_tomorrow = now + timedelta(days=2)

    entity_df = pd.DataFrame(
        data=[
            {"driver_id": 1001, "event_timestamp": day_after_tomorrow},
            {"driver_id": 1002, "event_timestamp": day_after_tomorrow},
        ]
    )

    driver_stats_df = pd.DataFrame(
        data=[
            # Duplicated rows simple case
            {
                "driver_id": 1001,
                "avg_daily_trips": 10,
                "event_timestamp": now,
                "created": now,
            },
            {
                "driver_id": 1001,
                "avg_daily_trips": 20,
                "event_timestamp": now,
                "created": tomorrow,
            },
            # Duplicated rows after a backfill
            {
                "driver_id": 1002,
                "avg_daily_trips": 30,
                "event_timestamp": now,
                "created": tomorrow,
            },
            {
                "driver_id": 1002,
                "avg_daily_trips": 40,
                "event_timestamp": tomorrow,
                "created": now,
            },
        ]
    )

    expected_df = pd.DataFrame(
        data=[
            {
                "driver_id": 1001,
                "event_timestamp": day_after_tomorrow,
                "avg_daily_trips": 20,
            },
            {
                "driver_id": 1002,
                "event_timestamp": day_after_tomorrow,
                "avg_daily_trips": 40,
            },
        ]
    )

    driver_stats_data_source = environment.data_source_creator.create_data_source(
        df=driver_stats_df,
        destination_name=f"test_driver_stats_{int(time.time_ns())}_{random.randint(1000, 9999)}",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    driver = Entity(name="driver", join_keys=["driver_id"])
    driver_fv = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="avg_daily_trips", dtype=Int32)],
        source=driver_stats_data_source,
    )

    store.apply([driver, driver_fv])

    offline_job = store.get_historical_features(
        entity_df=entity_df,
        features=["driver_stats:avg_daily_trips"],
        full_feature_names=False,
    )

    start_time = _utc_now()
    actual_df = offline_job.to_df()

    print(f"actual_df shape: {actual_df.shape}")
    end_time = _utc_now()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

    assert sorted(expected_df.columns) == sorted(actual_df.columns)
    validate_dataframes(
        expected_df,
        actual_df,
        sort_by=["driver_id"],
    )
