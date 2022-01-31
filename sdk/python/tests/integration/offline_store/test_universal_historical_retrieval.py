import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from pytz import utc

from feast import utils
from feast.entity import Entity
from feast.errors import (
    FeatureNameCollisionError,
    RequestDataNotFoundInEntityDfException,
)
from feast.feature import Feature
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.value_type import ValueType
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
    table_name_from_data_source,
)
from tests.integration.feature_repos.universal.data_sources.snowflake import (
    SnowflakeDataSourceCreator,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)

np.random.seed(0)


def convert_timestamp_records_to_utc(
    records: List[Dict[str, Any]], column: str
) -> List[Dict[str, Any]]:
    for record in records:
        record[column] = utils.make_tzaware(record[column]).astimezone(utc)
    return records


# Find the latest record in the given time range and filter
def find_asof_record(
    records: List[Dict[str, Any]],
    ts_key: str,
    ts_start: datetime,
    ts_end: datetime,
    filter_keys: Optional[List[str]] = None,
    filter_values: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    filter_keys = filter_keys or []
    filter_values = filter_values or []
    assert len(filter_keys) == len(filter_values)
    found_record: Dict[str, Any] = {}
    for record in records:
        if (
            all(
                [
                    record[filter_key] == filter_value
                    for filter_key, filter_value in zip(filter_keys, filter_values)
                ]
            )
            and ts_start <= record[ts_key] <= ts_end
        ):
            if not found_record or found_record[ts_key] < record[ts_key]:
                found_record = record
    return found_record


def get_expected_training_df(
    customer_df: pd.DataFrame,
    customer_fv: FeatureView,
    driver_df: pd.DataFrame,
    driver_fv: FeatureView,
    orders_df: pd.DataFrame,
    order_fv: FeatureView,
    location_df: pd.DataFrame,
    location_fv: FeatureView,
    global_df: pd.DataFrame,
    global_fv: FeatureView,
    field_mapping_df: pd.DataFrame,
    field_mapping_fv: FeatureView,
    entity_df: pd.DataFrame,
    event_timestamp: str,
    full_feature_names: bool = False,
):
    # Convert all pandas dataframes into records with UTC timestamps
    customer_records = convert_timestamp_records_to_utc(
        customer_df.to_dict("records"), customer_fv.batch_source.event_timestamp_column
    )
    driver_records = convert_timestamp_records_to_utc(
        driver_df.to_dict("records"), driver_fv.batch_source.event_timestamp_column
    )
    order_records = convert_timestamp_records_to_utc(
        orders_df.to_dict("records"), event_timestamp
    )
    location_records = convert_timestamp_records_to_utc(
        location_df.to_dict("records"), location_fv.batch_source.event_timestamp_column
    )
    global_records = convert_timestamp_records_to_utc(
        global_df.to_dict("records"), global_fv.batch_source.event_timestamp_column
    )
    field_mapping_records = convert_timestamp_records_to_utc(
        field_mapping_df.to_dict("records"),
        field_mapping_fv.batch_source.event_timestamp_column,
    )
    entity_rows = convert_timestamp_records_to_utc(
        entity_df.to_dict("records"), event_timestamp
    )

    # Manually do point-in-time join of driver, customer, and order records against
    # the entity df
    for entity_row in entity_rows:
        customer_record = find_asof_record(
            customer_records,
            ts_key=customer_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - customer_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id"],
            filter_values=[entity_row["customer_id"]],
        )
        driver_record = find_asof_record(
            driver_records,
            ts_key=driver_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - driver_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["driver_id"],
            filter_values=[entity_row["driver_id"]],
        )
        order_record = find_asof_record(
            order_records,
            ts_key=customer_fv.batch_source.event_timestamp_column,
            ts_start=entity_row[event_timestamp] - order_fv.ttl,
            ts_end=entity_row[event_timestamp],
            filter_keys=["customer_id", "driver_id"],
            filter_values=[entity_row["customer_id"], entity_row["driver_id"]],
        )
        origin_record = find_asof_record(
            location_records,
            ts_key=location_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - location_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_keys=["location_id"],
            filter_values=[order_record["origin_id"]],
        )
        destination_record = find_asof_record(
            location_records,
            ts_key=location_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - location_fv.ttl,
            ts_end=order_record[event_timestamp],
            filter_keys=["location_id"],
            filter_values=[order_record["destination_id"]],
        )
        global_record = find_asof_record(
            global_records,
            ts_key=global_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - global_fv.ttl,
            ts_end=order_record[event_timestamp],
        )

        field_mapping_record = find_asof_record(
            field_mapping_records,
            ts_key=field_mapping_fv.batch_source.event_timestamp_column,
            ts_start=order_record[event_timestamp] - field_mapping_fv.ttl,
            ts_end=order_record[event_timestamp],
        )

        entity_row.update(
            {
                (
                    f"customer_profile__{k}" if full_feature_names else k
                ): customer_record.get(k, None)
                for k in (
                    "current_balance",
                    "avg_passenger_count",
                    "lifetime_trip_count",
                )
            }
        )
        entity_row.update(
            {
                (f"driver_stats__{k}" if full_feature_names else k): driver_record.get(
                    k, None
                )
                for k in ("conv_rate", "avg_daily_trips")
            }
        )
        entity_row.update(
            {
                (f"order__{k}" if full_feature_names else k): order_record.get(k, None)
                for k in ("order_is_success",)
            }
        )
        entity_row.update(
            {
                "origin__temperature": origin_record.get("temperature", None),
                "destination__temperature": destination_record.get("temperature", None),
            }
        )
        entity_row.update(
            {
                (f"global_stats__{k}" if full_feature_names else k): global_record.get(
                    k, None
                )
                for k in ("num_rides", "avg_ride_length",)
            }
        )

        # get field_mapping_record by column name, but label by feature name
        entity_row.update(
            {
                (
                    f"field_mapping__{feature}" if full_feature_names else feature
                ): field_mapping_record.get(column, None)
                for (column, feature) in field_mapping_fv.input.field_mapping.items()
            }
        )

    # Convert records back to pandas dataframe
    expected_df = pd.DataFrame(entity_rows)

    # Move "event_timestamp" column to front
    current_cols = expected_df.columns.tolist()
    current_cols.remove(event_timestamp)
    expected_df = expected_df[[event_timestamp] + current_cols]

    # Cast some columns to expected types, since we lose information when converting pandas DFs into Python objects.
    if full_feature_names:
        expected_column_types = {
            "order__order_is_success": "int32",
            "driver_stats__conv_rate": "float32",
            "customer_profile__current_balance": "float32",
            "customer_profile__avg_passenger_count": "float32",
            "global_stats__avg_ride_length": "float32",
            "field_mapping__feature_name": "int32",
        }
    else:
        expected_column_types = {
            "order_is_success": "int32",
            "conv_rate": "float32",
            "current_balance": "float32",
            "avg_passenger_count": "float32",
            "avg_ride_length": "float32",
            "feature_name": "int32",
        }

    for col, typ in expected_column_types.items():
        expected_df[col] = expected_df[col].astype(typ)

    conv_feature_name = "driver_stats__conv_rate" if full_feature_names else "conv_rate"
    conv_plus_feature_name = response_feature_name(
        "conv_rate_plus_100", full_feature_names
    )
    expected_df[conv_plus_feature_name] = expected_df[conv_feature_name] + 100
    expected_df[
        response_feature_name("conv_rate_plus_100_rounded", full_feature_names)
    ] = (
        expected_df[conv_plus_feature_name]
        .astype("float")
        .round()
        .astype(pd.Int32Dtype())
    )
    if "val_to_add" in expected_df.columns:
        expected_df[
            response_feature_name("conv_rate_plus_val_to_add", full_feature_names)
        ] = (expected_df[conv_feature_name] + expected_df["val_to_add"])

    return expected_df


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features(environment, universal_data_sources, full_feature_names):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    entity_df_with_request_data = datasets["entity"].copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]
    entity_df_with_request_data["driver_age"] = [
        i + 100 for i in range(len(entity_df_with_request_data))
    ]

    feature_service = FeatureService(
        name="convrate_plus100",
        features=[
            feature_views["driver"][["conv_rate"]],
            feature_views["driver_odfv"],
            feature_views["driver_age_request_fv"],
        ],
    )
    feature_service_entity_mapping = FeatureService(
        name="entity_mapping",
        features=[
            feature_views["location"]
            .with_name("origin")
            .with_join_key_map({"location_id": "origin_id"}),
            feature_views["location"]
            .with_name("destination")
            .with_join_key_map({"location_id": "destination_id"}),
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
        if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in datasets["orders"].columns
        else "e_ts"
    )
    full_expected_df = get_expected_training_df(
        datasets["customer"],
        feature_views["customer"],
        datasets["driver"],
        feature_views["driver"],
        datasets["orders"],
        feature_views["order"],
        datasets["location"],
        feature_views["location"],
        datasets["global"],
        feature_views["global"],
        datasets["field_mapping"],
        feature_views["field_mapping"],
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
            "driver_age:driver_age",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    start_time = datetime.utcnow()
    actual_df_from_df_entities = job_from_df.to_df()

    print(f"actual_df_from_df_entities shape: {actual_df_from_df_entities.shape}")
    end_time = datetime.utcnow()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

    assert sorted(expected_df.columns) == sorted(actual_df_from_df_entities.columns)
    assert_frame_equal(
        expected_df,
        actual_df_from_df_entities,
        keys=[event_timestamp, "order_id", "driver_id", "customer_id"],
    )

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

    assert_frame_equal(
        expected_df,
        table_from_df_entities,
        keys=[event_timestamp, "order_id", "driver_id", "customer_id"],
    )


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_with_missing_request_data(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    # If request data is missing that's needed for on demand transform, throw an error
    with pytest.raises(RequestDataNotFoundInEntityDfException):
        store.get_historical_features(
            entity_df=datasets["entity"],
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
            full_feature_names=full_feature_names,
        )

    # If request data is missing that's needed for a request feature view, throw an error
    with pytest.raises(RequestDataNotFoundInEntityDfException):
        store.get_historical_features(
            entity_df=datasets["entity"],
            features=[
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "driver_age:driver_age",
                "global_stats:num_rides",
                "global_stats:avg_ride_length",
                "field_mapping:feature_name",
            ],
            full_feature_names=full_feature_names,
        )


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_with_entities_from_query(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    orders_table = table_name_from_data_source(data_sources["orders"])
    if not orders_table:
        raise pytest.skip("Offline source is not sql-based")

    if (
        environment.test_repo_config.offline_store_creator.__name__
        == SnowflakeDataSourceCreator.__name__
    ):
        entity_df_query = f'''SELECT "customer_id", "driver_id", "order_id", "origin_id", "destination_id", "event_timestamp" FROM "{orders_table}"'''
    else:
        entity_df_query = f"SELECT customer_id, driver_id, order_id, origin_id, destination_id, event_timestamp FROM {orders_table}"

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

    start_time = datetime.utcnow()
    actual_df_from_sql_entities = job_from_sql.to_df()
    end_time = datetime.utcnow()
    print(str(f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'"))

    event_timestamp = (
        DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in datasets["orders"].columns
        else "e_ts"
    )
    full_expected_df = get_expected_training_df(
        datasets["customer"],
        feature_views["customer"],
        datasets["driver"],
        feature_views["driver"],
        datasets["orders"],
        feature_views["order"],
        datasets["location"],
        feature_views["location"],
        datasets["global"],
        feature_views["global"],
        datasets["field_mapping"],
        feature_views["field_mapping"],
        datasets["entity"],
        event_timestamp,
        full_feature_names,
    )

    # Not requesting the on demand transform with an entity_df query (can't add request data in them)
    expected_df_query = full_expected_df.drop(
        columns=[
            response_feature_name("conv_rate_plus_100", full_feature_names),
            response_feature_name("conv_rate_plus_100_rounded", full_feature_names),
            response_feature_name("avg_daily_trips", full_feature_names),
            response_feature_name("conv_rate", full_feature_names),
            "origin__temperature",
            "destination__temperature",
        ]
    )
    assert_frame_equal(
        expected_df_query,
        actual_df_from_sql_entities,
        keys=[event_timestamp, "order_id", "driver_id", "customer_id"],
    )

    table_from_sql_entities = job_from_sql.to_arrow().to_pandas()
    for col in table_from_sql_entities.columns:
        expected_df_query[col] = expected_df_query[col].astype(
            table_from_sql_entities[col].dtype
        )

    assert_frame_equal(
        expected_df_query,
        table_from_sql_entities,
        keys=[event_timestamp, "order_id", "driver_id", "customer_id"],
    )


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features_persisting(
    environment, universal_data_sources, full_feature_names
):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    entity_df = datasets["entity"].drop(
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
        storage=environment.data_source_creator.create_saved_dataset_destination(),
        tags={"env": "test"},
    )

    event_timestamp = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    expected_df = get_expected_training_df(
        datasets["customer"],
        feature_views["customer"],
        datasets["driver"],
        feature_views["driver"],
        datasets["orders"],
        feature_views["order"],
        datasets["location"],
        feature_views["location"],
        datasets["global"],
        feature_views["global"],
        datasets["field_mapping"],
        feature_views["field_mapping"],
        entity_df,
        event_timestamp,
        full_feature_names,
    ).drop(
        columns=[
            response_feature_name("conv_rate_plus_100", full_feature_names),
            response_feature_name("conv_rate_plus_100_rounded", full_feature_names),
            response_feature_name("avg_daily_trips", full_feature_names),
            response_feature_name("conv_rate", full_feature_names),
            "origin__temperature",
            "destination__temperature",
        ]
    )

    assert_frame_equal(
        expected_df,
        saved_dataset.to_df(),
        keys=[event_timestamp, "driver_id", "customer_id"],
    )

    assert_frame_equal(
        job.to_df(),
        saved_dataset.to_df(),
        keys=[event_timestamp, "driver_id", "customer_id"],
    )


@pytest.mark.integration
@pytest.mark.universal
def test_historical_features_from_bigquery_sources_containing_backfills(environment):
    store = environment.feature_store

    now = datetime.now().replace(microsecond=0, second=0, minute=0)
    tomorrow = now + timedelta(days=1)

    entity_df = pd.DataFrame(
        data=[
            {"driver_id": 1001, "event_timestamp": now + timedelta(days=2)},
            {"driver_id": 1002, "event_timestamp": now + timedelta(days=2)},
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
                "event_timestamp": now + timedelta(days=2),
                "avg_daily_trips": 20,
            },
            {
                "driver_id": 1002,
                "event_timestamp": now + timedelta(days=2),
                "avg_daily_trips": 40,
            },
        ]
    )

    driver_stats_data_source = environment.data_source_creator.create_data_source(
        df=driver_stats_df,
        destination_name=f"test_driver_stats_{int(time.time_ns())}_{random.randint(1000, 9999)}",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )

    driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
    driver_fv = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=[Feature(name="avg_daily_trips", dtype=ValueType.INT32)],
        batch_source=driver_stats_data_source,
        ttl=None,
    )

    store.apply([driver, driver_fv])

    offline_job = store.get_historical_features(
        entity_df=entity_df,
        features=["driver_stats:avg_daily_trips"],
        full_feature_names=False,
    )

    start_time = datetime.utcnow()
    actual_df = offline_job.to_df()

    print(f"actual_df shape: {actual_df.shape}")
    end_time = datetime.utcnow()
    print(str(f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"))

    assert sorted(expected_df.columns) == sorted(actual_df.columns)
    assert_frame_equal(expected_df, actual_df, keys=["driver_id"])


def response_feature_name(feature: str, full_feature_names: bool) -> str:
    if feature in {"conv_rate", "avg_daily_trips"} and full_feature_names:
        return f"driver_stats__{feature}"

    if (
        feature
        in {
            "conv_rate_plus_100",
            "conv_rate_plus_100_rounded",
            "conv_rate_plus_val_to_add",
        }
        and full_feature_names
    ):
        return f"conv_rate_plus_100__{feature}"

    return feature


def assert_feature_service_correctness(
    store, feature_service, full_feature_names, entity_df, expected_df, event_timestamp
):

    job_from_df = store.get_historical_features(
        entity_df=entity_df,
        features=feature_service,
        full_feature_names=full_feature_names,
    )

    actual_df_from_df_entities = job_from_df.to_df()

    expected_df = expected_df[
        [
            event_timestamp,
            "order_id",
            "driver_id",
            "customer_id",
            response_feature_name("conv_rate", full_feature_names),
            response_feature_name("conv_rate_plus_100", full_feature_names),
            "driver_age",
        ]
    ]

    assert_frame_equal(
        expected_df,
        actual_df_from_df_entities,
        keys=[event_timestamp, "order_id", "driver_id", "customer_id"],
    )


def assert_feature_service_entity_mapping_correctness(
    store, feature_service, full_feature_names, entity_df, expected_df, event_timestamp
):
    if full_feature_names:
        job_from_df = store.get_historical_features(
            entity_df=entity_df,
            features=feature_service,
            full_feature_names=full_feature_names,
        )
        actual_df_from_df_entities = job_from_df.to_df()

        expected_df: pd.DataFrame = (
            expected_df.sort_values(
                by=[
                    event_timestamp,
                    "order_id",
                    "driver_id",
                    "customer_id",
                    "origin_id",
                    "destination_id",
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )
        expected_df = expected_df[
            [
                event_timestamp,
                "order_id",
                "driver_id",
                "customer_id",
                "origin_id",
                "destination_id",
                "origin__temperature",
                "destination__temperature",
            ]
        ]

        assert_frame_equal(
            expected_df,
            actual_df_from_df_entities,
            keys=[
                event_timestamp,
                "order_id",
                "driver_id",
                "customer_id",
                "origin_id",
                "destination_id",
            ],
        )
    else:
        # using 2 of the same FeatureView without full_feature_names=True will result in collision
        with pytest.raises(FeatureNameCollisionError):
            job_from_df = store.get_historical_features(
                entity_df=entity_df,
                features=feature_service,
                full_feature_names=full_feature_names,
            )


def assert_frame_equal(expected_df, actual_df, keys):
    expected_df: pd.DataFrame = (
        expected_df.sort_values(by=keys).drop_duplicates().reset_index(drop=True)
    )

    actual_df = (
        actual_df[expected_df.columns]
        .sort_values(by=keys)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    pd_assert_frame_equal(
        expected_df, actual_df, check_dtype=False,
    )
