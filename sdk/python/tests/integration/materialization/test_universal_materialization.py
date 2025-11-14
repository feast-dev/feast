from datetime import timedelta

import pandas as pd
import pytest

from feast import (
    Entity,
    FeatureView,
    Field,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64
from tests.data.data_creator import create_basic_driver_dataset
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


def _create_test_entities():
    """Helper function to create standard test entities."""
    customer = Entity(name="customer_id", join_keys=["customer_id"])
    product = Entity(name="product_id", join_keys=["product_id"])
    return customer, product


def _create_test_dataframe(include_revenue=False):
    """Helper function to create standard test DataFrame."""
    data = {
        "customer_id": [1, 2],
        "product_id": [10, 20],
        "price": [100.0, 200.0],
        "event_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        "created_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"]),
    }
    if include_revenue:
        data["revenue"] = [5.0, 10.0]
    return pd.DataFrame(data)


def _create_revenue_dataframe():
    """Helper function to create revenue test DataFrame."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2],
            "product_id": [10, 20],
            "revenue": [5.0, 7.0],
            "event_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "created_timestamp": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        }
    )


def _create_feature_view(name, entities, schema_fields, source):
    """Helper function to create a standard FeatureView."""
    return FeatureView(
        name=name,
        entities=entities,
        ttl=timedelta(days=1),
        schema=schema_fields,
        online=True,
        source=source,
    )


def _materialize_and_assert(fs, df, feature_ref, entity_row, expected_value):
    """Helper function to materialize and assert feature values."""
    feature_view_name = feature_ref.split(":")[0]
    fs.materialize(
        start_date=df["event_timestamp"].min() - timedelta(days=1),
        end_date=df["event_timestamp"].max() + timedelta(days=1),
        feature_views=[feature_view_name],
    )
    resp = fs.get_online_features(
        features=[feature_ref],
        entity_rows=[entity_row],
    ).to_dict()
    feature_name = feature_ref.split(":")[-1]
    assert resp[feature_name][0] == expected_value


def _assert_online_features(
    fs, feature_ref, entity_row, expected_value, message_prefix="Expected"
):
    """Helper function to assert online feature values."""
    resp = fs.get_online_features(
        features=[feature_ref],
        entity_rows=[entity_row],
    ).to_dict()
    feature_name = feature_ref.split(":")[-1]
    assert resp[feature_name][0] == expected_value, (
        f"{message_prefix} {expected_value}, got {resp[feature_name][0]}"
    )


def _get_standard_entity_row():
    """Helper function to get standard entity row for testing."""
    return {"customer_id": 1, "product_id": 10}


@pytest.mark.integration
def test_odfv_materialization_single_source(environment):
    fs = environment.feature_store
    df = _create_test_dataframe()
    ds = environment.data_source_creator.create_data_source(
        df,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
    customer, product = _create_test_entities()

    fv1 = _create_feature_view(
        "fv1", [customer, product], [Field(name="price", dtype=Float32)], ds
    )

    @on_demand_feature_view(
        entities=[customer, product],
        sources=[fv1],
        schema=[Field(name="price_plus_10", dtype=Float64)],
        write_to_online_store=True,
    )
    def odfv_single(df: pd.DataFrame) -> pd.DataFrame:
        df["price_plus_10"] = df["price"] + 10
        return df

    fs.apply([customer, product, fv1, odfv_single])
    _materialize_and_assert(
        fs, df, "odfv_single:price_plus_10", _get_standard_entity_row(), 110.0
    )


@pytest.mark.integration
def test_odfv_materialization_multi_source(environment):
    fs = environment.feature_store
    df1 = _create_test_dataframe()
    df2 = _create_revenue_dataframe()
    ds1 = environment.data_source_creator.create_data_source(
        df1,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
    ds2 = environment.data_source_creator.create_data_source(
        df2,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    customer, product = _create_test_entities()
    fv1 = _create_feature_view(
        "fv1", [customer, product], [Field(name="price", dtype=Float32)], ds1
    )
    fv2 = _create_feature_view(
        "fv2", [customer, product], [Field(name="revenue", dtype=Float32)], ds2
    )

    @on_demand_feature_view(
        entities=[customer, product],
        sources=[fv1, fv2],
        schema=[Field(name="price_plus_revenue", dtype=Float64)],
        write_to_online_store=True,
    )
    def odfv_multi(df: pd.DataFrame) -> pd.DataFrame:
        df["price_plus_revenue"] = df["price"] + df["revenue"]
        return df

    fs.apply([customer, product, fv1, fv2, odfv_multi])
    _materialize_and_assert(
        fs, df1, "odfv_multi:price_plus_revenue", _get_standard_entity_row(), 105.0
    )


@pytest.mark.integration
def test_odfv_materialization_incremental_multi_source(environment):
    fs = environment.feature_store
    df1 = _create_test_dataframe()
    df2 = _create_revenue_dataframe()
    ds1 = environment.data_source_creator.create_data_source(
        df1,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )
    ds2 = environment.data_source_creator.create_data_source(
        df2,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    customer, product = _create_test_entities()
    fv1 = _create_feature_view(
        "fv1", [customer, product], [Field(name="price", dtype=Float32)], ds1
    )
    fv2 = _create_feature_view(
        "fv2", [customer, product], [Field(name="revenue", dtype=Float32)], ds2
    )

    @on_demand_feature_view(
        entities=[customer, product],
        sources=[fv1, fv2],
        schema=[Field(name="price_plus_revenue", dtype=Float64)],
        write_to_online_store=True,
    )
    def odfv_multi(df: pd.DataFrame) -> pd.DataFrame:
        df["price_plus_revenue"] = df["price"] + df["revenue"]
        return df

    fs.apply([customer, product, fv1, fv2, odfv_multi])
    fs.materialize_incremental(
        end_date=df1["event_timestamp"].max() + timedelta(days=1)
    )

    resp = fs.get_online_features(
        features=["odfv_multi:price_plus_revenue"],
        entity_rows=[_get_standard_entity_row()],
    ).to_dict()
    assert resp["price_plus_revenue"][0] == 105.0


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("materialization_pull_latest", [True, False])
def test_universal_materialization_consistency(
    environment, materialization_pull_latest
):
    environment.materialization.pull_latest_features = materialization_pull_latest

    fs = environment.feature_store
    df = create_basic_driver_dataset()
    ds = environment.data_source_creator.create_data_source(
        df,
        fs.project,
        field_mapping={"ts_1": "ts"},
    )
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )
    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        schema=[Field(name="value", dtype=Float32)],
        source=ds,
    )
    fs.apply([driver, driver_stats_fv])
    split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)
    validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)


@pytest.mark.integration
def test_odfv_write_methods(environment):
    """
    Comprehensive test for ODFV on-write transformations not persisting.
    Tests store.push(), store.write_to_online_store(), and materialize() methods.
    """
    fs = environment.feature_store
    df = _create_test_dataframe(include_revenue=True)
    ds = environment.data_source_creator.create_data_source(
        df,
        fs.project,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    customer, product = _create_test_entities()
    fv = _create_feature_view(
        "price_revenue_fv",
        [customer, product],
        [Field(name="price", dtype=Float32), Field(name="revenue", dtype=Float32)],
        ds,
    )

    @on_demand_feature_view(
        entities=[customer, product],
        sources=[fv],
        schema=[Field(name="total_value", dtype=Float64)],
        write_to_online_store=True,
    )
    def total_value_odfv(df: pd.DataFrame) -> pd.DataFrame:
        df["total_value"] = df["price"] + df["revenue"]
        return df

    fs.apply([customer, product, fv, total_value_odfv])
    _materialize_and_assert(
        fs, df, "total_value_odfv:total_value", _get_standard_entity_row(), 105.0
    )

    new_data = pd.DataFrame(
        {
            "customer_id": [3],
            "product_id": [30],
            "price": [300.0],
            "revenue": [15.0],
            "event_timestamp": [pd.Timestamp.now()],
            "created_timestamp": [pd.Timestamp.now()],
        }
    )

    transformed_data = fs._transform_on_demand_feature_view_df(
        total_value_odfv, new_data
    )
    fs.write_to_online_store("total_value_odfv", df=transformed_data)
    _assert_online_features(
        fs, "total_value_odfv:total_value", {"customer_id": 3, "product_id": 30}, 315.0
    )

    @on_demand_feature_view(
        entities=[customer, product],
        sources=[fv],
        schema=[Field(name="price_doubled", dtype=Float64)],
        write_to_online_store=False,  # This is on-read only
    )
    def price_doubled_odfv(df: pd.DataFrame) -> pd.DataFrame:
        df["price_doubled"] = df["price"] * 2
        return df

    fs.apply([price_doubled_odfv])
    # Materialize the underlying feature view so the on-read ODFV can access the price feature
    fs.materialize(
        start_date=df["event_timestamp"].min() - timedelta(days=1),
        end_date=df["event_timestamp"].max() + timedelta(days=1),
        feature_views=["price_revenue_fv"],
    )
    _assert_online_features(
        fs, "price_doubled_odfv:price_doubled", _get_standard_entity_row(), 200.0
    )

    resp = fs.get_online_features(
        features=["total_value_odfv:total_value", "price_doubled_odfv:price_doubled"],
        entity_rows=[_get_standard_entity_row()],
    ).to_dict()
    assert resp["total_value"][0] == 105.0, "On-write ODFV failed"
    assert resp["price_doubled"][0] == 200.0, "On-read ODFV failed"
