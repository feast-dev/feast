from copy import deepcopy

import pandas as pd
import pytest

from feast import (
    BigQuerySource,
    Entity,
    Feature,
    FeatureService,
    FileSource,
    RedshiftSource,
    RepoConfig,
    SnowflakeSource,
    ValueType,
)
from feast.data_source import RequestSource
from feast.errors import (
    DataSourceNoNameException,
    RegistryInferenceFailure,
    SpecifiedFeaturesNotPresentError,
)
from feast.feature_view import FeatureView
from feast.field import Field
from feast.inference import (
    update_data_sources_with_inferred_event_timestamp_col,
    update_feature_views_with_inferred_features_and_entities,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String, UnixTimestamp
from tests.utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_query_arg,
    simple_bq_source_using_table_arg,
)


def test_infer_datasource_names_file():
    file_path = "path/to/test.csv"
    data_source = FileSource(path=file_path)
    assert data_source.name == file_path

    source_name = "my_name"
    data_source = FileSource(name=source_name, path=file_path)
    assert data_source.name == source_name


def test_infer_datasource_names_dwh():
    table = "project.table"
    dwh_classes = [BigQuerySource, RedshiftSource, SnowflakeSource, SparkSource]

    for dwh_class in dwh_classes:
        data_source = dwh_class(table=table)
        assert data_source.name == table

        source_name = "my_name"
        data_source_with_table = dwh_class(name=source_name, table=table)
        assert data_source_with_table.name == source_name
        data_source_with_query = dwh_class(
            name=source_name, query=f"SELECT * from {table}"
        )
        assert data_source_with_query.name == source_name

        # If we have a query and no name, throw an error
        if dwh_class == SparkSource:
            with pytest.raises(DataSourceNoNameException):
                print(f"Testing dwh {dwh_class}")
                data_source = dwh_class(query="test_query")
        else:
            data_source = dwh_class(query="test_query")
            assert data_source.name == ""


@pytest.mark.integration
def test_update_file_data_source_with_inferred_event_timestamp_col(simple_dataset_1):
    df_with_two_viable_timestamp_cols = simple_dataset_1.copy(deep=True)
    df_with_two_viable_timestamp_cols["ts_2"] = simple_dataset_1["ts_1"]

    with prep_file_source(df=simple_dataset_1) as file_source:
        data_sources = [
            file_source,
            simple_bq_source_using_table_arg(simple_dataset_1),
            simple_bq_source_using_query_arg(simple_dataset_1),
        ]
        update_data_sources_with_inferred_event_timestamp_col(
            data_sources, RepoConfig(provider="local", project="test")
        )
        actual_event_timestamp_cols = [
            source.timestamp_field for source in data_sources
        ]

        assert actual_event_timestamp_cols == ["ts_1", "ts_1", "ts_1"]

    with prep_file_source(df=df_with_two_viable_timestamp_cols) as file_source:
        with pytest.raises(RegistryInferenceFailure):
            # two viable timestamp_fields
            update_data_sources_with_inferred_event_timestamp_col(
                [file_source], RepoConfig(provider="local", project="test")
            )


@pytest.mark.integration
def test_update_data_sources_with_inferred_event_timestamp_col(universal_data_sources):
    (_, _, data_sources) = universal_data_sources
    data_sources_copy = deepcopy(data_sources)

    # remove defined timestamp_field to allow for inference
    for data_source in data_sources_copy.values():
        data_source.timestamp_field = None
        data_source.event_timestamp_column = None

    update_data_sources_with_inferred_event_timestamp_col(
        data_sources_copy.values(), RepoConfig(provider="local", project="test"),
    )
    actual_event_timestamp_cols = [
        source.timestamp_field for source in data_sources_copy.values()
    ]

    assert actual_event_timestamp_cols == ["event_timestamp"] * len(
        data_sources_copy.values()
    )


def test_on_demand_features_type_inference():
    # Create Feature Views
    date_request = RequestSource(
        name="date_request", schema=[Field(name="some_date", dtype=UnixTimestamp)],
    )

    @on_demand_feature_view(
        sources=[date_request],
        schema=[
            Field(name="output", dtype=UnixTimestamp),
            Field(name="string_output", dtype=String),
        ],
    )
    def test_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
        return data

    test_view.infer_features()

    @on_demand_feature_view(
        # Note: we deliberately use `inputs` instead of `sources` to test that `inputs`
        # still works correctly, even though it is deprecated.
        # TODO(felixwang9817): Remove references to `inputs` once it is fully deprecated.
        inputs={"date_request": date_request},
        features=[
            Feature(name="output", dtype=ValueType.UNIX_TIMESTAMP),
            Feature(name="object_output", dtype=ValueType.STRING),
        ],
    )
    def invalid_test_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["object_output"] = features_df["some_date"].astype(str)
        return data

    with pytest.raises(ValueError, match="Value with native type object"):
        invalid_test_view.infer_features()

    @on_demand_feature_view(
        # Note: we deliberately use positional arguments here to test that they work correctly,
        # even though positional arguments are deprecated in favor of keyword arguments.
        # TODO(felixwang9817): Remove positional arguments once they are fully deprecated.
        [
            Feature(name="output", dtype=ValueType.UNIX_TIMESTAMP),
            Feature(name="missing", dtype=ValueType.STRING),
        ],
        {"date_request": date_request},
    )
    def test_view_with_missing_feature(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        return data

    with pytest.raises(SpecifiedFeaturesNotPresentError):
        test_view_with_missing_feature.infer_features()


# TODO(kevjumba): remove this in feast 0.24 when deprecating
@pytest.mark.parametrize(
    "request_source_schema",
    [
        [Field(name="some_date", dtype=UnixTimestamp)],
        {"some_date": ValueType.UNIX_TIMESTAMP},
    ],
)
def test_datasource_inference(request_source_schema):
    # Create Feature Views
    date_request = RequestSource(name="date_request", schema=request_source_schema,)

    @on_demand_feature_view(
        # Note: we deliberately use positional arguments here to test that they work correctly,
        # even though positional arguments are deprecated in favor of keyword arguments.
        # TODO(felixwang9817): Remove positional arguments once they are fully deprecated.
        [
            Feature(name="output", dtype=ValueType.UNIX_TIMESTAMP),
            Feature(name="string_output", dtype=ValueType.STRING),
        ],
        sources=[date_request],
    )
    def test_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
        return data

    test_view.infer_features()

    @on_demand_feature_view(
        sources=[date_request],
        schema=[
            Field(name="output", dtype=UnixTimestamp),
            Field(name="object_output", dtype=String),
        ],
    )
    def invalid_test_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["object_output"] = features_df["some_date"].astype(str)
        return data

    with pytest.raises(ValueError, match="Value with native type object"):
        invalid_test_view.infer_features()

    @on_demand_feature_view(
        sources=[date_request],
        features=[
            Feature(name="output", dtype=ValueType.UNIX_TIMESTAMP),
            Feature(name="missing", dtype=ValueType.STRING),
        ],
    )
    def test_view_with_missing_feature(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        return data

    with pytest.raises(SpecifiedFeaturesNotPresentError):
        test_view_with_missing_feature.infer_features()


def test_feature_view_inference_respects_basic_inference():
    """
    Tests that feature view inference respects the basic inference that occurs during creation.
    """
    file_source = FileSource(name="test", path="test path")
    entity1 = Entity(name="test1", join_keys=["test_column_1"])
    entity2 = Entity(name="test2", join_keys=["test_column_2"])
    feature_view_1 = FeatureView(
        name="test1",
        entities=[entity1],
        schema=[
            Field(name="feature", dtype=Float32),
            Field(name="test_column_1", dtype=String),
        ],
        source=file_source,
    )
    feature_view_2 = FeatureView(
        name="test2",
        entities=[entity1, entity2],
        schema=[
            Field(name="feature", dtype=Float32),
            Field(name="test_column_1", dtype=String),
            Field(name="test_column_2", dtype=String),
        ],
        source=file_source,
    )

    assert len(feature_view_1.schema) == 2
    assert len(feature_view_1.features) == 1
    assert len(feature_view_1.entity_columns) == 1

    update_feature_views_with_inferred_features_and_entities(
        [feature_view_1], [entity1], RepoConfig(provider="local", project="test")
    )
    assert len(feature_view_1.schema) == 2
    assert len(feature_view_1.features) == 1
    assert len(feature_view_1.entity_columns) == 1

    assert len(feature_view_2.schema) == 3
    assert len(feature_view_2.features) == 1
    assert len(feature_view_2.entity_columns) == 2

    update_feature_views_with_inferred_features_and_entities(
        [feature_view_2],
        [entity1, entity2],
        RepoConfig(provider="local", project="test"),
    )
    assert len(feature_view_2.schema) == 3
    assert len(feature_view_2.features) == 1
    assert len(feature_view_2.entity_columns) == 2


def test_feature_view_inference_on_entity_columns(simple_dataset_1):
    """
    Tests that feature view inference correctly infers entity columns.
    """
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(name="test1", join_keys=["id_join_key"])
        feature_view_1 = FeatureView(
            name="test1",
            entities=[entity1],
            schema=[Field(name="int64_col", dtype=Int64)],
            source=file_source,
        )

        assert len(feature_view_1.schema) == 1
        assert len(feature_view_1.features) == 1
        assert len(feature_view_1.entity_columns) == 0

        update_feature_views_with_inferred_features_and_entities(
            [feature_view_1], [entity1], RepoConfig(provider="local", project="test")
        )

        # The schema is only used as a parameter, as is therefore not updated during inference.
        assert len(feature_view_1.schema) == 1

        # Since there is already a feature specified, additional features are not inferred.
        assert len(feature_view_1.features) == 1

        # The single entity column is inferred correctly.
        assert len(feature_view_1.entity_columns) == 1


def test_feature_view_inference_respects_entity_value_type(simple_dataset_1):
    """
    Tests that feature view inference still respects an entity's value type.
    """
    # TODO(felixwang9817): Remove this test once entity value_type is removed.
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(
            name="test1", join_keys=["id_join_key"], value_type=ValueType.STRING
        )
        feature_view_1 = FeatureView(
            name="test1",
            entities=[entity1],
            schema=[Field(name="int64_col", dtype=Int64)],
            source=file_source,
        )

        assert len(feature_view_1.schema) == 1
        assert len(feature_view_1.features) == 1
        assert len(feature_view_1.entity_columns) == 0

        update_feature_views_with_inferred_features_and_entities(
            [feature_view_1], [entity1], RepoConfig(provider="local", project="test")
        )

        # The schema is only used as a parameter, as is therefore not updated during inference.
        assert len(feature_view_1.schema) == 1

        # Since there is already a feature specified, additional features are not inferred.
        assert len(feature_view_1.features) == 1

        # The single entity column is inferred correctly and has type String.
        assert len(feature_view_1.entity_columns) == 1
        assert feature_view_1.entity_columns[0].dtype == String


def test_feature_view_inference_on_feature_columns(simple_dataset_1):
    """
    Tests that feature view inference correctly infers feature columns.
    """
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(name="test1", join_keys=["id_join_key"])
        feature_view_1 = FeatureView(
            name="test1",
            entities=[entity1],
            schema=[Field(name="id_join_key", dtype=Int64)],
            source=file_source,
        )

        assert len(feature_view_1.schema) == 1
        assert len(feature_view_1.features) == 0
        assert len(feature_view_1.entity_columns) == 1

        update_feature_views_with_inferred_features_and_entities(
            [feature_view_1], [entity1], RepoConfig(provider="local", project="test")
        )

        # The schema is only used as a parameter, as is therefore not updated during inference.
        assert len(feature_view_1.schema) == 1

        # All three feature columns are inferred correctly.
        assert len(feature_view_1.features) == 3
        print(feature_view_1.features)
        feature_column_1 = Field(name="float_col", dtype=Float64)
        feature_column_2 = Field(name="int64_col", dtype=Int64)
        feature_column_3 = Field(name="string_col", dtype=String)
        assert feature_column_1 in feature_view_1.features
        assert feature_column_2 in feature_view_1.features
        assert feature_column_3 in feature_view_1.features

        # The single entity column remains.
        assert len(feature_view_1.entity_columns) == 1


def test_update_feature_services_with_inferred_features(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(name="test1", join_keys=["id_join_key"])
        feature_view_1 = FeatureView(
            name="test1", entities=[entity1], source=file_source,
        )
        feature_view_2 = FeatureView(
            name="test2", entities=[entity1], source=file_source,
        )

        feature_service = FeatureService(
            name="fs_1", features=[feature_view_1[["string_col"]], feature_view_2]
        )
        assert len(feature_service.feature_view_projections) == 2
        assert len(feature_service.feature_view_projections[0].features) == 0
        assert len(feature_service.feature_view_projections[0].desired_features) == 1
        assert len(feature_service.feature_view_projections[1].features) == 0
        assert len(feature_service.feature_view_projections[1].desired_features) == 0

        update_feature_views_with_inferred_features_and_entities(
            [feature_view_1, feature_view_2],
            [entity1],
            RepoConfig(provider="local", project="test"),
        )
        feature_service.infer_features(
            fvs_to_update={
                feature_view_1.name: feature_view_1,
                feature_view_2.name: feature_view_2,
            }
        )

        assert len(feature_view_1.schema) == 0
        assert len(feature_view_1.features) == 3
        assert len(feature_view_2.schema) == 0
        assert len(feature_view_2.features) == 3
        assert len(feature_service.feature_view_projections[0].features) == 1
        assert len(feature_service.feature_view_projections[1].features) == 3


# TODO(felixwang9817): Add tests that interact with field mapping.
