import pandas as pd
import pytest

from feast import BigQuerySource, FileSource, RedshiftSource, SnowflakeSource
from feast.data_source import RequestSource
from feast.entity import Entity
from feast.errors import DataSourceNoNameException, SpecifiedFeaturesNotPresentError
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.inference import update_feature_views_with_inferred_features_and_entities
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.repo_config import RepoConfig
from feast.types import Float32, Float64, Int64, String, UnixTimestamp
from feast.value_type import ValueType
from tests.utils.data_source_test_creator import prep_file_source


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
        with pytest.raises(DataSourceNoNameException):
            print(f"Testing dwh {dwh_class}")
            data_source = dwh_class(query="test_query")


def test_on_demand_features_type_inference():
    # Create Feature Views
    date_request = RequestSource(
        name="date_request",
        schema=[Field(name="some_date", dtype=UnixTimestamp)],
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
        schema=[
            Field(name="output", dtype=UnixTimestamp),
            Field(name="missing", dtype=String),
        ],
        sources=[date_request],
    )
    def test_view_with_missing_feature(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        return data

    with pytest.raises(SpecifiedFeaturesNotPresentError):
        test_view_with_missing_feature.infer_features()


def test_datasource_inference():
    # Create Feature Views
    date_request = RequestSource(
        name="date_request",
        schema=[Field(name="some_date", dtype=UnixTimestamp)],
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
        schema=[
            Field(name="output", dtype=UnixTimestamp),
            Field(name="missing", dtype=String),
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
        [feature_view_1],
        [entity1],
        RepoConfig(
            provider="local",
            project="test",
            entity_key_serialization_version=2,
            registry="dummy_registry.pb",
        ),
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
        RepoConfig(
            provider="local",
            project="test",
            entity_key_serialization_version=2,
            registry="dummy_registry.pb",
        ),
    )
    assert len(feature_view_2.schema) == 3
    assert len(feature_view_2.features) == 1
    assert len(feature_view_2.entity_columns) == 2


def test_feature_view_inference_on_entity_value_types():
    """
    Tests that feature view inference correctly uses the entity `value_type` attribute.
    """
    entity1 = Entity(
        name="test1", join_keys=["id_join_key"], value_type=ValueType.INT64
    )
    file_source = FileSource(path="some path")
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
        [feature_view_1],
        [entity1],
        RepoConfig(
            provider="local",
            project="test",
            entity_key_serialization_version=2,
            registry="dummy_registry.pb",
        ),
    )

    # The schema must be entity and features
    assert len(feature_view_1.schema) == 2

    # Since there is already a feature specified, additional features are not inferred.
    assert len(feature_view_1.features) == 1

    # The single entity column is inferred correctly and has the expected type.
    assert len(feature_view_1.entity_columns) == 1
    assert feature_view_1.entity_columns[0].dtype == Int64


def test_conflicting_entity_value_types():
    """
    Tests that an error is thrown when the entity value types conflict.
    """
    entity1 = Entity(
        name="test1", join_keys=["id_join_key"], value_type=ValueType.INT64
    )
    file_source = FileSource(path="some path")

    with pytest.raises(ValueError):
        _ = FeatureView(
            name="test1",
            entities=[entity1],
            schema=[
                Field(name="int64_col", dtype=Int64),
                Field(
                    name="id_join_key", dtype=Float64
                ),  # Conflicts with the defined entity
            ],
            source=file_source,
        )

    # There should be no error here.
    _ = FeatureView(
        name="test1",
        entities=[entity1],
        schema=[
            Field(name="int64_col", dtype=Int64),
            Field(name="id_join_key", dtype=Int64),  # Conflicts with the defined entity
        ],
        source=file_source,
    )


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
            [feature_view_1],
            [entity1],
            RepoConfig(
                provider="local",
                project="test",
                entity_key_serialization_version=2,
                registry="dummy_registry.pb",
            ),
        )

        # Since there is already a feature specified, additional features are not inferred.
        assert len(feature_view_1.features) == 1

        # The single entity column is inferred correctly.
        assert len(feature_view_1.entity_columns) == 1

        # The schema is a property concatenating features and entity_columns
        assert len(feature_view_1.schema) == 2


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
            [feature_view_1],
            [entity1],
            RepoConfig(
                provider="local",
                project="test",
                entity_key_serialization_version=2,
                registry="dummy_registry.pb",
            ),
        )

        # The schema is a property concatenating features and entity_columns
        assert len(feature_view_1.schema) == 4

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
    """
    Tests that a feature service that references feature views without specified features will
    be updated with the correct projections after feature inference.
    """
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(name="test1", join_keys=["id_join_key"])
        feature_view_1 = FeatureView(
            name="test1",
            entities=[entity1],
            source=file_source,
        )
        feature_view_2 = FeatureView(
            name="test2",
            entities=[entity1],
            source=file_source,
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
            RepoConfig(
                provider="local",
                project="test",
                entity_key_serialization_version=2,
                registry="dummy_registry.pb",
            ),
        )
        feature_service.infer_features(
            fvs_to_update={
                feature_view_1.name: feature_view_1,
                feature_view_2.name: feature_view_2,
            }
        )

        assert len(feature_view_1.schema) == 4
        assert len(feature_view_1.features) == 3
        assert len(feature_view_2.schema) == 4
        assert len(feature_view_2.features) == 3
        assert len(feature_service.feature_view_projections[0].features) == 1
        assert len(feature_service.feature_view_projections[1].features) == 3


def test_update_feature_services_with_specified_features(simple_dataset_1):
    """
    Tests that a feature service that references feature views with specified features will
    have the correct projections both before and after feature inference.
    """
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity1 = Entity(name="test1", join_keys=["id_join_key"])
        feature_view_1 = FeatureView(
            name="test1",
            entities=[entity1],
            schema=[
                Field(name="float_col", dtype=Float32),
                Field(name="id_join_key", dtype=Int64),
            ],
            source=file_source,
        )
        feature_view_2 = FeatureView(
            name="test2",
            entities=[entity1],
            schema=[
                Field(name="int64_col", dtype=Int64),
                Field(name="id_join_key", dtype=Int64),
            ],
            source=file_source,
        )

        feature_service = FeatureService(
            name="fs_1", features=[feature_view_1[["float_col"]], feature_view_2]
        )
        assert len(feature_service.feature_view_projections) == 2
        assert len(feature_service.feature_view_projections[0].features) == 1
        assert len(feature_service.feature_view_projections[0].desired_features) == 0
        assert len(feature_service.feature_view_projections[1].features) == 1
        assert len(feature_service.feature_view_projections[1].desired_features) == 0

        update_feature_views_with_inferred_features_and_entities(
            [feature_view_1, feature_view_2],
            [entity1],
            RepoConfig(
                provider="local",
                project="test",
                entity_key_serialization_version=2,
                registry="dummy_registry.pb",
            ),
        )
        assert len(feature_view_1.features) == 1
        assert len(feature_view_2.features) == 1

        feature_service.infer_features(
            fvs_to_update={
                feature_view_1.name: feature_view_1,
                feature_view_2.name: feature_view_2,
            }
        )

        assert len(feature_service.feature_view_projections[0].features) == 1
        assert len(feature_service.feature_view_projections[1].features) == 1


# TODO(felixwang9817): Add tests that interact with field mapping.
