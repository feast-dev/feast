import pandas as pd
import pytest

from feast import Entity, Feature, RepoConfig, ValueType
from feast.data_source import RequestDataSource
from feast.errors import RegistryInferenceFailure, SpecifiedFeaturesNotPresentError
from feast.feature_view import FeatureView
from feast.inference import (
    update_data_sources_with_inferred_event_timestamp_col,
    update_entities_with_inferred_types_from_feature_views,
)
from feast.on_demand_feature_view import on_demand_feature_view
from tests.utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_query_arg,
    simple_bq_source_using_table_ref_arg,
)


def test_update_entities_with_inferred_types_from_feature_views(
    simple_dataset_1, simple_dataset_2
):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source, prep_file_source(
        df=simple_dataset_2, event_timestamp_column="ts_1"
    ) as file_source_2:

        fv1 = FeatureView(
            name="fv1", entities=["id"], batch_source=file_source, ttl=None,
        )
        fv2 = FeatureView(
            name="fv2", entities=["id"], batch_source=file_source_2, ttl=None,
        )

        actual_1 = Entity(name="id", join_key="id_join_key")
        actual_2 = Entity(name="id", join_key="id_join_key")

        update_entities_with_inferred_types_from_feature_views(
            [actual_1], [fv1], RepoConfig(provider="local", project="test")
        )
        update_entities_with_inferred_types_from_feature_views(
            [actual_2], [fv2], RepoConfig(provider="local", project="test")
        )
        assert actual_1 == Entity(
            name="id", join_key="id_join_key", value_type=ValueType.INT64
        )
        assert actual_2 == Entity(
            name="id", join_key="id_join_key", value_type=ValueType.STRING
        )

        with pytest.raises(RegistryInferenceFailure):
            # two viable data types
            update_entities_with_inferred_types_from_feature_views(
                [Entity(name="id", join_key="id_join_key")],
                [fv1, fv2],
                RepoConfig(provider="local", project="test"),
            )


@pytest.mark.integration
def test_update_data_sources_with_inferred_event_timestamp_col(simple_dataset_1):
    df_with_two_viable_timestamp_cols = simple_dataset_1.copy(deep=True)
    df_with_two_viable_timestamp_cols["ts_2"] = simple_dataset_1["ts_1"]

    with prep_file_source(df=simple_dataset_1) as file_source:
        data_sources = [
            file_source,
            simple_bq_source_using_table_ref_arg(simple_dataset_1),
            simple_bq_source_using_query_arg(simple_dataset_1),
        ]
        update_data_sources_with_inferred_event_timestamp_col(
            data_sources, RepoConfig(provider="local", project="test")
        )
        actual_event_timestamp_cols = [
            source.event_timestamp_column for source in data_sources
        ]

        assert actual_event_timestamp_cols == ["ts_1", "ts_1", "ts_1"]

    with prep_file_source(df=df_with_two_viable_timestamp_cols) as file_source:
        with pytest.raises(RegistryInferenceFailure):
            # two viable event_timestamp_columns
            update_data_sources_with_inferred_event_timestamp_col(
                [file_source], RepoConfig(provider="local", project="test")
            )


def test_on_demand_features_type_inference():
    # Create Feature Views
    date_request = RequestDataSource(
        name="date_request", schema={"some_date": ValueType.UNIX_TIMESTAMP}
    )

    @on_demand_feature_view(
        inputs={"date_request": date_request},
        features=[
            Feature("output", ValueType.UNIX_TIMESTAMP),
            Feature("string_output", ValueType.STRING),
        ],
    )
    def test_view(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        data["string_output"] = features_df["some_date"].astype(pd.StringDtype())
        return data

    test_view.infer_features()

    @on_demand_feature_view(
        inputs={"date_request": date_request},
        features=[
            Feature("output", ValueType.UNIX_TIMESTAMP),
            Feature("object_output", ValueType.STRING),
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
        inputs={"date_request": date_request},
        features=[
            Feature("output", ValueType.UNIX_TIMESTAMP),
            Feature("missing", ValueType.STRING),
        ],
    )
    def test_view_with_missing_feature(features_df: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame()
        data["output"] = features_df["some_date"]
        return data

    with pytest.raises(SpecifiedFeaturesNotPresentError):
        test_view_with_missing_feature.infer_features()
