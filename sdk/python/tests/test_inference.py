import pytest
from utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_query_arg,
    simple_bq_source_using_table_ref_arg,
)

from feast import Entity, ValueType
from feast.errors import RegistryInferenceFailure
from feast.feature_view import FeatureView
from feast.inference import (
    infer_entity_value_type_from_feature_views,
    update_data_sources_with_inferred_event_timestamp_col,
)


def test_infer_entity_value_type_from_feature_views(simple_dataset_1, simple_dataset_2):
    with prep_file_source(
        df=simple_dataset_1, event_timestamp_column="ts_1"
    ) as file_source, prep_file_source(
        df=simple_dataset_2, event_timestamp_column="ts_1"
    ) as file_source_2:

        fv1 = FeatureView(name="fv1", entities=["id"], input=file_source, ttl=None,)
        fv2 = FeatureView(name="fv2", entities=["id"], input=file_source_2, ttl=None,)

        actual_1 = infer_entity_value_type_from_feature_views(
            [Entity(name="id")], [fv1]
        )
        actual_2 = infer_entity_value_type_from_feature_views(
            [Entity(name="id")], [fv2]
        )
        assert actual_1 == [Entity(name="id", value_type=ValueType.INT64)]
        assert actual_2 == [Entity(name="id", value_type=ValueType.STRING)]

        with pytest.raises(RegistryInferenceFailure):
            # two viable data types
            infer_entity_value_type_from_feature_views([Entity(name="id")], [fv1, fv2])


@pytest.mark.integration
def test_infer_event_timestamp_column_for_data_source(simple_dataset_1):
    df_with_two_viable_timestamp_cols = simple_dataset_1.copy(deep=True)
    df_with_two_viable_timestamp_cols["ts_2"] = simple_dataset_1["ts_1"]

    with prep_file_source(df=simple_dataset_1) as file_source:
        data_sources = [
            file_source,
            simple_bq_source_using_table_ref_arg(simple_dataset_1),
            simple_bq_source_using_query_arg(simple_dataset_1),
        ]
        update_data_sources_with_inferred_event_timestamp_col(data_sources)
        actual_event_timestamp_cols = [
            source.event_timestamp_column for source in data_sources
        ]

        assert actual_event_timestamp_cols == ["ts_1", "ts_1", "ts_1"]

    with prep_file_source(df=df_with_two_viable_timestamp_cols) as file_source:
        with pytest.raises(RegistryInferenceFailure):
            # two viable event_timestamp_columns
            update_data_sources_with_inferred_event_timestamp_col([file_source])
