import pytest
from utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_query_arg,
    simple_bq_source_using_table_ref_arg,
)

from feast import Entity, ValueType
from feast.feature_view import FeatureView
from feast.inference import infer_entity_value_type_from_feature_views


@pytest.mark.integration
def test_data_source_ts_col_inference_success(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1) as file_source:
        actual_file_source = file_source.event_timestamp_column
        actual_bq_1 = simple_bq_source_using_table_ref_arg(
            simple_dataset_1
        ).event_timestamp_column
        actual_bq_2 = simple_bq_source_using_query_arg(
            simple_dataset_1
        ).event_timestamp_column
        expected = "ts_1"

        assert expected == actual_file_source == actual_bq_1 == actual_bq_2


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

        with pytest.raises(ValueError):
            # two viable data types
            infer_entity_value_type_from_feature_views([Entity(name="id")], [fv1, fv2])
