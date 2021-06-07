import pytest
from utils.data_source_utils import (
    nonexistent_bq_source,
    simple_bq_source_using_table_ref_arg,
)

from feast import Entity, ValueType
from feast.feature_view import FeatureView
from feast.inference import infer_entity_value_type_from_feature_views


@pytest.mark.integration
def test_existent_bq_source(simple_dataset_1):
    existent_bq = simple_bq_source_using_table_ref_arg(simple_dataset_1)


@pytest.mark.integration
def test_nonexistent_bq_source():
    with pytest.raises(NameError):
        nonexistent_bq = nonexistent_bq_source()
