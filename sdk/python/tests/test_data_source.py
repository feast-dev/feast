import pytest
from utils.data_source_utils import simple_bq_source_using_table_ref_arg

from feast.data_source import BigQuerySource


@pytest.mark.integration
def test_existent_bq_source(simple_dataset_1):
    simple_bq_source_using_table_ref_arg(simple_dataset_1)


@pytest.mark.integration
def test_nonexistent_bq_source():
    with pytest.raises(NameError):
        BigQuerySource(
            table_ref="project.dataset.nonexistent_table", event_timestamp_column=""
        )
