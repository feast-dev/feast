import pytest
from utils.data_source_utils import (
    prep_file_source,
    simple_bq_source_using_table_ref_arg,
)

from feast.data_source import BigQuerySource
from feast.errors import BigQuerySourceNotFoundException


def test_existent_file_source(simple_dataset_1):
    prep_file_source(df=simple_dataset_1)


@pytest.mark.integration
def test_existent_bq_source(simple_dataset_1):
    simple_bq_source_using_table_ref_arg(simple_dataset_1)


@pytest.mark.integration
def test_nonexistent_bq_source():
    with pytest.raises(BigQuerySourceNotFoundException):
        BigQuerySource(
            table_ref="project.dataset.nonexistent_table", event_timestamp_column=""
        )
