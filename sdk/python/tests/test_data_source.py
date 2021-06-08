import pytest
from google.cloud import bigquery
from utils.data_source_utils import simple_bq_source_using_table_ref_arg


@pytest.mark.integration
def test_existent_bq_source(simple_dataset_1):
    existent_bq = simple_bq_source_using_table_ref_arg(simple_dataset_1)


@pytest.mark.integration
def test_nonexistent_bq_source():
    client = bigquery.Client()
    table_ref = "project.dataset.nonexistent_table"

    with pytest.raises(NameError):
        nonexistent_bq = BigQuerySource(table_ref=table_ref, event_timestamp_column="")
