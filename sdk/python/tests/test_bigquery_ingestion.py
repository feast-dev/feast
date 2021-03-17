import time
from datetime import datetime, timedelta

import pandas as pd
import pytest
from google.cloud import bigquery

from feast.data_source import BigQuerySource
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import LocalOnlineStoreConfig, OnlineStoreConfig, RepoConfig
from feast.value_type import ValueType


@pytest.mark.integration
def test_bigquery_ingestion():
    # create dataset
    ts = pd.Timestamp.now(tz="UTC").round("ms")
    data = {
        "id": [1, 2, 1],
        "value": [0.1, 0.2, 0.3],
        "ts": [ts - timedelta(minutes=2), ts, ts],
        "created_ts": [ts, ts, ts],
    }
    df = pd.DataFrame.from_dict(data)

    # load dataset into BigQuery
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    gcp_project = client.project
    bigquery_dataset = "test_ingestion"
    dataset = bigquery.Dataset(f"{gcp_project}.{bigquery_dataset}")
    client.create_dataset(dataset, exists_ok=True)
    table_id = f"{gcp_project}.{bigquery_dataset}.table_{int(time.time())}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    # create FeatureView
    fv = FeatureView(
        "test_fv",
        ["id_mapped"],
        [Feature("value", ValueType.FLOAT)],
        timedelta(minutes=5),
        BigQuerySource(
            "timestamp_mapped",
            table_id,
            "created_ts",
            {"ts": "timestamp_mapped", "id": "id_mapped"},
            "",
        ),
    )
    config = RepoConfig(
        metadata_store="./metadata.db",
        project="default",
        provider="gcp",
        online_store=OnlineStoreConfig(local=LocalOnlineStoreConfig("online_store.db")),
    )
    fs = FeatureStore(config=config)
    fs.apply([fv])

    # run materialize()
    fs.materialize(
        ["test_fv"],
        datetime.utcnow() - timedelta(minutes=5),
        datetime.utcnow() - timedelta(minutes=0),
    )

    # check result of materialize()
    entity_key = EntityKeyProto(
        entity_names=["id_mapped"], entity_values=[ValueProto(int32_val=1)]
    )
    _, val = fs._get_provider().online_read("default", fv, entity_key)
    assert abs(val["value"].double_val - 0.3) < 1e-6
