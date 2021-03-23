import random
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
class TestMaterializeFromBigQueryToDatastore:
    def setup_method(self):
        self.client = bigquery.Client()
        self.gcp_project = self.client.project
        self.bigquery_dataset = "test_ingestion"
        dataset = bigquery.Dataset(f"{self.gcp_project}.{self.bigquery_dataset}")
        self.client.create_dataset(dataset, exists_ok=True)
        dataset.default_table_expiration_ms = (
            1000 * 60 * 60 * 24 * 14
        )  # 2 weeks in milliseconds
        self.client.update_dataset(dataset, ["default_table_expiration_ms"])

    def test_bigquery_ingestion_correctness(self):
        # create dataset
        ts = pd.Timestamp.now(tz="UTC").round("ms")
        checked_value = (
            random.random()
        )  # random value so test doesn't still work if no values written to online store
        data = {
            "id": [1, 2, 1],
            "value": [0.1, 0.2, checked_value],
            "ts_1": [ts - timedelta(minutes=2), ts, ts],
            "created_ts": [ts, ts, ts],
        }
        df = pd.DataFrame.from_dict(data)

        # load dataset into BigQuery
        job_config = bigquery.LoadJobConfig()
        table_id = (
            f"{self.gcp_project}.{self.bigquery_dataset}.correctness_{int(time.time())}"
        )
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        # create FeatureView
        fv = FeatureView(
            name="test_bq_correctness",
            entities=["driver_id"],
            features=[Feature("value", ValueType.FLOAT)],
            ttl=timedelta(minutes=5),
            input=BigQuerySource(
                event_timestamp_column="ts",
                table_ref=table_id,
                created_timestamp_column="created_ts",
                field_mapping={"ts_1": "ts", "id": "driver_id"},
                date_partition_column="",
            ),
        )
        config = RepoConfig(
            metadata_store="./metadata.db",
            project="default",
            provider="gcp",
            online_store=OnlineStoreConfig(
                local=LocalOnlineStoreConfig("online_store.db")
            ),
        )
        fs = FeatureStore(config=config)
        fs.apply([fv])

        # run materialize()
        fs.materialize(
            ["test_bq_correctness"],
            datetime.utcnow() - timedelta(minutes=5),
            datetime.utcnow() - timedelta(minutes=0),
        )

        # check result of materialize()
        entity_key = EntityKeyProto(
            entity_names=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )
        read_rows = fs._get_provider().online_read("default", fv, [entity_key])
        assert len(read_rows) == 1
        _, val = read_rows[0]
        assert abs(val["value"].double_val - checked_value) < 1e-6
