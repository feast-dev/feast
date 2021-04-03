import time
from datetime import datetime, timedelta

import pandas as pd
import pytest
from google.cloud import bigquery

from feast.data_source import BigQuerySource
from feast.feature import Feature
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig
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

    def test_bigquery_table_to_datastore_correctness(self):
        # create dataset
        now = datetime.utcnow()
        ts = pd.Timestamp(now).round("ms")
        data = {
            "id": [1, 2, 1, 3, 3],
            "value": [0.1, 0.2, 0.3, 4, 5],
            "ts_1": [
                ts - timedelta(seconds=4),
                ts,
                ts - timedelta(seconds=3),
                ts - timedelta(seconds=4),
                ts - timedelta(seconds=1),
            ],
            "created_ts": [ts, ts, ts, ts, ts],
        }
        df = pd.DataFrame.from_dict(data)

        # load dataset into BigQuery
        job_config = bigquery.LoadJobConfig()
        table_id = f"{self.gcp_project}.{self.bigquery_dataset}.table_correctness_{int(time.time())}"
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        # create FeatureView
        fv = FeatureView(
            name="test_bq_table_correctness",
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
            registry="./metadata.db",
            project=f"test_bq_table_correctness_{int(time.time())}",
            provider="gcp",
        )
        fs = FeatureStore(config=config)
        fs.apply([fv])

        # run materialize()
        fs.materialize(
            [fv.name], now - timedelta(seconds=5), now - timedelta(seconds=2),
        )

        # check result of materialize()
        response_dict = fs.get_online_features(
            [f"{fv.name}:value"], [{"driver_id": 1}]
        ).to_dict()
        assert abs(response_dict[f"{fv.name}:value"][0] - 0.3) < 1e-6

        # check prior value for materialize_incremental()
        response_dict = fs.get_online_features(
            [f"{fv.name}:value"], [{"driver_id": 3}]
        ).to_dict()
        assert abs(response_dict[f"{fv.name}:value"][0] - 4) < 1e-6

        # run materialize_incremental()
        fs.materialize_incremental(
            [fv.name], now - timedelta(seconds=0),
        )

        # check result of materialize_incremental()
        response_dict = fs.get_online_features(
            [f"{fv.name}:value"], [{"driver_id": 3}]
        ).to_dict()
        assert abs(response_dict[f"{fv.name}:value"][0] - 5) < 1e-6

    def test_bigquery_query_to_datastore_correctness(self):
        # create dataset
        ts = pd.Timestamp.now(tz="UTC").round("ms")
        data = {
            "id": [1, 2, 1],
            "value": [0.1, 0.2, 0.3],
            "ts_1": [ts - timedelta(minutes=2), ts, ts],
            "created_ts": [ts, ts, ts],
        }
        df = pd.DataFrame.from_dict(data)

        # load dataset into BigQuery
        job_config = bigquery.LoadJobConfig()
        table_id = f"{self.gcp_project}.{self.bigquery_dataset}.query_correctness_{int(time.time())}"
        query = f"SELECT * FROM `{table_id}`"
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        # create FeatureView
        fv = FeatureView(
            name="test_bq_query_correctness",
            entities=["driver_id"],
            features=[Feature("value", ValueType.FLOAT)],
            ttl=timedelta(minutes=5),
            input=BigQuerySource(
                event_timestamp_column="ts",
                created_timestamp_column="created_ts",
                field_mapping={"ts_1": "ts", "id": "driver_id"},
                date_partition_column="",
                query=query,
            ),
        )
        config = RepoConfig(
            registry="./metadata.db",
            project=f"test_bq_query_correctness_{int(time.time())}",
            provider="gcp",
        )
        fs = FeatureStore(config=config)
        fs.apply([fv])

        # run materialize()
        fs.materialize(
            [fv.name],
            datetime.utcnow() - timedelta(minutes=5),
            datetime.utcnow() - timedelta(minutes=0),
        )

        # check result of materialize()
        response_dict = fs.get_online_features(
            [f"{fv.name}:value"], [{"driver_id": 1}]
        ).to_dict()
        assert abs(response_dict[f"{fv.name}:value"][0] - 0.3) < 1e-6
