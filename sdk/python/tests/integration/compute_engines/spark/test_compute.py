from datetime import datetime, timedelta
from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

from feast.infra.compute_engines.base import HistoricalRetrievalTask
from feast.infra.compute_engines.spark.compute import SparkComputeEngine
from feast.infra.compute_engines.spark.job import SparkDAGRetrievalJob
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
)
from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from tests.example_repos.example_feature_repo_with_bfvs_compute import (
    global_stats_feature_view,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)


@pytest.mark.integration
def test_spark_compute_engine_get_historical_features():
    now = datetime.utcnow()

    spark_config = IntegrationTestRepoConfig(
        provider="local",
        online_store_creator=RedisOnlineStoreCreator,
        offline_store_creator=SparkDataSourceCreator,
        batch_engine={"type": "spark.engine", "partitions": 10},
    )
    spark_environment = construct_test_environment(
        spark_config, None, entity_key_serialization_version=2
    )

    spark_environment.setup()

    # 👷 Prepare test parquet feature file
    df = pd.DataFrame(
        [
            {
                "driver_id": 1001,
                "event_timestamp": now - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.8,
                "acc_rate": 0.95,
                "avg_daily_trips": 15,
            },
            {
                "driver_id": 1001,
                "event_timestamp": now - timedelta(days=2),
                "created": now - timedelta(hours=3),
                "conv_rate": 0.75,
                "acc_rate": 0.9,
                "avg_daily_trips": 14,
            },
            {
                "driver_id": 1002,
                "event_timestamp": now - timedelta(days=1),
                "created": now - timedelta(hours=2),
                "conv_rate": 0.7,
                "acc_rate": 0.88,
                "avg_daily_trips": 12,
            },
        ]
    )

    ds = spark_environment.data_source_creator.create_data_source(
        df,
        spark_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )
    global_stats_feature_view.source = ds

    # 📥 Entity DataFrame to join with
    entity_df = pd.DataFrame(
        [
            {"driver_id": 1001, "event_timestamp": now},
            {"driver_id": 1002, "event_timestamp": now},
        ]
    )

    # 🛠 Build retrieval task
    task = HistoricalRetrievalTask(
        entity_df=entity_df,
        feature_view=global_stats_feature_view,
        full_feature_name=False,
        registry=MagicMock(),
        config=spark_environment.config,
        start_time=now - timedelta(days=1),
        end_time=now,
    )

    # 🧪 Run SparkComputeEngine
    engine = SparkComputeEngine(
        repo_config=task.config,
        offline_store=SparkOfflineStore(),
        online_store=MagicMock(),
        registry=MagicMock(),
    )

    spark_dag_retrieval_job = engine.get_historical_features(task)
    spark_df = cast(SparkDAGRetrievalJob, spark_dag_retrieval_job).to_spark_df()
    df_out = spark_df.to_pandas().sort_values("driver_id").reset_index(drop=True)

    # ✅ Assert output
    assert list(df_out.driver_id) == [1001, 1002]
    assert abs(df_out.loc[0]["conv_rate"] - 0.8) < 1e-6
    assert abs(df_out.loc[1]["conv_rate"] - 0.7) < 1e-6


if __name__ == "__main__":
    test_spark_compute_engine_get_historical_features()
