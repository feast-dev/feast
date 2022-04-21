import datetime
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound

from feast.feature_logging import FeatureServiceLoggingSource, LoggingConfig
from feast.feature_service import FeatureService
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.repo_configuration import (
    UniversalDatasets,
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import conv_rate_plus_100


@pytest.mark.integration
@pytest.mark.universal
def test_feature_service_logging(environment, universal_data_sources):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources

    feature_views = construct_universal_feature_views(data_sources)
    store.apply([driver(), *feature_views.values()])

    logs_df = prepare_logs(datasets)

    feature_service = FeatureService(
        name="test_service",
        features=[
            feature_views.driver[["conv_rate", "avg_daily_trips"]],
            feature_views.driver_odfv[
                ["conv_rate_plus_val_to_add", "conv_rate_plus_100_rounded"]
            ],
        ],
        logging_config=LoggingConfig(
            destination=environment.data_source_creator.create_logged_features_destination()
        ),
    )

    num_rows = logs_df.shape[0]
    first_batch = logs_df.iloc[: num_rows // 2, :]
    second_batch = logs_df.iloc[num_rows // 2 :, :]

    schema = FeatureServiceLoggingSource(
        feature_service=feature_service, project=store.project
    ).get_schema(store._registry)

    store.write_logged_features(
        source=feature_service, logs=pa.Table.from_pandas(first_batch, schema=schema),
    )

    store.write_logged_features(
        source=feature_service, logs=pa.Table.from_pandas(second_batch, schema=schema),
    )
    expected_columns = list(set(logs_df.columns) - {"log_date"})

    def retrieve():
        retrieval_job = store._get_provider().retrieve_feature_service_logs(
            feature_service=feature_service,
            from_=logs_df["log_timestamp"].min(),
            to=logs_df["log_timestamp"].max() + datetime.timedelta(seconds=1),
            config=store.config,
            registry=store._registry,
        )
        try:
            df = retrieval_job.to_df()
        except NotFound:
            # Table was not created yet
            return None, False

        return df, df.shape[0] == logs_df.shape[0]

    persisted_logs = wait_retry_backoff(
        retrieve, timeout_secs=60, timeout_msg="Logs retrieval failed"
    )

    persisted_logs = persisted_logs[expected_columns]
    logs_df = logs_df[expected_columns]
    pd.testing.assert_frame_equal(
        logs_df.sort_values("request_id").reset_index(drop=True),
        persisted_logs.sort_values("request_id").reset_index(drop=True),
        check_dtype=False,
    )


def prepare_logs(datasets: UniversalDatasets) -> pd.DataFrame:
    driver_df = datasets.driver_df
    driver_df["val_to_add"] = 50
    driver_df = driver_df.join(conv_rate_plus_100(driver_df))
    num_rows = driver_df.shape[0]

    logs_df = driver_df[["driver_id", "val_to_add"]]
    logs_df["request_id"] = [str(uuid.uuid4()) for _ in range(num_rows)]
    logs_df["log_timestamp"] = pd.Series(
        np.random.randint(0, 7 * 24 * 3600, num_rows)
    ).map(lambda secs: pd.Timestamp.utcnow() - datetime.timedelta(seconds=secs))
    logs_df["log_date"] = logs_df["log_timestamp"].dt.date

    for view, features in (
        ("driver_stats", ("conv_rate", "avg_daily_trips")),
        (
            "conv_rate_plus_100",
            ("conv_rate_plus_val_to_add", "conv_rate_plus_100_rounded"),
        ),
    ):
        for feature in features:
            logs_df[f"{view}__{feature}"] = driver_df[feature]
            logs_df[f"{view}__{feature}__timestamp"] = driver_df["event_timestamp"]
            logs_df[f"{view}__{feature}__status"] = FieldStatus.PRESENT

    return logs_df
