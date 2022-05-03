import contextlib
import datetime
import tempfile
import uuid
from pathlib import Path
from typing import Iterator, Union

import numpy as np
import pandas as pd
import pyarrow
import pyarrow as pa
import pytest
from google.api_core.exceptions import NotFound

from feast.feature_logging import (
    LOG_DATE_FIELD,
    LOG_TIMESTAMP_FIELD,
    REQUEST_ID_FIELD,
    FeatureServiceLoggingSource,
    LoggingConfig,
)
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
@pytest.mark.parametrize("pass_as_path", [True, False], ids=lambda v: str(v))
def test_feature_service_logging(environment, universal_data_sources, pass_as_path):
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

    schema = FeatureServiceLoggingSource(
        feature_service=feature_service, project=store.project
    ).get_schema(store._registry)

    num_rows = logs_df.shape[0]
    first_batch = pa.Table.from_pandas(logs_df.iloc[: num_rows // 2, :], schema=schema)
    second_batch = pa.Table.from_pandas(logs_df.iloc[num_rows // 2 :, :], schema=schema)

    with to_logs_dataset(first_batch, pass_as_path) as logs:
        store.write_logged_features(
            source=feature_service, logs=logs,
        )

    with to_logs_dataset(second_batch, pass_as_path) as logs:
        store.write_logged_features(
            source=feature_service, logs=logs,
        )
    expected_columns = list(set(logs_df.columns) - {LOG_DATE_FIELD})

    def retrieve():
        retrieval_job = store._get_provider().retrieve_feature_service_logs(
            feature_service=feature_service,
            start_date=logs_df[LOG_TIMESTAMP_FIELD].min(),
            end_date=logs_df[LOG_TIMESTAMP_FIELD].max() + datetime.timedelta(seconds=1),
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
        logs_df.sort_values(REQUEST_ID_FIELD).reset_index(drop=True),
        persisted_logs.sort_values(REQUEST_ID_FIELD).reset_index(drop=True),
        check_dtype=False,
    )


def prepare_logs(datasets: UniversalDatasets) -> pd.DataFrame:
    driver_df = datasets.driver_df
    driver_df["val_to_add"] = 50
    driver_df = driver_df.join(conv_rate_plus_100(driver_df))
    num_rows = driver_df.shape[0]

    logs_df = driver_df[["driver_id", "val_to_add"]]
    logs_df[REQUEST_ID_FIELD] = [str(uuid.uuid4()) for _ in range(num_rows)]
    logs_df[LOG_TIMESTAMP_FIELD] = pd.Series(
        np.random.randint(0, 7 * 24 * 3600, num_rows)
    ).map(lambda secs: pd.Timestamp.utcnow() - datetime.timedelta(seconds=secs))
    logs_df[LOG_DATE_FIELD] = logs_df[LOG_TIMESTAMP_FIELD].dt.date

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


@contextlib.contextmanager
def to_logs_dataset(
    table: pyarrow.Table, pass_as_path: bool
) -> Iterator[Union[pyarrow.Table, Path]]:
    if not pass_as_path:
        yield table
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        pyarrow.parquet.write_to_dataset(table, root_path=temp_dir)
        yield Path(temp_dir)
