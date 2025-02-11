import datetime

import pandas as pd
import pyarrow as pa
import pytest

from feast.feature_logging import (
    LOG_DATE_FIELD,
    LOG_TIMESTAMP_FIELD,
    REQUEST_ID_FIELD,
    FeatureServiceLoggingSource,
    LoggingConfig,
)
from feast.feature_service import FeatureService
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)
from tests.integration.feature_repos.universal.feature_views import conv_rate_plus_100
from tests.utils.test_log_creator import prepare_logs, to_logs_dataset


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("pass_as_path", [True, False], ids=lambda v: str(v))
def test_feature_service_logging(environment, universal_data_sources, pass_as_path):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources

    feature_views = construct_universal_feature_views(data_sources)
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

    store.apply(
        [customer(), driver(), location(), *feature_views.values()], feature_service
    )

    # Added to handle the case that the offline store is remote
    store.registry.apply_feature_service(feature_service, store.config.project)
    store.registry.apply_data_source(
        feature_service.logging_config.destination.to_data_source(),
        store.config.project,
    )

    driver_df = datasets.driver_df
    driver_df["val_to_add"] = 50
    driver_df = driver_df.join(conv_rate_plus_100(driver_df))

    logs_df = prepare_logs(driver_df, feature_service, store)

    schema = FeatureServiceLoggingSource(
        feature_service=feature_service, project=store.project
    ).get_schema(store._registry)

    num_rows = logs_df.shape[0]
    first_batch = pa.Table.from_pandas(logs_df.iloc[: num_rows // 2, :], schema=schema)
    second_batch = pa.Table.from_pandas(logs_df.iloc[num_rows // 2 :, :], schema=schema)

    with to_logs_dataset(first_batch, pass_as_path) as logs:
        store.write_logged_features(
            source=feature_service,
            logs=logs,
        )

    with to_logs_dataset(second_batch, pass_as_path) as logs:
        store.write_logged_features(
            source=feature_service,
            logs=logs,
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
        except Exception:
            # Table was not created yet
            return None, False

        return df, df.shape[0] == logs_df.shape[0]

    persisted_logs = wait_retry_backoff(
        retrieve, timeout_secs=60, timeout_msg="Logs retrieval failed"
    )

    persisted_logs = persisted_logs[expected_columns]

    logs_df = logs_df[expected_columns]

    # Convert timezone-aware datetime values to naive datetime values
    logs_df[LOG_TIMESTAMP_FIELD] = logs_df[LOG_TIMESTAMP_FIELD].dt.tz_localize(None)
    persisted_logs[LOG_TIMESTAMP_FIELD] = persisted_logs[
        LOG_TIMESTAMP_FIELD
    ].dt.tz_localize(None)

    pd.testing.assert_frame_equal(
        logs_df.sort_values(REQUEST_ID_FIELD).reset_index(drop=True),
        persisted_logs.sort_values(REQUEST_ID_FIELD).reset_index(drop=True),
        check_dtype=False,
    )
