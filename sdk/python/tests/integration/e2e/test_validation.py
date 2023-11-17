import datetime
import shutil

import pandas as pd
import pyarrow as pa
import pytest
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

from feast import FeatureService
from feast.dqm.errors import ValidationFailed
from feast.dqm.profilers.ge_profiler import ge_profiler
from feast.feature_logging import (
    LOG_TIMESTAMP_FIELD,
    FeatureServiceLoggingSource,
    LoggingConfig,
)
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus
from feast.utils import make_tzaware
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.test_log_creator import prepare_logs

_features = [
    "customer_profile:current_balance",
    "customer_profile:avg_passenger_count",
    "customer_profile:lifetime_trip_count",
    "order:order_is_success",
    "global_stats:num_rides",
    "global_stats:avg_ride_length",
]


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_historical_retrieval_with_validation(environment, universal_data_sources):
    store = environment.feature_store
    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    store.apply([driver(), customer(), location(), *feature_views.values()])

    # Create two identical retrieval jobs
    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )
    reference_job = store.get_historical_features(
        entity_df=entity_df,
        features=_features,
    )
    job = store.get_historical_features(
        entity_df=entity_df,
        features=_features,
    )

    # Save dataset using reference job and retrieve it
    store.create_saved_dataset(
        from_=reference_job,
        name="my_training_dataset",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
        allow_overwrite=True,
    )
    saved_dataset = store.get_saved_dataset("my_training_dataset")

    # If validation pass there will be no exceptions on this point
    reference = saved_dataset.as_reference(name="ref", profiler=configurable_profiler)
    job.to_df(validation_reference=reference)


@pytest.mark.integration
def test_historical_retrieval_fails_on_validation(environment, universal_data_sources):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    reference_job = store.get_historical_features(
        entity_df=entity_df,
        features=_features,
    )

    store.create_saved_dataset(
        from_=reference_job,
        name="my_other_dataset",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
        allow_overwrite=True,
    )

    job = store.get_historical_features(
        entity_df=entity_df,
        features=_features,
    )

    ds = store.get_saved_dataset("my_other_dataset")
    profiler_expectation_suite = ds.get_profile(
        profiler=profiler_with_unrealistic_expectations
    )

    assert len(profiler_expectation_suite.expectation_suite["expectations"]) == 3

    with pytest.raises(ValidationFailed) as exc_info:
        job.to_df(
            validation_reference=store.get_saved_dataset(
                "my_other_dataset"
            ).as_reference(name="ref", profiler=profiler_with_unrealistic_expectations)
        )

    failed_expectations = exc_info.value.report.errors
    assert len(failed_expectations) == 2

    assert failed_expectations[0].check_name == "expect_column_max_to_be_between"
    assert failed_expectations[0].column_name == "current_balance"

    assert failed_expectations[1].check_name == "expect_column_values_to_be_in_set"
    assert failed_expectations[1].column_name == "avg_passenger_count"


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_logged_features_validation(environment, universal_data_sources):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    feature_service = FeatureService(
        name="test_service",
        features=[
            feature_views.customer[
                ["current_balance", "avg_passenger_count", "lifetime_trip_count"]
            ],
            feature_views.order[["order_is_success"]],
            feature_views.global_fv[["num_rides", "avg_ride_length"]],
        ],
        logging_config=LoggingConfig(
            destination=environment.data_source_creator.create_logged_features_destination()
        ),
    )

    store.apply(
        [driver(), customer(), location(), feature_service, *feature_views.values()]
    )

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id"]
    )

    # add some non-existing entities to check NotFound feature handling
    for i in range(5):
        entity_df = pd.concat(
            [
                entity_df,
                pd.DataFrame.from_records(
                    [
                        {
                            "customer_id": 2000 + i,
                            "driver_id": 6000 + i,
                            "event_timestamp": datetime.datetime.now(),
                        }
                    ]
                ),
            ]
        )

    store_fs = store.get_feature_service(feature_service.name)
    reference_dataset = store.create_saved_dataset(
        from_=store.get_historical_features(
            entity_df=entity_df, features=store_fs, full_feature_names=True
        ),
        name="reference_for_validating_logged_features",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
        allow_overwrite=True,
    )

    log_source_df = store.get_historical_features(
        entity_df=entity_df, features=store_fs, full_feature_names=False
    ).to_df()
    logs_df = prepare_logs(log_source_df, feature_service, store)

    schema = FeatureServiceLoggingSource(
        feature_service=feature_service, project=store.project
    ).get_schema(store._registry)
    store.write_logged_features(
        pa.Table.from_pandas(logs_df, schema=schema), source=feature_service
    )

    def validate():
        """
        Return Tuple[succeed, completed]
        Succeed will be True if no ValidateFailed exception was raised
        """
        try:
            store.validate_logged_features(
                feature_service,
                start=logs_df[LOG_TIMESTAMP_FIELD].min(),
                end=logs_df[LOG_TIMESTAMP_FIELD].max() + datetime.timedelta(seconds=1),
                reference=reference_dataset.as_reference(
                    name="ref", profiler=profiler_with_feature_metadata
                ),
            )
        except ValidationFailed:
            return False, True
        except Exception:
            # log table is still being created
            return False, False

        return True, True

    success = wait_retry_backoff(validate, timeout_secs=30)
    assert success, "Validation failed (unexpectedly)"


@pytest.mark.integration
def test_e2e_validation_via_cli(environment, universal_data_sources):
    runner = CliRunner()
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    feature_service = FeatureService(
        name="test_service",
        features=[
            feature_views.customer[
                ["current_balance", "avg_passenger_count", "lifetime_trip_count"]
            ],
        ],
        logging_config=LoggingConfig(
            destination=environment.data_source_creator.create_logged_features_destination()
        ),
    )
    store.apply([customer(), feature_service, feature_views.customer])

    entity_df = datasets.entity_df.drop(
        columns=["order_id", "origin_id", "destination_id", "driver_id"]
    )
    retrieval_job = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service(feature_service.name),
        full_feature_names=True,
    )
    logs_df = prepare_logs(retrieval_job.to_df(), feature_service, store)
    saved_dataset = store.create_saved_dataset(
        from_=retrieval_job,
        name="reference_for_validating_logged_features",
        storage=environment.data_source_creator.create_saved_dataset_destination(),
        allow_overwrite=True,
    )
    reference = saved_dataset.as_reference(
        name="test_reference", profiler=configurable_profiler
    )

    schema = FeatureServiceLoggingSource(
        feature_service=feature_service, project=store.project
    ).get_schema(store._registry)
    store.write_logged_features(
        pa.Table.from_pandas(logs_df, schema=schema), source=feature_service
    )

    with runner.local_repo(example_repo_py="", offline_store="file") as local_repo:
        local_repo.apply(
            [customer(), feature_views.customer, feature_service, reference]
        )
        local_repo._registry.apply_saved_dataset(saved_dataset, local_repo.project)
        validate_args = [
            "validate",
            "--feature-service",
            feature_service.name,
            "--reference",
            reference.name,
            (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),
            datetime.datetime.now().isoformat(),
        ]
        p = runner.run(validate_args, cwd=local_repo.repo_path)

        assert p.returncode == 0, p.stderr.decode()
        assert "Validation successful" in p.stdout.decode(), p.stderr.decode()

        # make sure second validation will use cached profile
        shutil.rmtree(saved_dataset.storage.file_options.uri)

        # Add some invalid data that would lead to failed validation
        invalid_data = pd.DataFrame(
            data={
                "customer_id": [0],
                "current_balance": [0],
                "avg_passenger_count": [0],
                "lifetime_trip_count": [0],
                "event_timestamp": [
                    make_tzaware(datetime.datetime.utcnow())
                    - datetime.timedelta(hours=1)
                ],
            }
        )
        invalid_logs = prepare_logs(invalid_data, feature_service, store)
        store.write_logged_features(
            pa.Table.from_pandas(invalid_logs, schema=schema), source=feature_service
        )

        p = runner.run(validate_args, cwd=local_repo.repo_path)
        assert p.returncode == 1, p.stdout.decode()
        assert "Validation failed" in p.stdout.decode(), p.stderr.decode()


# Great expectations profilers created for testing


@ge_profiler
def configurable_profiler(dataset: PandasDataset) -> ExpectationSuite:
    from great_expectations.profile.user_configurable_profiler import (
        UserConfigurableProfiler,
    )

    return UserConfigurableProfiler(
        profile_dataset=dataset,
        ignored_columns=["event_timestamp"],
        excluded_expectations=[
            "expect_table_columns_to_match_ordered_list",
            "expect_table_row_count_to_be_between",
        ],
        value_set_threshold="few",
    ).build_suite()


@ge_profiler(with_feature_metadata=True)
def profiler_with_feature_metadata(dataset: PandasDataset) -> ExpectationSuite:
    from great_expectations.profile.user_configurable_profiler import (
        UserConfigurableProfiler,
    )

    # always present
    dataset.expect_column_values_to_be_in_set(
        "global_stats__avg_ride_length__status", {FieldStatus.PRESENT}
    )

    # present at least in 70% of rows
    dataset.expect_column_values_to_be_in_set(
        "customer_profile__current_balance__status", {FieldStatus.PRESENT}, mostly=0.7
    )

    return UserConfigurableProfiler(
        profile_dataset=dataset,
        ignored_columns=["event_timestamp"]
        + [
            c
            for c in dataset.columns
            if c.endswith("__timestamp") or c.endswith("__status")
        ],
        excluded_expectations=[
            "expect_table_columns_to_match_ordered_list",
            "expect_table_row_count_to_be_between",
        ],
        value_set_threshold="few",
    ).build_suite()


@ge_profiler
def profiler_with_unrealistic_expectations(dataset: PandasDataset) -> ExpectationSuite:
    # note: there are 4 expectations here and only 3 are returned from the profiler
    # need to create dataframe with corrupted data first
    df = pd.DataFrame()
    df["current_balance"] = [-100]
    df["avg_passenger_count"] = [0]

    other_ds = PandasDataset(df)
    other_ds.expect_column_max_to_be_between("current_balance", -1000, -100)
    other_ds.expect_column_values_to_be_in_set("avg_passenger_count", value_set={0})

    # this should pass
    other_ds.expect_column_min_to_be_between("avg_passenger_count", 0, 1000)
    # this should fail
    other_ds.expect_column_to_exist("missing random column")

    return other_ds.get_expectation_suite()
